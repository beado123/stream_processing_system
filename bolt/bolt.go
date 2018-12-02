package bolt

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"io/ioutil"
	"os"
	"encoding/json"
	"sync"
	//"io"
	"time"
	"sort"
)

type Bolt struct {
	VmId string
	VmIpAddress string
	Ln net.Listener
	PortTCP string
	Ser *net.UDPConn
	PortUDP string
	Children []string	
	IsActive bool
	Type string
	App string
	WordCountMap map[string]int
	MyMutex *sync.Mutex
	ConnToChildren map[string]net.Conn
	NumOfFather int
	FilterRedditMap map[string]int
	NasaLogMap map[string]map[string]int
}

var wg sync.WaitGroup

func NewBolt(t string, app string, children []string, father int) (b *Bolt) {
	if t == "boltc" {
		wg.Add(1)
	} else if t == "boltl" {
		wg.Add(father + 1)
	}
	
	ip_address := getIPAddrAndLogfile()
	vm_id := ip_address[15:17]
	l, err := net.Listen("tcp", ip_address + ":5555")
	if err != nil {
		fmt.Println(err)
                return
	}
	addr := net.UDPAddr{
        	Port: 4444,
        	IP: net.ParseIP(ip_address),
    	}
	ser, err := net.ListenUDP("udp", &addr)
	if err != nil {
        	fmt.Println("Failed to set up listener! Error: ", err)
        	return
    	}
	mutex := &sync.Mutex{}
	b = &Bolt {
		VmId: vm_id,
		VmIpAddress: ip_address,
		Ln: l,
		PortTCP: "5555",
		Ser: ser,	
		PortUDP: "4444",
		Children: children,
		IsActive: true,
		Type: t,		
		App: app,
		WordCountMap: make(map[string]int),
		MyMutex: mutex,
		ConnToChildren: make(map[string]net.Conn),
		NumOfFather: father,
		FilterRedditMap: make(map[string]int),
		NasaLogMap: make(map[string]map[string]int),
	}
	return
}

func (self *Bolt) BoltListen() {
	go self.BoltListenForDOWN()
	defer self.Ln.Close()
	if self.Type == "boltl" && self.App == "wordcount" {
		go self.WordCountBoltlTimeToExitCheck()
	} else if self.Type == "boltl" && self.App == "reddit" {
                go self.FilterRedditBoltlTimeToExitCheck()
        } else if self.Type == "boltl" && self.App == "nasalog" {
		go self.NasaLogBoltlTimeToExitCheck()
	}

	count := self.NumOfFather
	for true {
		if count == 0 {
			break
		}
		conn, err := self.Ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		if self.Type == "boltc" && self.App == "wordcount" {
                	go self.HandleWordCountBoltc(conn)
                } else if self.Type == "boltl" && self.App == "wordcount" {
                	go self.HandleWordCountBoltl(conn)
                } else if self.Type == "boltc" && self.App == "reddit" {
                	go self.HandleFilterRedditBoltc(conn)
                } else if self.Type == "boltl" && self.App == "reddit" {
                	go self.HandleFilterRedditBoltl(conn)
                }
		count -= 1
	}
	wg.Wait()
	fmt.Println("bolt listen shut down")
}

func (self *Bolt) BoltListenForDOWN() {
	defer self.Ser.Close();
	buf := make([]byte, 1024)
	
	for true {
		reqLen, _, err := self.Ser.ReadFromUDP(buf)
		if err != nil {
			fmt.Println(err)
			return
		}
		reqArr := strings.Split(string(buf[:reqLen]), " ")
		msg := reqArr[0]
		if(msg == "DOWN") {
			fmt.Println("Receive DOWN, need to shut down!")
			if self.Type == "boltc" {
				wg.Done()
			} else if self.Type == "boltl" {
				count := self.NumOfFather + 1
				for true {
					if count == 0 {
						break
					}
					fmt.Println("hello")
					wg.Done()
					count -= 1
				}
			}
			break
		}
	}
}

//This function fill string into specific length by :
func fillString(retunString string, toLength int) string {
	for {
		lengtString := len(retunString)
		if lengtString < toLength {
			retunString = retunString + ":"
			continue
		}
		break
	}
	return retunString
}

func getIPAddrAndLogfile() string{
	data, err := ioutil.ReadFile("ip_address")
	if err != nil {
		panic(err)
	}

	ip := string(data[:len(data)])
	
	//remove \n from end of line
	if strings.HasSuffix(ip, "\n") {
		ip = ip[:(len(ip) - 1)]
	}
	//fmt.Println("ip address of current VM:\n", ip)
	return ip
}

///////////////////////////////////////////////////word_count//////////////////////////////////////////
func (self *Bolt) HandleWordCountBoltc(conn net.Conn) {
	defer conn.Close()
	//set up connection to children
	for _, child := range self.Children {
		connToChild, err := net.Dial("tcp", "fa18-cs425-g69-" + child + ".cs.illinois.edu:" + self.PortTCP)
                if err != nil {
                        fmt.Println(err)
                        return
        	}
		self.ConnToChildren[child] = connToChild
	}

	for true {
		bufferSize := make([]byte, 32)
		_, err := conn.Read(bufferSize)
                if err != nil {
			wg.Done()
                        fmt.Println(err)
                        return
                }
		tupleSize := strings.Trim(string(bufferSize), ":")
		//fmt.Println(tupleSize)
		if tupleSize == "END" {
			for _, curr := range self.ConnToChildren {
				curr.Write([]byte(fillString("END", 32)))
			}
			wg.Done()
			break
		}
		num, _ := strconv.Atoi(tupleSize)
		bufferTuple := make([]byte, num)
		conn.Read(bufferTuple)
		fmt.Println(string(bufferTuple))
		var in map[string]string
		json.Unmarshal(bufferTuple, &in)
		//filter http and 2008
		sentence := in["line"]
        	words := strings.Split(sentence, " ")
		if len(words) == 0 {
			continue
		}
		if len(words[0]) > 8 {
			if words[0][0:4] == "http" || words[0][0:4] == "2008" {
				continue
			}
		}
		out := self.WordCountFirst(in)
		/*for key, value := range out {
                        fmt.Println(key, value)
                }*/
		self.SendToChildren(out)	
	}
}

func (self *Bolt) SendToChildren(out map[string]string) {
	// Marshal the map into a JSON string.
   	empData, err := json.Marshal(out)   
    	if err != nil {
        	fmt.Println(err)
        	return
    	}
	encode := string(empData)
	for _, conn := range self.ConnToChildren {
		conn.Write([]byte(fillString(strconv.Itoa(len(encode)), 32)))
		conn.Write([]byte(encode))
	}
}

func (self *Bolt) HandleWordCountBoltl(conn net.Conn) {
        defer conn.Close()
        for true {
		bufferSize := make([]byte, 32)
                _, err := conn.Read(bufferSize)
                if err != nil {
			fmt.Println(err)
			wg.Done()
			break
		}

                tupleSize := strings.Trim(string(bufferSize), ":")
		//fmt.Println(tupleSize)
		if tupleSize == "END" {
			self.NumOfFather -= 1
			//fmt.Println(self.NumOfFather)
			wg.Done()
                	break
                }
                num, _ := strconv.Atoi(tupleSize)
                bufferTuple := make([]byte, num)
                conn.Read(bufferTuple)		
		var in map[string]string
                json.Unmarshal(bufferTuple, &in)
		for key, value := range in {
                        fmt.Println(key, value)
                }
		self.WordCountSecond(in)
	}
}

func (self *Bolt) WordCountBoltlTimeToExitCheck() {
	for true {
		if self.NumOfFather == 0 {
			self.WriteIntoFileWordCount()
			break
		}
		time.Sleep(time.Millisecond * 500)
	}
}

func (self *Bolt) WriteIntoFileWordCount() {
	newFile, err := os.Create("local/" + self.App)
	if err != nil {
		wg.Done()
		fmt.Println(err)
	}
	defer newFile.Close()
	for word, count := range self.WordCountMap {
		fmt.Fprintf(newFile, word + ":" + strconv.Itoa(count) + "\n")
	}
	fmt.Println("==Successfully write wordcount file!==")
	wg.Done()
}

////////////////////////////////////////////////////reddit/////////////////////////////////////////////////////////
func (self *Bolt) HandleFilterRedditBoltc(conn net.Conn) {
	defer conn.Close()
        //set up connection to children
        for _, child := range self.Children {
                connToChild, err := net.Dial("tcp", "fa18-cs425-g69-" + child + ".cs.illinois.edu:" + self.PortTCP)
                if err != nil {
                        fmt.Println(err)
                        return
                }
                self.ConnToChildren[child] = connToChild
        }

        for true {
                bufferSize := make([]byte, 32)
                _, err := conn.Read(bufferSize)
                if err != nil {
                        fmt.Println(err)
			//wg.Done()
                        break
                }
                tupleSize := strings.Trim(string(bufferSize), ":")
                //fmt.Println(tupleSize)
                if tupleSize == "END" {
                        for _, curr := range self.ConnToChildren {
                                curr.Write([]byte(fillString("END", 32)))
                        }
			wg.Done()
                        break
                }
                num, _ := strconv.Atoi(tupleSize)
                bufferTuple := make([]byte, num)
                conn.Read(bufferTuple)
                //fmt.Println(string(bufferTuple))
                var in map[string]string
                json.Unmarshal(bufferTuple, &in)
		score, err := strconv.Atoi(in["score"])
		if score < 0 {
			continue
		}
		self.SendToChildren(in)
        }
}

func (self *Bolt) HandleFilterRedditBoltl(conn net.Conn) {
	defer conn.Close()
        for true {
                bufferSize := make([]byte, 32)
                _, err := conn.Read(bufferSize)
                if err != nil {
                        fmt.Println(err)
			//fmt.Println("here1")
			//wg.Done()
                        break
                }
                tupleSize := strings.Trim(string(bufferSize), ":")
                //fmt.Println(tupleSize)
                if tupleSize == "END" {
                        self.NumOfFather -= 1
			fmt.Println("here2")
			wg.Done()
                        break
                }

                num, _ := strconv.Atoi(tupleSize)
                bufferTuple := make([]byte, num)
                conn.Read(bufferTuple)
                var in map[string]string
                json.Unmarshal(bufferTuple, &in)
                for key, value := range in {
                        fmt.Println(key, value)
                }
                self.FilterRedditSecond(in)
        }
}

func (self *Bolt) FilterRedditBoltlTimeToExitCheck() {
        for true {
                if self.NumOfFather == 0 {
                        self.WriteIntoFileFilterReddit()
                        break
                }
                time.Sleep(time.Millisecond * 500)
        }
}

func (self *Bolt) WriteIntoFileFilterReddit() {
	newFile, err := os.Create("local/" + self.App)
        if err != nil {
		fmt.Println("here3")
		wg.Done()
                fmt.Println(err)
        }
        defer newFile.Close()
	//sort FilterRedditMap by number of posts in descending order
	p := rankByWordCount(self.FilterRedditMap)
        for i, curr := range p {
		if i == 50 {
			break
		}
                fmt.Fprintf(newFile, curr.Key + ":" + strconv.Itoa(curr.Value) + "\n")
        }
        fmt.Println("==Successfully write wordcount file!==")
	wg.Done()
	fmt.Println("here4")
}

func rankByWordCount(wordFrequencies map[string]int) PairList{
  pl := make(PairList, len(wordFrequencies))
  i := 0
  for k, v := range wordFrequencies {
    pl[i] = Pair{k, v}
    i++
  }
  sort.Sort(sort.Reverse(pl))
  return pl
}

type Pair struct {
  Key string
  Value int
}

type PairList []Pair
func (p PairList) Len() int { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int){ p[i], p[j] = p[j], p[i] }

//////////////////////////////////////////////////nasan log///////////////////////////////////////////////////////////////
func (self *Bolt) HandleNasaLogBoltc(conn net.Conn) {
	defer conn.Close()
        //set up connection to children
        for _, child := range self.Children {
                connToChild, err := net.Dial("tcp", "fa18-cs425-g69-" + child + ".cs.illinois.edu:" + self.PortTCP)
                if err != nil {
                        fmt.Println(err)
                        return
                }
                self.ConnToChildren[child] = connToChild
        }

        for true {
                bufferSize := make([]byte, 32)
                _, err := conn.Read(bufferSize)
                if err != nil {
                        fmt.Println(err)
                        break
                }
                tupleSize := strings.Trim(string(bufferSize), ":")
                //fmt.Println(tupleSize)
                if tupleSize == "END" {
                        for _, curr := range self.ConnToChildren {
                                curr.Write([]byte(fillString("END", 32)))
                        }
                        wg.Done()
                        break
                }
                num, _ := strconv.Atoi(tupleSize)
                bufferTuple := make([]byte, num)
                conn.Read(bufferTuple)
                fmt.Println(string(bufferTuple))
                var in map[string]string
                json.Unmarshal(bufferTuple, &in)
                if in["status"] != "200" {
			continue
		}
		out := self.NasaLogFirst(in)
                self.SendToChildren(out)
        }
}

func (self *Bolt) HandleNasaLogBoltl(conn net.Conn) {
	defer conn.Close()
        for true {
                bufferSize := make([]byte, 32)
                _, err := conn.Read(bufferSize)
                if err != nil {
                        fmt.Println(err)
                        break
                }
                tupleSize := strings.Trim(string(bufferSize), ":")
                if tupleSize == "END" {
                        self.NumOfFather -= 1
			wg.Done()
                        break
                }
                num, _ := strconv.Atoi(tupleSize)
                bufferTuple := make([]byte, num)
                conn.Read(bufferTuple)
                var in map[string]string
                json.Unmarshal(bufferTuple, &in)
                for key, value := range in {
                        fmt.Println(key, value)
                }
                self.NasaLogSecond(in)
        }	
}

func (self *Bolt) NasaLogBoltlTimeToExitCheck() {
        for true {
                if self.NumOfFather == 0 {
                        self.WriteIntoFileNasaLog()
                        break
                }
                time.Sleep(time.Millisecond * 500)
        }
}

func (self *Bolt) WriteIntoFileNasaLog() {
	newFile, err := os.Create("local/" + self.App)
	if err != nil {
		wg.Done()
		fmt.Println(err)
	}
	defer newFile.Close()
	for host, _ := range self.NasaLogMap {
		fmt.Fprintf(newFile, host + ":\n")
		for route, count := range self.NasaLogMap[host] {
			fmt.Fprintf(newFile, route + ":" + strconv.Itoa(count) + "\n")
		}
	}
	fmt.Println("==Successfully write wordcount file!==")
	wg.Done()
}

//////////////////////////////////////////////////apps////////////////////////////////////////////////////////////////////
func (self *Bolt) WordCountFirst(in map[string]string) map[string]string {
	linenumber := in["linenumber"]
	sentence := in["line"]
	words := strings.Split(sentence, " ")
	m := make(map[string]int)
	for _, word := range words {
		if _, ok := m[word]; ok {
			m[word] += 1
		} else {
			m[word] = 1
		}
	}
	out := make(map[string]string)
	out["linenumber"] = linenumber
	ret := ""
	for word, count := range m {
		ret += word + ":" + strconv.Itoa(count) + " "
	}
	out["lcounts"] = ret
	return out
}

func (self *Bolt)WordCountSecond(in map[string]string) {
	//linenumber := in["linenumber"]
        sentence := in["lcounts"]
	words := strings.Split(sentence, " ")
	self.MyMutex.Lock()
	for i, word := range words {
		if i != len(words) - 1 {
			tuple := strings.Split(word, ":")
			count, _ := strconv.Atoi(tuple[1]) 
			if _, ok := self.WordCountMap[tuple[0]]; ok {
                		self.WordCountMap[tuple[0]] += count
                	} else {
                        	self.WordCountMap[tuple[0]] = count
                	}
		}
	}
	self.MyMutex.Unlock()
}

func (self *Bolt) FilterRedditSecond(in map[string]string) {
	username := in["username"]
	self.MyMutex.Lock()
	if _, ok := self.FilterRedditMap[username]; ok {
		self.FilterRedditMap[username] += 1
	} else {
		self.FilterRedditMap[username] = 1
	}
	self.MyMutex.Unlock()
}

func (self *Bolt) NasaLogFirst(in map[string]string) map[string]string {
	host := in["host"]
	url := in["url"]
	out := make(map[string]string)
	out["host"] = host
	out["route"] = host + url
	return out
}

func (self *Bolt) NasaLogSecond(in map[string]string) {
	host := in["host"]
        route := in["route"]
	self.MyMutex.Lock()
	if _, ok := self.NasaLogMap[host]; ok {
        	if _, ok := self.NasaLogMap[host][route]; ok {
                	self.NasaLogMap[host][route] += 1
		} else {
			self.NasaLogMap[host][route] = 1
		}
        } else {
        	self.NasaLogMap[host][route] = 1	
        }
	self.MyMutex.Unlock()
}

