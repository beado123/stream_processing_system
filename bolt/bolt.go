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
	"io"
)

type Bolt struct {
	VmId string
	VmIpAddress string
	Ln net.Listener
	PortTCP string
	Children []string	
	IsActive bool
	Type string
	App string
	WordCountMap map[string]int
	MyMutex *sync.Mutex
	ConnToChildren map[string]net.Conn
}

func NewBolt(t string, app string, children []string) (b *Bolt) {
	ip_address := getIPAddrAndLogfile()
	vm_id := ip_address[15:17]
	l, err := net.Listen("tcp", ip_address + ":5555")
	if err != nil {
		fmt.Println(err)
                return
	}
	mutex := &sync.Mutex{}
	b = &Bolt {
		VmId: vm_id,
		VmIpAddress: ip_address,
		Ln: l,
		PortTCP: "5555",
		Children: children,
		IsActive: true,
		Type: t,		
		App: app,
		WordCountMap: make(map[string]int),
		MyMutex: mutex,
		ConnToChildren: make(map[string]net.Conn),
	}
	return
}

func (self *Bolt) BoltListen() {
	if self.IsActive == false {
		return
	}
	defer self.Ln.Close()
	for true {
		conn, err := self.Ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		//fmt.Println("TCP Accept:", conn.RemoteAddr().String())
		if self.Type == "boltc" && self.App == "wordcount" {
			go self.HandleWordCountBoltc(conn)
		} else if self.Type == "boltl" && self.App == "wordcount" {
			go self.HandleWordCountBoltl(conn)	
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
		if err == io.EOF {
			break
		}
		tupleSize := strings.Trim(string(bufferSize), ":")
		num, _ := strconv.Atoi(tupleSize)
		bufferTuple := make([]byte, num)
		conn.Read(bufferTuple)
		fmt.Println(string(bufferTuple))
		var in map[string]string
		json.Unmarshal(bufferTuple, &in)
		for key, value := range in {
			fmt.Println(key, value)
		}
		out := self.WordCountFirst(in)
		for key, value := range out {
                        fmt.Println(key, value)
                }
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
                if err == io.EOF {
                        break
                }
                tupleSize := strings.Trim(string(bufferSize), ":")
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
	self.WriteIntoFileWordCount()
}

func (self *Bolt) WriteIntoFileWordCount() {
	newFile, err := os.Create("local/" + self.App)
	if err != nil {
		fmt.Println(err)
	}
	defer newFile.Close()
	for word, count := range self.WordCountMap {
		fmt.Fprintf(newFile, word + ":" + strconv.Itoa(count) + "\n")
	}
}
///////////////////////apps//////////////////////////////////
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
