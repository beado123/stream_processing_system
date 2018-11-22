package bolt

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"io/ioutil"
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
}

func NewBolt(t string, app string, children []string) (b *Bolt) {
	ip_address := getIPAddrAndLogfile()
	vm_id := ip_address[15:17]
	l, err := net.Listen("tcp", ip_address + ":8888")
	if err != nil {
		fmt.Println(err)
                return
	}
	
	b = &Bolt {
		VmId: vm_id,
		VmIpAddress: ip_address,
		Ln: l,
		PortTCP: "8888",
		Children: children,
		IsActive: true,
		Type: t,		
		App: app,
	}
	return
}

func (self *Bolt) BoltListen() {
	if self.IsActive == false {
		return
	}

	for true {
		conn, err := self.Ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		if self.Type == "boltc" && self.App == "wordcount" {
			go HandleWordCountBoltc(conn)
		} else if self.Type == "boltl" && self.App == "wordcount" {
			
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
	fmt.Println("ip address of current VM:\n", ip)
	return ip
}

func (self *Bolt) HandleWordCountBoltc(conn net.Conn) {
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
		in = make(map[string]string)
		json.Unmarshal(bufferTuple, in)
		out := WordCountFirst(in)
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
	for _, child := range self.Children {
		conn, err := net.Dial("tcp", "fa18-cs425-g69-" + child + ".cs.illinois.edu:" + self.PortTCP)
        	if err != nil {
                	fmt.Println(err)
                	return
        	}
		conn.Write([]byte(fillString(strconv.Itoa(len(encode)), 32)))
		conn.Write([]byte(encode))
	}
}

///////////////////////apps//////////////////////////////////
func WordCountFirst(in map[string]string) map[string]string {
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

