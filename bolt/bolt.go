package bolt

import (

)

type Bolt struct {
	VmId string
	VmIpAddress string
	Ln net.Listener
	PortTCP string
	Children []string	
	IsActive bool
	Type string
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
		
	}
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
		go self.ParseRequest(conn)
	}
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

func (self *Bolt) ParseRequest(conn net.Conn) {
	bufferRequest := make([]byte, 8)
	conn.Read(bufferRequest)
	request := string(bufferRequest)
}

