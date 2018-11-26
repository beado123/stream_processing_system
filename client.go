package main

import (
	"./daemon"
	"./spout"
	"./bolt"
	"os"
	"bufio"
	"fmt"
	"strings"
	"net"
	"io/ioutil"
	"strconv"
)
var appMap map[string]string

//TCP: 6666, UDP:3333
func main() {

	appMap = make(map[string]string)
	appMap["wordcount"] = "./wordcount_dataset"
	appMap["reddit"] = "./reddit_dataset"

	if len(os.Args) < 2 {
		fmt.Println("Please type in master id!")
		return
	}
	master_id := os.Args[1]
	d, err := daemon.NewDaemon(master_id)
  	if err != nil {
    		return
  	}

	for true {
		buf := bufio.NewReader(os.Stdin)
		input, err := buf.ReadBytes('\n')
		if err != nil {
		    fmt.Println(err)
		} else {
			cmd := string(input)
			if strings.Contains(cmd, "JOIN") {
				buf, err := d.JoinGroup()
        			if err != nil {
                			return
        			}
				d.ResponseLIST(buf)
				d.CleanOutSdfs()
				go d.PingToMembers()
        			go d.TimeOutCheck()
        			go d.DaemonListenUDP()
				go d.DaemonListenTCP()
				go ListenFromNimbus()

			} else if strings.Contains(cmd, "LIST") {
                                d.PrintMembershipList()
                        } else if strings.Contains(cmd, "SELF") {
				d.PrintId()
			} else if strings.Contains(cmd, "get-versions") {
                                d.SendGetVersionRequest(cmd)
                        } else if strings.Contains(cmd, "put") {
				d.SendPutRequest(cmd)
			} else if strings.Contains(cmd, "get") {
				d.SendGetRequest(cmd)
			} else if strings.Contains(cmd, "delete") {
				d.SendDeleteRequest(cmd)
			} else if strings.Contains(cmd, "ls") {
				d.SendLsRequest(cmd)
			} else if strings.Contains(cmd, "store") {
				d.StoreRequest()
			} else if strings.Contains(cmd, "wordcount")|| strings.Contains(cmd, "reddit") {
				conn, err := net.Dial("tcp", "fa18-cs425-g69-" + master_id + ".cs.illinois.edu:8000")
	        		if err != nil {
        		        	fmt.Println(err)
        			}
				conn.Write([]byte(cmd))
				conn.Close()
			} else {
				fmt.Println("Input does not match any commads!")	
			}
		}
	}
}

//port number 8000
func ListenFromNimbus() {
	ip_address := getIPAddrAndLogfile()
	l, err := net.Listen("tcp", ip_address + ":8000")
	if err != nil {
		fmt.Println(err)
                return
	}

	for true {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go ParseRequest(conn)
	}
}

func ParseRequest(conn net.Conn) {
	buf := make([]byte, 1024)
	reqLen, _ := conn.Read(buf)
	reqArr := strings.Split(string(buf[:reqLen]), " ")
	if reqArr[0] == "boltc" {
		t := reqArr[0]
		app := reqArr[1]
		father, _ := strconv.Atoi(reqArr[2])
		var children []string
		for i, child := range reqArr {
			if i == 0 || i == 1 || i == 2{
				continue
			}
			children = append(children, child)
		}
		fmt.Println(t)
		fmt.Println(app)
		fmt.Println(father)
		for _, curr := range children {
			fmt.Println(curr)
		}
		bolt := bolt.NewBolt(t, app, children, father)
		bolt.BoltListen()
		
	} else if reqArr[0] == "boltl" {
		t := reqArr[0]
		app := reqArr[1]
		father, _ := strconv.Atoi(reqArr[2])
                var children []string
		fmt.Println(t)
                fmt.Println(app)
		fmt.Println(father)
		for _, curr := range children {
                        fmt.Println(curr)
                }
		bolt := bolt.NewBolt(t, app, children, father)
		bolt.BoltListen()
	} else if reqArr[0] == "spout" {
		t := reqArr[0]
                app := reqArr[1]
                var children []string
                for i, child := range reqArr {
                        if i == 0 || i == 1 {
                                continue
                        }
                        children = append(children, child)
                }
		fmt.Println(t)
                fmt.Println(app)
                for _, curr := range children {
                        fmt.Println(curr)
                }
		spout := new(spout.Spout)
		spout.Init(appMap[app], app, children)
		spout.Open()
		spout.Start()
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
	//fmt.Println("ip address of current VM:\n", ip)
	return ip
}
