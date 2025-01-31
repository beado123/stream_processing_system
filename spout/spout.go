package spout

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"encoding/json"
	"encoding/csv"
	"net"
	"time"
	"io"
	"io/ioutil"
	"strings"
)

var connMap map[string]net.Conn
var acceptMachineAddr *net.UDPAddr
var selfId string
var logWriter io.Writer
var quit bool

type Spout struct {
	App string
	FilePath string
	Children []string
	LineNum int
	isActive bool
	Scanner *bufio.Scanner
	Reader *csv.Reader
}

//This is a helper function that prints the error
func checkErr(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
	}
}

//This function extracts ip address of current VM from file "ip_address" in current directory
func getIPAddr() string{

	data, err := ioutil.ReadFile("ip_address")
	if err != nil {
		panic(err)
	}

	ip := string(data[:len(data)])
	
	//remove \n from end of line
	if strings.HasSuffix(ip, "\n") {
		ip = ip[:(len(ip) - 1)]
	}
	fmt.Println("ip address of current VM:", ip)
	return ip
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

//This function initializes Spout struct
func (self *Spout) Init(filePath string, app string, children []string) {
	self.FilePath = filePath
	self.App = app
	self.Children = children
	self.LineNum = 0
	self.isActive = true
}

//This function initializes corresponding scanner(file), reader(CSV) for reading files
func (self *Spout) Open() {
	file, err := os.Open(self.FilePath)
	if err != nil {
		log.Fatal(err)
	}
	if self.App == "wordcount" {
		scanner := bufio.NewScanner(file)
		self.Scanner = scanner
	} else if self.App == "reddit" {
		reader := csv.NewReader(bufio.NewReader(file))
		self.Reader = reader
	} else if self.App == "nasalog" {
		scanner := bufio.NewScanner(file)
		self.Scanner = scanner
	}
}

//This function send tuples to bolts
func SendToBolt(machine string, jsonStr string) {

	connMap[machine].Write([]byte(fillString(strconv.Itoa(len(jsonStr)), 32)))
	//checkErr(err)
	//fmt.Println("Wrote", len, "bytes")
	connMap[machine].Write([]byte(jsonStr))
	//checkErr(err)
	//fmt.Println("Wrote",len, "bytes" )
}

//This function transforms map data structure to JSON string
func Encode(machine string, emit map[string]string) {
	emitData, err := json.Marshal(emit)
	checkErr(err)
	jsonStr := string(emitData)
	fmt.Fprintln(logWriter, "JSON data is\n", jsonStr)
	SendToBolt(machine, jsonStr)
}

//This function listens for failure information(DOWN XX) from Nimbus
func (self *Spout) listenFromNimbus() {

	//get ip address from servers list	
	ip := getIPAddr()
	selfId = ip[15:17]

	addr := net.UDPAddr{
		Port: 4444,
		IP: net.ParseIP(ip),
	}
	
    ser, err := net.ListenUDP("udp", &addr)

    checkErr(err)
    defer ser.Close()

	fmt.Println("Spout Listening udp on port 4444")
	fmt.Fprintln(logWriter, "Spout Listening udp on port 4444")

	//Listen for incoming connections
	buf := make([]byte, 1024)

    for {
        n, remoteAddr, err := ser.ReadFromUDP(buf)
		checkErr(err)
		fmt.Println( "=============\nReceived a message from %v:%s", remoteAddr, string(buf[:n]))
		fmt.Fprintln(logWriter,  "=============\nReceived a message from %v:%s \n", remoteAddr, string(buf[:n]))
		quit = false
		break
	}
}

//This function starts the spout, reads files line by line
func (self *Spout) Start() {

	//create local log file for debugging
	file, err := os.Create("logger")
	checkErr(err)
	logWriter = io.MultiWriter(file)

	quit = true

	go self.listenFromNimbus()
	if self.App == "nasalog" {

		index := 0
		length := len(self.Children)
		connMap = make(map[string]net.Conn)
		time.Sleep(time.Millisecond* 1000)
		for _, vm := range self.Children {
			fmt.Println("vm", vm)
			conn, err := net.Dial("tcp", "fa18-cs425-g69-" + vm + ".cs.illinois.edu:5555")
			fmt.Println("conn", conn)
			checkErr(err)
			connMap[vm] = conn
		}
		for self.Scanner.Scan() {

			fmt.Fprintln(logWriter, "quit", quit)
			if quit == false {
				fmt.Println("Quit Spout detected failure! Drop task...")
				fmt.Fprintln(logWriter, "Spout detected failure! Drop task...")
				return
			}
			//fmt.Println("index", index)
			self.LineNum += 1
			fmt.Println("line number", self.LineNum)
			line := self.Scanner.Text()
			arr := strings.Fields(line)
			emit := make(map[string]string)
			if len(arr) < 9 {
				continue
			}
			emit["host"] = arr[0]
			emit["date"] = arr[3][1:3]
			emit["request"] = arr[5][1:]
			emit["url"] = arr[6]
			emit["status"] = arr[8] 
			Encode(self.Children[index], emit)
			if index == length -1 {
				index = 0
			} else {
				index += 1
			}
		}
		fmt.Println("==========File End==========")
		for _, vm := range self.Children {
			len, err := connMap[vm].Write([]byte(fillString("END", 32)))
			checkErr(err)
			fmt.Println("Wrote", len, "bytes")
		}
		
	} else if(self.App == "wordcount"){

		index := 0
		length := len(self.Children)
		connMap = make(map[string]net.Conn)
		time.Sleep(time.Millisecond* 1000)
		for _, vm := range self.Children {
			fmt.Println("vm", vm)
			conn, err := net.Dial("tcp", "fa18-cs425-g69-" + vm + ".cs.illinois.edu:5555")
			fmt.Println("conn", conn)
			checkErr(err)
			connMap[vm] = conn
		}
		for self.Scanner.Scan() {

			fmt.Fprintln(logWriter, "quit", quit)
			if quit == false {
				fmt.Println("Quit Spout detected failure! Drop task...")
				fmt.Fprintln(logWriter, "Spout detected failure! Drop task...")
				return
			}
			fmt.Println("index", index)
			self.LineNum += 1
			emit := make(map[string]string)
			emit["linenumber"] = strconv.Itoa(self.LineNum)
			emit["line"] = self.Scanner.Text()
			fmt.Println(emit["linenumber"], emit["line"])
			Encode(self.Children[index], emit)
			if index == length -1 {
				index = 0
			} else {
				index += 1
			}
		}
		fmt.Println("==========File End==========")
		for _, vm := range self.Children {
			len, err := connMap[vm].Write([]byte(fillString("END", 32)))
			checkErr(err)
			fmt.Println("Wrote", len, "bytes")
		}

	} else if(self.App == "reddit"){

		index := 0
		length := len(self.Children)
		connMap = make(map[string]net.Conn)
		time.Sleep(time.Millisecond* 1000)
		for _, vm := range self.Children {
			fmt.Println("vm", vm)
			conn, err := net.Dial("tcp", "fa18-cs425-g69-" + vm + ".cs.illinois.edu:5555")
			fmt.Println("conn", conn)
			checkErr(err)
			connMap[vm] = conn
		}
		for {
			fmt.Fprintln(logWriter, "quit", quit)
			if quit == false {
				fmt.Println("Quit Spout detected failure! Drop task...")
				fmt.Fprintln(logWriter, "Spout detected failure! Drop task...")
				return
			}
			arr, err := self.Reader.Read()
			if err == io.EOF {
				break;
			}
			//fmt.Println("index", index)
			self.LineNum += 1
			emit := make(map[string]string)
			emit["rawtime"] = arr[2]
			emit["title"] = arr[3]
			emit["total_votes"] = arr[4]
			emit["reddit_id"] = arr[5]
			emit["score"] = arr[10]
			emit["number_of_comments"] = arr[11]
			emit["username"] = arr[12]
			//fmt.Println(emit["reddit_id"], emit["title"])
			Encode(self.Children[index], emit)
			if index == length -1 {
				index = 0
			} else {
				index += 1
			}
			time.Sleep(time.Microsecond * 100)
		}
		fmt.Println("==========File End==========")
		for _, vm := range self.Children {
			len, err := connMap[vm].Write([]byte(fillString("END", 32)))
			checkErr(err)
			fmt.Println("Wrote", len, "bytes")
		}
	}
	
}

