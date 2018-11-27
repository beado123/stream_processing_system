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
)

var connMap map[string]net.Conn
type Spout struct {
	App string
	FilePath string
	Children []string
	LineNum int
	Scanner *bufio.Scanner
	Reader *csv.Reader
}

//This is a helper function that prints the error
func checkErr(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
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

func (self *Spout) Init(filePath string, app string, children []string) {
	self.FilePath = filePath
	self.App = app
	self.Children = children
	self.LineNum = 0
}

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
	}
	
	
}

func SendToBolt(machine string, jsonStr string) {
	fmt.Println("machine sendToBolt", machine)
	len, err := connMap[machine].Write([]byte(fillString(strconv.Itoa(len(jsonStr)), 32)))
	checkErr(err)
	fmt.Println("Wrote", len, "bytes")
	len, err = connMap[machine].Write([]byte(jsonStr))
	checkErr(err)
	fmt.Println("Wrote",len, "bytes" )
}

func Encode(machine string, emit map[string]string) {
	emitData, err := json.Marshal(emit)
	checkErr(err)
	jsonStr := string(emitData)
	fmt.Println("JSON data is\n", jsonStr)
	SendToBolt(machine, jsonStr)
}

func (self *Spout) Start() {

	if(self.App == "wordcount"){

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
			arr, err := self.Reader.Read()
			if err == io.EOF {
				break;
			}
			fmt.Println("index", index)
			self.LineNum += 1
			emit := make(map[string]string)
			emit["rawtime"] = arr[2]
			emit["title"] = arr[3]
			emit["total_votes"] = arr[4]
			emit["reddit_id"] = arr[5]
			emit["score"] = arr[10]
			emit["number_of_comments"] = arr[11]
			emit["username"] = arr[12]
			fmt.Println(emit["reddit_id"], emit["title"])
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
	}
	
}
