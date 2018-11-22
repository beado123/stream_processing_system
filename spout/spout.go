package spout

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"encoding/json"
)

type Spout struct {
	App string
	FilePath string
	Children []string
	LineNum int
	Scanner *bufio.Scanner
}

//This is a helper function that prints the error
func checkErr(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
	}
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
	scanner := bufio.NewScanner(file)
	self.Scanner = scanner
}

func SendToBolt(machine string, jsonStr string) {
	conn, err := net.Dial("udp", "fa18-cs425-g69-" + machine + ".cs.illinois.edu:8888")
	checkErr(err)
	_, err = conn.Write([]byte(fillString(strconv.Itoa(len(jsonStr)), 32)))
	checkErr(err)
	_, err = conn.Write([]byte(jsonStr))
	checkErr(err)
}

func (self *Spout) Encode(machine string, emit map[string]string) {
	emitData, err := json.Marshal(emit)
	checkErr(err)
	jsonStr := string(emitData)
	fmt.Println("JSON data is\n", jsonStr)
	SendToBolt(machine, jsonStr)
}

func (self *Spout) Start() {

	index := 0
	length := len(self.Children)
	for self.Scanner.Scan() {
		self.LineNum += 1
		emit := make(map[string]string)
		emit["linenumber"] = strconv.Itoa(self.LineNum)
		emit["line"] = self.Scanner.Text()
		fmt.Println(emit["linenumber"], emit["line"])
		Encode(self.Children[index], emit)
		if index == length -1 {
			indexx = 0
		} else {
			index += 1
		}
	}
	fmt.Println("==========File End==========")
}
