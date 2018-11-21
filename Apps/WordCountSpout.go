//package Apps
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
)

type WordCountSpout struct {
	FilePath string
	LineNum int
	Scanner *bufio.Scanner
}

func (self *WordCountSpout) Init(filePath string) {
	self.FilePath = filePath
	self.LineNum = 0
}

func (self *WordCountSpout) Open() {
	file, err := os.Open(self.FilePath)
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(file)
	self.Scanner = scanner
}

func (self *WordCountSpout) NextTuple() map[string]string {
	notEnd := self.Scanner.Scan()
	if notEnd == true {
		self.LineNum += 1
		emit := make(map[string]string)
		emit["linenumber"] = strconv.Itoa(self.LineNum)
		emit["line"] = self.Scanner.Text()
		fmt.Println(emit["linenumber"], emit["line"])
	} else {
		fmt.Println("==========File End==========")
	}
	return nil
}

/*func main() {
	s := new(WordCountSpout)
	s.Init("testSpout.txt")
	s.Open()
	s.NextTuple()
	s.NextTuple()
	s.NextTuple()
}*/
