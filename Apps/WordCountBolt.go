package Apps

import (
	"strings"
	"strconv"
)

type WordCountBolt struct {
	
}

/*
 * count word number for each line
 * @param [line number] : [line content]
 * @return [word] : [count] 
 */
func (self *WordCountBolt) Execute(in map[string]string) map[string]string {
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
