package main

import (
	"bufio"
	"failure"
	"file_sys"
	"fmt"
	"grep_server"
	"log"
	"os"
	"os/exec"
	"shared"
	"strings"
)

const CMD_PROMPT = "> "

func main() {
	println("Starting server")
	log.SetFlags(log.Lshortfile)

	failure.Initialize()

	go func() {
		println("Type 'help' for list of commands")
		fmt.Printf("[Server %d]%s", shared.GetOwnServerNumber(), CMD_PROMPT)
		for {
			reader := bufio.NewReader(os.Stdin)
			cmd, _ := reader.ReadString('\n')
			go func() {
				HandleServerCommand(strings.TrimSuffix(cmd, "\n"))
				fmt.Printf("[Server %d]%s", shared.GetOwnServerNumber(), CMD_PROMPT)
			}()
		}
	}()

	file_sys.Initialize()
	grep_server.Initialize()

	println("Finished starting server")
	// Keep the program running so it doesn't close the port
	for {
	}
}

func HandleServerCommand(cmd string) {
	switch com := strings.Split(cmd, " "); com[0] {
	case "": {
		println()
	}
	case "leave": {
		fmt.Println("Leaving group")
		failure.LeaveGroup()
		file_sys.Leave()
	}
	case "print_fail": {
		shared.PrintFailDetectInfo = !shared.PrintFailDetectInfo
	}
	case "memlist": {
		println(failure.MemList.Str(true))
	}
	case "clear" : {
    cmd := exec.Command("clear")
    cmd.Stdout = os.Stdout
    cmd.Run()
	}
	case "put", "get", "delete", "ls", "store", "get-versions", "test": {
		fileCmdError := file_sys.HandleFileCmd(com[0], com[1:])
		if fileCmdError != nil {
			fmt.Printf("%v\n", fileCmdError)
		}
		println()
	}
	case "help": {
		fmt.Printf("leave\nprint_fail\nmem_list\nput\nget\ndelete\nls\nstore\nget-versions\n\n")
	}
	default:
		println("Invalid Command")
	}
}
