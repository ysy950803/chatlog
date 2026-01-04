package main

import (
	"log"

	"github.com/ysy950803/chatlog/cmd/chatlog"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	chatlog.Execute()
}
