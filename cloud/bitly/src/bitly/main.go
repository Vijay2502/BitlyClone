package main

import (
	"os"
)

func main() {

	port := os.Getenv("PORT")
	if len(port) == 0 {
		port = "3000"
	}

	server := NewServer()
	go queue_receive()
	server.Run(":" + port)
}
