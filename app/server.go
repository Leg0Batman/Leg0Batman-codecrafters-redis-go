package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

var (
	store    = make(map[string]string)
	expiry   = make(map[string]time.Time)
	mu       sync.Mutex
	expiryMu sync.Mutex
)

func main() {
	port := flag.String("port", "6379", "Port to run the Redis server on")
	flag.Parse()

	fmt.Println("Logs from your program will appear here!")
	l, err := net.Listen("tcp", "0.0.0.0:"+*port)
	if err != nil {
		fmt.Println("Failed to bind to port", *port)
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
			return
		}
		// fmt.Printf("Received data: %v\n", string(buf))
		command := string(buf)
		if command == "PING\n" {
			conn.Write([]byte("PONG\n"))
		} else if command == "QUIT\n" {
			conn.Write([]byte("OK\n"))
			return
		} else {
			// fmt.Println("Command not recognized")
			conn.Write([]byte("-ERR unknown command '" + command + "'\n"))
		}
	}
}
