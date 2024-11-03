package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	store      = make(map[string]storeValue)
	mu         sync.Mutex
	serverRole = "master"
)

type storeValue struct {
	value     string
	expiry    time.Time
	hasExpiry bool
}

func main() {
	port := "6379"
	for i, arg := range os.Args {
		if arg == "--port" && i+1 < len(os.Args) {
			port = os.Args[i+1]
		} else if arg == "--replicaof" && i+1 < len(os.Args) {
			serverRole = "slave"
		}
	}

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting:", err.Error())
			os.Exit(1)
		}
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		request, err := parseRequest(reader)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
			return
		}
		if len(request) > 0 {
			command := strings.ToUpper(request[0])
			switch command {
			case "PING":
				conn.Write([]byte("+PONG\r\n"))
			case "ECHO":
				if len(request) > 1 {
					conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(request[1]), request[1])))
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
				}
			case "SET":
				if len(request) > 2 {
					key := request[1]
					value := request[2]
					var expiry time.Time
					var hasExpiry bool
					if len(request) > 4 && strings.ToLower(request[3]) == "px" {
						expiryDuration, err := parseInteger(request[4])
						if err != nil {
							conn.Write([]byte("-ERR invalid expire time in set\r\n"))
							continue
						}
						expiry = time.Now().Add(time.Duration(expiryDuration) * time.Millisecond)
						hasExpiry = true
					}
					mu.Lock()
					store[key] = storeValue{value: value, expiry: expiry, hasExpiry: hasExpiry}
					mu.Unlock()
					conn.Write([]byte("+OK\r\n"))
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				}
			case "GET":
				if len(request) > 1 {
					key := request[1]
					mu.Lock()
					val, exists := store[key]
					if exists && val.hasExpiry && time.Now().After(val.expiry) {
						delete(store, key)
						exists = false
					}
					mu.Unlock()
					if exists {
						conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val.value), val.value)))
					} else {
						conn.Write([]byte("$-1\r\n"))
					}
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				}
			case "INFO":
				if len(request) > 1 && strings.ToLower(request[1]) == "replication" {
					info := fmt.Sprintf("role:%s\r\n", serverRole)
					conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)))
				} else {
					conn.Write([]byte("-ERR unknown INFO subcommand\r\n"))
				}
			default:
				conn.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", command)))
			}
		}
	}
}

func parseRequest(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("invalid request")
	}
	numArgs, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	request := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if len(line) == 0 || line[0] != '$' {
			return nil, fmt.Errorf("invalid request")
		}
		argLen, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return nil, err
		}
		arg := make([]byte, argLen)
		_, err = reader.Read(arg)
		if err != nil {
			return nil, err
		}
		request[i] = string(arg)
		_, err = reader.ReadString('\n') // read the trailing \r\n
		if err != nil {
			return nil, err
		}
	}
	return request, nil
}

func parseInteger(s string) (int, error) {
	return strconv.Atoi(s)
}
