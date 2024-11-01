package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	store      = make(map[string]string)
	expiry     = make(map[string]time.Time)
	mu         sync.Mutex
	expiryMu   sync.Mutex
)

func main() {
	fmt.Println("Logs from your program will appear here!")
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
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
					var px int
					if len(request) > 4 && strings.ToUpper(request[3]) == "PX" {
						var err error
						px, err = parseInteger(request[4])
						if err != nil {
							conn.Write([]byte("-ERR invalid PX value\r\n"))
							continue
						}
					}
					mu.Lock()
					store[key] = value
					mu.Unlock()
					if px > 0 {
						expiryMu.Lock()
						expiry[key] = time.Now().Add(time.Duration(px) * time.Millisecond)
						expiryMu.Unlock()
						go func(key string, duration time.Duration) {
							time.Sleep(duration)
							mu.Lock()
							expiryMu.Lock()
							delete(store, key)
							delete(expiry, key)
							expiryMu.Unlock()
							mu.Unlock()
						}(key, time.Duration(px)*time.Millisecond)
					}
					conn.Write([]byte("+OK\r\n"))
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				}
			case "GET":
				if len(request) > 1 {
					key := request[1]
					mu.Lock()
					value, exists := store[key]
					mu.Unlock()
					if exists {
						expiryMu.Lock()
						expiryTime, hasExpiry := expiry[key]
						expiryMu.Unlock()
						if hasExpiry && time.Now().After(expiryTime) {
							mu.Lock()
							expiryMu.Lock()
							delete(store, key)
							delete(expiry, key)
							expiryMu.Unlock()
							mu.Unlock()
							conn.Write([]byte("$-1\r\n"))
						} else {
							conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
						}
					} else {
						conn.Write([]byte("$-1\r\n"))
					}
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
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
	numArgs, err := parseInteger(line[1:])
	if err != nil {
		return nil, err
	}
	request := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 || line[0] != '$' {
			return nil, fmt.Errorf("invalid request")
		}
		argLen, err := parseInteger(line[1:])
		if err != nil {
			return nil, err
		}
		arg := make([]byte, argLen)
		_, err = reader.Read(arg)
		if err != nil {
			return nil, err
		}
		request[i] = string(arg)
		// Read the trailing \r\n
		_, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
	}
	return request, nil
}

func parseInteger(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
