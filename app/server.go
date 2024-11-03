package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	roleMaster = "master"
)

type storeValue struct {
	value     string
	expiry    time.Time
	hasExpiry bool
}

var (
	store = make(map[string]storeValue)
	mu    sync.Mutex
)

func main() {
	// Define the port flag
	port := flag.Int("port", 6379, "Port to run the Redis server on")
	flag.Parse()

	fmt.Println("Logs from your program will appear here!")
	address := fmt.Sprintf("0.0.0.0:%d", *port)
	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", *port)
		os.Exit(1)
	}

	go expireKeys()

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
					info := "role:master\r\n"
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

func expireKeys() {
	for {
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		now := time.Now()
		for key, val := range store {
			if val.hasExpiry && now.After(val.expiry) {
				delete(store, key)
			}
		}
		mu.Unlock()
	}
}

// Add a function to handle replication
func replicateToFollowers(command string, args []string) {
	// This function will be implemented in the next stages
	// For now, it's just a placeholder
}
