package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

var (
	store = make(map[string]string)
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
					mu.Lock()
					store[key] = value
					mu.Unlock()
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
						conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
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
// Add a function to handle replication
func replicateToFollowers(command string, args []string) {
	// This function will be implemented in the next stages
	// For now, it's just a placeholder
}