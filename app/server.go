package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
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
