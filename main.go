package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func main() {
	fmt.Println("Listening on port: 6379")

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Failed to bind to port :6379 ", err.Error())
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		resp := NewResp(conn)
		writer := NewWriter(conn)

		value, err := resp.Read()
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed ", conn.RemoteAddr())
			} else {
				fmt.Println("Error while reading the message: ", err)
			}
			os.Exit(1)
		}

		if value.typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		result := handler(args)

		writer.Write(result)
		writer.writer.Flush()
	}
}
