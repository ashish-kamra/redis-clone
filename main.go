package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

var port = flag.String("port", "6379", "Listening port address")

func main() {
	log.Printf("Listening on port: %s", *port)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatalf("Failed to bind to port %s: %v", *port, err)
	}
	defer listener.Close()

	aof, err := NewAof("redis.aof", false)
	if err != nil {
		log.Fatalf("Failed to open/create AOF: %v", err)
	}
	defer aof.Close()

	rebuildCacheFromAOF(aof)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn, aof)
	}
}

func handleConnection(conn net.Conn, aof *Aof) {
	defer conn.Close()
	reader := NewReader(conn)
	writer := NewWriter(conn)

	for {
		respObject, err := reader.Deserialize()
		if err != nil {
			if err == io.EOF {
				log.Printf("Connection closed %v", conn.RemoteAddr())
			} else {
				log.Printf("Error reading message: %v", err)
			}
			return
		}

		result := processCommand(respObject, aof)
		if err := writer.Write(result); err != nil {
			log.Printf("Error writing response: %v", err)
			return
		}
	}
}

func processCommand(respObject RESPObject, aof *Aof) RESPObject {
	if respObject.Type != Array {
		return RESPObject{Type: Error, Value: "Invalid request, expected array"}
	}

	respObjectVal := respObject.Value.([]RESPObject)
	if len(respObjectVal) == 0 {
		return RESPObject{Type: Error, Value: "Invalid request, expected array length > 0"}
	}

	command := strings.ToUpper(respObjectVal[0].Value.(string))
	args := respObjectVal[1:]

	handler, ok := Handlers[command]
	if !ok {
		return RESPObject{Type: Error, Value: fmt.Sprintf("Invalid command: %s", command)}
	}

	if command == "SET" || command == "HSET" {
		if err := aof.Write(respObject); err != nil {
			log.Printf("Error writing to AOF: %v", err)
		}
	}

	return handler(args)
}

func rebuildCacheFromAOF(aof *Aof) {
	err := aof.Read(func(respObject RESPObject) {
		command := strings.ToUpper(respObject.Value.([]RESPObject)[0].Value.(string))
		args := respObject.Value.([]RESPObject)[1:]
		handler, ok := Handlers[command]
		if !ok {
			log.Printf("Unknown command in AOF: %s", command)
			return
		}
		handler(args)
	})
	if err != nil {
		log.Printf("Error rebuilding cache from AOF: %v", err)
	}
}
