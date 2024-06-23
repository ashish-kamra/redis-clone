package main

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

var Handlers = map[string]func([]Value) Value{
	"COMMAND": command,
	"ECHO":    echo,
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
}

type val struct {
	value     string
	expiresAt time.Time
}

var (
	SETs    = map[string]val{}
	HSETs   = map[string]map[string]string{}
	SETsMu  = sync.RWMutex{}
	HSETsMu = sync.RWMutex{}
)

func command(args []Value) Value {
	//*2\r\n$7COMMAND\r\n$4DOCS\r\n
	//[{typ:bulk str: num:0 bulk:DOCS array:[]}]
	return Value{typ: "array", str: args[0].bulk}
}

func echo(args []Value) Value {
	if len(args) > 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'echo' command"}
	}
	return Value{typ: "string", str: args[0].bulk}
}

func ping(args []Value) Value {
	len := len(args)
	if len == 0 {
		return Value{typ: "string", str: "PONG"}
	} else if len == 1 {
		return Value{typ: "string", str: args[0].bulk}
	}
	return Value{typ: "error", str: "ERR wrong number of arguments for 'ping' command"}
}

func set(args []Value) Value {
	if len(args) < 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	var expiresAt time.Time

	key := args[0].bulk
	value := args[1].bulk

	if len(args) == 4 {
		cmd := strings.ToUpper(args[2].bulk)
		duration, err := strconv.ParseInt(args[3].bulk, 10, 64)
		if err != nil {
			return Value{typ: "eror", str: "ERR value is not an integer or out of range"}
		}

		switch cmd {
		case "PX":
			expiresAt = time.Now().Add(time.Duration(duration) * time.Millisecond)
		case "EX":
			expiresAt = time.Now().Add(time.Duration(duration) * time.Second)
		}

	}

	SETsMu.Lock()
	SETs[key] = val{value: value, expiresAt: expiresAt}
	SETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	if !value.expiresAt.IsZero() && value.expiresAt.Before(time.Now()) {
		SETsMu.Lock()
		delete(SETs, key)
		SETsMu.Unlock()
		return Value{typ: "null"}
	}
	return Value{typ: "bulk", bulk: value.value}
}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}
