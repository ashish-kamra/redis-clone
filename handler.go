package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ErrWrongArgCount = "ERR wrong number of arguments for '%s' command"
	ErrInvalidInt    = "ERR value is not an integer or out of range"
)

var Handlers = map[string]func([]RESPObject) RESPObject{
	"COMMAND": command,
	"ECHO":    echo,
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"KEYS":    keys,
}

type Value struct {
	Data      string
	ExpiresAt time.Time
}

var (
	SETs  = sync.Map{}
	HSETs = sync.Map{}
)

func command(args []RESPObject) RESPObject {
	if len(args) != 1 {
		return RESPObject{Type: Error, Value: fmt.Sprintf(ErrWrongArgCount, "command")}
	}
	return RESPObject{Type: SimpleString, Value: args[0].Value}
}

func echo(args []RESPObject) RESPObject {
	if len(args) != 1 {
		return RESPObject{Type: Error, Value: fmt.Sprintf(ErrWrongArgCount, "echo")}
	}
	return RESPObject{Type: SimpleString, Value: args[0].Value}
}

func ping(args []RESPObject) RESPObject {
	switch len(args) {
	case 0:
		return RESPObject{Type: SimpleString, Value: "PONG"}
	case 1:
		return RESPObject{Type: SimpleString, Value: args[0].Value}
	default:
		return RESPObject{Type: Error, Value: fmt.Sprintf(ErrWrongArgCount, "ping")}
	}
}

func set(args []RESPObject) RESPObject {
	if len(args) < 2 || len(args) > 4 {
		return RESPObject{Type: Error, Value: fmt.Sprintf(ErrWrongArgCount, "set")}
	}

	key, value := args[0].Value.(string), args[1].Value.(string)
	var expiresAt time.Time
	//TODO: Handle key expiration for AOF entries
	if len(args) == 4 {
		cmd := strings.ToUpper(args[2].Value.(string))
		duration, err := strconv.ParseInt(args[3].Value.(string), 10, 64)
		if err != nil {
			return RESPObject{Type: Error, Value: ErrInvalidInt}
		}

		switch cmd {
		case "PX":
			expiresAt = time.Now().Add(time.Duration(duration) * time.Millisecond)
		case "EX":
			expiresAt = time.Now().Add(time.Duration(duration) * time.Second)
		default:
			return RESPObject{Type: Error, Value: "ERR syntax error"}
		}
	}

	SETs.Store(key, Value{Data: value, ExpiresAt: expiresAt})
	return RESPObject{Type: SimpleString, Value: "OK"}
}

func get(args []RESPObject) RESPObject {
	if len(args) != 1 {
		return RESPObject{Type: Error, Value: fmt.Sprintf(ErrWrongArgCount, "get")}
	}

	key := args[0].Value.(string)
	if val, ok := SETs.Load(key); ok {
		value := val.(Value)
		if !value.ExpiresAt.IsZero() && value.ExpiresAt.Before(time.Now()) {
			SETs.Delete(key)
			return RESPObject{Type: Null}
		}
		return RESPObject{Type: BulkString, Value: value.Data}
	}
	return RESPObject{Type: Null}
}

func hset(args []RESPObject) RESPObject {
	if len(args) != 3 {
		return RESPObject{Type: Error, Value: fmt.Sprintf(ErrWrongArgCount, "hset")}
	}

	hash, key, value := args[0].Value.(string), args[1].Value.(string), args[2].Value.(string)

	hm, _ := HSETs.LoadOrStore(hash, &sync.Map{})
	hm.(*sync.Map).Store(key, value)

	return RESPObject{Type: SimpleString, Value: "OK"}
}

func hget(args []RESPObject) RESPObject {
	if len(args) != 2 {
		return RESPObject{Type: Error, Value: fmt.Sprintf(ErrWrongArgCount, "hget")}
	}

	hash, key := args[0].Value.(string), args[1].Value.(string)

	if hm, ok := HSETs.Load(hash); ok {
		if value, ok := hm.(*sync.Map).Load(key); ok {
			return RESPObject{Type: BulkString, Value: value.(string)}
		}
	}
	return RESPObject{Type: Null}
}

func keys(args []RESPObject) RESPObject {
	if len(args) != 1 {
		return RESPObject{Type: Error, Value: fmt.Sprintf(ErrWrongArgCount, "keys")}
	}

	pattern := args[0].Value.(string)
	var values []RESPObject

	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		SETs.Range(func(k, v interface{}) bool {
			if strings.HasPrefix(k.(string), prefix) {
				values = append(values, RESPObject{Type: BulkString, Value: k.(string)})
			}
			return true
		})
		HSETs.Range(func(k, v interface{}) bool {
			if strings.HasPrefix(k.(string), prefix) {
				values = append(values, RESPObject{Type: BulkString, Value: k.(string)})
			}
			return true
		})
	} else {
		if _, ok := SETs.Load(pattern); ok {
			values = append(values, RESPObject{Type: BulkString, Value: pattern})
		} else if _, ok := HSETs.Load(pattern); ok {
			values = append(values, RESPObject{Type: BulkString, Value: pattern})
		}
	}

	return RESPObject{Type: Array, Value: values}
}
