package handler

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ashish-kamra/redis-clone/internal/protocol"
)

const (
	ErrWrongArgCount = "ERR wrong number of arguments for '%s' command"
	ErrInvalidInt    = "ERR value is not an integer or out of range"
)

var Handlers = map[string]func([]protocol.RESPObject) protocol.RESPObject{
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

func command(args []protocol.RESPObject) protocol.RESPObject {
	if len(args) != 1 {
		return protocol.RESPObject{Type: protocol.Error, Value: fmt.Sprintf(ErrWrongArgCount, "command")}
	}
	return protocol.RESPObject{Type: protocol.SimpleString, Value: args[0].Value}
}

func echo(args []protocol.RESPObject) protocol.RESPObject {
	if len(args) != 1 {
		return protocol.RESPObject{Type: protocol.Error, Value: fmt.Sprintf(ErrWrongArgCount, "echo")}
	}
	return protocol.RESPObject{Type: protocol.SimpleString, Value: args[0].Value}
}

func ping(args []protocol.RESPObject) protocol.RESPObject {
	switch len(args) {
	case 0:
		return protocol.RESPObject{Type: protocol.SimpleString, Value: "PONG"}
	case 1:
		return protocol.RESPObject{Type: protocol.SimpleString, Value: args[0].Value}
	default:
		return protocol.RESPObject{Type: protocol.Error, Value: fmt.Sprintf(ErrWrongArgCount, "ping")}
	}
}

func set(args []protocol.RESPObject) protocol.RESPObject {
	if len(args) < 2 || len(args) > 4 {
		return protocol.RESPObject{Type: protocol.Error, Value: fmt.Sprintf(ErrWrongArgCount, "set")}
	}

	key, value := args[0].Value.(string), args[1].Value.(string)
	var expiresAt time.Time
	//TODO: Handle key expiration for AOF entries
	if len(args) == 4 {
		cmd := strings.ToUpper(args[2].Value.(string))
		duration, err := strconv.ParseInt(args[3].Value.(string), 10, 64)
		if err != nil {
			return protocol.RESPObject{Type: protocol.Error, Value: ErrInvalidInt}
		}

		switch cmd {
		case "PX":
			expiresAt = time.Now().Add(time.Duration(duration) * time.Millisecond)
		case "EX":
			expiresAt = time.Now().Add(time.Duration(duration) * time.Second)
		default:
			return protocol.RESPObject{Type: protocol.Error, Value: "ERR syntax error"}
		}
	}

	SETs.Store(key, Value{Data: value, ExpiresAt: expiresAt})
	return protocol.RESPObject{Type: protocol.SimpleString, Value: "OK"}
}

func get(args []protocol.RESPObject) protocol.RESPObject {
	if len(args) != 1 {
		return protocol.RESPObject{Type: protocol.Error, Value: fmt.Sprintf(ErrWrongArgCount, "get")}
	}

	key := args[0].Value.(string)
	if val, ok := SETs.Load(key); ok {
		value := val.(Value)
		if !value.ExpiresAt.IsZero() && value.ExpiresAt.Before(time.Now()) {
			SETs.Delete(key)
			return protocol.RESPObject{Type: protocol.Null}
		}
		return protocol.RESPObject{Type: protocol.BulkString, Value: value.Data}
	}
	return protocol.RESPObject{Type: protocol.Null}
}

func hset(args []protocol.RESPObject) protocol.RESPObject {
	if len(args) != 3 {
		return protocol.RESPObject{Type: protocol.Error, Value: fmt.Sprintf(ErrWrongArgCount, "hset")}
	}

	hash, key, value := args[0].Value.(string), args[1].Value.(string), args[2].Value.(string)

	hm, _ := HSETs.LoadOrStore(hash, &sync.Map{})
	hm.(*sync.Map).Store(key, value)

	return protocol.RESPObject{Type: protocol.SimpleString, Value: "OK"}
}

func hget(args []protocol.RESPObject) protocol.RESPObject {
	if len(args) != 2 {
		return protocol.RESPObject{Type: protocol.Error, Value: fmt.Sprintf(ErrWrongArgCount, "hget")}
	}

	hash, key := args[0].Value.(string), args[1].Value.(string)

	if hm, ok := HSETs.Load(hash); ok {
		if value, ok := hm.(*sync.Map).Load(key); ok {
			return protocol.RESPObject{Type: protocol.BulkString, Value: value.(string)}
		}
	}
	return protocol.RESPObject{Type: protocol.Null}
}

func keys(args []protocol.RESPObject) protocol.RESPObject {
	if len(args) != 1 {
		return protocol.RESPObject{Type: protocol.Error, Value: fmt.Sprintf(ErrWrongArgCount, "keys")}
	}

	pattern := args[0].Value.(string)
	var values []protocol.RESPObject

	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		SETs.Range(func(k, v interface{}) bool {
			if strings.HasPrefix(k.(string), prefix) {
				values = append(values, protocol.RESPObject{Type: protocol.BulkString, Value: k.(string)})
			}
			return true
		})
		HSETs.Range(func(k, v interface{}) bool {
			if strings.HasPrefix(k.(string), prefix) {
				values = append(values, protocol.RESPObject{Type: protocol.BulkString, Value: k.(string)})
			}
			return true
		})
	} else {
		if _, ok := SETs.Load(pattern); ok {
			values = append(values, protocol.RESPObject{Type: protocol.BulkString, Value: pattern})
		} else if _, ok := HSETs.Load(pattern); ok {
			values = append(values, protocol.RESPObject{Type: protocol.BulkString, Value: pattern})
		}
	}

	return protocol.RESPObject{Type: protocol.Array, Value: values}
}
