package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type RESPType int

const (
	SimpleString RESPType = iota
	Error
	Integer
	BulkString
	Array
	Null
)

const (
	SimpleStringPrefix = '+'
	ErrorPrefix        = '-'
	IntegerPrefix      = ':'
	BulkStringPrefix   = '$'
	ArrayPrefix        = '*'
	CRLF               = "\r\n"
)

type RESPObject struct {
	Type  RESPType
	Value interface{}
}

type Reader struct {
	reader *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{reader: bufio.NewReader(rd)}
}

type Writer struct {
	writer *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bufio.NewWriter(w)}
}

func (obj RESPObject) Serialize() string {
	var sb strings.Builder
	switch obj.Type {
	case SimpleString:
		fmt.Fprintf(&sb, "%c%v%s", SimpleStringPrefix, obj.Value, CRLF)
	case Error:
		fmt.Fprintf(&sb, "%c%v%s", ErrorPrefix, obj.Value, CRLF)
	case Integer:
		fmt.Fprintf(&sb, "%c%v%s", IntegerPrefix, obj.Value, CRLF)
	case BulkString:
		str, ok := obj.Value.(string)
		if !ok {
			return fmt.Sprintf("%c-1%s", BulkStringPrefix, CRLF) // Null bulk string
		}
		fmt.Fprintf(&sb, "%c%d%s%s%s", BulkStringPrefix, len(str), CRLF, str, CRLF)
	case Null:
		fmt.Fprintf(&sb, "%c-1%s", BulkStringPrefix, CRLF)
	case Array:
		arr, ok := obj.Value.([]RESPObject)
		if !ok {
			return fmt.Sprintf("%c-1%s", ArrayPrefix, CRLF) // Null array
		}
		fmt.Fprintf(&sb, "%c%d%s", ArrayPrefix, len(arr), CRLF)
		for _, item := range arr {
			sb.WriteString(item.Serialize())
		}
	}
	return sb.String()
}

func (r *Reader) Deserialize() (RESPObject, error) {
	typeByte, err := r.reader.ReadByte()
	if err != nil {
		return RESPObject{}, fmt.Errorf("failed to read type byte: %w", err)
	}

	line, err := r.reader.ReadString('\n')
	if err != nil {
		return RESPObject{}, fmt.Errorf("failed to read line: %w", err)
	}
	line = strings.TrimSpace(line)

	switch typeByte {
	case SimpleStringPrefix:
		return RESPObject{Type: SimpleString, Value: line}, nil
	case ErrorPrefix:
		return RESPObject{Type: Error, Value: line}, nil
	case IntegerPrefix:
		val, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return RESPObject{}, fmt.Errorf("failed to parse integer: %w", err)
		}
		return RESPObject{Type: Integer, Value: val}, nil
	case BulkStringPrefix:
		return r.deserializeBulkString(line)
	case ArrayPrefix:
		return r.deserializeArray(line)
	default:
		return RESPObject{}, fmt.Errorf("unknown RESP type: %c", typeByte)
	}
}

func (r *Reader) deserializeBulkString(line string) (RESPObject, error) {
	length, err := strconv.Atoi(line)
	if err != nil {
		return RESPObject{}, fmt.Errorf("failed to parse bulk string length: %w", err)
	}

	if length == -1 {
		return RESPObject{Type: BulkString, Value: nil}, nil
	}

	bulkStr := make([]byte, length)
	_, err = io.ReadFull(r.reader, bulkStr)
	if err != nil {
		return RESPObject{}, fmt.Errorf("failed to read bulk string: %w", err)
	}

	// Consume CRLF
	_, err = r.reader.ReadString('\n')
	if err != nil {
		return RESPObject{}, fmt.Errorf("failed to consume CRLF: %w", err)
	}

	return RESPObject{Type: BulkString, Value: string(bulkStr)}, nil
}

func (r *Reader) deserializeArray(line string) (RESPObject, error) {
	count, err := strconv.Atoi(line)
	if err != nil {
		return RESPObject{}, fmt.Errorf("failed to parse array length: %w", err)
	}

	if count == -1 {
		return RESPObject{Type: Array, Value: nil}, nil
	}

	array := make([]RESPObject, count)
	for i := 0; i < count; i++ {
		obj, err := r.Deserialize()
		if err != nil {
			return RESPObject{}, fmt.Errorf("failed to deserialize array element %d: %w", i, err)
		}
		array[i] = obj
	}

	return RESPObject{Type: Array, Value: array}, nil
}

func (w *Writer) Write(respObj RESPObject) error {
	_, err := w.writer.WriteString(respObj.Serialize())
	if err != nil {
		return fmt.Errorf("failed to write RESP object: %w", err)
	}
	return w.writer.Flush()
}
