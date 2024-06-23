package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{
		reader: bufio.NewReader(rd),
	}
}

type Writer struct {
	writer *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bufio.NewWriter(w)}
}

func (r *Resp) readLine() (line []byte, length int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		length += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], length, nil
}

func (r *Resp) readInteger() (intVal int, err error) {
	line, _, err := r.readLine()
	if err != nil {
		return 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, err
	}
	return int(i64), err
}

/*
From: Redis serialization protocol specification:
"Clients send commands to a Redis server as an array of bulk strings.
The first (and sometimes also the second) bulk string in the array is the command's name.
Subsequent elements of the array are the arguments for the command."
*/
func (r *Resp) Read() (Value, error) {
	/*
		From: Redis serialization protocol specification
		The first byte in an RESP-serialized payload always identifies its type.
		Subsequent bytes constitute the type's contents.
	*/
	_type, err := r.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type %v", string(_type))
		return Value{}, nil
	}
}

func (r *Resp) readArray() (Value, error) {
	//Format: *<number-of-elements>\r\n<element-1>...<element-n>
	v := Value{
		typ: "array",
	}

	len, err := r.readInteger()
	if err != nil {
		return v, err
	}

	v.array = make([]Value, 0)
	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		v.array = append(v.array, val)
	}

	return v, nil
}

func (r *Resp) readBulk() (Value, error) {
	//Format: $<length>\r\n<data>\r\n
	v := Value{
		typ: "bulk",
	}

	len, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)
	r.reader.Read(bulk)
	v.bulk = string(bulk)
	r.readLine()

	return v, nil
}

func (v Value) Marshal() []byte {
	switch v.typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshalNull()
	case "error":
		return v.marshalError()
	default:
		return []byte{}
	}
}
func (v Value) marshalString() []byte {
	//Format: +<data>\r\n
	var bytes []byte

	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalBulk() []byte {
	//Format: $<length>\r\n<data>\r\n
	var bytes []byte

	bytes = append(bytes, BULK)
	bytes = append(bytes, []byte(strconv.Itoa(len(v.bulk)))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalArray() []byte {
	//Format: *<number-of-elements>\r\n<element-1>...<element-n>
	var bytes []byte
	len := len(v.array)

	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

func (v Value) marshalError() []byte {
	//Format: -ErrorMessage\r\n
	var bytes []byte

	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}
