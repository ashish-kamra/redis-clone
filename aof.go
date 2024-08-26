package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Aof struct {
	file        *os.File
	rd          *bufio.Reader
	mu          sync.RWMutex
	shouldFsync bool
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewAof(path string, shouldFsync bool) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open AOF file: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	//TODO: Add file size limit and Log compaction once file size reahces the limit.
	aof := &Aof{
		file:        f,
		rd:          bufio.NewReader(f),
		shouldFsync: shouldFsync,
		ctx:         ctx,
		cancel:      cancel,
	}

	if !shouldFsync {
		go aof.periodicSync()
	}

	return aof, nil
}

func (aof *Aof) periodicSync() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-aof.ctx.Done():
			return
		case <-ticker.C:
			if err := aof.sync(); err != nil {
				fmt.Printf("Error during periodic sync: %v\n", err)
			}
		}
	}
}

func (aof *Aof) sync() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Sync()
}

func (aof *Aof) Close() error {
	aof.cancel()
	aof.mu.Lock()
	defer aof.mu.Unlock()
	if err := aof.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file before closing: %w", err)
	}
	return aof.file.Close()
}

func (aof *Aof) Write(obj RESPObject) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	data := []byte(obj.Serialize())
	_, err := aof.file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write to AOF: %w", err)
	}

	if aof.shouldFsync {
		if err := aof.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync AOF: %w", err)
		}
	}

	return nil
}

func (aof *Aof) Read(fn func(obj RESPObject)) error {
	aof.mu.RLock()
	defer aof.mu.RUnlock()

	if _, err := aof.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start of file: %w", err)
	}

	reader := NewReader(aof.file)
	for {
		value, err := reader.Deserialize()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to deserialize AOF entry: %w", err)
		}
		fn(value)
	}

	return nil
}
