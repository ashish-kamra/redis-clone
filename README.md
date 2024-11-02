# Redis Clone

A lightweight Redis-compatible server implementation in Go, supporting core Redis functionality and persistence.

## Features

- RESP (Redis Serialization Protocol) implementation for Redis compatibility 
- Basic Redis commands support:
    - `PING` - Test server connectivity
    - `ECHO` - Echo back the input
    - `SET` - Set key-value pairs with optional expiration
    - `GET` - Retrieve values by key
    - `HSET` - Set hash map entries
    - `HGET` - Retrieve hash map values
    - `KEYS` - Pattern-based key search
- Persistence through Append-Only File (AOF) and automatic AOF recovery on server restart
- Supports Key expiration
- Supports concurrent connections while ensuring thread-safe operations

## Getting Started

### Prerequisites

- Go 1.16 or higher

### Installation
```bash
git clone https://github.com/ashish-kamra/redis-clone.git

cd redis-clone

go build ./cmd/server
```
### Running the Server
```bash
./server -port 6379
```
The server will start listening on the specified port (default: 6379).
### Connecting to the Server
You can connect to the server using any Redis client. For example, using `redis-cli`:
```bash
redis-cli -p 6379
```
