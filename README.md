# O_DIRECT gRPC File Server

A Rust gRPC server that provides file storage with O_DIRECT mode for high-performance I/O operations. The server maintains a dictionary mapping request IDs to file offsets and supports asynchronous read/write operations.

## Features

- **gRPC Server**: Provides WriteData and ReadData RPC endpoints
- **O_DIRECT Mode**: Bypasses kernel cache for direct disk I/O
- **Asynchronous I/O**: All file operations are performed asynchronously
- **Request Tracking**: Maintains a dictionary mapping request IDs to file offsets
- **Append-Only Writes**: All writes are appended to the end of the file
- **Aligned I/O**: Automatically aligns data to 512-byte blocks for O_DIRECT compatibility

## Architecture

### Components

1. **FileManager**: Handles O_DIRECT file operations with proper alignment
2. **FileServiceImpl**: gRPC service implementation
3. **Request Tracking**: HashMap-based tracking of request IDs to file offsets
4. **Async I/O**: All file operations use tokio's spawn_blocking for true async I/O

### Data Flow

1. **Write Request**: 
   - Client sends data with request ID
   - Server appends data to file end
   - Server stores request ID → offset mapping
   - Returns offset to client

2. **Read Request**:
   - Client sends request ID
   - Server looks up offset from mapping
   - Server reads data from file at offset
   - Returns data to client

## Building

```bash
# Build the project
cargo build --release

# Build with debug information
cargo build
```

## Running

### Server Mode

```bash
# Run the gRPC server
cargo run

# Or run the release binary
./target/release/o_direct_grpc
```

The server will start on `[::1]:50051` and create a `data.bin` file for storage.

### Client Mode (Testing)

```bash
# Run the test client
cargo run -- client
```

This will test the server by:
1. Writing three test messages with different request IDs
2. Reading back the messages using the request IDs

## API

### WriteData RPC

**Request:**
```protobuf
message WriteRequest {
    string request_id = 1;
    bytes data = 2;
}
```

**Response:**
```protobuf
message WriteResponse {
    string request_id = 1;
    uint64 offset = 2;
    bool success = 3;
    string error_message = 4;
}
```

### ReadData RPC

**Request:**
```protobuf
message ReadRequest {
    string request_id = 1;
}
```

**Response:**
```protobuf
message ReadResponse {
    string request_id = 1;
    bytes data = 2;
    bool success = 3;
    string error_message = 4;
}
```

## Technical Details

### O_DIRECT Mode

The server uses O_DIRECT mode which:
- Bypasses the kernel page cache
- Provides direct disk I/O
- Requires proper alignment (512-byte blocks)
- Ensures data consistency

### Data Alignment

All data is automatically aligned to 512-byte blocks:
- Write operations pad data to 512-byte boundaries
- Read operations read full 512-byte blocks
- Only the original data size is returned to clients

### Concurrency

- File operations are protected by Mutex for thread safety
- I/O operations use `spawn_blocking` for true async execution
- Request tracking uses Arc<Mutex<HashMap>> for shared state

## Dependencies

- `tonic`: gRPC framework
- `tokio`: Async runtime
- `libc`: For O_DIRECT flags
- `nix`: Unix system calls
- `uuid`: Request ID generation
- `tracing`: Logging
- `anyhow`: Error handling

## Performance Considerations

1. **O_DIRECT Benefits**:
   - Reduced memory usage (no kernel cache)
   - Predictable I/O performance
   - Direct disk access

2. **Alignment Overhead**:
   - Data is padded to 512-byte boundaries
   - Some storage space is wasted on alignment

3. **Async I/O**:
   - Non-blocking operations
   - Better resource utilization

## Error Handling

The server provides comprehensive error handling:
- File I/O errors are captured and returned
- Missing request IDs return appropriate errors
- All errors are logged with tracing

## Logging

The server uses structured logging with tracing:
- Request/response logging
- Error logging with context
- Performance metrics

## Testing

Run the test client to verify functionality:

```bash
# Terminal 1: Start server
cargo run

# Terminal 2: Run client
cargo run -- client
```

Expected output from client:
```
Running as client...
Testing write operations...
Write successful for test-1: offset = 0
Write successful for test-2: offset = 512
Write successful for test-3: offset = 1024

Testing read operations...
Read successful for test-1: 'Hello, World!'
Read successful for test-2: 'This is a test message'
Read successful for test-3: 'Another test message'
``` 