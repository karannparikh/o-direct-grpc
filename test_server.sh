#!/bin/bash

echo "Building the project..."
cargo build --release

echo -e "\nStarting the gRPC server in the background..."
./target/release/o_direct_grpc &
SERVER_PID=$!

# Wait a moment for the server to start
sleep 2

echo -e "\nRunning the test client..."
./target/release/o_direct_grpc client

echo -e "\nStopping the server..."
kill $SERVER_PID

echo -e "\nTest completed!" 