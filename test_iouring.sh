#!/bin/bash

# Simple test to check if io_uring server responds
echo "Testing io_uring server..."

# Start warehouse server in background
./run-warehouse.sh &
SERVER_PID=$!

# Wait for server to start
sleep 5

# Send a simple POST request
echo "Sending test request..."
curl -X POST http://localhost:30003/test -d "test data" -v

# Kill server
kill $SERVER_PID

echo "Test complete" 