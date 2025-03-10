#!/bin/bash

echo "Starting dialect detection service..."

# Set timeout for model loading (in seconds)
TIMEOUT=600
START_TIME=$(date +%s)

# Start the application with Gunicorn
echo "Starting Gunicorn server..."
gunicorn --bind 0.0.0.0:3003 --workers 1 --threads 4 dialect_server:app &
SERVER_PID=$!

# Function to check if the model is loaded
check_model_loaded() {
    response=$(curl -s http://localhost:3003/health)
    if [[ $response == *"model_loaded\":true"* ]]; then
        return 0
    else
        return 1
    fi
}

echo "Waiting for model to initialize..."
while true; do
    # Check if server is still running
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "Error: Server process died unexpectedly"
        exit 1
    fi
    
    # Check if model is loaded
    if check_model_loaded; then
        echo "Model successfully loaded!"
        break
    fi
    
    # Check timeout
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED -gt $TIMEOUT ]; then
        echo "Timeout waiting for model to initialize"
        break
    fi
    
    echo "Model still initializing... ($ELAPSED seconds elapsed)"
    sleep 5
done

# Keep the container running by waiting on the server process
wait $SERVER_PID
