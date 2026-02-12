#!/bin/bash
# Start script for backend - loads environment variables from root .env

# Change to script directory
cd "$(dirname "$0")"

# Load environment variables
if [ -f "../.env" ]; then
    export $(cat ../.env | grep -v '^#' | xargs)
    echo "✓ Loaded environment variables from .env"
else
    echo "⚠️  Warning: .env file not found in parent directory"
fi

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
source .venv/bin/activate

# Install dependencies if needed
if [ ! -f ".venv/.installed" ]; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
    touch .venv/.installed
fi

# Start the server
echo "🚀 Starting backend on ${BACKEND_HOST:-0.0.0.0}:${BACKEND_PORT:-8010}"
python main.py
