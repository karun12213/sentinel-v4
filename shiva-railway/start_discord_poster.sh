#!/bin/bash
# Start SHIVA Godmode Overload - Discord Signal Poster
# Run this on Railway to post signals to Discord

set -e

echo "🔱 Starting SHIVA Discord Signal Poster..."

# Load environment
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Install dependencies if needed
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate
pip install -r requirements.txt 2>/dev/null

# Create logs directory
mkdir -p logs

# Start the signal poster
echo "🚀 Starting Discord signal poster..."
python post_signal_unified.py
