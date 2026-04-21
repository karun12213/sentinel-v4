#!/bin/bash
cd /Users/karunaditya/shiva-dday

# Kill old processes
pkill -f push_octafx_live 2>/dev/null
pkill -f shiva_vercel_sync 2>/dev/null
sleep 2

# Read token
export METAAPI_TOKEN=$(grep '^METAAPI_TOKEN=' ~/.shiva_env | head -1 | sed 's/^METAAPI_TOKEN=//')
export VERCEL_URL=https://shiva-godmode-overlord-dday.vercel.app

# Start OctaFX live push
echo "🚀 Starting OctaFX Live → Vercel..."
nohup node push_octafx_live.js > logs/octafx_live.log 2>&1 &
echo "PID: $!"

# Wait and check
sleep 15
tail -10 logs/octafx_live.log
