#!/bin/bash
# Railway startup — runs both the trading bot AND Discord sync
echo "🔱 SHIVA Railway startup..."

# Install Python (Railway Node.js image doesn't include it)
echo "📦 Installing Python..."
apt-get update -qq 2>&1 | tail -1 && apt-get install -y -qq python3 python3-pip 2>&1 | tail -1
echo "✅ Python installed"

# Install Python deps
echo "📦 Installing Python packages..."
pip3 install --break-system-packages requests 2>&1 | tail -3
echo "✅ Python packages installed"

# Make sure logs dir exists
mkdir -p logs

# Export env vars for the sync script
export VERCEL_URL="${VERCEL_URL:-https://shiva-admin-dun.vercel.app}"
export DISCORD_TRADES="${DISCORD_TRADES}"
export DISCORD_ALERTS="${DISCORD_ALERTS}"
export DISCORD_STATUS="${DISCORD_STATUS}"
export SHIVA_LOG_FILE="${SHIVA_LOG_FILE:-logs/shiva_live.log}"

# Start trading bot in background — write to shiva_live.log so sync can find it
echo "🤖 Starting trading bot..."
node shiva_live_bot.js >> logs/shiva_live.log 2>&1 &
BOT_PID=$!
echo "   Bot PID: $BOT_PID"

# Wait for bot to connect to MT4 and create log file
echo "⏳ Waiting for bot to connect..."
for i in $(seq 1 60); do
    if [ -f "logs/shiva_live.log" ] && [ -s "logs/shiva_live.log" ]; then
        echo "   Bot log found after ${i}s ($(wc -l < logs/shiva_live.log) lines)"
        break
    fi
    sleep 2
done

# Start Discord sync — output goes to log file + Railway logs via tail
mkdir -p logs
PYTHONUNBUFFERED=1 python3 -u shiva_sync_all.py 2>&1 | while IFS= read -r line; do
    echo "[SYNC] $line"
done &
echo "   Sync PID: $SYNC_PID"

echo "🔱 All services started"
echo "   Bot: $BOT_PID | Sync: $SYNC_PID"

# Tail bot log to stdout so Railway logs show bot output
tail -f logs/shiva_live.log 2>/dev/null &
TAIL_PID=$!

# Keep alive — monitor both processes
while true; do
    if ! kill -0 $BOT_PID 2>/dev/null; then
        echo "⚠️ Bot died at $(date), restarting..."
        node shiva_live_bot.js >> logs/shiva_live.log 2>&1 &
        BOT_PID=$!
        sleep 10
    fi
    if ! kill -0 $SYNC_PID 2>/dev/null; then
        echo "⚠️ Sync died at $(date), restarting..."
        python3 shiva_sync_all.py 2>&1 &
        SYNC_PID=$!
    fi
    if ! kill -0 $TAIL_PID 2>/dev/null; then
        tail -f logs/shiva_live.log 2>/dev/null &
        TAIL_PID=$!
    fi
    sleep 10
done
