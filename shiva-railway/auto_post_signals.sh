#!/bin/bash
# SHIVA Godmode Overload - Auto Signal Poster
# Runs continuously and posts signals to Discord every 5 minutes

set -e

# Create logs directory
mkdir -p logs

LOG_FILE="logs/auto_post.log"

echo "🔱 Starting SHIVA Auto Signal Poster..." | tee -a "$LOG_FILE"

# Load environment
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Install dependencies
pip install requests 2>/dev/null

echo "🚀 Auto-posting signals to Discord every 5 minutes..." | tee -a "$LOG_FILE"
echo "📊 Source: Vercel API (shiva-godmode-overlord-dday.vercel.app)" | tee -a "$LOG_FILE"
echo "💬 Target: Discord Webhook" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Initial post immediately
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$LOG_FILE"
echo "🕐 $(date '+%Y-%m-%d %H:%M:%S') - Posting initial signal..." | tee -a "$LOG_FILE"
python3 post_signal_live.py 2>&1 | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Loop forever
while true; do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$LOG_FILE"
    echo "🕐 $(date '+%Y-%m-%d %H:%M:%S') - Posting signal..." | tee -a "$LOG_FILE"
    
    python3 post_signal_live.py 2>&1 | tee -a "$LOG_FILE"
    
    echo "" | tee -a "$LOG_FILE"
    echo "⏳ Next post in 5 minutes..." | tee -a "$LOG_FILE"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    
    sleep 300  # 5 minutes
done
