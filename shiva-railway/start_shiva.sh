#!/bin/bash
# SHIVA Live Trading Bot - JS Version
# Auto-restarts if it crashes

while true; do
    # Load environment
    source /Users/karunaditya/.shiva_env
    export METAAPI_TOKEN
    export METAAPI_ACCOUNT_ID
    
    echo "🔱 Starting SHIVA Bot at $(date)"
    echo "📌 Account: $METAAPI_ACCOUNT_ID"
    echo ""
    
    # Clear old log
    > /Users/karunaditya/logs/shiva_live.log
    
    # Start bot
    node /Users/karunaditya/shiva_live_bot.js 2>&1 | tee -a /Users/karunaditya/logs/shiva_live.log
    
    echo ""
    echo "⚠️  Bot crashed or stopped. Restarting in 5 seconds..."
    sleep 5
done
