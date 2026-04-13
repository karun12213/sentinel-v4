#!/bin/bash
cd /Users/karunaditya/shiva-railway

# Read token
export METAAPI_TOKEN=$(grep '^METAAPI_TOKEN=' ~/.shiva_env | head -1 | sed 's/^METAAPI_TOKEN=//')
export METAAPI_ACCOUNT_ID=381743bc-f3c9-4b59-af77-37806fc89839

echo "🚀 Starting SHIVA on Railway (Pepperstone Demo)..."
node bot.js
