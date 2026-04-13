#!/bin/bash
cd /Users/karunaditya/shiva-railway

# Read token
export METAAPI_TOKEN=$(grep '^METAAPI_TOKEN=' ~/.shiva_env | head -1 | sed 's/^METAAPI_TOKEN=//')
export METAAPI_ACCOUNT_ID=c8320edc-c0a2-475c-849c-d83099992491

echo "🚀 Starting SHIVA on Railway (OctaFX Live)..."
node bot.js
