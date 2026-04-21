#!/bin/bash
cd /Users/karunaditya/shiva-dday

# Read env vars from .shiva_env
export METAAPI_TOKEN=$(grep '^METAAPI_TOKEN=' ~/.shiva_env | head -1 | sed 's/^METAAPI_TOKEN=//')
export METAAPI_ACCOUNT_ID=$(grep '^METAAPI_ACCOUNT_ID=' ~/.shiva_env | head -1 | sed 's/^METAAPI_ACCOUNT_ID=//')

echo "Token: ${METAAPI_TOKEN:0:20}..."
echo "Account: $METAAPI_ACCOUNT_ID"

node update_sl.js
