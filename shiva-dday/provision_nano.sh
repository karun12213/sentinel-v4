#!/bin/bash
# Auto-provision NanoDemo account after rate limit expires
# Run: bash provision_nano.sh

set -e

TOKEN=$(grep '^METAAPI_TOKEN=' /Users/karunaditya/shiva-dday/.env | sed 's/^METAAPI_TOKEN=//')
LOGIN="13327381"
SERVER="${1:-NanoDemo}"  # Pass server name as arg if known
PASSWORD='&$8YJze6*Hf'

echo "⏳ Waiting for MetaApi rate limit to clear (expires 06:34 UTC)..."
echo "Current time: $(date -u)"
echo ""

# Try provisioning
echo "🔄 Attempting to provision NanoDemo account..."
RESULT=$(curl -s -X POST "https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts" \
  -H "auth-token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"login\": \"$LOGIN\",
    \"password\": \"$PASSWORD\",
    \"name\": \"NanoDemo-$LOGIN\",
    \"server\": \"$SERVER\",
    \"type\": \"cloud-g2\",
    \"platform\": \"mt4\",
    \"magic\": 123456
  }" 2>/dev/null)

echo "Response: $RESULT"

ACCOUNT_ID=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id',''))" 2>/dev/null)

if [ -n "$ACCOUNT_ID" ] && [[ "$ACCOUNT_ID" =~ ^[a-f0-9-]{36}$ ]]; then
  echo ""
  echo "✅ Account provisioned! ID: $ACCOUNT_ID"
  echo ""
  echo "🔄 Updating .env with new account ID..."

  # Update .env
  sed -i '' "s/^METAAPI_ACCOUNT_ID=.*/METAAPI_ACCOUNT_ID=$ACCOUNT_ID/" /Users/karunaditya/shiva-dday/.env

  echo "✅ .env updated: METAAPI_ACCOUNT_ID=$ACCOUNT_ID"
  echo ""
  echo "🔄 Restarting bot..."
  pm2 restart shiva-bot
  echo "✅ Bot restarted with NanoDemo account"
  pm2 logs shiva-bot --lines 20 --nostream
else
  echo ""
  echo "❌ Provisioning failed. Check error above."
  echo "   If rate limited: retry after 06:34 UTC"
  echo "   If server not found: run:  bash provision_nano.sh <EXACT_SERVER_NAME>"
fi
