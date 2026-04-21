#!/bin/bash
cd /Users/karunaditya/shiva-dday

# Test OctaFX account connection
export METAAPI_TOKEN=$(grep '^METAAPI_TOKEN=' ~/.shiva_env | head -1 | sed 's/^METAAPI_TOKEN=//')
export METAAPI_ACCOUNT_ID="c8320edc-c0a2-475c-849c-d83099992491"

echo "🔗 Connecting to OctaFX Live account..."
node -e "
const MetaApi = require('metaapi.cloud-sdk').default;
const api = new MetaApi(process.env.METAAPI_TOKEN, {
  provisioningUrl: 'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai',
  mtUrl: 'https://mt-client-api-v1.london.agiliumtrade.agiliumtrade.ai'
});

(async () => {
  try {
    const account = await api.metatraderAccountApi.getAccount(process.env.METAAPI_ACCOUNT_ID);
    console.log('✅ Account:', account.stringId);
    console.log('📊 Name:', account.name);
    console.log('💰 Type:', account.type);
    const connection = account.getRPCConnection();
    await connection.connect();
    await connection.waitSynchronized();
    const info = await connection.getAccountInformation();
    console.log('💵 Balance: \$' + (info.balance || 0).toFixed(2));
    console.log('💰 Equity: \$' + (info.equity || 0).toFixed(2));
    const pos = await connection.getPositions();
    console.log('📋 Open Positions:', pos.length);
    process.exit(0);
  } catch(e) {
    console.error('❌ Error:', e.message);
    process.exit(1);
  }
})();
"
