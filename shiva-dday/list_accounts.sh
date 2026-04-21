#!/bin/bash
cd /Users/karunaditya/shiva-dday
export METAAPI_TOKEN=$(grep '^METAAPI_TOKEN=' ~/.shiva_env | head -1 | sed 's/^METAAPI_TOKEN=//')

node -e "
const MetaApi = require('metaapi.cloud-sdk').default;
const api = new MetaApi(process.env.METAAPI_TOKEN, {
  provisioningUrl: 'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai'
});

(async () => {
  try {
    // Try different method names
    const methods = ['getAccounts', 'getMetatraderAccounts', 'metatraderAccounts'];
    const acctApi = api.metatraderAccountApi || api.metatraderApi;
    
    console.log('Available methods:', Object.keys(acctApi).filter(k => typeof acctApi[k] === 'function'));
    
    // Use the correct method
    const accounts = await acctApi.getMetatraderAccounts({});
    accounts.forEach(a => {
      console.log('\\nID:', a.id);
      console.log('  Name:', a.name);
      console.log('  Platform:', a.platform);
      console.log('  Status:', a.state);
    });
  } catch(e) {
    console.error('❌', e.message);
  }
  process.exit(0);
})();
" 2>&1 | grep -v '^\[' | grep -v '^Connecting\|^clientStickySocket\|^london:'
