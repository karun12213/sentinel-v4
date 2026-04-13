#!/usr/bin/env node
/**
 * Emergency: Close excess positions on Pepperstone Demo
 */
const MetaApi = require('metaapi.cloud-sdk').default;

const TOKEN = process.env.METAAPI_TOKEN || '';
const ACCOUNT_ID = process.env.METAAPI_ACCOUNT_ID || 'c8320edc-c0a2-475c-849c-d83099992491';

(async () => {
  console.log('🚨 EMERGENCY: Closing excess positions...');
  
  const api = new MetaApi(TOKEN, {
    provisioningUrl: 'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai',
    mtUrl: 'https://mt-client-api-v1.new-york.agiliumtrade.agiliumtrade.ai'
  });

  const account = await api.metatraderAccountApi.getAccount(ACCOUNT_ID);
  const connection = account.getRPCConnection();
  await connection.connect();
  await connection.waitSynchronized();

  const positions = await connection.getPositions();
  const spotCrudePos = positions.filter(p => p.symbol === 'SpotCrude');
  
  console.log(`Total positions: ${positions.length}`);
  console.log(`SpotCrude positions: ${spotCrudePos.length}`);
  
  // Keep only 6, close the rest
  const keep = spotCrudePos.slice(0, 6);
  const close = spotCrudePos.slice(6);
  
  console.log(`\nKeeping ${keep.length} positions`);
  console.log(`Closing ${close.length} positions\n`);
  
  for (const pos of close) {
    try {
      const info = await connection.getPositions();
      const current = info.find(p => p.id === pos.id);
      if (!current) { console.log(`⏭️ Already closed: ${pos.id.slice(0,8)}`); continue; }
      
      const pnl = current.profit || 0;
      await connection.closePosition(pos.id);
      console.log(`✅ Closed: ${pos.id.slice(0,8)} | ${current.type} | PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}`);
      await new Promise(r => setTimeout(r, 1000));
    } catch(e) {
      console.log(`❌ Failed: ${pos.id.slice(0,8)} | ${e.message.split('\n')[0]}`);
    }
  }
  
  // Check remaining
  const remaining = await connection.getPositions();
  console.log(`\nRemaining positions: ${remaining.filter(p => p.symbol === 'SpotCrude').length}`);
  
  const acctInfo = await connection.getAccountInformation();
  console.log(`Equity: $${acctInfo.equity.toFixed(2)}`);
  
  process.exit(0);
})();
