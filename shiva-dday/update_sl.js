#!/usr/bin/env node
/**
 * Update all existing MT4 positions to $3 max stop loss
 * 0.01 lot × $1 price move = $1 max loss per trade
 */
const MetaApi = require('metaapi.cloud-sdk').default;

const TOKEN = process.env.METAAPI_TOKEN || '';
const ACCOUNT_ID = process.env.METAAPI_ACCOUNT_ID || '';
const SYMBOL = 'SpotCrude';
const MAX_LOSS_PER_TRADE = 3.00; // $3 max loss
const LOT_SIZE = 0.01;

console.log('🔧 Updating SL on all open positions...');
console.log(`📊 Account: ${ACCOUNT_ID}`);
console.log(`💰 Max loss per trade: $${MAX_LOSS_PER_TRADE}`);
console.log(`📦 Lot size: ${LOT_SIZE}`);

async function updateAllPositions() {
  if (!TOKEN || !ACCOUNT_ID) {
    console.error('❌ Missing METAAPI_TOKEN or METAAPI_ACCOUNT_ID');
    console.log('Run: METAAPI_TOKEN=xxx METAAPI_ACCOUNT_ID=yyy node update_sl.js');
    process.exit(1);
  }

  console.log('🔗 Connecting to MetaApi...');
  const api = new MetaApi(TOKEN, {
    provisioningUrl: 'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai',
    mtUrl: 'https://mt-client-api-v1.new-york.agiliumtrade.agiliumtrade.ai'
  });

  const account = await api.metatraderAccountApi.getAccount(ACCOUNT_ID);
  const connection = account.getRPCConnection();
  await connection.connect();
  await connection.waitSynchronized();
  const tradingAccount = connection;

  console.log('✅ Connected! Fetching positions...');
  const positions = await tradingAccount.getPositions();
  const myPositions = positions.filter(p => p.symbol === SYMBOL);

  console.log(`📋 Found ${myPositions.length} open position(s) for ${SYMBOL}`);

  // Each barrel price move × 0.01 lots = PnL
  // For $3 max loss: SL distance = $3 / (0.01 × 100) = $3.00
  const SL_PRICE_DISTANCE = MAX_LOSS_PER_TRADE / (LOT_SIZE * 100);

  for (const pos of myPositions) {
    const entryPrice = pos.openPrice;
    const isBuy = pos.type === 'POSITION_TYPE_BUY';
    const currentSL = pos.stopLoss;

    // Calculate new SL
    let newSL;
    if (isBuy) {
      newSL = entryPrice - SL_PRICE_DISTANCE;
    } else {
      newSL = entryPrice + SL_PRICE_DISTANCE;
    }

    // Don't widen SL - only tighten
    if (isBuy && currentSL && newSL < currentSL) {
      console.log(`⏭️ Skipping ${pos.id.slice(0,12)} - new SL ($${newSL.toFixed(2)}) worse than current ($${currentSL.toFixed(2)})`);
      continue;
    }
    if (!isBuy && currentSL && newSL > currentSL) {
      console.log(`⏭️ Skipping ${pos.id.slice(0,12)} - new SL ($${newSL.toFixed(2)}) worse than current ($${currentSL.toFixed(2)})`);
      continue;
    }

    const potentialLoss = LOT_SIZE * 100 * Math.abs(entryPrice - newSL);
    console.log(`📝 ${pos.type.replace('POSITION_TYPE_', '')} | Entry: $${entryPrice.toFixed(2)} | Current SL: $${currentSL?.toFixed(2) || 'none'} → New SL: $${newSL.toFixed(2)} (max loss: $${potentialLoss.toFixed(2)})`);

    try {
      await tradingAccount.modifyPosition(pos.id, { stopLoss: newSL });
      console.log(`✅ Updated ${pos.id.slice(0,12)} SL to $${newSL.toFixed(2)}`);
    } catch (e) {
      console.log(`❌ Failed to update ${pos.id.slice(0,12)}: ${e.message}`);
    }
  }

  console.log('\n✅ All positions updated!');
  process.exit(0);
}

updateAllPositions().catch(e => {
  console.error(`❌ Error: ${e.message}`);
  process.exit(1);
});
