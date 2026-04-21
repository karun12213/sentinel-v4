#!/usr/bin/env node
/**
 * Pushes live OctaFX positions to Vercel dashboard
 */
const MetaApi = require('metaapi.cloud-sdk').default;
const https = require('https');
const http = require('http');

const TOKEN = process.env.METAAPI_TOKEN || '';
const ACCOUNT_ID = 'c8320edc-c0a2-475c-849c-d83099992491';
const VERCEL_URL = process.env.VERCEL_URL || 'https://shiva-godmode-overlord-dday.vercel.app';
const POLL_INTERVAL = 15000; // 15 seconds

console.log('📡 OctaFX Live → Vercel Dashboard');
console.log(`🔗 Account: ${ACCOUNT_ID}`);
console.log(`🌐 Vercel: ${VERCEL_URL}`);

async function pushToVercel(payload) {
  return new Promise((resolve, reject) => {
    const url = new URL(VERCEL_URL + '/api/push-logs');
    const lib = url.protocol === 'https:' ? https : http;
    const data = JSON.stringify(payload);
    const options = {
      hostname: url.hostname,
      path: url.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
      },
    };
    const req = lib.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(body)); }
        catch (e) { resolve({ success: res.statusCode === 200 }); }
      });
    });
    req.on('error', reject);
    req.setTimeout(10000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(data);
    req.end();
  });
}

async function syncLoop() {
  console.log('🔗 Connecting to MetaApi...');
  const api = new MetaApi(TOKEN, {
    provisioningUrl: 'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai',
    mtUrl: 'https://mt-client-api-v1.new-york.agiliumtrade.agiliumtrade.ai'
  });

  const account = await api.metatraderAccountApi.getAccount(ACCOUNT_ID);
  const connection = account.getRPCConnection();
  await connection.connect();
  await connection.waitSynchronized();
  console.log('✅ Connected to OctaFX Live!\n');

  while (true) {
    try {
      const info = await connection.getAccountInformation();
      const equity = info.equity || 0;
      const balance = info.balance || 0;
      const pnl = equity - balance;

      const positions = await connection.getPositions();
      const posData = positions.map(p => ({
        id: p.id,
        type: p.type,
        symbol: p.symbol,
        openPrice: p.openPrice,
        currentPrice: p.currentPrice || p.openPrice,
        volume: p.volume,
        profit: p.profit || 0,
        stopLoss: p.stopLoss,
        time: p.time,
        comment: p.comment || '',
      }));

      const logEntry = {
        timestamp: new Date().toISOString(),
        type: 'info',
        icon: '📊',
        message: `Equity: $${equity.toFixed(2)} | Balance: $${balance.toFixed(2)} | PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)} | Positions: ${positions.length}`
      };

      const payload = {
        logs: [logEntry],
        account_info: { equity, balance, pnl },
        live_positions: posData,
      };

      const result = await pushToVercel(payload);
      if (result.success) {
        console.log(`✅ Pushed: Equity=$${equity.toFixed(2)} | Positions=${positions.length} | PnL=${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}`);
      }

    } catch (e) {
      console.error(`❌ Error: ${e.message}`);
    }

    await new Promise(r => setTimeout(r, POLL_INTERVAL));
  }
}

syncLoop().catch(e => {
  console.error(`❌ Fatal: ${e.message}`);
  process.exit(1);
});
