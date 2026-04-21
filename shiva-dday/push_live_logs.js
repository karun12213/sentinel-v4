#!/usr/bin/env node
/**
 * Pushes live bot logs from Mac/Railway to Vercel dashboard
 * Reads shiva_live.log and pushes parsed entries to Vercel API
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

const VERCEL_URL = process.env.VERCEL_URL || 'https://shiva-godmode-overlord-dday.vercel.app';
const LOG_FILE = path.join(require('os').homedir(), 'logs', 'shiva_live.log');

console.log(`📡 Live Log Sync → ${VERCEL_URL}`);
console.log(`📂 Log File: ${LOG_FILE}`);

let lastPushedLines = 0;
let lastKnownBlock = '';

function parseLogToEntries(text) {
  const entries = [];
  const lines = text.split('\n');
  let currentTimestamp = null;
  
  for (const line of lines) {
    // Extract timestamp from log lines like "[2026-04-06T18:02:29.128Z] message"
    const tsMatch = line.match(/^\[(\d{4}-\d{2}-\d{2}T[\d:]+Z?)\]\s*(.*)/);
    if (tsMatch) {
      currentTimestamp = tsMatch[1];
      entries.push({ timestamp: currentTimestamp, message: tsMatch[2], type: 'info', icon: '📋' });
    } else if (line.includes('✅')) {
      entries.push({ timestamp: new Date().toISOString(), message: line.trim(), type: 'success', icon: '✅' });
    } else if (line.includes('❌') || line.includes('error') || line.includes('Error')) {
      entries.push({ timestamp: new Date().toISOString(), message: line.trim(), type: 'error', icon: '❌' });
    } else if (line.includes('📊') || line.includes('💹')) {
      entries.push({ timestamp: new Date().toISOString(), message: line.trim(), type: 'trade', icon: '📊' });
    } else if (line.includes('🔱') || line.includes('SHIVA')) {
      entries.push({ timestamp: new Date().toISOString(), message: line.trim(), type: 'info', icon: '🔱' });
    }
  }
  return entries;
}

function parseDashboardData(text) {
  // Parse equity, balance, pnl from dashboard block
  const equityMatch = text.match(/EQUITY:\s*\$([\d,]+\.?\d*)/);
  const balanceMatch = text.match(/Balance:\s*\$([\d,]+\.?\d*)/);
  const pnlMatch = text.match(/PnL:\s*([+-]?)\$([\d,]+\.?\d*)/);
  const priceMatch = text.match(/Price:\s*\$([\d,]+\.?\d*)/);
  
  return {
    equity: equityMatch ? parseFloat(equityMatch[1].replace(/,/g, '')) : null,
    balance: balanceMatch ? parseFloat(balanceMatch[1].replace(/,/g, '')) : null,
    pnl: pnlMatch ? (pnlMatch[1] === '-' ? -1 : 1) * parseFloat(pnlMatch[2].replace(/,/g, '')) : null,
    price: priceMatch ? parseFloat(priceMatch[1].replace(/,/g, '')) : null,
  };
}

function getLatestLogs() {
  try {
    const content = fs.readFileSync(LOG_FILE, 'utf-8');
    const blocks = content.split('🔱 SHIVA LIVE TRADING BOT');
    if (blocks.length < 2) return { entries: [], data: {}, newLines: 0 };
    
    const latestBlock = blocks.slice(-2).join('🔱 SHIVA LIVE TRADING BOT');
    if (latestBlock === lastKnownBlock) return { entries: [], data: {}, newLines: 0 };
    
    lastKnownBlock = latestBlock;
    
    const allLines = content.split('\n');
    const newLines = allLines.slice(lastPushedLines);
    lastPushedLines = allLines.length;
    
    const entries = parseLogToEntries(newLines.join('\n'));
    const data = parseDashboardData(latestBlock);
    
    return { entries, data, newLines: newLines.length };
  } catch (e) {
    console.error(`❌ Log read error: ${e.message}`);
    return { entries: [], data: {}, newLines: 0 };
  }
}

function pushToVercel(payload) {
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
        try {
          resolve(JSON.parse(body));
        } catch (e) {
          resolve({ success: res.statusCode === 200 });
        }
      });
    });
    
    req.on('error', reject);
    req.setTimeout(10000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(data);
    req.end();
  });
}

async function syncLoop() {
  while (true) {
    try {
      const { entries, data, newLines } = getLatestLogs();
      
      if (newLines > 0 && entries.length > 0) {
        const payload = { logs: entries.slice(-50) }; // Push last 50 entries
        
        if (data.equity) {
          payload.account_info = {
            equity: data.equity,
            balance: data.balance || data.equity,
            pnl: data.pnl || 0,
          };
        }
        
        const result = await pushToVercel(payload);
        if (result.success) {
          console.log(`✅ Pushed ${entries.length} entries (${newLines} new lines)`);
        } else {
          console.log(`⚠️ Push returned: ${JSON.stringify(result)}`);
        }
      }
    } catch (e) {
      console.error(`❌ Sync error: ${e.message}`);
    }
    
    await new Promise(r => setTimeout(r, 10000)); // Sync every 10 seconds
  }
}

syncLoop();
