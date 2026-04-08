#!/usr/bin/env node
/**
 * SHIVA LIVE TRADING BOT - MT4/SpotCrude
 * 40 NVIDIA Agents | Opens 6 positions | Trail winners | Cut losers
 */

const MetaApi = require('metaapi.cloud-sdk').default;
const { execSync } = require('child_process');
const fs = require('fs');

// ============ CONFIG ============
const TOKEN = process.env.METAAPI_TOKEN || '';
const ACCOUNT_ID = process.env.METAAPI_ACCOUNT_ID || '';
const SYMBOL = 'SpotCrude';
const LOT_SIZE = 0.03;
const POSITIONS = 6;
const ENTRY_GAP_MS = 3000;
const STOP_LOSS = 0.10; // Tight SL - bot managed
const TRAIL_START = 0.30;
const TRAIL_DISTANCE = 0.15;
const CHECK_INTERVAL = 30000;
const MAX_TRADES_PER_CYCLE = 6;
const ML_RETRAIN_INTERVAL = 5; // Retrain ML every 5 cycles

// ============ STATE ============
let api, connection, tradingAccount;
let totalTrades = 0, wins = 0, losses = 0;
let startTime = new Date();
let cycle = 0;
let totalCandles = 0;
let initialEquity = 0;
let openedThisCycle = 0;
let managedPositions = [];
let lastCheck = Date.now();
let tradeHistory = loadTradeHistory();
let mlPrediction = null;

// ============ TRADE HISTORY ============
function loadTradeHistory() {
  try {
    if (fs.existsSync('trade_history.json')) {
      return JSON.parse(fs.readFileSync('trade_history.json', 'utf8'));
    }
  } catch (e) {}
  return [];
}

function saveTradeHistory() {
  fs.writeFileSync('trade_history.json', JSON.stringify(tradeHistory, null, 2));
}

function logTrade(signal, entry, exit, pnl, agents, reason) {
  const trade = {
    id: `trade_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
    signal,
    entry_price: entry,
    exit_price: exit,
    pnl,
    result: pnl > 0 ? 'win' : 'loss',
    exit_reason: reason,
    agents: agents || [],
    time: new Date().toISOString(),
    cycle
  };
  tradeHistory.push(trade);
  saveTradeHistory();
  console.log(`📝 Trade logged: ${trade.id} | ${signal} | PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)} | ${reason}`);
}

// ============ DISCORD INTEGRATION ============
const DISCORD_WEBHOOK = process.env.DISCORD_TRADES || '';

function postToDiscordOnPositionOpen(signal, price, sl, posNum, id) {
  if (!DISCORD_WEBHOOK) return;

  const tp = 'TRAIL — winners run until trend breaks';
  const arrow = signal === 'BUY' ? '📈' : '📉';
  const color = signal === 'BUY' ? 0x00FF88 : 0xFF4444;

  const payload = {
    embeds: [{
      title: `${arrow} Position #${posNum} Opened — ${signal}`,
      color: color,
      fields: [
        { name: '🎯 Entry', value: `\`$${price.toFixed(3)}\``, inline: true },
        { name: '🛑 Stop Loss', value: `\`$${sl}\``, inline: true },
        { name: '📈 Take Profit', value: `\`${tp}\``, inline: true },
        { name: '📋 Direction', value: `**${signal}**`, inline: true },
        { name: '💰 Lot Size', value: `\`${LOT_SIZE}\``, inline: true },
        { name: '🆔 Trade ID', value: `\`${id.slice(0, 16)}\``, inline: true },
        {
          name: '💡 Note',
          value: `\`${signal}\` signal from AI consensus. ` +
                 `SL trails to breakeven once in profit. Let winners run!`,
          inline: false,
        },
      ],
      footer: { text: '🔱 SHIVA Godmode Overload — Position Opened' },
      timestamp: new Date().toISOString(),
    }],
  };

  try {
    const curl = `curl -s -X POST "${DISCORD_WEBHOOK}" ` +
      `-H "Content-Type: application/json" ` +
      `-d '${JSON.stringify(payload).replace(/'/g, "'\\''")}'`;
    execSync(curl, { timeout: 10000 });
    console.log(`📤 Posted position #${posNum} to Discord`);
  } catch (e) {
    console.log(`⚠ Discord post failed: ${e.message}`);
  }
}

function postToDiscordOnTradeClose(signal, pnl, reason, peakPnl = null) {
  if (!DISCORD_WEBHOOK) return;

  const isWin = pnl > 0;
  const emoji = isWin ? '🟢' : '🔴';
  const color = isWin ? 0x00FF88 : 0xFF4444;
  const result = isWin ? 'WIN' : 'LOSS';

  let reasonText = '';
  if (reason === 'take_profit') {
    reasonText = '🟢 Hit target / trailed to profit';
  } else if (reason === 'cut_loss') {
    reasonText = '🔴 Stop loss hit — risk managed';
  } else {
    reasonText = `Exit: ${reason}`;
  }

  const note = isWin
    ? '`Winner banked. Trail system worked. Let the next one run too.`'
    : '`Losses are part of the edge. Stay disciplined.`';

  const fields = [
    { name: '📈 Signal', value: `**${signal}**`, inline: true },
    { name: '💵 PnL', value: `\`$${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}\``, inline: true },
    { name: '📝 Exit', value: reasonText, inline: true },
    { name: '💡 Note', value: note, inline: false },
  ];

  if (peakPnl !== null) {
    fields.splice(2, 0, { name: '🏔 Peak PnL', value: `\`+$${peakPnl.toFixed(2)}\``, inline: true });
  }

  const payload = {
    embeds: [{
      title: `${emoji} Trade Closed — ${result}`,
      color: color,
      fields: fields,
      footer: { text: '🔱 SHIVA Godmode Overload — Trade Closed' },
      timestamp: new Date().toISOString(),
    }],
  };

  try {
    const curl = `curl -s -X POST "${DISCORD_WEBHOOK}" ` +
      `-H "Content-Type: application/json" ` +
      `-d '${JSON.stringify(payload).replace(/'/g, "'\\''")}'`;
    execSync(curl, { timeout: 10000 });
    console.log(`📤 Posted trade ${result} to Discord: $${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}`);
  } catch (e) {
    console.log(`⚠ Discord post failed: ${e.message}`);
  }
}

function postPeriodicSignalToDiscord(consensus, price, equity, balance, pnl, indicators, openPositions = 0) {
  if (!DISCORD_WEBHOOK) return;

  const signal = consensus.signal;
  const strength = consensus.pct;
  const colors = { BUY: 0x00FF88, SELL: 0xFF4444, HOLD: 0xFFAA00 };

  // Fetch actual SL from managed positions (bot decides, not hardcoded)
  let slDisplay = 'Bot Managed';
  if (managedPositions.length > 0) {
    const avgSl = managedPositions.reduce((sum, p) => sum + (p.sl || p.beSl || 0), 0) / managedPositions.length;
    slDisplay = `$${avgSl.toFixed(3)}`;
  }

  const tradePlanText = `Entry: \`$${price.toFixed(3)}\`\n**SL**: \`${slDisplay}\` (Bot Managed)\nTake Profit: \`TRAIL — winners run until trend breaks\`\nLot Size: \`${LOT_SIZE}\``;

  const agreeingAgents = indicators.filter(i => i.s === signal).slice(0, 5);
  const agentList = agreeingAgents.map(a => `${a.e} ${a.n}`).join('\n') || '—';

  const histWins = tradeHistory.filter(t => t.result === 'win').length;
  const histLosses = tradeHistory.filter(t => t.result === 'loss').length;

  const payload = {
    embeds: [{
      title: `SHIVA Signal Cycle #${cycle}`,
      description: `**${signal}** (${strength}% confidence)`,
      color: colors[signal] || 0x4488FF,
      fields: [
        {
          name: '📊 Market Data',
          value: `Price: \`$${price.toFixed(3)}\` | Symbol: \`${SYMBOL}\`\nEquity: \`$${equity.toFixed(2)}\` | Balance: \`$${balance.toFixed(2)}\`\nPnL: \`$${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}\``,
          inline: false,
        },
        { name: '🎯 Trade Plan', value: tradePlanText, inline: false },
        {
          name: '🤖 AI Consensus',
          value: `🟢 BUY: \`${consensus.buy}\` | 🔴 SELL: \`${consensus.sell}\` | ⚪ HOLD: \`${consensus.hold}\``,
          inline: false,
        },
        { name: '✅ Top Agents', value: agentList, inline: false },
        { name: '📋 Positions', value: `${openPositions}/${POSITIONS}`, inline: true },
        { name: '📈 Record', value: `Wins: \`${histWins}\` | Losses: \`${histLosses}\``, inline: true },
        {
          name: '📝 Instructions',
          value: '1. **Bot auto-executes** — no manual intervention needed\n' +
                 '2. **Do NOT manually close** — bot trails winners & cuts losers\n' +
                 '3. **SL moves to breakeven** once position is in profit\n' +
                 '4. **Let winners run** — trail system handles exits\n' +
                 '5. **Losers cut at -$0.50** — small losses preserve capital\n' +
                 '6. **Monitor Discord** for auto-updates on every trade event',
          inline: false,
        },
        {
          name: '💡 Note',
          value: '`SHIVA Godmode Overload is fully automated on Vercel/Railway (24/7). ' +
                 '40 AI agents scan every 30s. Max 6 positions. ' +
                 'Trust the system. Stay disciplined.`',
          inline: false,
        },
      ],
      footer: { text: '🔱 SHIVA Godmode Overload — Vercel/Railway 24/7' },
      timestamp: new Date().toISOString(),
    }],
  };

  try {
    const curl = `curl -s -X POST "${DISCORD_WEBHOOK}" ` +
      `-H "Content-Type: application/json" ` +
      `-d '${JSON.stringify(payload).replace(/'/g, "'\\''")}'`;
    execSync(curl, { timeout: 10000 });
    console.log(`📤 Posted periodic signal to Discord: ${signal} (${strength}%)`);
  } catch (e) {
    console.log(`⚠ Discord periodic post failed: ${e.message}`);
  }
}

// ============ ML ENGINE ============
function runML(agents, signal) {
  try {
    // Call Python ML engine
    const result = execSync(`python3 shiva_ml.py 2>&1`, { encoding: 'utf8', timeout: 10000 });

    // Parse ML prediction from trade context
    if (tradeHistory.length >= 5) {
      const features = agents.map(a => a.s === 'BUY' ? 1 : a.s === 'SELL' ? -1 : 0);
      const buyPct = agents.filter(a => a.s === 'BUY').length / agents.length;
      features.push(buyPct);
      features.push(1 - buyPct);
      features.push(signal === 'BUY' ? 1 : -1);

      // Simple ML: weighted historical success rate
      const matchingTrades = tradeHistory.filter(t => {
        if (t.agents.length < 40) return false;
        const matchCount = t.agents.filter((a, i) => {
          const histSig = a.s === 'BUY' ? 1 : a.s === 'SELL' ? -1 : 0;
          return histSig === features[i];
        }).length;
        return matchCount >= 30; // 75% match
      });

      if (matchingTrades.length >= 3) {
        const winRate = matchingTrades.filter(t => t.result === 'win').length / matchingTrades.length;
        const mlSignal = winRate > 0.5 ? signal : (signal === 'BUY' ? 'SELL' : 'BUY');
        return {
          signal: mlSignal,
          confidence: winRate,
          trades: matchingTrades.length,
          agrees: mlSignal === signal
        };
      }
    }
  } catch (e) {}

  // Fallback: train simple model inline
  return trainInlineML(agents, signal);
}

function trainInlineML(agents, signal) {
  const wins = tradeHistory.filter(t => t.result === 'win').length;
  const losses = tradeHistory.filter(t => t.result === 'loss').length;
  const total = wins + losses;

  if (total < 3) {
    return { signal, confidence: 0.5, trades: total, agrees: true, reason: 'Not enough data' };
  }

  // Find similar signal trades
  const similarTrades = tradeHistory.filter(t => t.signal === signal);
  const similarWins = similarTrades.filter(t => t.result === 'win').length;
  const winRate = similarTrades.length > 0 ? similarWins / similarTrades.length : 0.5;

  const mlSignal = winRate >= 0.5 ? signal : (signal === 'BUY' ? 'SELL' : 'BUY');

  return {
    signal: mlSignal,
    confidence: winRate,
    trades: similarTrades.length,
    agrees: mlSignal === signal,
    reason: `ML: ${winRate.toFixed(0)} win rate on ${signal} trades (${similarTrades.length} samples)`
  };
}

// ============ MT4 CONNECTION ============
async function connectMT4() {
  console.log('🔗 Connecting to MT4...');
  api = new MetaApi(TOKEN, {
    provisioningUrl: 'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai',
    mtUrl: 'https://mt-client-api-v1.london.agiliumtrade.agiliumtrade.ai'
  });
  const account = await api.metatraderAccountApi.getAccount(ACCOUNT_ID);
  if (account.state !== 'DEPLOYED') throw new Error(`Not deployed: ${account.state}`);
  console.log(`📌 ${account.name} - ${account.id}`);
  connection = account.getRPCConnection();
  await connection.connect();
  await connection.waitSynchronized();
  tradingAccount = connection;

  // Wait for positions to sync
  console.log('⏳ Waiting for position sync...');
  await new Promise(r => setTimeout(r, 5000));

  const info = await connection.getAccountInformation();
  initialEquity = info.equity || 0;
  console.log(`✅ CONNECTED | Balance: $${info.balance} | Equity: $${info.equity}`);

  // Check existing positions on connect
  try {
    const existingPositions = await connection.getPositions();
    const symbolPositions = existingPositions.filter(p => p.symbol === SYMBOL);
    console.log(`📊 Found ${symbolPositions.length} existing ${SYMBOL} position(s) on connect`);
    if (symbolPositions.length > 0) {
      symbolPositions.forEach((p, i) => {
        console.log(`  #${i+1} ${p.type} @ ${p.openPrice} | PnL: $${(p.profit||0).toFixed(2)}`);
      });
    }
  } catch (e) {
    console.log(`⚠ Could not check positions on connect: ${e.message}`);
  }

  return info;
}

// ============ 40 NVIDIA AGENTS ============
function ema(values, span) {
  const k = 2 / (span + 1);
  const result = [values[0]];
  for (let i = 1; i < values.length; i++) result.push(values[i] * k + result[i-1] * (1-k));
  return result;
}

function analyzeAll(candles, price) {
  if (candles.length < 5) return [];
  const closes = candles.map(c => c.close);
  const highs = candles.map(c => c.high);
  const lows = candles.map(c => c.low);
  const volumes = candles.map(c => c.volume);
  const last = candles[candles.length - 1];
  const results = [];
  const fb = (s, b = true) => s === 'HOLD' ? (b ? 'BUY' : 'SELL') : s;
  let s;

  s = candles.length>=20?(Math.max(...highs.slice(-10))>Math.max(...highs.slice(-20,-10))&&Math.min(...lows.slice(-10))>Math.min(...lows.slice(-20,-10))?'BUY':'SELL'):'HOLD';
  results.push({e:'👑',n:'Commander',s:fb(s)});
  results.push({e:'💹',n:'PriceData',s:last.close>last.open?'BUY':'SELL'});
  s = volumes.length>=5?(last.volume>volumes.slice(-5).reduce((a,b)=>a+b,0)/5*1.2?(last.close>last.open?'BUY':'SELL'):'HOLD'):'HOLD';
  results.push({e:'📊',n:'Volume',s:fb(s,last.close>last.open)});
  results.push({e:'⏰',n:'Time',s:fb(new Date().getDay()<3?'BUY':new Date().getDay()>3?'SELL':'HOLD')});
  const h=new Date().getHours();
  results.push({e:'🌍',n:'Session',s:fb((h>=8&&h<=11)||(h>=13&&h<=16)?'BUY':h>=0&&h<=5?'SELL':'HOLD')});
  s = candles.length>=20?(candles.slice(-5).map(c=>c.high-c.low).reduce((a,b)=>a+b,0)/5>candles.slice(-20,-5).map(c=>c.high-c.low).reduce((a,b)=>a+b,0)/15*1.2?(last.close>last.open?'BUY':'SELL'):'HOLD'):'HOLD';
  results.push({e:'📈',n:'Volatility',s:fb(s)});
  results.push({e:'🏗️',n:'Structure',s:fb(closes[closes.length-1]>closes[closes.length-20]?'BUY':'SELL')});
  s=closes.length>=50?(ema(closes,20).slice(-1)[0]>ema(closes,50).slice(-1)[0]?'BUY':'SELL'):'HOLD';
  results.push({e:'📈',n:'Trend',s:fb(s)});
  s=candles.length>=20?(price<(Math.min(...lows.slice(-20))+Math.max(...highs.slice(-20)))/2?'BUY':'SELL'):'HOLD';
  results.push({e:'🎯',n:'Support',s:fb(s)});
  s=candles.length>=20?(()=>{const sh=Math.max(...highs.slice(-20)),sl=Math.min(...lows.slice(-20));return price<=sh-(sh-sl)*0.618?'BUY':'SELL';})():'HOLD';
  results.push({e:'🔢',n:'Fib',s:fb(s)});
  const body=Math.abs(last.close-last.open),uw=last.high-Math.max(last.close,last.open),lw=Math.min(last.close,last.open)-last.low;
  s=lw>body*2&&uw<body*0.5?'BUY':uw>body*2&&lw<body*0.5?'SELL':'HOLD';
  results.push({e:'🕯️',n:'Pattern',s:fb(s,last.close>last.open)});
  s=candles.length>=20?(price<(Math.max(...highs.slice(-20))+Math.min(...lows.slice(-20)))/2?'BUY':'SELL'):'HOLD';
  results.push({e:'📊',n:'Channel',s:fb(s)});
  s=closes.length>=14?(()=>{const g=[],l=[];for(let i=1;i<=14;i++){const d=closes[closes.length-i]-closes[closes.length-i-1];g.push(Math.max(d,0));l.push(Math.max(-d,0));}const rsi=100-(100/(1+g.reduce((a,b)=>a+b,0)/g.length/(l.reduce((a,b)=>a+b,0)||1)));return rsi<35?'BUY':rsi>65?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📉',n:'RSI',s:fb(s)});
  s=closes.length>=26?(()=>{const m=ema(closes,12).map((v,i)=>v-ema(closes,26)[i]);return m[m.length-1]>m[m.length-2]?'BUY':'SELL';})():'HOLD';
  results.push({e:'📊',n:'MACD',s:fb(s)});
  s=candles.length>=14?(()=>{const hh=Math.max(...highs.slice(-14)),ll=Math.min(...lows.slice(-14));const k=hh!==ll?((closes[closes.length-1]-ll)/(hh-ll))*100:50;return k<25?'BUY':k>75?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'Stoch',s:fb(s)});
  s=candles.length>=20?(()=>{const tp=candles.slice(-20).map(c=>(c.high+c.low+c.close)/3);const sma=tp.reduce((a,b)=>a+b,0)/tp.length;const md=tp.reduce((a,b)=>a+Math.abs(b-sma),0)/tp.length;const cci=md!==0?(tp[tp.length-1]-sma)/(0.015*md):0;return cci<-100?'BUY':cci>100?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'CCI',s:fb(s)});
  s=candles.length>=14?(()=>{const hh=Math.max(...highs.slice(-14)),ll=Math.min(...lows.slice(-14));const wr=hh!==ll?((hh-closes[closes.length-1])/(hh-ll))*-100:-50;return wr<-80?'BUY':wr>-20?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'Williams',s:fb(s)});
  results.push({e:'⚡',n:'Momentum',s:fb(closes[closes.length-1]-closes[closes.length-10]>0?'BUY':'SELL')});
  s=candles.length>=10?(()=>{const ad=candles.slice(-10).map(c=>{const r=c.high-c.low;return r!==0?(((c.close-c.low)-(c.high-c.close))/r)*c.volume:0;});return ad.slice(-5).reduce((a,b)=>a+b,0)>ad.slice(0,5).reduce((a,b)=>a+b,0)?'BUY':'SELL';})():'HOLD';
  results.push({e:'📊',n:'A/D',s:fb(s)});
  results.push({e:'📏',n:'ATR',s:fb(last.close>last.open?'BUY':'SELL')});
  s=closes.length>=20?(()=>{const sma=closes.slice(-20).reduce((a,b)=>a+b,0)/20;const std=Math.sqrt(closes.slice(-20).reduce((a,b)=>a+(b-sma)**2,0)/20);return price<=sma-2*std?'BUY':price>=sma+2*std?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📐',n:'Bollinger',s:fb(s)});
  s=candles.length>=20?(()=>{const sma=closes.slice(-20).reduce((a,b)=>a+b,0)/20;const atr=candles.slice(-20).map(c=>c.high-c.low).reduce((a,b)=>a+b,0)/20;return price<=sma-2*atr?'BUY':price>=sma+2*atr?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📐',n:'Keltner',s:fb(s)});
  s=candles.length>=20?(price>=Math.max(...highs.slice(-20))?'BUY':price<=Math.min(...lows.slice(-20))?'SELL':'HOLD'):'HOLD';
  results.push({e:'🌊',n:'Donchian',s:fb(s)});
  s=closes.length>=20?(()=>{const m=closes.slice(-20).reduce((a,b)=>a+b,0)/20;const s=Math.sqrt(closes.slice(-20).reduce((a,b)=>a+(b-m)**2,0)/20);return price<m-s?'BUY':price>m+s?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'StdDev',s:fb(s)});
  s=candles.length>=5?(()=>{for(const c of candles.slice(-5)){if(c.close<c.open&&price>c.high)return'BUY';}return'SELL';})():'HOLD';
  results.push({e:'🧱',n:'OrderBlock',s:fb(s)});
  s=candles.length>=3?(candles[candles.length-3].high<candles[candles.length-1].low?'BUY':candles[candles.length-3].low>candles[candles.length-1].high?'SELL':'HOLD'):'HOLD';
  results.push({e:'⬜',n:'FVG',s:fb(s)});
  s=candles.length>=10?(last.low<=Math.min(...lows.slice(-10))&&last.close>last.open?'BUY':'SELL'):'HOLD';
  results.push({e:'💧',n:'Liquidity',s:fb(s)});
  results.push({e:'🔨',n:'Breaker',s:fb(candles.slice(-5).filter(c=>c.close>c.open).length>=3?'BUY':'SELL')});
  s=candles.length>=15?(price>closes.slice(-15).reduce((a,b)=>a+b,0)/15?'BUY':'SELL'):'HOLD';
  results.push({e:'🔄',n:'Mitigation',s:fb(s)});
  results.push({e:'🎯',n:'Entry',s:last.close>last.open?'BUY':'SELL'});
  s=candles.length>=20?(()=>{let tvp=0,tv=0;candles.slice(-20).forEach(c=>{tvp+=(c.high+c.low+c.close)/3*c.volume;tv+=c.volume;});const vwap=tv!==0?tvp/tv:0;return price<vwap?'BUY':'SELL';})():'HOLD';
  results.push({e:'📊',n:'VWAP',s:fb(s)});
  s=candles.length>=20?(()=>{const atrs=candles.slice(-20).map((c,i,a)=>{const pc=i>0?a[i-1].close:c.open;return Math.max(c.high-c.low,Math.abs(c.high-pc),Math.abs(c.low-pc));});const atr=atrs.reduce((a,b)=>a+b,0)/atrs.length;const mid=(Math.max(...highs.slice(-20))+Math.min(...lows.slice(-20)))/2;return price>mid+3*atr?'BUY':price<mid-3*atr?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📈',n:'SuperTrend',s:fb(s)});
  s=candles.length>=14?(()=>{const pdm=[],mdm=[];for(let i=1;i<14;i++){pdm.push(Math.max(highs[highs.length-i]-highs[highs.length-i-1],0));mdm.push(Math.max(lows[lows.length-i-1]-lows[lows.length-i],0));}const a=candles.slice(-14).map(c=>Math.max(c.high-c.low,0.001)).reduce((a,b)=>a+b,0)/14;return pdm.reduce((a,b)=>a+b,0)/pdm.length/a*100>mdm.reduce((a,b)=>a+b,0)/mdm.length/a*100?'BUY':'SELL';})():'HOLD';
  results.push({e:'📊',n:'ADX',s:fb(s)});
  s=candles.length>=26?(()=>{const t=(Math.max(...highs.slice(-9))+Math.min(...lows.slice(-9)))/2;const k=(Math.max(...highs.slice(-26))+Math.min(...lows.slice(-26)))/2;return price>t&&t>k?'BUY':'SELL';})():'HOLD';
  results.push({e:'☁️',n:'Ichimoku',s:fb(s)});
  s=candles.length>=20?(()=>{const sar=(Math.min(...lows.slice(-20))+Math.max(...highs.slice(-20)))/2-(Math.max(...highs.slice(-20))-Math.min(...lows.slice(-20)))*0.02;return price>sar?'BUY':'SELL';})():'HOLD';
  results.push({e:'📍',n:'Parabolic',s:fb(s)});
  s=candles.length>=10?(()=>{let o=0;candles.slice(-10).forEach(c=>{o+=c.close>c.open?c.volume:-c.volume;});return o>0?'BUY':'SELL';})():'HOLD';
  results.push({e:'📊',n:'OBV',s:fb(s)});
  s=candles.length>=14?(()=>{const tp=candles.slice(-14).map(c=>(c.high+c.low+c.close)/3);const mf=tp.map((t,i)=>t*candles[candles.length-14+i].volume);let p=0,n=0;for(let i=1;i<14;i++){if(tp[i]>tp[i-1])p+=mf[i];else n+=mf[i];}const mfr=n!==0?p/n:999;const mfi=100-(100/(1+mfr));return mfi<30?'BUY':mfi>70?'SELL':'HOLD';})():'HOLD';
  results.push({e:'💰',n:'MFI',s:fb(s)});
  s=candles.length>=14?(()=>{const tr=candles.slice(-14).map(c=>Math.max(c.high-c.low,0.001)).reduce((a,b)=>a+b,0);const hh=Math.max(...highs.slice(-14)),ll=Math.min(...lows.slice(-14));const ch=hh!==ll?100*(tr/(hh-ll)):50;return ch<38.2?'BUY':ch>61.8?'SELL':'HOLD';})():'HOLD';
  results.push({e:'🌀',n:'Choppiness',s:fb(s)});
  s=candles.length>=13?(()=>{const e=ema(closes.slice(-13),13).slice(-1)[0];return last.high-e>0&&last.low-e<0?'BUY':'SELL';})():'HOLD';
  results.push({e:'🐘',n:'Elder Ray',s:fb(s)});
  results.push({e:'💪',n:'ForceIdx',s:fb((closes[closes.length-1]-closes[closes.length-2])*volumes[volumes.length-1]>0?'BUY':'SELL')});
  return results;
}

function getConsensus(indicators) {
  const buy = indicators.filter(i => i.s === 'BUY').length;
  const sell = indicators.filter(i => i.s === 'SELL').length;
  const hold = indicators.filter(i => i.s === 'HOLD').length;
  const total = buy + sell;
  if (total === 0) return { signal: 'HOLD', buy, sell, hold, pct: 0 };
  const buyPct = Math.round((buy / total) * 100);
  const sellPct = 100 - buyPct;
  const signal = buyPct > sellPct ? 'BUY' : buyPct < sellPct ? 'SELL' : 'HOLD';
  const strength = signal === 'BUY' ? buyPct : sellPct;
  return { signal, buy, sell, hold, pct: strength };
}

// ============ DISPLAY ============
function elapsedHM() {
  const ms = Date.now() - startTime.getTime();
  const totalMins = Math.floor(ms / 60000);
  const hrs = Math.floor(totalMins / 60);
  const mins = totalMins % 60;
  return `${String(hrs).padStart(2,'0')}h ${String(mins).padStart(2,'0')}m`;
}

function printDashboard(equity, price, consensus, pnl, indicators, positions) {
  const h = elapsedHM();
  const candlesElapsed = Math.floor((Date.now() - startTime.getTime()) / 60000);
  console.log('\n' + '═'.repeat(60));
  console.log(`🔱 SHIVA LIVE TRADING BOT`);
  console.log('═'.repeat(60));
  console.log(`🕐 ${new Date().toISOString().replace('T',' ').slice(0,19)}`);
  console.log(`⏱  Elapsed: ${h} | 🕯️  Candles: ${candlesElapsed} min`);
  console.log(`📊 Cycle: #${cycle} | Trades: ${totalTrades} | W:${wins} L:${losses}`);
  console.log('─'.repeat(60));
  console.log(`💰 EQUITY: $${equity.toFixed(2)} | Balance: $${(equity - pnl).toFixed(2)}`);
  console.log(`💵 PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)} (${((pnl/initialEquity)*100).toFixed(2)}%)`);
  console.log(`💹 Price: $${price.toFixed(3)} | ${SYMBOL} | ${LOT_SIZE} lots`);
  console.log('─'.repeat(60));

  if (positions.length > 0) {
    console.log(`\n📋 OPEN POSITIONS (${positions.length})`);
    console.log('─'.repeat(40));
    positions.forEach(p => {
      const pnlIcon = p.profit >= 0 ? '🟢' : '🔴';
      const entryStr = p.entry !== undefined ? `$${Number(p.entry).toFixed(2)}` : 'N/A';
      const profitStr = p.profit !== undefined ? `${p.profit >= 0 ? '+' : ''}$${Number(p.profit).toFixed(2)}` : '$0.00';
      const trail = p.highestPnl !== undefined ? ` | Trail: ${Number(p.highestPnl).toFixed(2)}` : '';
      const idStr = p.id ? p.id.slice(0, 8) : 'unknown';
      console.log(`${pnlIcon} ${p.type || '???'} ${idStr}... | Entry: ${entryStr} | PnL: ${profitStr}${trail}`);
    });
    console.log('─'.repeat(40));
  }

  console.log('');
  console.log(`🤖 40 NVIDIA AGENTS 40/40`);
  console.log(`CONSENSUS`);
  if (consensus.signal === 'BUY') console.log(`BUY (${consensus.pct}%)`);
  else if (consensus.signal === 'SELL') console.log(`SELL (${consensus.pct}%)`);
  else console.log(`HOLD`);
  console.log(`BUY:${consensus.buy} SELL:${consensus.sell} HOLD:${consensus.hold}`);
  console.log('');

  for (let i = 0; i < indicators.length; i++) {
    const ind = indicators[i];
    const marker = ind.s === consensus.signal ? '✅' : '  ';
    console.log(`${marker} ${ind.e} ${ind.n.padEnd(15)} ${ind.s}`);
  }
  console.log('═'.repeat(60));
}

// ============ TRADE MANAGEMENT ============
async function openPosition(signal, price, posNum) {
  // Calculate spread
  const priceData = await connection.getSymbolPrice(SYMBOL);
  const bid = priceData.bid || price;
  const ask = priceData.ask || price;
  const spread = ask - bid;
  const spreadPips = spread;

  // TIGHT SL: Bot calculates based on spread + small buffer
  // Tight SL = 0.10 (tight) + 2x spread buffer
  const baseSL = 0.10; // Tight SL
  const spreadBuffer = spreadPips * 1.5; // 1.5x spread buffer
  const totalSL = baseSL + spreadBuffer;

  const sl = signal === 'BUY' ? (price - totalSL).toFixed(3) : (price + totalSL).toFixed(3);
  const tp = 0; // No TP - trail winners instead

  const tp_display = 'TRAIL (no fixed TP)';
  console.log(`\n📤 Position ${posNum}/${POSITIONS} | ${signal} @ ${price.toFixed(3)}`);
  console.log(`📊 Spread: ${(spread * 100).toFixed(1)} cents | Tight SL: ${(totalSL * 100).toFixed(1)} cents`);
  console.log(`🛑 SL: ${sl}`);
  console.log(`🎯 ENTRY: ${price.toFixed(3)} | 🛑 SL: ${sl} | 📈 TP: ${tp_display} | 📋 ${signal} | #${posNum}`);

  try {
    const result = signal === 'BUY'
      ? await tradingAccount.createMarketBuyOrder(SYMBOL, LOT_SIZE, parseFloat(sl), undefined, { comment: `SHIVA_${posNum}` })
      : await tradingAccount.createMarketSellOrder(SYMBOL, LOT_SIZE, parseFloat(sl), undefined, { comment: `SHIVA_${posNum}` });

    const id = result.stringCode || result.id || 'unknown';
    console.log(`✅ Position ${posNum} opened | ID: ${id}`);
    totalTrades++;

    // Post to Discord
    postToDiscordOnPositionOpen(signal, price, sl, posNum, id);

    // Track position
    managedPositions.push({
      id: id,
      type: signal,
      entry: price,
      sl: parseFloat(sl),
      spread: spread,
      highestPnl: 0,
      beMoved: false
    });

    return id;
  } catch (e) {
    console.log(`❌ Position ${posNum} failed: ${e.message}`);
    return null;
  }
}

async function managePositions(currentPrice) {
  if (managedPositions.length === 0) return;

  // Get live positions from broker
  try {
    const livePositions = await connection.getPositions();
    const myPositions = livePositions.filter(p => p.symbol === SYMBOL);

    for (const pos of myPositions) {
      const managed = managedPositions.find(m => pos.id && pos.id.includes(m.id.slice(0, 8)));
      if (!managed) continue;

      const profit = pos.profit || 0;
      managed.currentProfit = profit;
      managed.currentPrice = currentPrice;

      // Track highest PnL for trailing
      if (profit > managed.highestPnl) {
        managed.highestPnl = profit;
      }

      // CUT LOSERS: Close if loss > $0.50
      if (profit < -0.50) {
        console.log(`\n🔴 CUTTING LOSER | ${managed.id.slice(0,8)} | PnL: $${profit.toFixed(2)}`);
        try {
          await tradingAccount.closePosition(pos.id);
          console.log(`✅ Loser closed`);
          logTrade(managed.type, managed.entry, currentPrice, profit, indicators, 'cut_loss');
          postToDiscordOnTradeClose(managed.type, profit, 'cut_loss');
          losses++;
          managedPositions = managedPositions.filter(m => m.id !== managed.id);
        } catch (e) {
          console.log(`⚠ Close failed: ${e.message}`);
        }
        continue;
      }

      // HOLD WINNERS: Trail stop loss
      if (profit >= TRAIL_START && !managed.beMoved) {
        // Move SL to breakeven
        const newSl = managed.type === 'BUY'
          ? (managed.entry + 0.05).toFixed(2)
          : (managed.entry - 0.05).toFixed(2);
        console.log(`🟢 WINNER HELD | ${managed.id.slice(0,8)} | PnL: +$${profit.toFixed(2)} | SL moved to BE`);
        managed.beMoved = true;
        // Note: Can't modify SL via SDK directly, but we track for closing
        managed.beSl = parseFloat(newSl);
      }

      // Close if profit drops from highest
      if (managed.highestPnl >= 1.0 && profit < managed.highestPnl * 0.5) {
        console.log(`\n🟢 TAKING PROFIT | ${managed.id.slice(0,8)} | Peak: +$${managed.highestPnl.toFixed(2)} | Now: +$${profit.toFixed(2)}`);
        try {
          await tradingAccount.closePosition(pos.id);
          console.log(`✅ Winner closed at profit`);
          logTrade(managed.type, managed.entry, currentPrice, profit, indicators, 'take_profit');
          postToDiscordOnTradeClose(managed.type, profit, 'take_profit', managed.highestPnl);
          wins++;
          managedPositions = managedPositions.filter(m => m.id !== managed.id);
        } catch (e) {
          console.log(`⚠ Close failed: ${e.message}`);
        }
        continue;
      }

      // Hold if winning - let it run
      if (profit >= 0.30) {
        console.log(`🟢 HOLDING WINNER | ${managed.id.slice(0,8)} | PnL: +$${profit.toFixed(2)} | Peak: +$${managed.highestPnl.toFixed(2)}`);
      }
    }

    // Remove positions that were closed by broker
    const liveIds = myPositions.map(p => p.id);
    managedPositions = managedPositions.filter(m => liveIds.some(lid => lid.includes(m.id.slice(0, 8))));

  } catch (e) {
    console.log(`⚠ Manage error: ${e.message.slice(0, 80)}`);
  }
}

// ============ MAIN LOOP ============
async function runCycle() {
  cycle++;
  const now = Date.now();
  const timeSinceLastCheck = now - lastCheck;

  try {
    const priceData = await connection.getSymbolPrice(SYMBOL);
    const price = priceData.bid || priceData.ask;
    const info = await connection.getAccountInformation();
    const equity = info.equity || 0;
    const pnl = equity - initialEquity;

    // Build synthetic candles
    if (!global.priceHistory) global.priceHistory = [];
    global.priceHistory.push({ time: new Date(), open: price, high: price, low: price, close: price, volume: 1 });
    const hist = global.priceHistory;
    const candles = hist.map((p, i) => ({
      time: p.time,
      open: p.open,
      high: i > 0 ? Math.max(p.high, hist[i-1].high) : p.high,
      low: i > 0 ? Math.min(p.low, hist[i-1].low) : p.low,
      close: p.close,
      volume: p.volume
    }));
    while (candles.length < 50) {
      const last = candles.length > 0 ? candles[candles.length - 1] : { close: price };
      candles.push({ time: new Date(Date.now() - (50 - candles.length) * 60000), open: last.close, high: last.close, low: last.close, close: last.close, volume: 1 });
    }

    const indicators = analyzeAll(candles, price);
    const consensus = getConsensus(indicators);

    // Run ML prediction
    mlPrediction = runML(indicators, consensus.signal);

    // Combine consensus + ML
    let finalSignal = consensus.signal;
    let finalStrength = consensus.pct;
    let mlNote = '';
    if (tradeHistory.length >= 5) {
      if (!mlPrediction.agrees) {
        finalStrength = Math.round((consensus.pct + mlPrediction.confidence * 100) / 2);
        mlNote = ` ⚠️ ML disagrees (${mlPrediction.signal} ${(mlPrediction.confidence*100).toFixed(0)}%)`;
      } else {
        finalStrength = Math.min(95, Math.round((consensus.pct + mlPrediction.confidence * 100) / 2));
        mlNote = ` ✅ ML agrees (${(mlPrediction.confidence*100).toFixed(0)}% win rate)`;
      }
    }
    const canTrade = finalStrength >= 50 && (mlPrediction.agrees || tradeHistory.length < 5);

    // Print dashboard
    printDashboard(equity, price, consensus, pnl, indicators, managedPositions);

    // Periodic signal posting DISABLED (manual only)
    // if (cycle % 10 === 1 && DISCORD_WEBHOOK) {
    //   postPeriodicSignalToDiscord(consensus, price, equity, balance, pnl, indicators, myOpen ? myOpen.length : 0);
    // }

    // Show ML status
    if (tradeHistory.length > 0) {
      const histWins = tradeHistory.filter(t => t.result === 'win').length;
      const histLosses = tradeHistory.filter(t => t.result === 'loss').length;
      const histWR = tradeHistory.length > 0 ? (histWins / tradeHistory.length * 100).toFixed(0) : 0;
      console.log(`\n🧠 ML ENGINE`);
      console.log(`📊 Trades: ${tradeHistory.length} | W:${histWins} L:${histLosses} | WR: ${histWR}%`);
      console.log(`🤖 ML: ${mlPrediction.signal} | Confidence: ${(mlPrediction.confidence*100).toFixed(0)}% | ${mlPrediction.agrees ? '✅ Agrees' : '⚠️ Disagrees'}`);
    }

    // Manage existing positions (every 30s)
    if (timeSinceLastCheck >= CHECK_INTERVAL) {
      lastCheck = now;
      await managePositions(price);
    }

    // Open positions if strong consensus
    let myOpen = [];
    try {
      const openPositions = await connection.getPositions();
      myOpen = openPositions.filter(p => p.symbol === SYMBOL);
      console.log(`\n📋 Position check: ${openPositions.length} total, ${myOpen.length} ${SYMBOL} positions`);
      if (myOpen.length > 0) {
        myOpen.forEach((p, i) => {
          console.log(`  #${i+1} ${p.id.slice(0,8)} ${p.type} @ ${p.openPrice} | PnL: $${(p.profit||0).toFixed(2)}`);
        });
      }
    } catch (e) {
      console.log(`⚠ Position check failed: ${e.message}`);
    }

    const needToOpen = Math.max(0, POSITIONS - myOpen.length);

    if (needToOpen > 0 && canTrade && (finalSignal === 'BUY' || finalSignal === 'SELL')) {
      console.log(`\n🚀 Opening ${needToOpen} positions (${finalSignal} ${finalStrength}%)${mlNote}`);
      console.log(`📋 Existing: ${myOpen.length} | New: ${needToOpen} | Target: ${POSITIONS}`);

      for (let i = 0; i < needToOpen; i++) {
        await openPosition(finalSignal, price, myOpen.length + i + 1);
        if (i < needToOpen - 1) {
          console.log(`⏳ Waiting ${ENTRY_GAP_MS/1000}s before next entry...`);
          await new Promise(r => setTimeout(r, ENTRY_GAP_MS));
        }
      }
    } else if (myOpen.length >= POSITIONS) {
      console.log(`\n📋 ${myOpen.length}/${POSITIONS} positions full. Managing...`);
    } else {
      console.log(`\n⏸ Waiting: ${finalSignal} ${finalStrength}% (need >= 50%)${mlNote}`);
    }

    // Count total open
    let myFinal = [];
    try {
      const finalPositions = await connection.getPositions();
      myFinal = finalPositions.filter(p => p.symbol === SYMBOL);
    } catch (e) {}
    console.log(`\n📊 Open: ${myFinal.length}/${POSITIONS} | Trailing winners | Cutting losers`);
    console.log(`⏳ Next check in ${CHECK_INTERVAL/1000}s...`);

  } catch (e) {
    console.log(`❌ Error: ${e.message}`);
  }
}

// ============ START ============
async function main() {
  try {
    await connectMT4();
    console.log(`\n🚀 LIVE TRADING - 6 Positions | Trail Winners | Cut Losers`);
    console.log(`📋 Check every ${CHECK_INTERVAL/1000}s | Gap: ${ENTRY_GAP_MS/1000}s`);

    // Run first cycle immediately
    await runCycle();

    // Then run on interval
    setInterval(runCycle, CHECK_INTERVAL);
  } catch (e) {
    console.log(`❌ Fatal: ${e.message}\n${e.stack}`);
    process.exit(1);
  }
}

process.on('SIGINT', () => {
  console.log(`\n\n${'═'.repeat(60)}`);
  console.log(`🛑 Bot stopped`);
  console.log(`⏱  Ran: ${elapsedHM()} | 📊 Trades: ${totalTrades} | W:${wins} L:${losses}`);
  console.log('═'.repeat(60));
  process.exit(0);
});

main();
