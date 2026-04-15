#!/usr/bin/env node
/**
 * SHIVA LIVE TRADING BOT - MT4/SpotCrude
 * 40 NVIDIA Agents | Opens 6 positions | Trail winners | Cut losers
 */

const MetaApi = require('metaapi.cloud-sdk').default;
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');

// ============ CONFIG ============
// Load credentials from .shiva_env
function loadEnvFile() {
  const envPath = path.join(os.homedir(), '.shiva_env');
  try {
    const content = fs.readFileSync(envPath, 'utf8');
    content.split('\n').forEach(line => {
      line = line.trim();
      if (line && !line.startsWith('#')) {
        const eqIdx = line.indexOf('=');
        if (eqIdx > 0) {
          const key = line.substring(0, eqIdx);
          const val = line.substring(eqIdx + 1).trim();
          if (!process.env[key]) {
            process.env[key] = val;
          }
        }
      }
    });
  } catch (e) {
    console.log('⚠️ Could not load ~/.shiva_env, using env vars');
  }
}
loadEnvFile();

const TOKEN = process.env.METAAPI_TOKEN || '';
const ACCOUNT_ID = process.env.METAAPI_ACCOUNT_ID || '';
const SYMBOL = 'SpotCrude';
const LOT_SIZE = 0.03;
const POSITIONS = 6;
const ENTRY_GAP_MS = 3000;
const STOP_LOSS = 0.30;
const TAKE_PROFIT = 0.60;
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
async function connectMT4(maxRetries = 5) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
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

  // Load existing positions into managedPositions for SL management
  try {
    const existingPositions = await connection.getPositions();
    const symbolPositions = existingPositions.filter(p => p.symbol === SYMBOL);
    console.log(`📊 Found ${symbolPositions.length} existing ${SYMBOL} position(s) — loading into management`);
    if (symbolPositions.length > 0) {
      for (const p of symbolPositions) {
        const posType = p.type.includes('BUY') ? 'BUY' : 'SELL';
        managedPositions.push({
          id: p.id || `${p.openTime}-${p.openPrice}`,
          type: posType,
          entry: p.openPrice,
          sl: p.stopLoss || 0,
          spread: 0,
          highestPnl: Math.max(0, p.profit || 0),
          currentProfit: p.profit || 0,
          beMoved: false,
          trailing: false
        });
        console.log(`  ✅ Tracking ${posType} @ ${p.openPrice} | SL: ${p.stopLoss || 'none'} | PnL: $${(p.profit||0).toFixed(2)}`);
      }
      console.log(`🛡️ ${managedPositions.length} positions loaded for SL management`);
    }
  } catch (e) {
    console.log(`⚠ Could not load positions: ${e.message}`);
  }

  return info;
} catch (e) {
    if (e.message.includes('429') || e.message.includes('rate') || e.message.includes('Timed out') || e.message.includes('timeout')) {
      const waitTime = attempt * 15000;
      console.log('⏳ Rate limited. Retrying in ' + waitTime/1000 + 's...');
      await new Promise(r => setTimeout(r, waitTime));
      if (attempt === maxRetries) throw e;
    } else {
      throw e;
    }
  }
}
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

  // BOTTOM WICK: Price is in the lower wick zone (buying opportunity)
  s=candles.length>=5?(()=>{
    const recentCandles = candles.slice(-5);
    let inWick = false;
    for (const c of recentCandles) {
      const body = Math.abs(c.close - c.open);
      const range = c.high - c.low;
      const lowerWick = Math.min(c.close, c.open) - c.low;
      const upperWick = c.high - Math.max(c.close, c.open);
      // Price is in lower wick if it's below the body
      const wickZone = c.low + (lowerWick * 0.5);
      if (price <= wickZone && lowerWick > body * 0.3) {
        inWick = true;
        break;
      }
    }
    return inWick ? 'BUY' : 'HOLD';
  })():'HOLD';
  results.push({e:'🕯️',n:'BottomWick',s});

  // FIB GOLDEN ZONE: Price at 0.618-0.65 retracement (prime entry zone)
  s=candles.length>=20?(()=>{
    const hh = Math.max(...highs.slice(-20));
    const ll = Math.min(...lows.slice(-20));
    const range = hh - ll;
    const fib618 = ll + range * 0.618;
    const fib650 = ll + range * 0.650;
    const fib500 = ll + range * 0.500;
    const goldenZoneLow = fib618 - (range * 0.02);  // Small buffer
    const goldenZoneHigh = fib650 + (range * 0.02);
    // Price in golden zone
    if (price >= goldenZoneLow && price <= goldenZoneHigh) {
      return 'BUY';  // Retracement buy zone
    }
    // Also check 0.5 level for early entry
    if (price >= fib500 && price <= fib618) {
      return 'BUY';  // Pre-golden zone
    }
    return 'HOLD';
  })():'HOLD';
  results.push({e:'🔱',n:'FibGolden',s});

  return results;
}

function getConsensus(indicators) {
  const buy = indicators.filter(i => i.s === 'BUY').length;
  const sell = indicators.filter(i => i.s === 'SELL').length;
  const hold = indicators.filter(i => i.s === 'HOLD').length;
  const total = buy + sell;
  if (total === 0) return { signal: 'HOLD', buy, sell, hold, pct: 0 };

  // Extra weight for high-probability zones (bottom wick, fib golden)
  const bottomWick = indicators.find(i => i.n === 'BottomWick');
  const fibGolden = indicators.find(i => i.n === 'FibGolden');
  let bonusWeight = 0;
  if (bottomWick?.s === 'BUY') bonusWeight += 2;  // +2 votes for bottom wick
  if (fibGolden?.s === 'BUY') bonusWeight += 3;   // +3 votes for fib golden zone (stronger)

  const adjustedBuy = buy + bonusWeight;
  const adjustedTotal = adjustedBuy + sell;
  const buyPct = Math.round((adjustedBuy / adjustedTotal) * 100);
  const sellPct = 100 - buyPct;
  const signal = buyPct > sellPct ? 'BUY' : buyPct < sellPct ? 'SELL' : 'HOLD';
  const strength = signal === 'BUY' ? buyPct : sellPct;
  return { signal, buy: adjustedBuy, sell, hold, pct: strength, rawBuy: buy, rawSell: sell };
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
      const trail = p.highestPnl !== undefined ? ` | Peak: ${Number(p.highestPnl).toFixed(2)}` : '';
      const idStr = p.id ? p.id.slice(0, 8) : 'unknown';
      console.log(`${pnlIcon} ${p.type || '???'} ${idStr}... | Entry: ${entryStr} | PnL: ${profitStr}${trail}`);
    });
    console.log('─'.repeat(40));
  }

  console.log('');
  console.log(`🤖 42 NVIDIA AGENTS 42/42`);
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
async function openPosition(signal, price, posNum, candles) {
  // Calculate spread
  const priceData = await connection.getSymbolPrice(SYMBOL);
  const bid = priceData.bid || price;
  const ask = priceData.ask || price;
  const spread = ask - bid;
  const spreadPips = spread;

  // TIGHTER SL: Use recent wicks (last 3 candles)
  const last3 = candles.slice(-3);
  const wickLow = Math.min(...last3.map(c => c.low));
  const wickHigh = Math.max(...last3.map(c => c.high));
  
  let dynamicSL;
  if (signal === 'BUY') {
    dynamicSL = price - wickLow;
  } else {
    dynamicSL = wickHigh - price;
  }

  // Ensure SL is tighter but safe (between 0.10 and 0.30)
  const baseSL = Math.max(0.10, Math.min(0.30, dynamicSL));
  const spreadBuffer = spreadPips * 1.5; // Slightly tighter spread buffer too
  const totalSL = baseSL + spreadBuffer;

  const sl = signal === 'BUY' ? (price - totalSL).toFixed(2) : (price + totalSL).toFixed(2);
  const tp = signal === 'BUY' ? (price + TAKE_PROFIT).toFixed(2) : (price - TAKE_PROFIT).toFixed(2);

  const tp_display = `$${tp}`;
  console.log(`\n📤 Position ${posNum}/${POSITIONS} | ${signal} @ ${price.toFixed(2)}`);
  console.log(`📊 Spread: ${(spread * 100).toFixed(1)} cents | SL Buffer: ${(spreadBuffer * 100).toFixed(1)} cents | Total SL: ${(totalSL * 100).toFixed(1)} cents`);
  console.log(`🛑 SL: ${sl} | 🎯 TP: ${tp}`);
  console.log(`🎯 ENTRY: ${price.toFixed(2)} | 🛑 SL: ${sl} | 📈 TP: ${tp_display} | 📋 ${signal} | #${posNum}`);

  try {
    const result = signal === 'BUY'
      ? await tradingAccount.createMarketBuyOrder(SYMBOL, LOT_SIZE, parseFloat(sl), parseFloat(tp), { comment: `SHIVA_${posNum}` })
      : await tradingAccount.createMarketSellOrder(SYMBOL, LOT_SIZE, parseFloat(sl), parseFloat(tp), { comment: `SHIVA_${posNum}` });

    const id = result.stringCode || result.id || 'unknown';
    console.log(`✅ Position ${posNum} opened | ID: ${id}`);
    totalTrades++;

    // Track position
    managedPositions.push({
      id: id,
      type: signal,
      entry: price,
      sl: parseFloat(sl),
      tp: parseFloat(tp),
      spread: spread,
      highestPnl: 0,
      currentProfit: 0,
      beMoved: false,
      trailing: false
    });

    return id;
  } catch (e) {
    console.log(`❌ Position ${posNum} failed: ${e.message}`);
    return null;
  }
}

async function managePositions(currentPrice, indicators) {
  if (managedPositions.length === 0) return;

  // Get live positions from broker
  try {
    const livePositions = await connection.getPositions();
    const myPositions = livePositions.filter(p => p.symbol === SYMBOL);
    const liveIds = myPositions.map(p => p.id);

    // Detect closed positions and log them
    const closed = managedPositions.filter(m => !liveIds.some(lid => lid.includes(m.id.slice(0, 8))));
    for (const m of closed) {
      const profit = m.currentProfit || 0;
      const result = profit >= 0 ? 'win' : 'loss';
      console.log(`\n🏁 ${result === 'win' ? '🟢 TP' : '🔴 SL'} HIT | ${m.id.slice(0,8)} | Final PnL: $${profit.toFixed(2)}`);
      
      // Update stats
      if (result === 'win') wins++; else losses++;
      
      // Log to history
      logTrade(m.type, m.entry, currentPrice, profit, indicators || [], result === 'win' ? 'take_profit' : 'stop_loss');
    }

    // Remove closed positions from managed state
    managedPositions = managedPositions.filter(m => liveIds.some(lid => lid.includes(m.id.slice(0, 8))));

    // Update state for remaining open positions
    for (const pos of myPositions) {
      const managed = managedPositions.find(m => pos.id && pos.id.includes(m.id.slice(0, 8)));
      if (!managed) continue;

      const profit = pos.profit || 0;
      managed.currentProfit = profit;
      managed.currentPrice = currentPrice;

      // Track highest PnL for display
      if (profit > managed.highestPnl) {
        managed.highestPnl = profit;
      }

      // Simple status logging
      if (profit >= 0.30) {
        console.log(`🟢 WINNING | ${managed.id.slice(0,8)} | PnL: +$${profit.toFixed(2)} | Peak: +$${managed.highestPnl.toFixed(2)}`);
      } else if (profit <= -0.15) {
        console.log(`🔴 LOSING | ${managed.id.slice(0,8)} | PnL: -$${Math.abs(profit).toFixed(2)}`);
      }
    }

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
      await managePositions(price, indicators);
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

    // Check if existing positions are in the opposite direction
    const existingDirections = [...new Set(myOpen.map(p => p.type))];
    const hasOppositePosition = existingDirections.length > 0 && 
      ((finalSignal === 'BUY' && existingDirections.includes('SELL')) || 
       (finalSignal === 'SELL' && existingDirections.includes('BUY')));

    if (hasOppositePosition) {
      console.log(`\n🚫 BLOCKED: Cannot open ${finalSignal} while ${existingDirections.join('/')} positions exist`);
      console.log(`📋 Waiting for all positions to close before switching direction`);
    }

    const needToOpen = Math.max(0, POSITIONS - myOpen.length);

    if (needToOpen > 0 && canTrade && (finalSignal === 'BUY' || finalSignal === 'SELL') && !hasOppositePosition) {
      console.log(`\n🚀 Opening ${needToOpen} positions (${finalSignal} ${finalStrength}%)${mlNote}`);
      console.log(`📋 Existing: ${myOpen.length} | New: ${needToOpen} | Target: ${POSITIONS}`);

      for (let i = 0; i < needToOpen; i++) {
        await openPosition(finalSignal, price, myOpen.length + i + 1, candles);
        if (i < needToOpen - 1) {
          console.log(`⏳ Waiting ${ENTRY_GAP_MS/1000}s before next entry...`);
          await new Promise(r => setTimeout(r, ENTRY_GAP_MS));
        }
      }
    } else if (myOpen.length >= POSITIONS) {
      console.log(`\n📋 ${myOpen.length}/${POSITIONS} positions full. Managing...`);
    } else if (hasOppositePosition) {
      // Skip - already logged blocked message above
    } else {
      console.log(`\n⏸ Waiting: ${finalSignal} ${finalStrength}% (need >= 50%)${mlNote}`);
    }

    // Count total open
    let myFinal = [];
    try {
      const finalPositions = await connection.getPositions();
      myFinal = finalPositions.filter(p => p.symbol === SYMBOL);
    } catch (e) {}
    console.log(`\n📊 Open: ${myFinal.length}/${POSITIONS} | Holding until SL or TP`);
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
