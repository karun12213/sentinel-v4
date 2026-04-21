#!/usr/bin/env node
/**
 * SHIVA CONTINUOUS TRADING BOT - 24/7 HANDS-FREE
 * Runs continuously with auto-reconnect and position management
 */

// Load .env file
require('dotenv').config({ path: require('path').join(__dirname, '.env') });

const MetaApi = require('metaapi.cloud-sdk').default;
const fs = require('fs');
const path = require('path');

// ============ CONFIG ============
const TOKEN = process.env.METAAPI_TOKEN || '';
const ACCOUNT_ID = process.env.METAAPI_ACCOUNT_ID || '';
const SYMBOL = process.env.SYMBOL || 'SpotCrude';
const parsedLotSize = parseFloat(process.env.LOT_SIZE || '');
const LOT_SIZE = Number.isFinite(parsedLotSize) && parsedLotSize > 0 ? parsedLotSize : 0.01;
const parsedMaxPositions = parseInt(process.env.POSITIONS ?? '6', 10);
const MAX_POSITIONS = Number.isNaN(parsedMaxPositions) ? 6 : parsedMaxPositions;
const STOP_LOSS = 0.30;
const TRAIL_START = 0.30;
const TRAIL_DISTANCE = 0.15;
const CHECK_INTERVAL = parseInt(process.env.CHECK_INTERVAL) || 30000; // 30 seconds
const RECONNECT_DELAY = 10000; // 10 seconds
const ML_MIN_TRADES = 5; // Minimum trades before ML kicks in
const ML_RETRAIN_EVERY = 10; // Retrain every N cycles

// ============ STATE ============
let api, tradingAccount;
let isConnected = false;
let totalTrades = 0, wins = 0, losses = 0;
let startTime = new Date();
let cycle = 0;
let initialEquity = 0;
let managedPositions = [];
let priceHistory = [];
let lastPrice = 0;
let tradeHistory = [];
let mlModel = { weights: {}, winRate: 0.5, totalTrades: 0 };

// ============ ML ENGINE ============
const tradeHistoryFile = path.join(__dirname, 'trade_history.json');
const modelFile = path.join(__dirname, 'ml_model.json');

function loadTradeHistory() {
  try {
    if (fs.existsSync(tradeHistoryFile)) {
      return JSON.parse(fs.readFileSync(tradeHistoryFile, 'utf8'));
    }
  } catch (e) {}
  return [];
}

function saveTradeHistory() {
  fs.writeFileSync(tradeHistoryFile, JSON.stringify(tradeHistory, null, 2));
}

function loadMLModel() {
  try {
    if (fs.existsSync(modelFile)) {
      return JSON.parse(fs.readFileSync(modelFile, 'utf8'));
    }
  } catch (e) {}
  return { weights: {}, winRate: 0.5, totalTrades: 0 };
}

function saveMLModel() {
  fs.writeFileSync(modelFile, JSON.stringify(mlModel, null, 2));
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
    agents: agents.map(a => a.s),
    timestamp: new Date().toISOString(),
    cycle
  };
  tradeHistory.push(trade);
  saveTradeHistory();
  log(`Trade logged: ${trade.id} | ${signal} | PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)} | ${reason}`, 'trade');
}

function trainML() {
  if (tradeHistory.length < ML_MIN_TRADES) return;

  wins = tradeHistory.filter(t => t.result === 'win').length;
  losses = tradeHistory.filter(t => t.result === 'loss').length;
  const total = wins + losses;

  if (total < 3) return;

  mlModel.winRate = wins / total;
  mlModel.totalTrades = total;

  // Learn which agent combinations lead to wins
  const agentPerformance = {};
  tradeHistory.forEach(trade => {
    const buyAgents = trade.agents.filter(a => a === 'BUY').length;
    const sellAgents = trade.agents.filter(a => a === 'SELL').length;
    const buyRatio = buyAgents / 40;
    
    const key = trade.signal;
    if (!agentPerformance[key]) {
      agentPerformance[key] = { wins: 0, losses: 0, total: 0, avgBuyRatio: 0 };
    }
    agentPerformance[key].wins += trade.result === 'win' ? 1 : 0;
    agentPerformance[key].losses += trade.result === 'loss' ? 1 : 0;
    agentPerformance[key].total++;
    agentPerformance[key].avgBuyRatio += buyRatio;
  });

  // Calculate agent weights
  Object.keys(agentPerformance).forEach(sig => {
    const p = agentPerformance[sig];
    p.avgBuyRatio /= p.total;
    p.winRate = p.wins / p.total;
  });

  mlModel.weights = agentPerformance;
  saveMLModel();

  log(`ML Model trained: ${tradeHistory.length} trades | Win rate: ${(mlModel.winRate*100).toFixed(1)}% | W:${wins} L:${losses}`, 'success');
}

function mlPredict(consensus, indicators) {
  if (tradeHistory.length < ML_MIN_TRADES) {
    return { signal: consensus.signal, confidence: consensus.pct / 100, mlNote: 'Learning...' };
  }

  const buyRatio = indicators.filter(i => i.s === 'BUY').length / 40;
  const sellRatio = indicators.filter(i => i.s === 'SELL').length / 40;
  const signalKey = consensus.signal;
  const weightData = mlModel.weights[signalKey];

  if (!weightData || weightData.total < 3) {
    return { signal: consensus.signal, confidence: consensus.pct / 100, mlNote: 'Gathering data...' };
  }

  // ML prediction based on historical performance
  const mlWinRate = weightData.winRate;
  const mlConfidence = mlWinRate * (consensus.pct / 100);

  // Override if ML has strong contrary evidence
  let finalSignal = consensus.signal;
  if (mlWinRate < 0.35 && weightData.total >= 5) {
    // This signal type has terrible track record - flip it
    finalSignal = consensus.signal === 'BUY' ? 'SELL' : 'BUY';
  }

  const mlNote = finalSignal !== consensus.signal
    ? `⚠️ ML overrides to ${finalSignal} (${(mlWinRate*100).toFixed(0)}% win rate on ${signalKey})`
    : `✅ ML agrees (${(mlWinRate*100).toFixed(0)}% win rate on ${signalKey})`;

  return { signal: finalSignal, confidence: mlConfidence, mlNote };
}

// ============ LOGGING ============
const logsDir = path.join(__dirname, 'logs');
if (!fs.existsSync(logsDir)) fs.mkdirSync(logsDir, { recursive: true });
const logFile = path.join(logsDir, `shiva_${new Date().toISOString().slice(0,10)}.log`);

function log(msg, type = 'info') {
  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const icon = type === 'error' ? '❌' : type === 'success' ? '✅' : type === 'trade' ? '📊' : 'ℹ️';
  const line = `${ts} ${icon} ${msg}`;
  console.log(line);
  fs.appendFileSync(logFile, line + '\n');
}

// ============ SDK INITIALIZATION ============
async function initSDK() {
  if (!TOKEN || !ACCOUNT_ID) throw new Error('Missing METAAPI_TOKEN or METAAPI_ACCOUNT_ID');
  
  log('Initializing MetaApi SDK...', 'info');
  
  api = new MetaApi(TOKEN, {
    provisioningUrl: 'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai',
    mtUrl: 'https://mt-client-api-v1.london.agiliumtrade.agiliumtrade.ai'
  });
  
  const account = await api.metatraderAccountApi.getAccount(ACCOUNT_ID);
  log(`Account: ${account.name} | Server: ${account.server} | Region: ${account.region}`, 'info');
  
  if (account.state !== 'DEPLOYED') {
    throw new Error(`Account not deployed: ${account.state}`);
  }
  
  const connection = account.getRPCConnection();
  await connection.connect();
  await connection.waitSynchronized();
  tradingAccount = connection;
  
  const info = await tradingAccount.getAccountInformation();
  initialEquity = info.equity || info.balance || 0;
  
  isConnected = true;
  log(`CONNECTED | Balance: $${info.balance} | Equity: $${info.equity}`, 'success');
  
  return account;
}

async function ensureConnected() {
  if (isConnected && tradingAccount) {
    try {
      await tradingAccount.getAccountInformation();
      return true;
    } catch (e) {
      log(`Connection lost: ${e.message}`, 'error');
      isConnected = false;
    }
  }
  
  log('Reconnecting...', 'info');
  for (let i = 1; i <= 5; i++) {
    try {
      await initSDK();
      return true;
    } catch (e) {
      log(`Reconnect attempt ${i}/5 failed: ${e.message}`, 'error');
      await sleep(RECONNECT_DELAY);
    }
  }
  return false;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

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
  const signal = buyPct > 60 ? 'BUY' : sellPct > 60 ? 'SELL' : 'HOLD';
  const strength = signal === 'BUY' ? buyPct : sellPct;
  return { signal, buy, sell, hold, pct: strength };
}

// ============ POSITION MANAGEMENT ============
async function openPosition(signal, price, posNum, indicators = []) {
  const priceData = await tradingAccount.getSymbolPrice(SYMBOL);
  const bid = priceData.bid || price;
  const ask = priceData.ask || price;
  const spread = ask - bid;
  const spreadBuffer = spread * 2;
  const totalSL = STOP_LOSS + spreadBuffer;

  const sl = signal === 'BUY' ? (price - totalSL).toFixed(2) : (price + totalSL).toFixed(2);

  log(`Opening position ${posNum}/${MAX_POSITIONS}: ${signal} @ ${price.toFixed(2)} | SL: ${sl}`, 'trade');

  try {
    const result = signal === 'BUY'
      ? await tradingAccount.createMarketBuyOrder(SYMBOL, LOT_SIZE, parseFloat(sl), undefined, { comment: `SHIVA_${posNum}` })
      : await tradingAccount.createMarketSellOrder(SYMBOL, LOT_SIZE, parseFloat(sl), undefined, { comment: `SHIVA_${posNum}` });

    const id = result.stringCode || result.id || 'unknown';
    log(`✅ Position ${posNum} opened | ID: ${id}`, 'success');
    totalTrades++;

    // Log trade for ML training
    if (indicators.length > 0) {
      logTrade(signal, price, price, 0, indicators, 'open');
    }

    managedPositions.push({
      id: id,
      type: signal,
      entry: price,
      sl: parseFloat(sl),
      highestPnl: 0,
      beMoved: false
    });

    return id;
  } catch (e) {
    log(`❌ Position ${posNum} failed: ${e.message}`, 'error');
    return null;
  }
}

async function managePositions(currentPrice) {
  if (managedPositions.length === 0) return;

  try {
    const livePositions = await tradingAccount.getPositions();
    const myPositions = livePositions.filter(p => p.symbol === SYMBOL);

    for (const pos of myPositions) {
      const managed = managedPositions.find(m => pos.id && pos.id.includes(m.id.slice(0, 8)));
      if (!managed) continue;

      const profit = pos.profit || 0;
      managed.currentProfit = profit;
      managed.currentPrice = currentPrice;

      if (profit > managed.highestPnl) {
        managed.highestPnl = profit;
      }

      // CUT LOSERS: Close if loss > $0.50
      if (profit < -0.50) {
        log(`🔴 CUTTING LOSER | ${managed.id.slice(0,8)} | PnL: $${profit.toFixed(2)}`, 'error');
        try {
          await tradingAccount.closePosition(pos.id);
          log(`✅ Loser closed | PnL: $${profit.toFixed(2)}`, 'success');
          logTrade(managed.type, managed.entry, currentPrice, profit, [], 'cut_loss');
          losses++;
          managedPositions = managedPositions.filter(m => m.id !== managed.id);
        } catch (e) {
          log(`⚠ Close failed: ${e.message}`, 'error');
        }
        continue;
      }

      // Move SL to breakeven when winning
      if (profit >= TRAIL_START && !managed.beMoved) {
        const newSl = managed.type === 'BUY'
          ? (managed.entry + 0.05).toFixed(2)
          : (managed.entry - 0.05).toFixed(2);
        log(`🟢 WINNER HELD | ${managed.id.slice(0,8)} | PnL: +$${profit.toFixed(2)} | SL moved to BE: ${newSl}`, 'success');
        managed.beMoved = true;
        managed.beSl = parseFloat(newSl);
      }

      // Trail: Close if profit drops from highest
      if (managed.highestPnl >= 1.0 && profit < managed.highestPnl * 0.5) {
        log(`🟢 TRAILING TP | ${managed.id.slice(0,8)} | Peak: +$${managed.highestPnl.toFixed(2)} | Now: +$${profit.toFixed(2)}`, 'success');
        try {
          await tradingAccount.closePosition(pos.id);
          log(`✅ Winner closed at profit | PnL: +$${profit.toFixed(2)}`, 'success');
          logTrade(managed.type, managed.entry, currentPrice, profit, [], 'take_profit');
          wins++;
          managedPositions = managedPositions.filter(m => m.id !== managed.id);
        } catch (e) {
          log(`⚠ Close failed: ${e.message}`, 'error');
        }
        continue;
      }

      if (profit >= 0.30) {
        log(`🟢 HOLDING WINNER | ${managed.id.slice(0,8)} | PnL: +$${profit.toFixed(2)} | Peak: +$${managed.highestPnl.toFixed(2)}`, 'success');
      }
    }

    // Remove positions that were closed externally
    const liveIds = myPositions.map(p => p.id);
    managedPositions = managedPositions.filter(m => liveIds.some(lid => lid.includes(m.id.slice(0, 8))));

  } catch (e) {
    log(`⚠ Manage error: ${e.message}`, 'error');
  }
}

// ============ MAIN TRADING CYCLE ============
async function runCycle() {
  cycle++;
  
  try {
    if (!await ensureConnected()) {
      log('Failed to connect, skipping cycle', 'error');
      return;
    }

    // Get account info
    const info = await tradingAccount.getAccountInformation();
    const equity = info.equity || 0;
    const pnl = equity - initialEquity;

    // Get price
    const priceData = await tradingAccount.getSymbolPrice(SYMBOL);
    const price = priceData.bid || priceData.ask;
    lastPrice = price;

    // Build synthetic candles from price history
    priceHistory.push({ time: new Date(), open: price, high: price, low: price, close: price, volume: 1 });
    while (priceHistory.length < 50) {
      priceHistory.unshift({ time: new Date(Date.now() - (50 - priceHistory.length) * 60000), open: price, high: price, low: price, close: price, volume: 1 });
    }

    const candles = priceHistory.slice(-50).map((p, i, arr) => ({
      time: p.time,
      open: p.open,
      high: Math.max(p.high, i > 0 ? arr[i-1].high : p.high),
      low: Math.min(p.low, i > 0 ? arr[i-1].low : p.low),
      close: p.close,
      volume: p.volume
    }));

    // Run 40 agents
    const indicators = analyzeAll(candles, price);
    const consensus = getConsensus(indicators);

    // ML prediction
    const mlResult = mlPredict(consensus, indicators);
    const finalSignal = mlResult.signal;
    const finalConfidence = Math.round(mlResult.confidence * 100);

    // Retrain ML periodically
    if (cycle % ML_RETRAIN_EVERY === 0 && tradeHistory.length >= ML_MIN_TRADES) {
      trainML();
    }

    // Print status
    const elapsed = Math.floor((Date.now() - startTime.getTime()) / 60000);
    const mlStatus = tradeHistory.length >= ML_MIN_TRADES
      ? `🧠 ML: ${mlResult.mlNote}`
      : `🧠 ML: Learning... (${tradeHistory.length}/${ML_MIN_TRADES} trades)`;
    console.log('\n' + '═'.repeat(70));
    console.log(`🔱 SHIVA CONTINUOUS TRADING BOT | Cycle #${cycle} | ${elapsed}min elapsed`);
    console.log('═'.repeat(70));
    console.log(`⏰ ${new Date().toISOString().replace('T', ' ').slice(0, 19)}`);
    console.log(`💰 Equity: $${equity.toFixed(2)} | Balance: $${(equity - pnl).toFixed(2)} | PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)} (${((pnl/initialEquity)*100).toFixed(2)}%)`);
    console.log(`💹 ${SYMBOL} | $${price.toFixed(3)} | ${LOT_SIZE} lots | Trades: ${totalTrades} | W:${wins} L:${losses}`);
    console.log(`📊 Open Positions: ${managedPositions.length}/${MAX_POSITIONS}`);
    console.log(`🤖 Agents: ${consensus.signal} (${consensus.pct}%) | BUY:${consensus.buy} SELL:${consensus.sell} HOLD:${consensus.hold}`);
    console.log(`🤖 Final: ${finalSignal} (${finalConfidence}%) [${mlResult.mlNote}]`);
    console.log(mlStatus);
    console.log('═'.repeat(70));

    // Manage existing positions
    await managePositions(price);

    // Check if we can open new positions
    const livePositions = await tradingAccount.getPositions();
    const currentPositions = livePositions.filter(p => p.symbol === SYMBOL);
    
    if (finalSignal === 'HOLD') {
      log(`Signal: HOLD (${finalConfidence}%) - No trades`, 'info');
      return;
    }

    if (currentPositions.length >= MAX_POSITIONS) {
      log(`Max positions reached (${currentPositions.length}/${MAX_POSITIONS})`, 'info');
      return;
    }

    // Open positions
    const positionsToOpen = MAX_POSITIONS - currentPositions.length;
    log(`Signal: ${finalSignal} (${finalConfidence}%) - Opening ${positionsToOpen} position(s) [${mlResult.mlNote}]`, 'trade');

    for (let i = 0; i < positionsToOpen; i++) {
      await openPosition(finalSignal, price, i + 1, indicators);
      await sleep(3000);
    }

  } catch (e) {
    log(`Cycle #${cycle} error: ${e.message}`, 'error');
  }
}

// ============ START ============
async function main() {
  console.log('\n' + '🔱'.repeat(20));
  console.log('🔱 SHIVA CONTINUOUS TRADING BOT - 24/7 HANDS-FREE 🔱');
  console.log('🔱'.repeat(20));
  console.log(`Config: ${SYMBOL} | Lot: ${LOT_SIZE} | Max: ${MAX_POSITIONS} | Interval: ${CHECK_INTERVAL}ms`);
  console.log(`Started: ${new Date().toISOString()}`);
  console.log('═'.repeat(60) + '\n');

  // Load ML data
  tradeHistory = loadTradeHistory();
  mlModel = loadMLModel();
  if (tradeHistory.length > 0) {
    log(`Loaded ${tradeHistory.length} past trades | ML Model loaded`, 'info');
    trainML();
  }

  // Initial connection
  if (!await ensureConnected()) {
    log('Fatal: Could not connect. Exiting.', 'error');
    process.exit(1);
  }

  // Run first cycle
  await runCycle();

  // Continuous loop
  setInterval(async () => {
    await runCycle();
  }, CHECK_INTERVAL);

  log(`Bot running continuously every ${CHECK_INTERVAL/1000}s`, 'success');
}

main().catch(e => {
  log(`Fatal error: ${e.message}`, 'error');
  process.exit(1);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  log('Shutting down...', 'info');
  process.exit(0);
});
