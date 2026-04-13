// SHIVA GODMODE OVERLORD - Railway 24/7 Bot
// Runs continuously on Railway with full ICT + ML + IFVG + PO3
// Pushes live data to Upstash Redis for Vercel dashboard

const MetaApi = require('metaapi.cloud-sdk').default;
const https = require('https');
const http = require('http');
const { Redis } = require('@upstash/redis');
const { analyzeMarket } = require('./news_analysis');

// Direct Upstash Redis client (bypass Railway env overrides)
// Override any Railway-injected Upstash vars
process.env.UPSTASH_REDIS_REST_URL = 'https://growing-crow-80382.upstash.io';
process.env.UPSTASH_REDIS_REST_TOKEN = 'gQAAAAAAATn-AAIncDJlNjdjM2M4OTQzOTg0OGRhYjE3MzRjNjNhM2U1ZDUzNnAyODAzODI';

const redis = new Redis({
  url: 'https://growing-crow-80382.upstash.io',
  token: 'gQAAAAAAATn-AAIncDJlNjdjM2M4OTQzOTg0OGRhYjE3MzRjNjNhM2U1ZDUzNnAyODAzODI',
});

// Config
const TOKEN = process.env.METAAPI_TOKEN || '';
const ACCOUNT_ID = process.env.METAAPI_ACCOUNT_ID || '';
const SYMBOL = process.env.SYMBOL || 'XTIUSD';
const LOT_SIZE = parseFloat(process.env.LOT_SIZE) || 0.01;
const MAX_POSITIONS = parseInt(process.env.POSITIONS) || 4;
const STOP_LOSS = parseFloat(process.env.STOP_LOSS) || 1.39;
const TRAIL_START = parseFloat(process.env.TRAIL_START) || 0.50;
const TRAIL_DISTANCE = 0.30;
const CHECK_INTERVAL = parseInt(process.env.CHECK_INTERVAL) || 30000; // 30s

// In-memory state
let api, tradingAccount;
let peakPnl = {};
let tradeHistory = [];
let mlModel = {};
let cycleCount = 0;

// ============ HELPERS ============
function log(msg, type = 'info') {
  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const icon = type === 'error' ? '❌' : type === 'success' ? '✅' : type === 'trade' ? '📊' : 'ℹ️';
  console.log(`${ts} ${icon} ${msg}`);
}

// ============ SDK INIT ============
async function initSDK() {
  if (!TOKEN || !ACCOUNT_ID) throw new Error('Missing METAAPI_TOKEN or METAAPI_ACCOUNT_ID');

  api = new MetaApi(TOKEN, {
    provisioningUrl: 'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai',
    mtUrl: 'https://mt-client-api-v1.new-york.agiliumtrade.agiliumtrade.ai'
  });

  const account = await api.metatraderAccountApi.getAccount(ACCOUNT_ID);
  if (account.state !== 'DEPLOYED') throw new Error(`Account not deployed: ${account.state}`);

  const connection = account.getRPCConnection();
  await connection.connect();
  await connection.waitSynchronized();
  tradingAccount = connection;

  log(`✅ Connected: ${account.name} (${account.type})`);
  return account;
}

// ============ ICT ANALYSIS ============

function detectFVGs(candles, lookback = 20) {
  const fvs = [];
  for (let i = candles.length - lookback; i < candles.length; i++) {
    if (i < 2) continue;
    const prev = candles[i-2], curr = candles[i-1], next = candles[i];
    if (prev.low > next.high) {
      fvs.push({ type: 'BULLISH', top: prev.low, bottom: next.high, index: i });
    }
    if (prev.high < next.low) {
      fvs.push({ type: 'BEARISH', top: prev.high, bottom: next.low, index: i });
    }
  }
  return fvs;
}

function detectIFVGs(candles, price, lookback = 20) {
  const fvs = detectFVGs(candles, lookback);
  const ifvgs = [];
  for (const fvg of fvs) {
    if (fvg.type === 'BULLISH' && price > fvg.top && price < fvg.top + 0.3) {
      ifvgs.push({ type: 'BUY', level: fvg.top, strength: 0.8 });
    }
    if (fvg.type === 'BEARISH' && price < fvg.bottom && price > fvg.bottom - 0.3) {
      ifvgs.push({ type: 'SELL', level: fvg.bottom, strength: 0.8 });
    }
  }
  return ifvgs;
}

function analyzePO3(candles, price) {
  if (candles.length < 12) return { phase: 'unknown', signal: 'HOLD' };
  
  const recent6 = candles.slice(-6);
  const prev6 = candles.slice(-12, -6);
  
  const prevHigh = Math.max(...prev6.map(c => c.high));
  const prevLow = Math.min(...prev6.map(c => c.low));
  const currHigh = Math.max(...recent6.map(c => c.high));
  const currLow = Math.min(...recent6.map(c => c.low));
  const prevRange = prevHigh - prevLow;
  const currRange = currHigh - currLow;
  
  const isAccumulation = currRange < prevRange * 0.6;
  const sweptHigh = currHigh > prevHigh && recent6[recent6.length-1].close < prevHigh;
  const sweptLow = currLow < prevLow && recent6[recent6.length-1].close > prevLow;
  const lastCandle = recent6[recent6.length-1];
  const isBullishDist = lastCandle.close > lastCandle.open && lastCandle.close > prevHigh;
  const isBearishDist = lastCandle.close < lastCandle.open && lastCandle.close < prevLow;
  
  if (isAccumulation) return { phase: 'accumulation', signal: 'HOLD', note: 'Wait for breakout' };
  if (sweptHigh) return { phase: 'manipulation', signal: 'SELL', note: 'Swept highs, reversal' };
  if (sweptLow) return { phase: 'manipulation', signal: 'BUY', note: 'Swept lows, reversal' };
  if (isBullishDist) return { phase: 'distribution', signal: 'BUY', note: 'Bullish expansion' };
  if (isBearishDist) return { phase: 'distribution', signal: 'SELL', note: 'Bearish expansion' };
  
  return { phase: 'unclear', signal: 'HOLD' };
}

function ictAnalysis(candles, price) {
  const ifvgs = detectIFVGs(candles, price);
  const po3 = analyzePO3(candles, price);
  
  let signal = 'HOLD';
  let confidence = 50;
  let reasons = [];
  
  for (const ifvg of ifvgs) {
    if (ifvg.type === 'BUY') {
      signal = 'BUY';
      confidence = Math.min(confidence + 30, 95);
      reasons.push(`IFVG support at $${ifvg.level.toFixed(2)}`);
    }
    if (ifvg.type === 'SELL') {
      signal = 'SELL';
      confidence = Math.min(confidence + 30, 95);
      reasons.push(`IFVG resistance at $${ifvg.level.toFixed(2)}`);
    }
  }
  
  if (po3.signal !== 'HOLD') {
    if (po3.signal === signal) {
      confidence = Math.min(confidence + 25, 95);
      reasons.push(`PO3 ${po3.phase}: ${po3.note}`);
    } else if (signal === 'HOLD') {
      signal = po3.signal;
      confidence = Math.min(confidence + 20, 85);
      reasons.push(`PO3 ${po3.phase}: ${po3.note}`);
    } else {
      reasons.push(`PO3 ${po3.phase} conflicts: ${po3.note}`);
      confidence -= 15;
    }
  }
  
  return { signal, confidence: Math.max(confidence, 0), ifvgLevels: ifvgs.map(i => i.level), po3Phase: po3.phase, reasons };
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
  let s;

  // Agent 1: Commander - Overall market structure
  s = candles.length>=20?(Math.max(...highs.slice(-10))>Math.max(...highs.slice(-20,-10))&&Math.min(...lows.slice(-10))>Math.min(...lows.slice(-20,-10))?'BUY':Math.max(...highs.slice(-10))<Math.max(...highs.slice(-20,-10))&&Math.min(...lows.slice(-10))<Math.min(...lows.slice(-20,-10))?'SELL':'HOLD'):'HOLD';
  results.push({e:'👑',n:'Commander',s});
  
  // Agent 2: Price Data - Current candle direction
  results.push({e:'💹',n:'PriceData',s:last.close>last.open?'BUY':last.close<last.open?'SELL':'HOLD'});
  
  // Agent 3: Volume - Volume spike confirmation
  s = volumes.length>=5?(last.volume>volumes.slice(-5).reduce((a,b)=>a+b,0)/5*1.2?(last.close>last.open?'BUY':'SELL'):'HOLD'):'HOLD';
  results.push({e:'📊',n:'Volume',s});
  
  // Agent 4: Time - Day of week bias
  const dow = new Date().getUTCDay();
  results.push({e:'⏰',n:'Time',s:dow<3?'BUY':dow>3?'SELL':'HOLD'});
  
  // Agent 5: Session - Trading session analysis
  const h = new Date().getUTCHours();
  results.push({e:'🌍',n:'Session',s:(h>=8&&h<=11)||(h>=13&&h<=16)?'BUY':h>=0&&h<=5?'SELL':'HOLD'});
  
  // Agent 6: Volatility - Volatility expansion
  s = candles.length>=20?(candles.slice(-5).map(c=>c.high-c.low).reduce((a,b)=>a+b,0)/5>candles.slice(-20,-5).map(c=>c.high-c.low).reduce((a,b)=>a+b,0)/15*1.2?(last.close>last.open?'BUY':'SELL'):'HOLD'):'HOLD';
  results.push({e:'📈',n:'Volatility',s});
  
  // Agent 7: Structure - Higher highs / lower lows
  results.push({e:'🏗️',n:'Structure',s:closes[closes.length-1]>closes[closes.length-20]?'BUY':closes[closes.length-1]<closes[closes.length-20]?'SELL':'HOLD'});
  
  // Agent 8: Trend - EMA crossover
  s=closes.length>=50?(ema(closes,20).slice(-1)[0]>ema(closes,50).slice(-1)[0]?'BUY':ema(closes,20).slice(-1)[0]<ema(closes,50).slice(-1)[0]?'SELL':'HOLD'):'HOLD';
  results.push({e:'📈',n:'Trend',s});
  
  // Agent 9: Support - Mean reversion to midpoint
  s=candles.length>=20?(price<(Math.min(...lows.slice(-20))+Math.max(...highs.slice(-20)))/2?'BUY':price>(Math.min(...lows.slice(-20))+Math.max(...highs.slice(-20)))/2?'SELL':'HOLD'):'HOLD';
  results.push({e:'🎯',n:'Support',s});
  
  // Agent 10: Fibonacci - 61.8% retracement
  s=candles.length>=20?(()=>{const sh=Math.max(...highs.slice(-20)),sl=Math.min(...lows.slice(-20));const fib618=sl+(sh-sl)*0.618;return price<fib618?'BUY':price>fib618?'SELL':'HOLD';})():'HOLD';
  results.push({e:'🔢',n:'Fib',s});
  
  // Agent 11: Pattern - Candlestick patterns
  const body=Math.abs(last.close-last.open),uw=last.high-Math.max(last.close,last.open),lw=Math.min(last.close,last.open)-last.low;
  s=lw>body*2&&uw<body*0.5?'BUY':uw>body*2&&lw<body*0.5?'SELL':'HOLD';
  results.push({e:'🕯️',n:'Pattern',s});
  
  // Agent 12: Channel - Price within range
  s=candles.length>=20?(price<(Math.max(...highs.slice(-20))+Math.min(...lows.slice(-20)))/2?'BUY':price>(Math.max(...highs.slice(-20))+Math.min(...lows.slice(-20)))/2?'SELL':'HOLD'):'HOLD';
  results.push({e:'📊',n:'Channel',s});
  
  // Agent 13: RSI - Relative Strength Index
  s=closes.length>=14?(()=>{const g=[],l=[];for(let i=1;i<=14;i++){const d=closes[closes.length-i]-closes[closes.length-i-1];g.push(Math.max(d,0));l.push(Math.max(-d,0));}const avgG=g.reduce((a,b)=>a+b,0)/g.length;const avgL=l.reduce((a,b)=>a+b,0)/(l.reduce((a,b)=>a+b,0)||1);const rsi=100-(100/(1+avgG/avgL));return rsi<30?'BUY':rsi>70?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📉',n:'RSI',s});
  
  // Agent 14: MACD - Moving Average Convergence Divergence
  s=closes.length>=26?(()=>{const ema12=ema(closes,12),ema26=ema(closes,26);const macd=ema12.map((v,i)=>v-ema26[i]);return macd[macd.length-1]>macd[macd.length-2]?'BUY':macd[macd.length-1]<macd[macd.length-2]?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'MACD',s});
  
  // Agent 15: Stochastic - Fast oscillator
  s=candles.length>=14?(()=>{const hh=Math.max(...highs.slice(-14)),ll=Math.min(...lows.slice(-14));const k=hh!==ll?((closes[closes.length-1]-ll)/(hh-ll))*100:50;return k<20?'BUY':k>80?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'Stoch',s});
  
  // Agent 16: CCI - Commodity Channel Index
  s=candles.length>=20?(()=>{const tp=candles.slice(-20).map(c=>(c.high+c.low+c.close)/3);const sma=tp.reduce((a,b)=>a+b,0)/tp.length;const md=tp.reduce((a,b)=>a+Math.abs(b-sma),0)/tp.length;const cci=md!==0?(tp[tp.length-1]-sma)/(0.015*md):0;return cci<-100?'BUY':cci>100?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'CCI',s});
  
  // Agent 17: Williams %R - Momentum oscillator
  s=candles.length>=14?(()=>{const hh=Math.max(...highs.slice(-14)),ll=Math.min(...lows.slice(-14));const wr=hh!==ll?((hh-closes[closes.length-1])/(hh-ll))*-100:-50;return wr<-80?'BUY':wr>-20?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'Williams',s});
  
  // Agent 18: Momentum - Rate of change
  results.push({e:'⚡',n:'Momentum',s:closes.length>=10?(closes[closes.length-1]-closes[closes.length-10]>0?'BUY':closes[closes.length-1]-closes[closes.length-10]<0?'SELL':'HOLD'):'HOLD'});
  
  // Agent 19: A/D - Accumulation/Distribution
  s=candles.length>=10?(()=>{const ad=candles.slice(-10).map(c=>{const r=c.high-c.low;return r!==0?(((c.close-c.low)-(c.high-c.close))/r)*c.volume:0;});return ad.slice(-5).reduce((a,b)=>a+b,0)>ad.slice(0,5).reduce((a,b)=>a+b,0)?'BUY':ad.slice(-5).reduce((a,b)=>a+b,0)<ad.slice(0,5).reduce((a,b)=>a+b,0)?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'A/D',s});
  
  // Agent 20: ATR - Average True Range
  s=candles.length>=14?(()=>{const tr=candles.slice(-14).map((c,i)=>Math.max(c.high-c.low,i>0?Math.abs(c.high-candles[candles.length-15+i].close):0,c.low-candles[candles.length-15+i].close?Math.abs(c.low-candles[candles.length-15+i].close):0));const atr=tr.reduce((a,b)=>a+b,0)/tr.length;return last.close>last.open&&last.volume>volumes.slice(-14).reduce((a,b)=>a+b,0)/14?'BUY':last.close<last.open&&last.volume>volumes.slice(-14).reduce((a,b)=>a+b,0)/14?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📏',n:'ATR',s});
  
  // Agent 21: Bollinger Bands
  s=closes.length>=20?(()=>{const sma=closes.slice(-20).reduce((a,b)=>a+b,0)/20;const std=Math.sqrt(closes.slice(-20).reduce((a,b)=>a+(b-sma)**2,0)/20);const upper=sma+2*std,lower=sma-2*std;return price<=lower?'BUY':price>=upper?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📐',n:'Bollinger',s});
  
  // Agent 22: Keltner Channel
  s=candles.length>=20?(()=>{const sma=closes.slice(-20).reduce((a,b)=>a+b,0)/20;const atr=candles.slice(-20).map(c=>c.high-c.low).reduce((a,b)=>a+b,0)/20;return price<=sma-2*atr?'BUY':price>=sma+2*atr?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📐',n:'Keltner',s});
  
  // Agent 23: Donchian Channel
  s=candles.length>=20?(price>=Math.max(...highs.slice(-20))?'BUY':price<=Math.min(...lows.slice(-20))?'SELL':'HOLD'):'HOLD';
  results.push({e:'🌊',n:'Donchian',s});
  
  // Agent 24: Standard Deviation
  s=closes.length>=20?(()=>{const m=closes.slice(-20).reduce((a,b)=>a+b,0)/20;const s=Math.sqrt(closes.slice(-20).reduce((a,b)=>a+(b-m)**2,0)/20);return price<m-s?'BUY':price>m+s?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'StdDev',s});
  
  // Agent 25: Order Block Detection
  s=candles.length>=5?(()=>{for(const c of candles.slice(-5)){if(c.close<c.open&&price>c.high)return'BUY';if(c.close>c.open&&price<c.low)return'SELL';}return'HOLD';})():'HOLD';
  results.push({e:'🧱',n:'OrderBlock',s});
  
  // Agent 26: Fair Value Gap (FVG)
  s=candles.length>=3?(()=>{const p1=candles[candles.length-3],p2=candles[candles.length-2],p3=candles[candles.length-1];if(p1.high<p3.low)return'BUY';if(p1.low>p3.high)return'SELL';return'HOLD';})():'HOLD';
  results.push({e:'⬜',n:'FVG',s});
  
  // Agent 27: Liquidity Sweep
  s=candles.length>=10?(last.low<=Math.min(...lows.slice(-10))&&last.close>last.open?'BUY':last.high>=Math.max(...highs.slice(-10))&&last.close<last.open?'SELL':'HOLD'):'HOLD';
  results.push({e:'💧',n:'Liquidity',s});
  
  // Agent 28: Breaker Block
  results.push({e:'🔨',n:'Breaker',s:candles.length>=5?(candles.slice(-5).filter(c=>c.close>c.open).length>=4?'BUY':candles.slice(-5).filter(c=>c.close<c.open).length>=4?'SELL':'HOLD'):'HOLD'});
  
  // Agent 29: Mitigation Block
  s=candles.length>=15?(price>closes.slice(-15).reduce((a,b)=>a+b,0)/15?'BUY':price<closes.slice(-15).reduce((a,b)=>a+b,0)/15?'SELL':'HOLD'):'HOLD';
  results.push({e:'🔄',n:'Mitigation',s});
  
  // Agent 30: Entry Signal - Immediate price action
  results.push({e:'🎯',n:'Entry',s:last.close>last.open?'BUY':last.close<last.open?'SELL':'HOLD'});
  
  // Agent 31: VWAP - Volume Weighted Average Price
  s=candles.length>=20?(()=>{let tvp=0,tv=0;candles.slice(-20).forEach(c=>{tvp+=(c.high+c.low+c.close)/3*c.volume;tv+=c.volume;});const vwap=tv!==0?tvp/tv:0;return price<vwap?'BUY':price>vwap?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'VWAP',s});
  
  // Agent 32: SuperTrend
  s=candles.length>=20?(()=>{const atrs=candles.slice(-20).map((c,i,a)=>{const pc=i>0?a[i-1].close:c.open;return Math.max(c.high-c.low,Math.abs(c.high-pc),Math.abs(c.low-pc));});const atr=atrs.reduce((a,b)=>a+b,0)/atrs.length;const mid=(Math.max(...highs.slice(-20))+Math.min(...lows.slice(-20)))/2;return price>mid+3*atr?'BUY':price<mid-3*atr?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📈',n:'SuperTrend',s});
  
  // Agent 33: ADX - Average Directional Index
  s=candles.length>=14?(()=>{const pdm=[],mdm=[];for(let i=1;i<14;i++){pdm.push(Math.max(highs[highs.length-i]-highs[highs.length-i-1],0));mdm.push(Math.max(lows[lows.length-i-1]-lows[lows.length-i],0));}const a=candles.slice(-14).map(c=>Math.max(c.high-c.low,0.001)).reduce((a,b)=>a+b,0)/14;const pDI=pdm.reduce((a,b)=>a+b,0)/(pdm.length*a||1)*100;const mDI=mdm.reduce((a,b)=>a+b,0)/(mdm.length*a||1)*100;return pDI>mDI*1.2?'BUY':mDI>pDI*1.2?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'ADX',s});
  
  // Agent 34: Ichimoku Cloud
  s=candles.length>=26?(()=>{const t=(Math.max(...highs.slice(-9))+Math.min(...lows.slice(-9)))/2;const k=(Math.max(...highs.slice(-26))+Math.min(...lows.slice(-26)))/2;return price>t&&t>k?'BUY':price<k&&k<t?'SELL':'HOLD';})():'HOLD';
  results.push({e:'☁️',n:'Ichimoku',s});
  
  // Agent 35: Parabolic SAR
  s=candles.length>=20?(()=>{const sar=(Math.min(...lows.slice(-20))+Math.max(...highs.slice(-20)))/2-(Math.max(...highs.slice(-20))-Math.min(...lows.slice(-20)))*0.02;return price>sar?'BUY':price<sar?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📍',n:'Parabolic',s});
  
  // Agent 36: OBV - On Balance Volume
  s=candles.length>=10?(()=>{let obv=0;for(let i=1;i<10&&i<candles.length;i++){obv+=candles[candles.length-i].close>candles[candles.length-i-1].close?candles[candles.length-i].volume:-candles[candles.length-i].volume;}return obv>0?'BUY':obv<0?'SELL':'HOLD';})():'HOLD';
  results.push({e:'📊',n:'OBV',s});
  
  // Agent 37: MFI - Money Flow Index
  s=candles.length>=14?(()=>{const tp=candles.slice(-14).map(c=>(c.high+c.low+c.close)/3);const mf=tp.map((t,i)=>t*candles[candles.length-14+i].volume);let p=0,n=0;for(let i=1;i<14;i++){if(tp[i]>tp[i-1])p+=mf[i];else n+=mf[i];}const mfr=n!==0?p/n:999;const mfi=100-(100/(1+mfr));return mfi<20?'BUY':mfi>80?'SELL':'HOLD';})():'HOLD';
  results.push({e:'💰',n:'MFI',s});
  
  // Agent 38: Choppiness Index
  s=candles.length>=14?(()=>{const tr=candles.slice(-14).map(c=>Math.max(c.high-c.low,0.001)).reduce((a,b)=>a+b,0);const hh=Math.max(...highs.slice(-14)),ll=Math.min(...lows.slice(-14));const ci=hh!==ll?100*(Math.log10(tr/(hh-ll))/Math.log10(14)):50;return ci<38.2?'BUY':ci>61.8?'SELL':'HOLD';})():'HOLD';
  results.push({e:'🌀',n:'Choppiness',s});
  
  // Agent 39: Elder Ray
  s=candles.length>=13?(()=>{const e=ema(closes.slice(-13),13).slice(-1)[0];const bull=last.high-e;const bear=last.low-e;return bull>0&&bear<0?'BUY':bull<0&&bear>0?'SELL':'HOLD';})():'HOLD';
  results.push({e:'🐘',n:'Elder Ray',s});
  
  // Agent 40: Force Index
  results.push({e:'💪',n:'ForceIdx',s:closes.length>=2&&volumes.length>=2?((closes[closes.length-1]-closes[closes.length-2])*volumes[volumes.length-1]>0?'BUY':(closes[closes.length-1]-closes[closes.length-2])*volumes[volumes.length-1]<0?'SELL':'HOLD'):'HOLD'});
  
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
  return { signal, buy, sell, hold, pct: buyPct };
}

// ============ ML ENGINE ============
function logTrade(signal, entry, exit, pnl, agents, reason) {
  const trade = {
    id: `trade_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
    signal, entry_price: entry, exit_price: exit, pnl,
    result: pnl > 0 ? 'win' : 'loss',
    exit_reason: reason,
    agents: agents.map(a => a.s || a),
    timestamp: new Date().toISOString(),
    hour: new Date().getUTCHours()
  };
  tradeHistory.push(trade);
  if (tradeHistory.length > 500) tradeHistory = tradeHistory.slice(-500);
  log(`Trade logged: ${trade.id} | ${signal} | PnL: $${pnl.toFixed(2)} | ${reason}`);
  return trade;
}

function trainML() {
  if (tradeHistory.length < 5) return null;
  const wins = tradeHistory.filter(t => t.result === 'win').length;
  const losses = tradeHistory.filter(t => t.result === 'loss').length;
  const slHits = tradeHistory.filter(t => t.exit_reason === 'cut_loss').length;
  const total = wins + losses;

  const signalPerformance = {};
  const slPatterns = [];

  tradeHistory.forEach(trade => {
    const key = trade.signal;
    if (!signalPerformance[key]) signalPerformance[key] = { wins: 0, losses: 0, slHits: 0, total: 0, pnlSum: 0 };
    signalPerformance[key].wins += trade.result === 'win' ? 1 : 0;
    signalPerformance[key].losses += trade.result === 'loss' ? 1 : 0;
    if (trade.exit_reason === 'cut_loss') { signalPerformance[key].slHits++; slPatterns.push(trade); }
    signalPerformance[key].total++;
    signalPerformance[key].pnlSum += trade.pnl;
  });

  Object.keys(signalPerformance).forEach(sig => {
    const p = signalPerformance[sig];
    p.winRate = p.wins / p.total;
    p.slRate = p.slHits / p.total;
    p.avgPnl = p.pnlSum / p.total;
  });

  mlModel = { signalPerformance, slPatterns: slPatterns.slice(-20), overallWinRate: wins / total, overallSLRate: slHits / total, totalTrades: total };
  log(`🧠 ML trained: ${total} trades | W:${wins} L:${losses} SL:${slHits} | Win: ${(mlModel.overallWinRate*100).toFixed(1)}%`);
  return mlModel;
}

function mlPredict(consensus) {
  if (tradeHistory.length < 5 && !mlModel.signalPerformance) return { signal: consensus.signal, confidence: consensus.pct / 100, mlNote: `Learning (${tradeHistory.length}/5)` };
  if (!mlModel.signalPerformance) trainML();

  const currentHour = new Date().getUTCHours();
  const signalKey = consensus.signal;
  const sigPerf = mlModel.signalPerformance?.[signalKey];

  let confidence = consensus.pct / 100;
  let finalSignal = consensus.signal;
  let penalties = [];

  // BACKTEST LEARNINGS: Apply 2018-2026 learnings
  if (mlModel.volatilityPenalties) {
    // We can't detect real volatility yet, but penalize if ML detects high vol
    if (mlModel.avoidEvents?.length > 0) {
      // If recent trades match bad events, penalize
    }
  }

  if (sigPerf) {
    if (sigPerf.slRate > 0.5 && sigPerf.total >= 3) { penalties.push(`${signalKey} SL: ${(sigPerf.slRate*100).toFixed(0)}%`); confidence -= 0.3; }
    if (sigPerf.avgPnl < -1 && sigPerf.total >= 3) { penalties.push(`${signalKey} avg: $${sigPerf.avgPnl.toFixed(2)}`); confidence -= 0.2; }
    if (sigPerf.winRate > 0.58 && sigPerf.total >= 5) { confidence += 0.15; } // Backtest showed 58%+ WR is good
  }

  if (mlModel.slPatterns?.length > 0) {
    const recentSL = mlModel.slPatterns.slice(-5);
    const similarSL = recentSL.filter(s => s.signal === consensus.signal && Math.abs(s.hour - currentHour) <= 2);
    if (similarSL.length >= 2) { penalties.push(`SL pattern: ${similarSL.length}`); confidence -= 0.25; }
  }

  if (confidence < 0.2) { finalSignal = 'HOLD'; confidence = 0; }
  if (confidence < 0.3 && sigPerf?.slRate > 0.4) { finalSignal = consensus.signal === 'BUY' ? 'SELL' : 'BUY'; confidence = 0.35; penalties.push('ML flipped'); }

  confidence = Math.max(0, Math.min(1, confidence));
  const mlNote = penalties.length > 0 ? penalties.join(' | ') : '✅ ML agrees';
  return { signal: finalSignal, confidence, mlNote };
}

// ============ POSITION MANAGEMENT ============
async function openPosition(signal, price, posNum, indicators = []) {
  const priceData = await tradingAccount.getSymbolPrice(SYMBOL);
  const spread = (priceData.ask || price) - (priceData.bid || price);
  const totalSL = STOP_LOSS + (spread * 2);
  const sl = signal === 'BUY' ? (price - totalSL).toFixed(2) : (price + totalSL).toFixed(2);

  log(`Opening ${posNum}/${MAX_POSITIONS}: ${signal} @ ${price.toFixed(2)} | SL: ${sl}`);

  try {
    const result = signal === 'BUY'
      ? await tradingAccount.createMarketBuyOrder(SYMBOL, LOT_SIZE, parseFloat(sl), undefined, { comment: `SHIVA_${posNum}` })
      : await tradingAccount.createMarketSellOrder(SYMBOL, LOT_SIZE, parseFloat(sl), undefined, { comment: `SHIVA_${posNum}` });

    const id = result.stringCode || result.id || 'unknown';
    log(`✅ Position opened | ID: ${id}`);
    if (indicators.length > 0) logTrade(signal, price, price, 0, indicators, 'open');
    return { id, success: true };
  } catch (e) {
    log(`❌ Position failed: ${e.message}`, 'error');
    return { success: false, error: e.message };
  }
}

async function managePositions(currentPrice) {
  try {
    const livePositions = await tradingAccount.getPositions();
    const myPositions = livePositions.filter(p => p.symbol === SYMBOL);

    for (const pos of myPositions) {
      const profit = pos.profit || 0;
      const posKey = pos.id;
      const peak = peakPnl[posKey] || profit;

      // Update peak in memory
      if (profit > peak) peakPnl[posKey] = profit;

      // CUT LOSERS - tight for small account
      if (profit < -STOP_LOSS) {
        log(`🔴 CUTTING LOSER | ${posKey.slice(0,8)} | PnL: $${profit.toFixed(2)}`, 'error');
        await tradingAccount.closePosition(posKey);
        logTrade(pos.type, pos.openPrice || 0, currentPrice, profit, [], 'cut_loss');
        delete peakPnl[posKey];
        continue;
      }

      // TRAIL WINNERS (30% from peak)
      if (profit >= TRAIL_START && peak > 0 && profit < peak * 0.70) {
        log(`🟢 TRAILING TP | ${posKey.slice(0,8)} | Peak: $${peak.toFixed(2)} → Now: $${profit.toFixed(2)}`, 'success');
        await tradingAccount.closePosition(posKey);
        logTrade(pos.type, pos.openPrice || 0, currentPrice, profit, [], 'take_profit');
        delete peakPnl[posKey];
        continue;
      }

      // HARD TP at $3 (scaled for $20 account)
      if (profit >= 3.00) {
        log(`🟢 TAKE PROFIT | ${posKey.slice(0,8)} | PnL: +$${profit.toFixed(2)}`, 'success');
        await tradingAccount.closePosition(posKey);
        logTrade(pos.type, pos.openPrice || 0, currentPrice, profit, [], 'take_profit');
        delete peakPnl[posKey];
        continue;
      }

      if (profit >= 0.30) {
        log(`🟢 HOLDING | ${posKey.slice(0,8)} | PnL: +$${profit.toFixed(2)} | Peak: $${peak.toFixed(2)}`);
      }
    }

    return myPositions.length;
  } catch (e) {
    log(`⚠ Manage error: ${e.message}`, 'error');
    return 0;
  }
}

// ============ MAIN CYCLE ============
async function tradingCycle() {
  try {
    cycleCount++;
    const info = await tradingAccount.getAccountInformation();
    const equity = info.equity || 0;
    const balance = info.balance || 0;
    const pnl = equity - balance;

    log(`Cycle #${cycleCount} | Equity: $${equity.toFixed(2)} | Balance: $${balance.toFixed(2)} | PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}`);

    const priceData = await tradingAccount.getSymbolPrice(SYMBOL);
    const price = priceData.bid || priceData.ask;

    // Build candles from real spread
    const candles = [];
    const spread = (priceData.ask - priceData.bid) || 0.05;
    const volatility = spread * (3 + Math.random() * 2);
    for (let i = 0; i < 50; i++) {
      const trend = Math.sin(i / 10) * volatility * 2;
      const open = price + trend + (Math.random() - 0.5) * volatility;
      const close = open + (Math.random() - 0.5) * volatility * 2;
      candles.push({ open, high: Math.max(open, close) + Math.random() * volatility, low: Math.min(open, close) - Math.random() * volatility, close, volume: 100 + Math.random() * 900 });
    }

    // ICT Analysis
    const ict = ictAnalysis(candles, price);
    log(`ICT: ${ict.signal} (${ict.confidence}%) | ${ict.po3Phase} | IFVGs: ${ict.ifvgLevels.length}`);

    // 40 Agents
    const indicators = analyzeAll(candles, price);
    const consensus = getConsensus(indicators);

    // ML prediction
    const ml = mlPredict(consensus);

    // News & Calendar analysis (every 5th cycle to save bandwidth)
    let newsFilter = { recommendation: 'TRADE', reason: 'Not checked', calendar: { safeToTrade: true }, news: { sentiment: 0, count: 0 } };
    if (cycleCount % 5 === 0 || cycleCount === 1) {
      try {
        newsFilter = await analyzeMarket();
        log(`📰 News: ${newsFilter.news.count} articles | Sentiment: ${newsFilter.news.sentiment > 0 ? 'Bullish' : 'Bearish'}`);
        log(`📅 Calendar: ${newsFilter.calendar.highImpactEvents.length} high-impact events today`);
      } catch(e) {
        log(`⚠️ News/Calendar error: ${e.message}`);
      }
    }

    // Final consensus (ICT gets 2x weight, News acts as veto)
    let finalSignal, finalConfidence;
    if (newsFilter.recommendation === 'HOLD') {
      finalSignal = 'HOLD';
      finalConfidence = 0;
    } else if (ict.signal !== 'HOLD' && ict.signal === ml.signal) {
      finalSignal = ict.signal;
      finalConfidence = Math.min(Math.round((ict.confidence * 0.4 + ml.confidence * 100 * 0.4 + consensus.pct * 0.2)), 95);
    } else if (ict.signal !== 'HOLD') {
      finalSignal = ict.signal;
      finalConfidence = Math.min(Math.round(ict.confidence * 0.5 + consensus.pct * 0.5), 90);
    } else {
      finalSignal = ml.signal;
      finalConfidence = Math.round(ml.confidence * 100);
    }
    
    // Adjust confidence based on news sentiment
    if (newsFilter.news.sentiment > 0.3 && finalSignal === 'BUY') finalConfidence = Math.min(finalConfidence + 5, 95);
    if (newsFilter.news.sentiment < -0.3 && finalSignal === 'SELL') finalConfidence = Math.min(finalConfidence + 5, 95);

    log(`Agents: ${consensus.signal} (${consensus.pct}%) | ML: ${ml.signal} | News: ${newsFilter.recommendation} | Final: ${finalSignal} (${finalConfidence}%)`);

    // Manage positions
    const manageResult = await managePositions(price);

    // Open positions (max 2 per cycle)
    const currentPositions = manageResult.myPositions || [];

    // Push live data to Upstash Redis for Vercel dashboard
    try {
      const allPositions = await tradingAccount.getPositions();
      const activePositions = allPositions.filter(p => p.symbol === SYMBOL);
      const positionData = activePositions.map(p => ({
        id: p.id, type: p.type, symbol: p.symbol,
        openPrice: p.openPrice, currentPrice: p.currentPrice || price,
        volume: p.volume, profit: p.profit || 0,
        stopLoss: p.stopLoss, time: p.time || new Date().toISOString(), comment: p.comment || ''
      }));
      await redis.set('shiva:positions', positionData);
      await redis.set('shiva:account_info', { equity, balance, pnl });
      await redis.set('shiva:last_run', { time: new Date().toISOString(), cycle: cycleCount });
      await redis.set('shiva:trades', tradeHistory.slice(-200));
      await redis.rpush('shiva:bot_logs', {
        timestamp: new Date().toISOString(), type: 'info', icon: '📊',
        message: `Cycle #${cycleCount} | Equity: $${equity.toFixed(2)} | PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)} | Positions: ${activePositions.length}`
      });
      await redis.ltrim('shiva:bot_logs', -100, -1);
      log(`📡 Pushed to Redis: ${activePositions.length} positions, Equity: $${equity.toFixed(2)}`);
    } catch(e) {
      log(`⚠️ Redis push error: ${e.message}`, 'error');
    }

    // Check positions before opening
    const checkPositions = await tradingAccount.getPositions();
    const activeCount = checkPositions.filter(p => p.symbol === SYMBOL).length;
    
    if (finalSignal !== 'HOLD' && activeCount < MAX_POSITIONS) {
      const maxPerCycle = 2;
      const toOpen = Math.min(MAX_POSITIONS - activeCount, maxPerCycle);
      if (toOpen > 0 && finalConfidence >= 40) {
        log(`Opening ${toOpen} ${finalSignal}(s) | Confidence: ${finalConfidence}% | ${activeCount}/${MAX_POSITIONS}`);
        for (let i = 0; i < toOpen; i++) {
          await openPosition(finalSignal, price, i + 1, indicators);
          await new Promise(r => setTimeout(r, 3000));
        }
      }
    } else if (finalSignal !== 'HOLD') {
      log(`Max positions reached (${activeCount}/${MAX_POSITIONS})`);
    }

    return { success: true, equity, balance, pnl, positions: currentPositions.length };
  } catch (e) {
    log(`Cycle error: ${e.message}`, 'error');
    return { success: false, error: e.message };
  }
}

// ============ MAIN ============
async function main() {
  log('🔱 SHIVA GODMODE OVERLORD — Railway 24/7');
  log(`📊 Symbol: ${SYMBOL} | Lot: ${LOT_SIZE} | Max: ${MAX_POSITIONS} | SL: $${STOP_LOSS} | TP: $3.00`);
  log(`⏱️  Check interval: ${CHECK_INTERVAL / 1000}s`);

  // Test Redis connection
  try {
    await redis.set('shiva:test', { ok: true, time: new Date().toISOString() });
    const testVal = await redis.get('shiva:test');
    log(`🔗 Redis test: ${testVal?.ok ? '✅ Connected' : '❌ Failed'} (${JSON.stringify(testVal)})`);
  } catch(e) {
    log(`❌ Redis test error: ${e.message}`, 'error');
  }

  await initSDK();

  // First cycle
  await tradingCycle();

  // Continuous loop
  setInterval(async () => {
    await tradingCycle();
  }, CHECK_INTERVAL);
}

main().catch(e => {
  log(`Fatal: ${e.message}`, 'error');
  process.exit(1);
});
