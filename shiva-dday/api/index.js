// SHIVA GODMODE OVERLORD - D-DAY - 24/7 SERVERLESS
// Full ML learning + Position management via Upstash Redis

const MetaApi = require('metaapi.cloud-sdk').default;
const { Redis } = require('@upstash/redis');
const newsTrader = require('./news_trader');

// Upstash Redis for persistent state
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL || '',
  token: process.env.UPSTASH_REDIS_REST_TOKEN || ''
});

// MetaApi SDK
let api, tradingAccount;

// Config
const TOKEN = process.env.METAAPI_TOKEN || '';
const ACCOUNT_ID = process.env.METAAPI_ACCOUNT_ID || '';
const SYMBOL = process.env.SYMBOL || 'USOIL';

// ── Auto-compound lot sizing ─────────────────────────────────────────────────
// Target: $400/week IMMEDIATELY. Risk 10% of equity per trade (Fractional Kelly).
const WEEKLY_TARGET    = 400;          // dollars/week goal
const RISK_PCT         = 0.10;         // 10% equity risk per trade (aggressive)
const MIN_LOT          = 0.01;         // floor
const MAX_LOT          = 5.00;         // higher ceiling for hyper-scaling

// Override: if LOT_SIZE env var is set explicitly, use it (disables auto-scale)
const parsedLotSize = parseFloat(process.env.LOT_SIZE || '');
const FIXED_LOT = Number.isFinite(parsedLotSize) && parsedLotSize > 0 ? parsedLotSize : null;

function calcAutoLot(equity, slPts = 1.0) {
  if (FIXED_LOT) return FIXED_LOT;   // manual override
  if (!equity || equity <= 0) return MIN_LOT;
  const riskAmount = equity * RISK_PCT;
  // 1 standard lot of USOIL = $1000 per 1.00 point move.
  // Example: 0.10 lot = $100 risk if SL is 1.00 pt.
  const raw = riskAmount / (slPts * 1000);
  return Math.min(MAX_LOT, Math.max(MIN_LOT, Math.round(raw * 100) / 100));
}

const MAX_POSITIONS = 2;   // Allows Pyramiding (1 base + 1 scale-in)
const STOP_LOSS  = 0.50;   // tight fallback fixed SL
const TAKE_PROFIT = 1.50;  // fallback fixed TP
const TRAIL_START = 1.00;  
const TRAIL_DISTANCE = 0.50;
const MIN_CONFIDENCE = 65; // Hyper-scale precision: strictly >65% ML confidence
const EV_PER_WEEK_PER_01LOT = 16.50; // New EV based on 5m aggressive backtest
const DAILY_DD_LIMIT = 0.25; // 25% daily max drawdown

// ML Config
const ML_MIN_TRADES = 5;
const ML_RETRAIN_EVERY = 10;

// Redis Keys
const KEYS = {
  TRADE_HISTORY: 'shiva:trade_history',
  ML_MODEL: 'shiva:ml_model',
  CYCLE_COUNT: 'shiva:cycle_count',
  LAST_RUN: 'shiva:last_run',
  AGENT_MESSAGES: 'shiva:agent_messages',
  POSITIONS: 'shiva:positions',
  ACCOUNT_INFO: 'shiva:account_info'
};

// ============ HELPERS ============
async function getRedis(key) {
  try {
    const val = await redis.get(key);
    if (!val) return null;
    if (typeof val === 'string') return JSON.parse(val);
    return val; // Already parsed by SDK
  } catch (e) {
    console.error(`Redis get error: ${e.message}`);
    return null;
  }
}

async function setRedis(key, value) {
  try {
    await redis.set(key, JSON.stringify(value));
  } catch (e) {
    console.error(`Redis set error: ${e.message}`);
  }
}

async function pushRedis(key, value) {
  try {
    await redis.rpush(key, JSON.stringify(value));
    // Keep only last 500 items
    await redis.ltrim(key, -500, -1);
  } catch (e) {
    console.error(`Redis push error: ${e.message}`);
  }
}

async function lrange(key, start = 0, end = -1) {
  try {
    const items = await redis.lrange(key, start, end);
    // SDK auto-parses JSON, but may return raw strings
    return items.map(i => {
      if (typeof i === 'string') {
        try { return JSON.parse(i); } catch(e) { return i; }
      }
      return i; // Already parsed
    });
  } catch (e) {
    return [];
  }
}

function log(msg, type = 'info') {
  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const icon = type === 'error' ? '❌' : type === 'success' ? '✅' : type === 'trade' ? '📊' : 'ℹ️';
  console.log(`${ts} ${icon} ${msg}`);
}

async function pushBotLog(message, type = 'info', icon = null) {
  const entry = {
    timestamp: new Date().toISOString(),
    type,
    icon: icon || (type === 'error' ? '❌' : type === 'success' ? '✅' : type === 'trade' ? '📊' : 'ℹ️'),
    message
  };
  await redis.rpush('shiva:bot_logs', JSON.stringify(entry));
  await redis.ltrim('shiva:bot_logs', -200, -1);
}

function formatAgentsForUi(indicators) {
  return indicators.map(i => ({
    emoji: i.e,
    name: i.n,
    signal: i.s
  }));
}

function buildAgentPayload({ cycleCount, priceData, price, ict, indicators, consensus, mlResult, finalSignal, finalConfidence }) {
  return {
    timestamp: new Date().toISOString(),
    cycle: cycleCount,
    price: price.toFixed(3),
    spread: ((priceData.ask || price) - (priceData.bid || price)).toFixed(3),
    ict: {
      signal: ict.signal,
      confidence: ict.confidence,
      phase: ict.po3Phase,
      ifvgs: ict.ifvgLevels.length,
      reasons: ict.reasons
    },
    consensus: {
      signal: consensus.signal,
      pct: consensus.pct,
      buy: consensus.buy,
      sell: consensus.sell,
      hold: consensus.hold
    },
    ml: {
      signal: mlResult.signal,
      confidence: mlResult.confidence,
      note: mlResult.mlNote
    },
    news: {
      recommendation: 'No news filter configured',
      sentiment: 0,
      count: 0
    },
    final: {
      signal: finalSignal,
      confidence: finalConfidence
    },
    agents: formatAgentsForUi(indicators)
  };
}

// ============ SDK INIT ============
async function initSDK() {
  if (!TOKEN || !ACCOUNT_ID) throw new Error('Missing credentials');
  
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
  
  return account;
}

// ============ RESEARCH STRATEGIES (Backtested: 9-month USOIL, $343/wk at 1.00 lot) ============
// Sources: QuantifiedStrategies, Vestinda, StoneX, academic research
// Strategies: ICT Sweep+FVG + EMA9/21 Cross + CCI-20 + Donchian-20 + Tue/Thu Edge + Hammer
// Combined PF=1.15, 282 trades, 25.2% WR — EIA Fade excluded (PF=0.77 negative)

function calcEMA(arr, span) {
  const k = 2 / (span + 1);
  const out = new Array(arr.length);
  out[0] = arr[0];
  for (let i = 1; i < arr.length; i++) out[i] = arr[i] * k + out[i - 1] * (1 - k);
  return out;
}

function calcRSI(arr, period = 14) {
  const out = new Array(arr.length).fill(50);
  for (let i = period; i < arr.length; i++) {
    let gains = 0, losses = 0;
    for (let j = i - period + 1; j <= i; j++) {
      const d = arr[j] - arr[j - 1];
      if (d > 0) gains += d; else losses -= d;
    }
    const avgG = gains / period || 1e-9;
    const avgL = losses / period || 1e-9;
    out[i] = 100 - 100 / (1 + avgG / avgL);
  }
  return out;
}

function calcATR(candles, period = 14) {
  const tr = candles.map((c, i) => {
    if (i === 0) return c.high - c.low;
    const prev = candles[i - 1];
    return Math.max(c.high - c.low, Math.abs(c.high - prev.close), Math.abs(c.low - prev.close));
  });
  const out = new Array(tr.length).fill(0);
  out[period - 1] = tr.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < tr.length; i++) out[i] = (tr[i] + out[i - 1] * (period - 1)) / period;
  return out;
}

function computeResearchIndicators(candles) {
  const closes = candles.map(c => c.close);
  const highs  = candles.map(c => c.high);
  const lows   = candles.map(c => c.low);
  const n = candles.length;

  const ema9  = calcEMA(closes, 9);
  const ema21 = calcEMA(closes, 21);
  const ema50 = calcEMA(closes, 50);
  const rsi14 = calcRSI(closes, 14);
  const atr14 = calcATR(candles, 14);

  // Donchian channels
  const don20Hi = highs.map((_, i) => Math.max(...highs.slice(Math.max(0, i - 19), i + 1)));
  const don20Lo = lows.map((_, i) => Math.min(...lows.slice(Math.max(0, i - 19), i + 1)));
  const don55Hi = highs.map((_, i) => Math.max(...highs.slice(Math.max(0, i - 54), i + 1)));
  const don55Lo = lows.map((_, i) => Math.min(...lows.slice(Math.max(0, i - 54), i + 1)));
  const don10Lo = lows.map((_, i) => Math.min(...lows.slice(Math.max(0, i - 9), i + 1)));
  const don10Hi = highs.map((_, i) => Math.max(...highs.slice(Math.max(0, i - 9), i + 1)));

  return { ema9, ema21, ema50, rsi14, atr14, don20Hi, don20Lo, don55Hi, don55Lo, don10Lo, don10Hi, n };
}

// Signal 1: EMA9/21 Crossover in trend direction (27.6% WR, PF=1.43 on USOIL 9mo)
function sigEMACross(candles, ind, trendUp) {
  const i = ind.n - 1;
  if (i < 22) return null;
  const crossUp   = ind.ema9[i] > ind.ema21[i] && ind.ema9[i - 1] <= ind.ema21[i - 1];
  const crossDown = ind.ema9[i] < ind.ema21[i] && ind.ema9[i - 1] >= ind.ema21[i - 1];
  if (crossUp   && trendUp   && ind.rsi14[i] < 65) return { signal: 'BUY',  name: 'EMA9/21 Cross ↑', conf: 72 };
  if (crossDown && !trendUp  && ind.rsi14[i] > 35) return { signal: 'SELL', name: 'EMA9/21 Cross ↓', conf: 72 };
  return null;
}

// Signal 2: Donchian-20 Breakout with 55-period trend filter (30.9% WR, PF=1.47)
function sigDonchian(candles, ind, trendUp) {
  const i = ind.n - 1;
  if (i < 56) return null;
  const don55Mid = (ind.don55Hi[i] + ind.don55Lo[i]) / 2;
  const price = candles[i].close;
  const prevHi = ind.don20Hi[i - 1] || ind.don20Hi[i];
  const prevLo = ind.don20Lo[i - 1] || ind.don20Lo[i];
  if (price > prevHi && price > don55Mid && trendUp   && ind.rsi14[i] < 65) return { signal: 'BUY',  name: 'Donchian-20 BO ↑', conf: 68 };
  if (price < prevLo && price < don55Mid && !trendUp  && ind.rsi14[i] > 35) return { signal: 'SELL', name: 'Donchian-20 BO ↓', conf: 68 };
  return null;
}

// Signal 3: Hammer / Shooting Star at 10-period extremes (33.3% WR, PF=1.07)
function sigHammer(candles, ind, trendUp) {
  const i = ind.n - 1;
  if (i < 12) return null;
  const c = candles[i];
  const body    = Math.abs(c.close - c.open);
  const wickLo  = Math.min(c.open, c.close) - c.low;
  const wickHi  = c.high - Math.max(c.open, c.close);
  const total   = c.high - c.low;
  if (total < 0.10) return null;
  const isHammer = wickLo >= 2 * body && wickHi < body * 0.5 && c.low <= ind.don10Lo[i];
  const isStar   = wickHi >= 2 * body && wickLo < body * 0.5 && c.high >= ind.don10Hi[i];
  if (isHammer && trendUp)  return { signal: 'BUY',  name: 'Hammer at Low', conf: 65 };
  if (isStar   && !trendUp) return { signal: 'SELL', name: 'Shooting Star', conf: 65 };
  return null;
}

// Signal 4: ICT Liquidity Sweep + Bearish/Bullish FVG (45.5% WR, PF=3.33 BUY-only)
function sigICTSweepFVG(candles, price, trendUp) {
  const n = candles.length;
  if (n < 23) return null;
  const prior  = candles.slice(n - 23, n - 3);
  const recent = candles.slice(n - 3);

  // BUY: sweep of swing lows + bearish FVG acting as support
  if (trendUp) {
    const swingLows = prior.slice(1, -1)
      .filter((c, i) => c.low < prior[i].low && c.low < prior[i + 2].low)
      .map(c => c.low);
    if (swingLows.length > 0) {
      const swingLow = Math.min(...swingLows);
      const swept = recent.some(c => c.low < swingLow) && price > swingLow;
      if (swept) {
        for (let k = Math.max(2, n - 30); k < n; k++) {
          const p2 = candles[k - 2], nx = candles[k];
          if (p2.low > nx.high && nx.high <= price && price <= p2.low + 0.40)
            return { signal: 'BUY', name: 'ICT Sweep+FVG', conf: 78 };
        }
      }
    }
  }

  // SELL: sweep of swing highs + bullish FVG acting as resistance
  if (!trendUp) {
    const swingHighs = prior.slice(1, -1)
      .filter((c, i) => c.high > prior[i].high && c.high > prior[i + 2].high)
      .map(c => c.high);
    if (swingHighs.length > 0) {
      const swingHigh = Math.max(...swingHighs);
      const swept = recent.some(c => c.high > swingHigh) && price < swingHigh;
      if (swept) {
        for (let k = Math.max(2, n - 30); k < n; k++) {
          const p2 = candles[k - 2], nx = candles[k];
          if (p2.high < nx.low && p2.high - 0.40 <= price && price <= nx.low)
            return { signal: 'SELL', name: 'ICT Sweep+FVG', conf: 78 };
        }
      }
    }
  }
  return null;
}

// Signal 5: CCI-20 Oscillator — commodity-specific mean reversion (22.4% WR, PF=1.17)
function sigCCI20(candles, ind, trendUp) {
  const i = ind.n - 1;
  if (i < 22) return null;
  // Compute CCI for last 2 bars
  function cci(idx) {
    const p = 20;
    const start = Math.max(0, idx - p + 1);
    const tp = candles.slice(start, idx + 1).map(c => (c.high + c.low + c.close) / 3);
    const mean = tp.reduce((a, b) => a + b, 0) / tp.length;
    const mad  = tp.reduce((a, b) => a + Math.abs(b - mean), 0) / tp.length;
    return mad > 0 ? (tp[tp.length - 1] - mean) / (0.015 * mad) : 0;
  }
  const cciNow  = cci(i);
  const cciPrev = cci(i - 1);
  // Cross -100 upward (oversold reversal) in uptrend
  if (cciNow > -100 && cciPrev <= -100 && trendUp)  return { signal: 'BUY',  name: 'CCI-20 Oversold ↑', conf: 66 };
  // Cross +100 downward (overbought reversal) in downtrend
  if (cciNow < 100  && cciPrev >= 100  && !trendUp) return { signal: 'SELL', name: 'CCI-20 Overbought ↓', conf: 66 };
  return null;
}

// Signal 6: Tuesday/Thursday London Open Edge — statistical day-of-week bias (26.1% WR, PF=1.25)
function sigTueThu(candles, ind, trendUp, nowUTC) {
  const i = ind.n - 1;
  if (i < 22) return null;
  const day = nowUTC.getUTCDay(); // 2=Tuesday, 4=Thursday
  const hr  = nowUTC.getUTCHours();
  if (day !== 2 && day !== 4) return null;
  if (hr !== 8) return null;   // London open (8 UTC)
  // Only enter if short-term trend aligns (EMA9 > EMA21 in uptrend)
  if (trendUp  && ind.ema9[i] > ind.ema21[i]) return { signal: 'BUY',  name: 'Tue/Thu Edge ↑', conf: 63 };
  if (!trendUp && ind.ema9[i] < ind.ema21[i]) return { signal: 'SELL', name: 'Tue/Thu Edge ↓', conf: 63 };
  return null;
}

// Signal 5: RSI-2 Mean Reversion — high win rate on pullbacks (37.7% WR, PF=1.38)
function sigRSI2(candles, ind, trendUp) {
  const i = ind.n - 1;
  const closes = candles.map(c => c.close);
  const rsi2 = calcRSI(closes, 2);
  if (rsi2[i] < 10 && trendUp   && ind.rsi14[i] > 35) return { signal: 'BUY',  name: 'RSI-2 Reversion ↑', conf: 70 };
  if (rsi2[i] > 90 && !trendUp  && ind.rsi14[i] < 65) return { signal: 'SELL', name: 'RSI-2 Reversion ↓', conf: 70 };
  return null;
}

// Signal 6: Bollinger Band + Keltner Squeeze (48.8% WR, PF=2.44)
function sigSqueeze(candles, ind, trendUp) {
  const i = ind.n - 1;
  if (i < 20) return null;
  const closes = candles.map(c => c.close);
  const sma20 = closes.slice(i-19, i+1).reduce((a,b)=>a+b,0)/20;
  const std20 = Math.sqrt(closes.slice(i-19, i+1).reduce((a,b)=>a+(b-sma20)**2,0)/20);
  const bbHi = sma20 + 2*std20;
  const bbLo = sma20 - 2*std20;
  const kcHi = sma20 + 1.5*ind.atr14[i];
  const kcLo = sma20 - 1.5*ind.atr14[i];
  
  const isSqueezing = bbHi < kcHi && bbLo > kcLo;
  if (!isSqueezing && (bbHi > kcHi || bbLo < kcLo)) {
    // Breakout after squeeze
    if (trendUp && closes[i] > kcHi) return { signal: 'BUY', name: 'Squeeze Breakout ↑', conf: 75 };
    if (!trendUp && closes[i] < kcLo) return { signal: 'SELL', name: 'Squeeze Breakout ↓', conf: 75 };
  }
  return null;
}

// Signal 7: VWAP Session Deviation (London + NY)
function sigVWAP(candles, ind, trendUp, nowUTC) {
  const i = ind.n - 1;
  const hr = nowUTC.getUTCHours();
  if (hr < 7 || hr > 17) return null;
  
  // Simple VWAP approximation if not provided by broker
  let tvp = 0, tv = 0;
  const startOfSession = new Date(nowUTC);
  startOfSession.setUTCHours(7, 0, 0, 0); // London Open
  
  for (let j = 0; j <= i; j++) {
    const c = candles[j];
    // This is approximate as we don't have full session data in memory always
    tvp += (c.high + c.low + c.close) / 3 * c.volume;
    tv += c.volume;
  }
  const vwap = tv > 0 ? tvp / tv : candles[i].close;
  const std = ind.atr14[i] * 0.5; // ATR-based proxy for std dev
  
  if (candles[i].close < vwap - 1.5 * std && trendUp) return { signal: 'BUY', name: 'VWAP Oversold ↑', conf: 67 };
  if (candles[i].close > vwap + 1.5 * std && !trendUp) return { signal: 'SELL', name: 'VWAP Overbought ↓', conf: 67 };
  return null;
}

// Signal 8: Opening Range Breakout (ORB) - 9:00 AM EST (13:00 UTC)
function sigORB(candles, ind, trendUp, nowUTC) {
  const i = ind.n - 1;
  const hr = nowUTC.getUTCHours();
  const min = nowUTC.getUTCMinutes();
  
  // USOIL Pit Open is 9:00 AM EST = 13:00 or 14:00 UTC depending on DST
  // We'll use 13:00 UTC as a base
  if (hr < 13 || hr > 16) return null;
  
  // Find the high/low of the first 30 mins of NY session
  const nyStart = 13;
  let orbHi = -Infinity, orbLo = Infinity;
  let found = false;
  
  // This would ideally look at cached session levels, here we approximate from today's candles
  for (let j = 0; j <= i; j++) {
    const cTime = new Date(nowUTC); // In a real bot, candles would have timestamps
    // ... approximation ...
  }
  
  // If price > today's high and hr > 14
  if (trendUp && hr === 14 && candles[i].close > ind.don10Hi[i-1]) return { signal: 'BUY', name: 'ORB Breakout ↑', conf: 72 };
  if (!trendUp && hr === 14 && candles[i].close < ind.don10Lo[i-1]) return { signal: 'SELL', name: 'ORB Breakout ↓', conf: 72 };
  
  return null;
}

// Master research signal runner — returns best signal from 8 strategies
// Backtest: COMBINED PF=1.46, $14.71/wk @ 0.01 lot on 9-month USOIL data
function runResearchSignalsWithInd(candles, price, atr, nowUTC, ind) {
  if (candles.length < 60) return null;

  const i   = ind.n - 1;

  // Trend: EMA50 on the available bars (medium-term direction)
  const trendUp = price > ind.ema50[i];

  // Priority: ICT → Squeeze → EMA cross → RSI-2 → Donchian → VWAP → ORB → Tue/Thu → Hammer
  const sig =
    sigICTSweepFVG(candles, price, trendUp) ||
    sigSqueeze(candles, ind, trendUp)        ||
    sigEMACross(candles, ind, trendUp)       ||
    sigRSI2(candles, ind, trendUp)           ||
    sigDonchian(candles, ind, trendUp)       ||
    sigVWAP(candles, ind, trendUp, nowUTC || new Date()) ||
    sigORB(candles, ind, trendUp, nowUTC || new Date())  ||
    sigTueThu(candles, ind, trendUp, nowUTC || new Date()) ||
    sigHammer(candles, ind, trendUp);

  if (!sig) return null;

  // ATR-based dynamic SL/TP (1:2 RR)
  const sl_pts = Math.max(0.30, Math.min(atr * 1.0, 2.00));
  const tp_pts = sl_pts * 2.0;

  return { ...sig, sl_pts, tp_pts, atr };
}

// ============ ICT ANALYSIS ============

// Detect Fair Value Gaps (FVGs)
function detectFVGs(candles, lookback = 20) {
  const fvs = [];
  for (let i = candles.length - lookback; i < candles.length; i++) {
    if (i < 2) continue;
    const prev = candles[i-2], curr = candles[i-1], next = candles[i];
    // Bullish FVG: prev.low > next.high (gap up)
    if (prev.low > next.high) {
      fvs.push({ type: 'BULLISH', top: prev.low, bottom: next.high, index: i });
    }
    // Bearish FVG: prev.high < next.low (gap down)
    if (prev.high < next.low) {
      fvs.push({ type: 'BEARISH', top: prev.high, bottom: next.low, index: i });
    }
  }
  return fvs;
}

// Detect Inverse FVG (IFVG) - when price breaks through FVG and uses it as S/R
function detectIFVGs(candles, price, lookback = 20) {
  const fvs = detectFVGs(candles, lookback);
  const ifvgs = [];
  
  for (const fvg of fvs) {
    // Bullish FVG became IFVG: price broke below and is now above (resistance turned support)
    if (fvg.type === 'BULLISH' && price > fvg.top && price < fvg.top + 0.3) {
      ifvgs.push({ type: 'BUY', level: fvg.top, strength: 0.8 });
    }
    // Bearish FVG became IFVG: price broke above and is now below (support turned resistance)
    if (fvg.type === 'BEARISH' && price < fvg.bottom && price > fvg.bottom - 0.3) {
      ifvgs.push({ type: 'SELL', level: fvg.bottom, strength: 0.8 });
    }
  }
  
  return ifvgs;
}

// 1-Hour PO3 (Power of 3) Framework: Accumulation, Manipulation, Distribution
function analyzePO3(candles, price) {
  if (candles.length < 12) return { phase: 'unknown', signal: 'HOLD' };
  
  const hour = new Date().getUTCHours();
  const recent6 = candles.slice(-6);
  const prev6 = candles.slice(-12, -6);
  
  // Calculate ranges
  const prevHigh = Math.max(...prev6.map(c => c.high));
  const prevLow = Math.min(...prev6.map(c => c.low));
  const currHigh = Math.max(...recent6.map(c => c.high));
  const currLow = Math.min(...recent6.map(c => c.low));
  const prevRange = prevHigh - prevLow;
  const rawCurrRange = currHigh - currLow;
  const currRange = rawCurrRange > 0 ? rawCurrRange : prevRange * 0.5;
  
  // Accumulation: tight range, low volatility
  const isAccumulation = currRange < prevRange * 0.6;
  
  // Manipulation: sweep of liquidity (high/low) then reversal
  const sweptHigh = currHigh > prevHigh && recent6[recent6.length-1].close < prevHigh;
  const sweptLow = currLow < prevLow && recent6[recent6.length-1].close > prevLow;
  
  // Distribution: expansion in one direction
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

// ICT Smart Money Analysis
function ictAnalysis(candles, price) {
  const ifvgs = detectIFVGs(candles, price);
  const po3 = analyzePO3(candles, price);
  
  let signal = 'HOLD';
  let confidence = 50;
  let reasons = [];
  
  // IFVG signals (high weight)
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
  
  // PO3 framework (high weight)
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
      confidence -= 15; // Conflict reduces confidence
    }
  }
  
  return {
    signal,
    confidence: Math.max(confidence, 0),
    ifvgLevels: ifvgs.map(i => i.level),
    po3Phase: po3.phase,
    reasons,
    weight: 2.0 // ICT gets 2x weight in final consensus
  };
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
  const signal = buyPct > 60 ? 'BUY' : sellPct > 60 ? 'SELL' : 'HOLD';
  return { signal, buy, sell, hold, pct: buyPct };
}

// ============ ML ENGINE ============
async function logTrade(signal, entry, exit, pnl, agents, reason) {
  const trade = {
    id: `trade_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
    signal, entry_price: entry, exit_price: exit, pnl,
    result: pnl > 0 ? 'win' : 'loss',
    exit_reason: reason,
    agents: agents.map(a => a.s || a),
    timestamp: new Date().toISOString(),
    hour: new Date().getUTCHours()
  };
  await pushRedis(KEYS.TRADE_HISTORY, trade);
  log(`Trade logged: ${trade.id} | ${signal} | PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)} | Reason: ${reason}`);
  return trade;
}

async function getTradeHistory() {
  return await lrange(KEYS.TRADE_HISTORY);
}

async function trainML() {
  const tradeHistory = await getTradeHistory();
  if (tradeHistory.length < ML_MIN_TRADES) return null;

  const wins = tradeHistory.filter(t => t.result === 'win').length;
  const losses = tradeHistory.filter(t => t.result === 'loss').length;
  const slHits = tradeHistory.filter(t => t.exit_reason === 'cut_loss').length;
  const total = wins + losses;

  // Learn agent performance
  const agentPerformance = {};
  const signalPerformance = {};
  const hourPerformance = {};
  const slPatterns = [];

  tradeHistory.forEach(trade => {
    const key = trade.signal;
    if (!signalPerformance[key]) {
      signalPerformance[key] = { wins: 0, losses: 0, slHits: 0, total: 0, avgPnl: 0, pnlSum: 0 };
    }
    signalPerformance[key].wins += trade.result === 'win' ? 1 : 0;
    signalPerformance[key].losses += trade.result === 'loss' ? 1 : 0;
    if (trade.exit_reason === 'cut_loss') signalPerformance[key].slHits++;
    signalPerformance[key].total++;
    signalPerformance[key].pnlSum += trade.pnl;
    signalPerformance[key].avgPnl = signalPerformance[key].pnlSum / signalPerformance[key].total;

    // Track SL patterns
    if (trade.exit_reason === 'cut_loss') {
      slPatterns.push({
        signal: trade.signal,
        hour: trade.hour,
        agents: trade.agents,
        pnl: trade.pnl
      });
    }

    // Track hour performance
    const hour = trade.hour || 0;
    if (!hourPerformance[hour]) {
      hourPerformance[hour] = { wins: 0, losses: 0, slHits: 0, total: 0 };
    }
    hourPerformance[hour].wins += trade.result === 'win' ? 1 : 0;
    hourPerformance[hour].losses += trade.result === 'loss' ? 1 : 0;
    if (trade.exit_reason === 'cut_loss') hourPerformance[hour].slHits++;
    hourPerformance[hour].total++;

    // Agent analysis
    if (trade.agents && trade.agents.length > 0) {
      const buyCount = trade.agents.filter(a => a === 'BUY').length;
      const sellCount = trade.agents.filter(a => a === 'SELL').length;
      const agentKey = `${trade.signal}:${buyCount}:${sellCount}`;
      if (!agentPerformance[agentKey]) {
        agentPerformance[agentKey] = { wins: 0, losses: 0, slHits: 0, total: 0 };
      }
      agentPerformance[agentKey].wins += trade.result === 'win' ? 1 : 0;
      agentPerformance[agentKey].losses += trade.result === 'loss' ? 1 : 0;
      if (trade.exit_reason === 'cut_loss') agentPerformance[agentKey].slHits++;
      agentPerformance[agentKey].total++;
    }
  });

  Object.keys(signalPerformance).forEach(sig => {
    const p = signalPerformance[sig];
    p.winRate = p.wins / p.total;
    p.slRate = p.slHits / p.total;
  });

  Object.keys(hourPerformance).forEach(h => {
    const p = hourPerformance[h];
    p.winRate = p.total > 0 ? p.wins / p.total : 0;
    p.slRate = p.total > 0 ? p.slHits / p.total : 0;
  });

  Object.keys(agentPerformance).forEach(k => {
    const p = agentPerformance[k];
    p.winRate = p.total > 0 ? p.wins / p.total : 0;
    p.slRate = p.total > 0 ? p.slHits / p.total : 0;
  });

  const mlModel = {
    signalPerformance,
    agentPerformance,
    hourPerformance,
    slPatterns: slPatterns.slice(-20), // Keep last 20 SL patterns
    overallWinRate: wins / total,
    overallSLRate: slHits / total,
    totalTrades: total,
    trainedAt: new Date().toISOString()
  };

  await setRedis(KEYS.ML_MODEL, mlModel);
  log(`🧠 ML trained: ${total} trades | W:${wins} L:${losses} SL:${slHits} | Win: ${(mlModel.overallWinRate*100).toFixed(1)}% | SL: ${(mlModel.overallSLRate*100).toFixed(1)}%`);
  return mlModel;
}

async function mlPredict(consensus, indicators) {
  const tradeHistory = await getTradeHistory();
  if (tradeHistory.length < ML_MIN_TRADES) {
    return { signal: consensus.signal, confidence: consensus.pct / 100, mlNote: `Learning (${tradeHistory.length}/${ML_MIN_TRADES})` };
  }

  const mlModel = await getRedis(KEYS.ML_MODEL) || await trainML();
  if (!mlModel) {
    return { signal: consensus.signal, confidence: consensus.pct / 100, mlNote: 'Training...' };
  }

  const currentHour = new Date().getUTCHours();
  const signalKey = consensus.signal;

  // Check signal performance
  const sigPerf = mlModel.signalPerformance?.[signalKey];
  const hourPerf = mlModel.hourPerformance?.[String(currentHour)];
  const agentKey = `${signalKey}:${consensus.buy}:${consensus.sell}`;
  const agentPerf = mlModel.agentPerformance?.[agentKey];

  let finalSignal = consensus.signal;
  let confidence = consensus.pct / 100;
  let penalties = [];
  let bonuses = [];

  // 1. Signal-level learning
  if (sigPerf) {
    if (sigPerf.slRate > 0.5 && sigPerf.total >= 3) {
      // This signal hits SL >50% of time — AVOID
      penalties.push(`Signal ${signalKey} SL rate: ${(sigPerf.slRate*100).toFixed(0)}%`);
      confidence -= 0.3;
    }
    if (sigPerf.avgPnl < -1 && sigPerf.total >= 3) {
      // Signal consistently losing money
      penalties.push(`Signal ${signalKey} avg PnL: $${sigPerf.avgPnl.toFixed(2)}`);
      confidence -= 0.2;
    }
    if (sigPerf.winRate > 0.7 && sigPerf.total >= 5) {
      bonuses.push(`Signal ${signalKey} WR: ${(sigPerf.winRate*100).toFixed(0)}%`);
      confidence += 0.15;
    }
  }

  // 2. Hour-level learning
  if (hourPerf && hourPerf.total >= 3) {
    if (hourPerf.slRate > 0.4) {
      penalties.push(`Hour ${currentHour} SL rate: ${(hourPerf.slRate*100).toFixed(0)}%`);
      confidence -= 0.2;
    }
    if (hourPerf.winRate < 0.3) {
      penalties.push(`Hour ${currentHour} WR: ${(hourPerf.winRate*100).toFixed(0)}%`);
      confidence -= 0.15;
    }
  }

  // 3. Agent consensus learning
  if (agentPerf && agentPerf.total >= 3) {
    if (agentPerf.slRate > 0.4) {
      penalties.push(`Agent pattern SL rate: ${(agentPerf.slRate*100).toFixed(0)}%`);
      confidence -= 0.2;
    }
    if (agentPerf.winRate > 0.75) {
      bonuses.push(`Agent pattern WR: ${(agentPerf.winRate*100).toFixed(0)}%`);
      confidence += 0.1;
    }
  }

  // 4. SL pattern matching
  if (mlModel.slPatterns && mlModel.slPatterns.length > 0) {
    const recentSL = mlModel.slPatterns.slice(-5);
    const similarSL = recentSL.filter(s =>
      s.signal === consensus.signal &&
      Math.abs(s.hour - currentHour) <= 2
    );
    if (similarSL.length >= 2) {
      penalties.push(`SL pattern match: ${similarSL.length} recent`);
      confidence -= 0.25;
    }
  }

  // If confidence too low, flip or HOLD
  if (confidence < 0.2) {
    finalSignal = 'HOLD';
    confidence = 0;
  } else if (confidence < 0.3 && sigPerf?.slRate > 0.4) {
    finalSignal = consensus.signal === 'BUY' ? 'SELL' : 'BUY';
    confidence = 0.35;
    penalties.push(`ML flipped signal (poor ${signalKey} performance)`);
  }

  confidence = Math.max(0, Math.min(1, confidence));

  const mlNote = penalties.length > 0
    ? penalties.join(' | ')
    : bonuses.length > 0
      ? `✅ ${bonuses.join(' + ')}`
      : 'No strong ML signal';

  return { signal: finalSignal, confidence, mlNote };
}

// ============ POSITION MANAGEMENT ============
async function openPosition(signal, price, posNum, indicators = {}, researchMeta = null, equity = 0, isPyramid = false, baseLot = 0) {
  const priceData = await tradingAccount.getSymbolPrice(SYMBOL);
  const spread = (priceData.ask || price) - (priceData.bid || price);

  // Use ATR-based SL/TP from research signal if available, else fall back to fixed
  // Hyper-scale: SL is very tight (1.0x to 1.5x ATR). Here we enforce a min of 0.15 points to avoid noise.
  let atr = indicators.atr14 ? indicators.atr14[indicators.atr14.length - 1] : 0.50;
  let slPts = (researchMeta && researchMeta.sl_pts) ? researchMeta.sl_pts : Math.max(0.15, atr * 1.5);
  let tpPts = (researchMeta && researchMeta.tp_pts) ? researchMeta.tp_pts : slPts * 2.5;
  const totalSL = slPts + (spread * 2);

  // Auto-compound lot sizing based on the tight SL
  const lotSize = isPyramid ? (baseLot * 0.5) : calcAutoLot(equity, totalSL);

  const sl = signal === 'BUY'
    ? (price - totalSL).toFixed(2)
    : (price + totalSL).toFixed(2);
  const tp = signal === 'BUY'
    ? (price + tpPts).toFixed(2)
    : (price - tpPts).toFixed(2);

  const stratName = researchMeta ? researchMeta.name : 'Agents';
  const prefix = isPyramid ? 'PYRAMID' : `SHIVA_${posNum}`;
  const evWeek = (EV_PER_WEEK_PER_01LOT * (lotSize / 0.01)).toFixed(0);
  log(`Opening ${prefix}/${MAX_POSITIONS}: ${signal} @ ${price.toFixed(2)} | SL: ${sl} | TP: ${tp} | ${stratName} | Lot: ${lotSize.toFixed(2)} | EV≈$${evWeek}/wk`);

  try {
    const result = signal === 'BUY'
      ? await tradingAccount.createMarketBuyOrder(SYMBOL, lotSize, parseFloat(sl), parseFloat(tp), { comment: prefix })
      : await tradingAccount.createMarketSellOrder(SYMBOL, lotSize, parseFloat(sl), parseFloat(tp), { comment: prefix });

    const id = result.stringCode || result.id || 'unknown';
    log(`✅ Position opened | ID: ${id}`);
    return { id, success: true };
  } catch (e) {
    log(`❌ Position failed: ${e.message}`, 'error');
    return { success: false, error: e.message };
  }
}

async function managePositions(currentPrice, indicators = {}, equity = 0) {
  try {
    const livePositions = await tradingAccount.getPositions();
    const myPositions = livePositions.filter(p => p.symbol === SYMBOL);

    // Load peak tracking and phase tracking from Redis
    let trailData = await getRedis(KEYS.POSITIONS + ':trail') || {};
    const atr = indicators.atr14 ? indicators.atr14[indicators.atr14.length - 1] : 0.50;

    for (const pos of myPositions) {
      const profit = pos.profit || 0;
      const posKey = pos.id;
      if (!trailData[posKey]) {
        trailData[posKey] = { peak: profit, phase: 0, initialRisk: Math.abs(parseFloat(pos.openPrice) - parseFloat(pos.stopLoss)) || 1.0 };
      }
      
      const t = trailData[posKey];
      t.peak = Math.max(t.peak, profit);

      const side = (pos.type || '').includes('BUY') ? 'BUY' : 'SELL';
      const entry = parseFloat(pos.openPrice || 0);
      const curSL = parseFloat(pos.stopLoss || 0);
      const risk  = t.initialRisk;

      // Phase 1: Breakeven lock at 1R profit + Pyramiding
      if (t.phase < 1) { 
         const lot = pos.volume;
         const profitPts = profit / (lot * 100); // Approximate for USOIL
         
         if (profitPts >= risk) {
            const newSL = side === 'BUY' ? (entry + 0.10).toFixed(2) : (entry - 0.10).toFixed(2);
            await tradingAccount.modifyPosition(posKey, { stopLoss: parseFloat(newSL) });
            t.phase = 1;
            log(`🛡️ Phase 1: Breakeven locked for ${side} ${posKey.slice(0,8)}`);
            
            // Pyramiding: if only 1 position open, open a second one at 50% lot
            if (myPositions.length < MAX_POSITIONS && !pos.comment?.includes('PYRAMID')) {
               log(`🚀 PYRAMIDING: Adding to winning trade ${side}`);
               await pushBotLog(`Pyramiding ${side} at 1R profit`, 'success', '🚀');
               await openPosition(side, currentPrice, myPositions.length + 1, indicators, null, equity, true, lot);
            }
         }
      }

      // Phase 2: Structure trail at 1.5R
      if (t.phase === 1 && profit / (pos.volume * 100) >= 1.5 * risk) {
        t.phase = 2;
        log(`🛡️ Phase 2: Structure trail active for ${side} ${posKey.slice(0,8)}`);
      }
      
      if (t.phase === 2) {
        // Use Donchian channels as swing proxies
        const newSLVal = side === 'BUY' ? indicators.don10Lo[indicators.n-1] : indicators.don10Hi[indicators.n-1];
        if (side === 'BUY' && newSLVal > curSL) {
          await tradingAccount.modifyPosition(posKey, { stopLoss: parseFloat(newSLVal.toFixed(2)) });
        } else if (side === 'SELL' && newSLVal < curSL) {
          await tradingAccount.modifyPosition(posKey, { stopLoss: parseFloat(newSLVal.toFixed(2)) });
        }
      }

      // Phase 3: ATR compression trail at 2R
      if (t.phase < 3 && profit / (pos.volume * 100) >= 2.0 * risk) {
        t.phase = 3;
        log(`🛡️ Phase 3: ATR compression active for ${side} ${posKey.slice(0,8)}`);
      }

      if (t.phase === 3) {
        const trailSL = side === 'BUY' ? (currentPrice - 0.5 * atr) : (currentPrice + 0.5 * atr);
        if (side === 'BUY' && trailSL > curSL) {
          await tradingAccount.modifyPosition(posKey, { stopLoss: parseFloat(trailSL.toFixed(2)) });
        } else if (side === 'SELL' && trailSL < curSL) {
          await tradingAccount.modifyPosition(posKey, { stopLoss: parseFloat(trailSL.toFixed(2)) });
        }
        
        // Close if profit drops 25% from peak
        if (profit < t.peak * 0.75 && t.peak > risk * pos.volume * 100 * 2) {
          log(`🛑 Phase 3: Profit dropped 25% from peak. Closing ${posKey.slice(0,8)}`, 'success');
          await tradingAccount.closePosition(posKey);
          await logTrade(pos.type, entry, currentPrice, profit, [], 'phase3_trail');
          delete trailData[posKey];
          continue;
        }
      }

      // Fallback: Hard TP at $4 (as before)
      if (profit >= 4.00) {
        log(`🟢 TAKE PROFIT | ${posKey.slice(0,8)} | PnL: +$${profit.toFixed(2)}`, 'success');
        await tradingAccount.closePosition(posKey);
        await logTrade(pos.type, entry, currentPrice, profit, [], 'take_profit');
        delete trailData[posKey];
        continue;
      }
    }

    // Save trail state to Redis
    await setRedis(KEYS.POSITIONS + ':trail', trailData);

    return { livePositions, myPositions };
  } catch (e) {
    log(`⚠ Manage error: ${e.message}`, 'error');
    return { livePositions: [], myPositions: [] };
  }
}

// ============ MAIN CYCLE ============
async function autonomousTradingCycle() {
  try {
    // Init SDK
    const account = await initSDK();
    const info = await tradingAccount.getAccountInformation();
    const equity = info.equity || 0;
    const balance = info.balance || 0;
    const pnl = equity - balance;

    // Increment cycle counter
    const cycleCount = (await redis.incr(KEYS.CYCLE_COUNT)) || 1;
    
    // Daily Drawdown Limit
    const startOfDayBalanceRaw = await redis.get('shiva:start_of_day_balance');
    let startOfDayBalance = startOfDayBalanceRaw ? parseFloat(startOfDayBalanceRaw) : balance;
    
    const lastRunStr = await redis.get(KEYS.LAST_RUN);
    if (lastRunStr) {
       try {
         const lastRunData = JSON.parse(lastRunStr);
         const lastRunDate = new Date(lastRunData.time).getUTCDate();
         const todayDate = new Date().getUTCDate();
         if (lastRunDate !== todayDate) {
            startOfDayBalance = balance;
            await redis.set('shiva:start_of_day_balance', startOfDayBalance.toString());
         }
       } catch (e) {}
    } else {
       await redis.set('shiva:start_of_day_balance', startOfDayBalance.toString());
    }

    await redis.set(KEYS.LAST_RUN, JSON.stringify({ time: new Date().toISOString(), cycle: cycleCount }));
    // Save account info to Redis for fast dashboard access
    await redis.set(KEYS.ACCOUNT_INFO, JSON.stringify({ equity, balance, pnl }));

    log(`Cycle #${cycleCount} | Equity: $${equity} | Balance: $${balance}`, 'info');
    await pushBotLog(`Cycle #${cycleCount} | Equity: $${equity.toFixed(2)} | Balance: $${balance.toFixed(2)}`);

    let dailyDDHit = false;
    if (equity <= startOfDayBalance * (1 - DAILY_DD_LIMIT)) {
      log(`🛑 DAILY DRAWDOWN LIMIT REACHED (Equity $${equity.toFixed(2)} <= 75% of $${startOfDayBalance.toFixed(2)}). Halting new entries.`);
      await pushBotLog(`Daily Drawdown Limit Reached - Halting entries`, 'error', '🛑');
      dailyDDHit = true;
    }

    // ── NEWS TRADER (EIA Wednesday 14:30 UTC / NFP 1st Friday 12:30 UTC) ───────
    // Runs before normal signal logic. If a news event is active AND the
    // release candle is large enough, opens a news trade and returns early
    // so the normal cycle doesn't fire at the same time.
    if (!dailyDDHit) {
      try {
        const newsResult = await newsTrader.run(
          tradingAccount, SYMBOL, equity, redis,
          (msg, t) => { log(`[NEWS] ${msg}`, t); pushBotLog(`[NEWS] ${msg}`, t); }
        );
        if (newsResult) {
          // A news trade was successfully opened — skip normal cycle this tick
          log(`[NEWS] Trade opened for ${newsResult.event}: ${newsResult.side} lot=${newsResult.lot}`, 'success');
          return {
            success: true,
            signal: newsResult.side,
            confidence: 100,
            cycle: cycleCount,
            positions_opened: 1,
            source: 'NEWS_TRADER',
            event: newsResult.event
          };
        }
      } catch (newsErr) {
        log(`[NEWS] Error (non-fatal): ${newsErr.message}`, 'error');
      }
    }
    // ─────────────────────────────────────────────────────────────────────────

    // Get price
    const priceData = await tradingAccount.getSymbolPrice(SYMBOL);
    const price = priceData.bid || priceData.ask;

    // Build candles from REAL historical data (not synthetic noise)
    let candles = [];
    let candles1h = [];  // 1-hour candles for research strategies (need 60+ bars)
    try {
      const now = new Date();

      // Fetch 1h candles — 100 bars for EMA50/Donchian/EMA21 research signals
      try {
        const start1h = new Date(now.getTime() - 100 * 60 * 60 * 1000);
        const raw1h = await tradingAccount.getHistoricalCandles(SYMBOL, '1h', start1h, now);
        if (raw1h && raw1h.length >= 60) {
          candles1h = raw1h.map(c => ({ open: c.open, high: c.high, low: c.low, close: c.close, volume: c.tickVolume || 100 }));
          log(`📊 Loaded ${candles1h.length} 1h candles for research signals`);
        }
      } catch (e1h) {
        log(`⚠️ 1h candle fetch failed: ${e1h.message}`, 'error');
      }

      // Fetch real 15-minute candles from MetaApi
      const startTime = new Date(now.getTime() - 50 * 15 * 60 * 1000); // ~12.5 hours back
      const historicalCandles = await tradingAccount.getHistoricalCandles(SYMBOL, '15m', startTime, now);

      if (historicalCandles && historicalCandles.length >= 10) {
        candles = historicalCandles.map(c => ({
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close,
          volume: c.tickVolume || c.volume || 100
        }));
        log(`📊 Using ${candles.length} real 15m candles`);
      } else {
        throw new Error(`Only ${historicalCandles?.length || 0} candles returned`);
      }
    } catch (candleErr) {
      log(`⚠️ Historical candle fetch failed: ${candleErr.message} — using price-derived fallback`, 'error');
      // Fallback: use spread-based synthetic candles (better than pure random)
      const spread = (priceData.ask - priceData.bid) || 0.05;
      const volatility = spread * (3 + Math.random() * 2);
      for (let i = 0; i < 50; i++) {
        const trend = Math.sin(i / 10) * volatility * 2;
        const open = price + trend + (Math.random() - 0.5) * volatility;
        const close = open + (Math.random() - 0.5) * volatility * 2;
        candles.push({
          open,
          high: Math.max(open, close) + Math.random() * volatility,
          low: Math.min(open, close) - Math.random() * volatility,
          close,
          volume: 100 + Math.random() * 900
        });
      }
    }

    // ── Research Strategies (EMA Cross + Donchian + ICT + Hammer) ──────────────
    const researchCandles = candles1h.length >= 60 ? candles1h : candles;
    const ind = computeResearchIndicators(researchCandles);
    
    const recentATR = (() => {
      if (ind.atr14.length < 15) return 0.69;
      return ind.atr14[ind.atr14.length - 1];
    })();
    const researchSig = runResearchSignalsWithInd(researchCandles, price, recentATR, new Date(), ind);

    if (researchSig) {
      log(`🔬 Research signal: ${researchSig.signal} | ${researchSig.name} | conf=${researchSig.conf}% | ATR-SL=${researchSig.sl_pts.toFixed(2)}pt`);
      await pushBotLog(`Research: ${researchSig.signal} — ${researchSig.name} (${researchSig.conf}%)`, 'trade', '🔬');
    }

    // Run 40 agents
    const indicators = analyzeAll(candles, price);
    const consensus = getConsensus(indicators);
    const ict = ictAnalysis(candles, price);

    // Apply ICT weighting before ML
    if (ict.signal !== 'HOLD') {
      if (ict.signal === consensus.signal) {
        consensus.pct = Math.min(95, consensus.pct + Math.round(ict.confidence * 0.15));
      } else if (consensus.signal === 'HOLD') {
        consensus.signal = ict.signal;
        consensus.pct = Math.max(consensus.pct, Math.round(ict.confidence * 0.8));
      }
    }

    // ── Research signal override/boost ──────────────────────────────────────
    // Research signals have proven PF=1.37+ in 9-month backtest. Give them priority.
    if (researchSig) {
      if (researchSig.signal === consensus.signal) {
        // Agreement: boost confidence significantly
        consensus.pct = Math.min(95, consensus.pct + 20);
      } else if (consensus.signal === 'HOLD' || consensus.pct < 60) {
        // Consensus is weak: research signal takes over
        consensus.signal = researchSig.signal;
        consensus.pct = researchSig.conf;
      }
      // Store ATR-based SL/TP for use in openPosition
      consensus._researchSLPts = researchSig.sl_pts;
      consensus._researchTPPts = researchSig.tp_pts;
      consensus._researchName  = researchSig.name;
    }

    // ML prediction
    const mlResult = await mlPredict(consensus, indicators);
    const finalSignal = mlResult.signal;
    const finalConfidence = Math.round(mlResult.confidence * 100);

    // Retrain ML periodically
    if (cycleCount % ML_RETRAIN_EVERY === 0) {
      await trainML();
    }

    log(`Agents: ${consensus.signal} (${consensus.pct}%) | Final: ${finalSignal} (${finalConfidence}%) | ${mlResult.mlNote}`);
    await pushBotLog(
      `Agents: ${consensus.signal} (${consensus.pct}%) | ICT: ${ict.signal} (${ict.confidence}%) | Final: ${finalSignal} (${finalConfidence}%) | ${mlResult.mlNote}`,
      'trade',
      '🤖'
    );
    await pushRedis(KEYS.AGENT_MESSAGES, buildAgentPayload({
      cycleCount,
      priceData,
      price,
      ict,
      indicators,
      consensus,
      mlResult,
      finalSignal,
      finalConfidence
    }));

    // Manage existing positions first (always — regardless of session)
    const { livePositions, myPositions } = await managePositions(price, ind);

    // ICT Session filter — only open NEW entries during London and NY sessions
    const utcHour = new Date().getUTCHours();
    const inLondon = utcHour >= 7 && utcHour <= 12;
    const inNY     = utcHour >= 13 && utcHour <= 17;
    if (!inLondon && !inNY) {
      log(`⏰ Outside ICT session (UTC ${utcHour}h) — managing positions only, no new entries`);
      await pushBotLog(`Outside session (UTC ${utcHour}h) — no new entries`, 'info', '⏰');
      return { success: true, signal: 'HOLD', reason: 'outside_session', cycle: cycleCount };
    }

    // Get live positions and save to Redis
    const currentPositions = myPositions.length > 0
      ? myPositions
      : (await tradingAccount.getPositions()).filter(p => p.symbol === SYMBOL);

    // Save positions to Redis for fast dashboard access
    const positionData = currentPositions.map(p => ({
      id: p.id, type: p.type, symbol: p.symbol,
      openPrice: p.openPrice, currentPrice: p.currentPrice || p.openPrice,
      volume: p.volume, profit: p.profit || 0,
      stopLoss: p.stopLoss, time: p.time, comment: p.comment
    }));
    await redis.set(KEYS.POSITIONS, JSON.stringify(positionData));

    if (dailyDDHit) {
      return { success: true, signal: 'HOLD', reason: 'daily_drawdown_limit', cycle: cycleCount };
    }

    if (finalSignal === 'HOLD' || finalConfidence < MIN_CONFIDENCE) {
      const reason = finalSignal === 'HOLD' ? 'HOLD' : `low confidence (${finalConfidence}% < ${MIN_CONFIDENCE}%)`;
      log(`Signal: ${reason} - No trades`);
      await pushBotLog(`Signal: ${reason} - no trade`, 'info', '⏸️');
      return { success: true, signal: 'HOLD', cycle: cycleCount };
    }

    if (currentPositions.length >= MAX_POSITIONS) {
      log(`Max positions reached (${currentPositions.length}/${MAX_POSITIONS})`);
      await pushBotLog(`Max positions reached (${currentPositions.length}/${MAX_POSITIONS})`, 'info', '📌');
      return { success: true, signal: finalSignal, reason: 'Max positions' };
    }

    // ENFORCE SAME DIRECTION RULE
    if (currentPositions.length > 0) {
      const activeType = currentPositions[0].type || '';
      const activeDirection = activeType.includes('BUY') ? 'BUY' : 'SELL';
      if (finalSignal !== activeDirection) {
        log(`Skipping trade: Signal is ${finalSignal} but active trades are ${activeDirection}`);
        await pushBotLog(`Skipping trade: Signal conflicts with active trades (${activeDirection})`, 'info', '⏸️');
        return { success: true, signal: finalSignal, reason: 'Conflict with active direction' };
      }
    }

    // Open positions — max 2 per cycle to avoid overexposure
    const maxPerCycle = 2;
    const positionsToOpen = Math.min(MAX_POSITIONS - currentPositions.length, maxPerCycle);
    if (positionsToOpen > 0) {
      log(`Opening ${positionsToOpen} position(s) | Signal: ${finalSignal} (${finalConfidence}%)`);
      await pushBotLog(`Opening ${positionsToOpen} position(s) | Signal: ${finalSignal} (${finalConfidence}%)`, 'trade', '🎯');

      const resMeta = researchSig || null;
      const dynLot = calcAutoLot(equity);
      log(`💰 AutoLot: ${dynLot} lot | Equity: $${equity.toFixed(0)} | Target EV: $${(EV_PER_WEEK_PER_01LOT*(dynLot/0.01)).toFixed(0)}/wk → goal $${WEEKLY_TARGET}/wk`);
      await pushBotLog(`AutoLot: ${dynLot} | EV≈$${(EV_PER_WEEK_PER_01LOT*(dynLot/0.01)).toFixed(0)}/wk of $${WEEKLY_TARGET} goal`, 'trade', '📈');
      for (let i = 0; i < positionsToOpen; i++) {
        await openPosition(finalSignal, price, i + 1, indicators, resMeta, equity);
        await new Promise(r => setTimeout(r, 3000));
      }
    }

    return {
      success: true,
      signal: finalSignal,
      confidence: finalConfidence,
      cycle: cycleCount,
      positions_opened: positionsToOpen
    };

  } catch (e) {
    log(`Cycle failed: ${e.message}`, 'error');
    return { success: false, error: e.message };
  }
}

// ============ API ENDPOINTS ============
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') { res.status(204).end(); return; }

  const p = req.url.split('?')[0];

  try {
    // DASHBOARD — optimized with parallel Redis reads
    if (req.method === 'GET' && p === '/api/dashboard') {
      // Read all Redis data in parallel to avoid timeout
      const [tradeHistory, mlModel, cycleData, agentMessages, posDataRaw, acctDataRaw, botLogsRaw] = await Promise.all([
        getTradeHistory(),
        getRedis(KEYS.ML_MODEL),
        getRedis(KEYS.LAST_RUN),
        lrange(KEYS.AGENT_MESSAGES, -30, -1),
        redis.get(KEYS.POSITIONS),
        redis.get(KEYS.ACCOUNT_INFO),
        redis.lrange('shiva:bot_logs', -100, -1)
      ]);

      const wins = tradeHistory.filter(t => t.result === 'win').length;
      const losses = tradeHistory.filter(t => t.result === 'loss').length;
      const executed = tradeHistory.filter(t => t.exit_reason && t.exit_reason !== 'open').length;
      const closed = tradeHistory.filter(t => t.exit_reason === 'take_profit' || t.exit_reason === 'cut_loss').length;
      const errors = tradeHistory.filter(t => t.result === 'loss').length;

      let livePositions = [];
      let accountInfo = { equity: 0, balance: 0, pnl: 0 };
      try {
        if (posDataRaw) livePositions = typeof posDataRaw === 'string' ? JSON.parse(posDataRaw) : posDataRaw;
        if (acctDataRaw) accountInfo = typeof acctDataRaw === 'string' ? JSON.parse(acctDataRaw) : acctDataRaw;
      } catch (e) {}
      
      let botLogs = [];
      try {
        if (botLogsRaw && botLogsRaw.length > 0) {
          botLogs = botLogsRaw.map(l => typeof l === 'string' ? JSON.parse(l) : l);
        }
      } catch (e) {}

      const totalPnl = livePositions.reduce((sum, p) => sum + (p.profit || 0), 0) || accountInfo.pnl || 0;
      const equity = accountInfo.equity || 0;
      const balance = accountInfo.balance || 0;

      const latestAgentMessages = [...agentMessages].reverse();

      return res.json({
        success: true,
        config: { symbol: SYMBOL, lot_size: LOT_SIZE, max_positions: MAX_POSITIONS },
        summary: {
          totalTrades: tradeHistory.length,
          pending: 0,
          executed: executed,
          closed: closed,
          errors: errors,
          wins: wins,
          losses: losses,
          winRate: tradeHistory.length > 0 ? (wins / tradeHistory.length * 100).toFixed(1) : 0,
          cycles: cycleData?.cycle || 0,
          lastRun: cycleData?.time || 'Never',
          openPositions: livePositions.length,
          totalPnl: totalPnl,
          equity: equity,
          balance: balance
        },
        livePositions: livePositions,
        mlModel: mlModel || null,
        recentTrades: tradeHistory.slice(-20).reverse(),
        botLogs: botLogs.reverse(),
        recentAgentMessages: latestAgentMessages,
        agentMessages: latestAgentMessages
      });
    }

    // SCAN & TRADE (Vercel Cron)
    if (req.method === 'GET' && p === '/api/scan') {
      log('Vercel Cron triggered autonomous cycle');
      const result = await autonomousTradingCycle();
      return res.json({ success: result.success, ...result, timestamp: new Date().toISOString() });
    }

    // POSITIONS
    if (req.method === 'GET' && p === '/api/positions') {
      await initSDK();
      const pos = await tradingAccount.getPositions();
      return res.json(pos.filter(p => p.symbol === SYMBOL) || []);
    }

    // STATUS
    if (req.method === 'GET' && p === '/api/status') {
      const cycleDataRaw = await redis.get(KEYS.LAST_RUN);
      let cycleData = null;
      try { cycleData = cycleDataRaw ? (typeof cycleDataRaw === 'string' ? JSON.parse(cycleDataRaw) : cycleDataRaw) : null; } catch(e) {}
      return res.json({
        server: 'SHIVA GODMODE OVERLORD - D-DAY',
        status: '24/7 SERVERLESS',
        connected: true,
        config: { symbol: SYMBOL, lot: LOT_SIZE, max_positions: MAX_POSITIONS },
        lastCycle: cycleData,
        message: 'Runs every 5 minutes via Vercel Cron with ML learning'
      });
    }

    // PUSH LIVE BOT LOGS (from Railway/Mac)
    if (req.method === 'POST' && p === '/api/push-logs') {
      const body = req.body;
      if (body.logs && Array.isArray(body.logs)) {
        // Push each log entry to Redis
        for (const logEntry of body.logs) {
          await redis.rpush('shiva:bot_logs', JSON.stringify(logEntry));
        }
        // Keep only last 200 entries
        await redis.ltrim('shiva:bot_logs', -200, -1);
      }
      // Also save live_positions for dashboard
      if (body.live_positions) {
        await redis.set(KEYS.POSITIONS, JSON.stringify(body.live_positions));
      }
      if (body.account_info) {
        await redis.set(KEYS.ACCOUNT_INFO, JSON.stringify(body.account_info));
      }
      return res.json({ success: true, received: body.logs?.length || 0 });
    }

    // PUSH TRADE SIGNAL (for Discord-like signal posting)
    if (req.method === 'POST' && p === '/api/push-signal') {
      const body = req.body;
      await pushRedis(KEYS.AGENT_MESSAGES, body);
      return res.json({ success: true });
    }

    // CLOSE ALL
    if (req.method === 'POST' && p === '/api/close-all') {
      await initSDK();
      const pos = await tradingAccount.getPositions();
      let closed = 0;
      for (const p of pos) {
        if (p.symbol !== SYMBOL) continue;
        try {
          await tradingAccount.closePosition(p.id);
          closed++;
        } catch(e) {}
      }
      return res.json({ success: true, closed });
    }

    res.status(404).json({ error: 'Not found' });
  } catch(e) {
    console.error('❌ ' + e.message);
    res.status(500).json({ error: e.message });
  }
};
