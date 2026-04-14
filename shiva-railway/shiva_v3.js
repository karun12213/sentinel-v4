#!/usr/bin/env node
/**
 * SHIVA Bot V3 — Quantitative WTI Crude Oil Trading Bot
 * ═══════════════════════════════════════════════════════
 * Platform: MetaAPI (cloud-g2) | Symbol: USOIL | Timeframe: M15
 * 
 * 40-agent weighted voting system across 8 categories
 * ICT Smart Money Concepts (OB, FVG, BOS, CHoCH, Liquidity)
 * Regime detection (ADX/ATR-based)
 * Adaptive ML confidence tracking
 * Advanced position management (BE, trailing, partial close)
 * Full performance tracking with agent accuracy
 * M15 candle-synced execution cycle
 * 
 * @version 3.0.0
 */

'use strict';

// ───────────────────────────────────────────────────────────
// DEPENDENCIES
// ───────────────────────────────────────────────────────────
const MetaApi = require('metaapi.cloud-sdk').default;
const { Redis } = require('@upstash/redis');
const fse = require('fs-extra');
const { DateTime } = require('luxon');
const chalk = require('chalk');

const colors = {
  green: chalk.green, red: chalk.red, yellow: chalk.yellow,
  cyan: chalk.cyan, magenta: chalk.magenta, blue: chalk.blue,
  white: chalk.white, gray: chalk.gray, bold: chalk.bold,
};

// ───────────────────────────────────────────────────────────
// CONFIGURATION
// ───────────────────────────────────────────────────────────
const CONFIG = {
  METAAPI_TOKEN: process.env.METAAPI_TOKEN || '',
  ACCOUNT_ID: process.env.METAAPI_ACCOUNT_ID || 'aa7256d0-f345-4e35-bcec-d2644bdf75c1',
  SYMBOL: 'USOIL',
  TIMEFRAME_MINUTES: 15,
  POSITION_MONITOR_INTERVAL: 30000,
  SPREAD_CHECK_INTERVAL: 60000,
  RISK_PCT: 0.025,                    // 2.5% per trade (increased for more action)
  RR_RATIO: 2.5,
  MAX_POSITIONS: 2,
  MAX_DAILY_LOSS_PCT: 0.05,
  MAX_DRAWDOWN_PCT: 0.10,
  MIN_LOT: 0.01, MAX_LOT: 0.03,       // Max 0.03 lot for $100 account
  MAX_SPREAD: 0.12,                   // Adjusted for USOIL on Axiory
  MIN_SCORE_THRESHOLD: 0.10,          // Take trades at 10%+ score
  TRADE_COOLDOWN: 9 * 60 * 1000,      // 9 min cooldown between trades
  KILL_ZONES: {
    london: { start: 7, end: 10 },
    newYork: { start: 12, end: 15 },
    londonClose: { start: 15, end: 16 },
    asian: { start: 0, end: 3 },
  },
  PERFORMANCE_FILE: 'data/performance.json',
  TRADES_FILE: 'data/trades.json',
  VOTES_LOG: 'data/votes.log',
  REDIS_URL: process.env.UPSTASH_REDIS_REST_URL || 'https://growing-crow-80382.upstash.io',
  REDIS_TOKEN: process.env.UPSTASH_REDIS_REST_TOKEN || 'gQAAAAAAATn-AAIncDJlNjdjM2M4OTQzOTg0OGRhYjE3MzRjNjNhM2U1ZDUzNnAyODAzODI',
};

// Ensure data directory exists
fse.ensureDirSync('data');

// ───────────────────────────────────────────────────────────
// UTILITIES
// ───────────────────────────────────────────────────────────
async function retryWithBackoff(fn, maxRetries = 3, baseDelay = 1000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try { return await fn(); }
    catch (err) {
      if (attempt === maxRetries) throw err;
      const delay = baseDelay * Math.pow(2, attempt - 1) + Math.random() * 500;
      console.log(colors.yellow(`    ⏳ Retry ${attempt}/${maxRetries} in ${Math.round(delay)}ms`));
      await sleep(delay);
    }
  }
}
const sleep = ms => new Promise(r => setTimeout(r, ms));
const roundLot = lot => Math.round(lot * 100) / 100;
const clamp = (v, min, max) => Math.max(min, Math.min(max, v));

// ───────────────────────────────────────────────────────────
// INDICATOR ENGINE (all built-in)
// ───────────────────────────────────────────────────────────
class IndicatorEngine {
  static ema(closes, period) {
    const k = 2 / (period + 1);
    const result = [closes[0]];
    for (let i = 1; i < closes.length; i++) result.push(closes[i] * k + result[i-1] * (1-k));
    return result;
  }

  static sma(closes, period) {
    const result = [];
    for (let i = 0; i < closes.length; i++) {
      if (i < period - 1) { result.push(null); }
      else { const s = closes.slice(i-period+1, i+1); result.push(s.reduce((a,b)=>a+b,0)/period); }
    }
    return result;
  }

  static rsi(closes, period = 14) {
    if (closes.length < period + 1) return [];
    const gains = [], losses = [];
    for (let i = 1; i < closes.length; i++) {
      const d = closes[i] - closes[i-1];
      gains.push(Math.max(d, 0)); losses.push(Math.max(-d, 0));
    }
    const result = [];
    let avgG = gains.slice(0, period).reduce((a,b)=>a+b,0)/period;
    let avgL = losses.slice(0, period).reduce((a,b)=>a+b,0)/period;
    for (let i = 0; i < closes.length - period; i++) {
      if (i > 0) { avgG = (avgG*(period-1)+gains[period-1+i])/period; avgL = (avgL*(period-1)+losses[period-1+i])/period; }
      const rs = avgL === 0 ? 100 : avgG/avgL;
      result.push(100 - 100/(1+rs));
    }
    return result;
  }

  static macd(closes, fast=12, slow=26, signal=9) {
    const ef = this.ema(closes, fast), es = this.ema(closes, slow);
    const ml = ef.map((v,i) => v - es[i]);
    const sl = this.ema(ml, signal);
    const hist = ml.map((v,i) => v - sl[i]);
    return { macd: ml, signal: sl, histogram: hist };
  }

  static stochastic(candles, kPeriod=14, dPeriod=3) {
    const H = candles.map(c=>c.high), L = candles.map(c=>c.low), C = candles.map(c=>c.close);
    const kV=[], dV=[];
    for (let i = 0; i < candles.length; i++) {
      if (i < kPeriod-1) { kV.push(null); dV.push(null); continue; }
      const hh = Math.max(...H.slice(i-kPeriod+1,i+1)), ll = Math.min(...L.slice(i-kPeriod+1,i+1));
      const k = hh!==ll ? ((C[i]-ll)/(hh-ll))*100 : 50;
      kV.push(k);
      const rk = kV.filter(v=>v!==null).slice(-dPeriod);
      dV.push(rk.length===dPeriod ? rk.reduce((a,b)=>a+b,0)/dPeriod : null);
    }
    return { k: kV, d: dV };
  }

  static atr(candles, period=14) {
    const tr = [];
    for (let i = 0; i < candles.length; i++) {
      if (i===0) tr.push(candles[i].high-candles[i].low);
      else tr.push(Math.max(candles[i].high-candles[i].low, Math.abs(candles[i].high-candles[i-1].close), Math.abs(candles[i].low-candles[i-1].close)));
    }
    const result = [];
    for (let i = 0; i < tr.length; i++) {
      if (i < period-1) result.push(null);
      else { const s = tr.slice(i-period+1,i+1); result.push(s.reduce((a,b)=>a+b,0)/period); }
    }
    return result;
  }

  static bollingerBands(closes, period=20, mult=2) {
    const result = [];
    for (let i = 0; i < closes.length; i++) {
      if (i < period-1) { result.push({upper:null,middle:null,lower:null}); continue; }
      const s = closes.slice(i-period+1,i+1);
      const sma = s.reduce((a,b)=>a+b,0)/period;
      const std = Math.sqrt(s.reduce((a,b)=>a+(b-sma)**2,0)/period);
      result.push({ upper: sma+mult*std, middle: sma, lower: sma-mult*std });
    }
    return result;
  }

  static vwap(candles) {
    let cvp=0, cv=0;
    return candles.map(c => { const tp=(c.high+c.low+c.close)/3; cvp+=tp*c.volume; cv+=c.volume; return cv!==0?cvp/cv:c.close; });
  }

  static supertrend(candles, period=10, mult=3) {
    const atr = this.atr(candles, period);
    const result = [];
    for (let i = 0; i < candles.length; i++) {
      if (i < period || !atr[i]) { result.push({value:null, direction:null}); continue; }
      const hl2 = (candles[i].high+candles[i].low)/2;
      const upper = hl2+mult*atr[i], lower = hl2-mult*atr[i];
      const prev = result[i-1];
      let dir = prev.direction || 'buy';
      if (candles[i].close > (prev.value||0) && prev.direction==='sell') dir='buy';
      if (candles[i].close < (prev.value||0) && prev.direction==='buy') dir='sell';
      result.push({ value: dir==='buy'?lower:upper, direction: dir });
    }
    return result;
  }

  static adx(candles, period=14) {
    const pDM=[], mDM=[];
    for (let i=1;i<candles.length;i++) {
      const up=candles[i].high-candles[i-1].high, down=candles[i-1].low-candles[i].low;
      pDM.push(up>down&&up>0?up:0); mDM.push(down>up&&down>0?down:0);
    }
    const tr=[];
    for (let i=0;i<candles.length;i++) {
      if (i===0) tr.push(candles[i].high-candles[i].low);
      else tr.push(Math.max(candles[i].high-candles[i].low,Math.abs(candles[i].high-candles[i-1].close),Math.abs(candles[i].low-candles[i-1].close)));
    }
    const result=[];
    let sP=pDM.slice(0,period).reduce((a,b)=>a+b,0), sM=mDM.slice(0,period).reduce((a,b)=>a+b,0), sT=tr.slice(0,period).reduce((a,b)=>a+b,0);
    for (let i=period;i<candles.length-1;i++) {
      if (i>period) { sP=sP-sP/period+pDM[i-1]; sM=sM-sM/period+mDM[i-1]; sT=sT-sT/period+tr[i]; }
      const pDI=sT!==0?(sP/sT)*100:0, mDI=sT!==0?(sM/sT)*100:0;
      const dx=pDI+mDI!==0?Math.abs(pDI-mDI)/(pDI+mDI)*100:0;
      result.push({adx:dx, plusDI:pDI, minusDI:mDI});
    }
    return result;
  }

  static williamsR(candles, period=14) {
    const result=[];
    for (let i=0;i<candles.length;i++) {
      if (i<period-1) { result.push(null); continue; }
      const hh=Math.max(...candles.slice(i-period+1,i+1).map(c=>c.high));
      const ll=Math.min(...candles.slice(i-period+1,i+1).map(c=>c.low));
      result.push(hh!==ll?((hh-candles[i].close)/(hh-ll))*-100:-50);
    }
    return result;
  }

  static cci(candles, period=20) {
    const result=[];
    for (let i=0;i<candles.length;i++) {
      if (i<period-1) { result.push(null); continue; }
      const s=candles.slice(i-period+1,i+1);
      const tps=s.map(c=>(c.high+c.low+c.close)/3);
      const sma=tps.reduce((a,b)=>a+b,0)/period;
      const md=tps.reduce((a,b)=>a+Math.abs(b-sma),0)/period;
      const tp=(candles[i].high+candles[i].low+candles[i].close)/3;
      result.push(md!==0?(tp-sma)/(0.015*md):0);
    }
    return result;
  }

  static roc(closes, period=10) {
    const result=[];
    for (let i=0;i<closes.length;i++) {
      if (i<period) { result.push(null); continue; }
      result.push(((closes[i]-closes[i-period])/closes[i-period])*100);
    }
    return result;
  }

  static cmf(candles, period=20) {
    const result=[];
    for (let i=0;i<candles.length;i++) {
      if (i<period-1) { result.push(null); continue; }
      const s=candles.slice(i-period+1,i+1);
      let mfv=0,tv=0;
      s.forEach(c=>{const r=c.high-c.low;const mfm=r!==0?((c.close-c.low)-(c.high-c.close))/r:0;mfv+=mfm*c.volume;tv+=c.volume;});
      result.push(tv!==0?mfv/tv:0);
    }
    return result;
  }

  static obv(candles) {
    const result=[candles[0].volume];
    for (let i=1;i<candles.length;i++) {
      if (candles[i].close>candles[i-1].close) result.push(result[i-1]+candles[i].volume);
      else if (candles[i].close<candles[i-1].close) result.push(result[i-1]-candles[i].volume);
      else result.push(result[i-1]);
    }
    return result;
  }

  static mfi(candles, period=14) {
    const result=[];
    for (let i=0;i<candles.length;i++) {
      if (i<period) { result.push(null); continue; }
      const s=candles.slice(i-period,i+1);
      let pf=0,nf=0;
      for (let j=1;j<s.length;j++) {
        const tp=(s[j].high+s[j].low+s[j].close)/3, ptp=(s[j-1].high+s[j-1].low+s[j-1].close)/3;
        const rmf=tp*s[j].volume;
        if (tp>ptp) pf+=rmf; else nf+=rmf;
      }
      const mfr=nf!==0?pf/nf:999;
      result.push(100-100/(1+mfr));
    }
    return result;
  }

  static aroon(candles, period=25) {
    const result=[];
    for (let i=0;i<candles.length;i++) {
      if (i<period) { result.push({up:null,down:null}); continue; }
      const s=candles.slice(i-period,i+1);
      const hh=Math.max(...s.map(c=>c.high)), ll=Math.min(...s.map(c=>c.low));
      const rs=[...s].reverse();
      const dHH=rs.findIndex(c=>c.high===hh), dLL=rs.findIndex(c=>c.low===ll);
      result.push({up:((period-dHH)/period)*100, down:((period-dLL)/period)*100});
    }
    return result;
  }

  static donchian(candles, period=20) {
    const result=[];
    for (let i=0;i<candles.length;i++) {
      if (i<period-1) { result.push({upper:null,lower:null,middle:null}); continue; }
      const s=candles.slice(i-period+1,i+1);
      const u=Math.max(...s.map(c=>c.high)), l=Math.min(...s.map(c=>c.low));
      result.push({upper:u, lower:l, middle:(u+l)/2});
    }
    return result;
  }
}

// ───────────────────────────────────────────────────────────
// MARKET DATA
// ───────────────────────────────────────────────────────────
class MarketData {
  constructor(account, connection, symbol='USOIL') {
    this.account = account;       // MetatraderAccount for historical data
    this.connection = connection; // RPC connection for live price/trading
    this.symbol = symbol;
    this.candles = [];
    this.price = { bid:0, ask:0, spread:0 };
  }

  async fetchCandles(count=300) {
    return retryWithBackoff(async () => {
      // MetaAPI getHistoricalCandles returns array of {time,open,high,low,close,volume}
      // Use '15m' timeframe directly for M15 candles
      const raw = await this.account.getHistoricalCandles(this.symbol, '15m');
      // raw may return all available; take the last `count`
      const slice = raw.slice(-count);
      this.candles = slice.map(c => ({
        time: new Date(c.time),
        open: parseFloat(c.open),
        high: parseFloat(c.high),
        low: parseFloat(c.low),
        close: parseFloat(c.close),
        volume: parseFloat(c.volume || 1),
      })).filter(c => c.open > 0 && c.close > 0);
      console.log(`    ✓ Loaded ${this.candles.length} M15 candles`);
      return this.candles;
    });
  }

  async updatePrice() {
    return retryWithBackoff(async () => {
      const pd = await this.connection.getSymbolPrice(this.symbol);
      this.price = { bid: pd.bid||0, ask: pd.ask||0, spread: ((pd.ask||0)-(pd.bid||0)) };
      return this.price;
    });
  }

  getLatestCandle() {
    return this.candles.length > 0 ? this.candles[this.candles.length-1] : null;
  }
}

// ───────────────────────────────────────────────────────────
// ICT ANALYZER
// ───────────────────────────────────────────────────────────
class ICTAnalyzer {
  constructor() { this.orderBlocks=[]; this.fairValueGaps=[]; }

  detectOrderBlocks(candles, lookback=20) {
    const blocks=[], recent=candles.slice(-lookback);
    for (let i=1;i<recent.length-1;i++) {
      if (recent[i].close<recent[i].open) {
        const next=recent.slice(i+1,i+4);
        if (next.some(c=>c.close>c.open&&(c.close-c.open)>(recent[i].high-recent[i].low)*2))
          blocks.push({type:'bullish', high:recent[i].high, low:recent[i].low, index:i, valid:true});
      }
      if (recent[i].close>recent[i].open) {
        const next=recent.slice(i+1,i+4);
        if (next.some(c=>c.close<c.open&&(c.open-c.close)>(recent[i].high-recent[i].low)*2))
          blocks.push({type:'bearish', high:recent[i].high, low:recent[i].low, index:i, valid:true});
      }
    }
    this.orderBlocks=blocks; return blocks;
  }

  detectFVGs(candles, lookback=20) {
    const fvs=[], recent=candles.slice(-lookback);
    for (let i=2;i<recent.length;i++) {
      if (recent[i-2].high<recent[i].low)
        fvs.push({type:'bullish', high:recent[i-2].high, low:recent[i].low, midpoint:(recent[i-2].high+recent[i].low)/2, index:i, age:recent.length-i, valid:(recent.length-i)<=20});
      if (recent[i-2].low>recent[i].high)
        fvs.push({type:'bearish', high:recent[i].high, low:recent[i-2].low, midpoint:(recent[i-2].low+recent[i].high)/2, index:i, age:recent.length-i, valid:(recent.length-i)<=20});
    }
    this.fairValueGaps=fvs; return fvs;
  }

  detectBOS(candles, lookback=30) {
    const recent=candles.slice(-lookback);
    const H=recent.map(c=>c.high), L=recent.map(c=>c.low);
    const lH=Math.max(...H.slice(-10)), pH=Math.max(...H.slice(-20,-10));
    const lL=Math.min(...L.slice(-10)), pL=Math.min(...L.slice(-20,-10));
    if (lH>pH&&lL>pL) return {type:'bullish', level:lH, confirmed:true};
    if (lL<pL&&lH<pH) return {type:'bearish', level:lL, confirmed:true};
    return {type:'none', confirmed:false};
  }

  detectChoCH(candles, lookback=30) {
    const recent=candles.slice(-lookback);
    const f=recent.slice(0,lookback/2), s=recent.slice(lookback/2);
    const fT=f[f.length-1].close>f[0].close?'bullish':'bearish';
    const sT=s[s.length-1].close>s[0].close?'bullish':'bearish';
    if (fT!==sT) return {type:sT, confirmed:true};
    return {type:'none', confirmed:false};
  }

  detectLiquiditySweeps(candles, lookback=20) {
    const recent=candles.slice(-lookback);
    const sL=Math.min(...recent.slice(0,-1).map(c=>c.low));
    const sH=Math.max(...recent.slice(0,-1).map(c=>c.high));
    const last=recent[recent.length-1];
    if (last.low<sL&&last.close>last.open) return {type:'bullish', level:sL, swept:true};
    if (last.high>sH&&last.close<last.open) return {type:'bearish', level:sH, swept:true};
    return {type:'none', swept:false};
  }

  isInKillZone() {
    const h=DateTime.utc().hour;
    for (const [name,z] of Object.entries(CONFIG.KILL_ZONES))
      if (h>=z.start&&h<z.end) return {inZone:true, zone:name.toUpperCase(), weight:name==='asian'?0.7:1.0};
    return {inZone:false, zone:'NONE', weight:0.5};
  }

  getPremiumDiscount(candles, lookback=20) {
    const r=candles.slice(-lookback);
    const hh=Math.max(...r.map(c=>c.high)), ll=Math.min(...r.map(c=>c.low));
    const range=hh-ll, eq=ll+range*0.5, disc=ll+range*0.382, prem=ll+range*0.618;
    const price=r[r.length-1].close;
    return {highest:hh,lowest:ll,range,equilibrium:eq,discount:disc,premium:prem,currentPrice:price,isDiscount:price<eq,isPremium:price>eq,percentInRange:(price-ll)/range};
  }

  analyze(candles) {
    return {
      orderBlocks:this.detectOrderBlocks(candles),
      fairValueGaps:this.detectFVGs(candles),
      bos:this.detectBOS(candles),
      choch:this.detectChoCH(candles),
      liquidity:this.detectLiquiditySweeps(candles),
      killZone:this.isInKillZone(),
      premiumDiscount:this.getPremiumDiscount(candles),
    };
  }
}

// ───────────────────────────────────────────────────────────
// AGENT VOTER (40 agents)
// ───────────────────────────────────────────────────────────
class AgentVoter {
  constructor() {
    this.groupWeights = { A:1.8, B:1.5, C:2.0, D:1.3, E:1.6, F:1.2, G:1.7, H:2.0 };
    this.disabledAgents = new Set();
    this.agentVotes = {};
  }

  _vote(group, name, rawVote, reason) {
    return { group, name, rawVote: clamp(rawVote,-1,1), reason, disabled:false };
  }

  vote(candles, ind, ict, regime, mlAdj, perf) {
    const closes=candles.map(c=>c.close), price=closes[closes.length-1];
    const votes=[];

    // GROUP A: TREND (1.8x)
    votes.push(this._A1(candles,ind)); votes.push(this._A2(candles,ind));
    votes.push(this._A3(candles,ind)); votes.push(this._A4(candles,ind));
    votes.push(this._A5(candles,ind));

    // GROUP B: MOMENTUM (1.5x)
    votes.push(this._B1(ind)); votes.push(this._B2(candles,ind));
    votes.push(this._B3(ind)); votes.push(this._B4(ind));
    votes.push(this._B5(ind)); votes.push(this._B6(ind));
    votes.push(this._B7(ind)); votes.push(this._B8(candles));

    // GROUP C: ICT (2.0x)
    votes.push(this._C1(ict,price)); votes.push(this._C2(ict,price));
    votes.push(this._C3(ict)); votes.push(this._C4(ict));
    votes.push(this._C5(ict)); votes.push(this._C6(ict));
    votes.push(this._C7(ict)); votes.push(this._C8(candles));

    // GROUP D: VOLATILITY (1.3x)
    votes.push(this._D1(candles,ind)); votes.push(this._D2(candles,ind));
    votes.push(this._D3(candles,ind)); votes.push(this._D4(candles,price));
    votes.push(this._D5(candles,ind));

    // GROUP E: VOLUME/FLOW (1.6x)
    votes.push(this._E1(candles,ind,price)); votes.push(this._E2(candles,ind,price));
    votes.push(this._E3(candles,ind)); votes.push(this._E4(candles,ind));
    votes.push(this._E5(candles,ind));

    // GROUP F: OSCILLATOR (1.2x)
    votes.push(this._F1(candles,ind)); votes.push(this._F2(candles,ind));
    votes.push(this._F3(candles)); votes.push(this._F4(candles));
    votes.push(this._F5(candles,ind));

    // GROUP G: PATTERN (1.7x)
    votes.push(this._G1(candles)); votes.push(this._G2(candles));
    votes.push(this._G3(candles)); votes.push(this._G4(candles,ind));
    votes.push(this._G5(candles,price));

    // GROUP H: ML/ADAPTIVE (2.0x)
    votes.push(this._H1(mlAdj,perf)); votes.push(this._H2(perf));
    votes.push(this._H3(perf)); votes.push(this._H4(perf,votes));
    votes.push(this._H5(regime));

    // Calculate weighted score
    let wSum=0, maxScore=0;
    votes.forEach(v => {
      if (this.disabledAgents.has(v.name)) { v.disabled=true; return; }
      const gw=this.groupWeights[v.group];
      wSum+=v.rawVote*gw; maxScore+=gw;
    });

    const normScore=maxScore>0?wSum/maxScore:0;
    const scorePct=Math.abs(normScore);
    const dir=normScore>=0?'BUY':'SELL';
    const shouldTrade=scorePct>=CONFIG.MIN_SCORE_THRESHOLD;

    this.agentVotes={votes,wSum,maxScore,normScore,scorePct,direction:dir,shouldTrade};
    return {score:scorePct, direction:shouldTrade?dir:'HOLD', votes, shouldTrade};
  }

  // ── GROUP A: TREND ──
  _A1(c,ind) {
    const {ema9,ema21,ema50,ema200}=ind, l=c.length-1;
    if (!ema200||ema200[l]===null) return this._vote('A','A1_EMA_Stack',0,'Insufficient data');
    if (ema9[l]>ema21[l]&&ema21[l]>ema50[l]&&ema50[l]>ema200[l]) return this._vote('A','A1_EMA_Stack',1,'Full bullish EMA stack');
    if (ema9[l]<ema21[l]&&ema21[l]<ema50[l]&&ema50[l]<ema200[l]) return this._vote('A','A1_EMA_Stack',-1,'Full bearish EMA stack');
    return this._vote('A','A1_EMA_Stack',0,'Mixed EMA stack');
  }
  _A2(c,ind) {
    const {ema50}=ind, l=c.length-1;
    if (l<5||!ema50[l]) return this._vote('A','A2_EMA_Slope',0,'Insufficient data');
    const s=ema50[l]-ema50[l-5];
    return this._vote('A','A2_EMA_Slope',s>0?1:-1,`EMA50 slope: ${s.toFixed(3)}`);
  }
  _A3(c,ind) {
    const {ema9,ema21}=ind, l=c.length-1;
    if (l<2||!ema9[l]) return this._vote('A','A3_EMA_Cross',0,'Insufficient data');
    const cBull=ema9[l-1]<=ema21[l-1]&&ema9[l]>ema21[l];
    const cBear=ema9[l-1]>=ema21[l-1]&&ema9[l]<ema21[l];
    if (cBull) return this._vote('A','A3_EMA_Cross',1,'Bullish EMA 9/21 cross');
    if (cBear) return this._vote('A','A3_EMA_Cross',-1,'Bearish EMA 9/21 cross');
    return this._vote('A','A3_EMA_Cross',ema9[l]>ema21[l]?0.5:-0.5,'No cross, following trend');
  }
  _A4(c,ind) {
    const {ema200}=ind, l=c.length-1, price=c[l].close;
    if (!ema200||ema200[l]===null) return this._vote('A','A4_EMA_Price',0,'Insufficient data');
    return this._vote('A','A4_EMA_Price',price>ema200[l]?1:-1,`Price ${price>ema200[l]?'above':'below'} EMA200`);
  }
  _A5(c,ind) {
    const {supertrend}=ind, l=c.length-1;
    if (!supertrend||!supertrend[l]||!supertrend[l].direction) return this._vote('A','A5_Supertrend',0,'Insufficient data');
    return this._vote('A','A5_Supertrend',supertrend[l].direction==='buy'?1:-1,`Supertrend: ${supertrend[l].direction}`);
  }

  // ── GROUP B: MOMENTUM ──
  _B1(ind) {
    const {rsi}=ind, l=rsi.length;
    if (l<2) return this._vote('B','B1_RSI_Dir',0,'Insufficient data');
    const cur=rsi[l-1], prev=rsi[l-2];
    if (prev<=50&&cur>50) return this._vote('B','B1_RSI_Dir',1,'RSI crossed above 50');
    if (prev>=50&&cur<50) return this._vote('B','B1_RSI_Dir',-1,'RSI crossed below 50');
    return this._vote('B','B1_RSI_Dir',cur>50?0.5:-0.5,`RSI: ${cur.toFixed(1)}`);
  }
  _B2(c,ind) {
    const {rsi}=ind, l=rsi.length;
    if (l<20) return this._vote('B','B2_RSI_Div',0,'Insufficient data');
    const pL1=Math.min(...c.slice(-20,-10).map(x=>x.low)), pL2=Math.min(...c.slice(-10).map(x=>x.low));
    const rL1=Math.min(...rsi.slice(-20,-10)), rL2=Math.min(...rsi.slice(-10));
    if (pL2<pL1&&rL2>rL1) return this._vote('B','B2_RSI_Div',1,'Bullish RSI divergence');
    if (pL2>pL1&&rL2<rL1) return this._vote('B','B2_RSI_Div',-1,'Bearish RSI divergence');
    return this._vote('B','B2_RSI_Div',0,'No divergence');
  }
  _B3(ind) {
    const {stoch}=ind;
    const k=stoch.k.filter(v=>v!==null), d=stoch.d.filter(v=>v!==null);
    if (k.length<2||d.length<2) return this._vote('B','B3_Stoch',0,'No Stoch data');
    const cKd=k[k.length-2]<=d[d.length-2]&&k[k.length-1]>d[d.length-1];
    if (cKd&&k[k.length-1]>20) return this._vote('B','B3_Stoch',1,'Stoch bullish cross above 20');
    return this._vote('B','B3_Stoch',k[k.length-1]>50?0.5:-0.5,`Stoch K: ${k[k.length-1].toFixed(1)}`);
  }
  _B4(ind) {
    const {macd}=ind, l=macd.histogram.length;
    if (l<2) return this._vote('B','B4_MACDHist',0,'Insufficient data');
    const cur=macd.histogram[l-1], prev=macd.histogram[l-2];
    if (Math.abs(cur)>Math.abs(prev)&&cur>0) return this._vote('B','B4_MACDHist',1,'MACD hist expanding bullish');
    if (Math.abs(cur)>Math.abs(prev)&&cur<0) return this._vote('B','B4_MACDHist',-1,'MACD hist expanding bearish');
    return this._vote('B','B4_MACDHist',0,'MACD hist contracting');
  }
  _B5(ind) {
    const {macd}=ind, l=macd.macd.length;
    if (l<2) return this._vote('B','B5_MACDCross',0,'Insufficient data');
    const cB=macd.macd[l-2]<=macd.signal[l-2]&&macd.macd[l-1]>macd.signal[l-1];
    const cS=macd.macd[l-2]>=macd.signal[l-2]&&macd.macd[l-1]<macd.signal[l-1];
    if (cB) return this._vote('B','B5_MACDCross',1,'MACD bullish cross');
    if (cS) return this._vote('B','B5_MACDCross',-1,'MACD bearish cross');
    return this._vote('B','B5_MACDCross',0,'No MACD cross');
  }
  _B6(ind) {
    const {macd}=ind, l=macd.macd.length-1;
    if (l<0) return this._vote('B','B6_MACDZero',0,'Insufficient data');
    return this._vote('B','B6_MACDZero',macd.macd[l]>0?1:-1,`MACD: ${macd.macd[l].toFixed(3)}`);
  }
  _B7(ind) {
    const {roc}=ind, v=roc.filter(x=>x!==null);
    if (v.length===0) return this._vote('B','B7_ROC',0,'Insufficient data');
    return this._vote('B','B7_ROC',v[v.length-1]>0?1:-1,`ROC: ${v[v.length-1].toFixed(2)}%`);
  }
  _B8(c) {
    const closes=c.map(x=>x.close), period=14;
    if (c.length<period*2) return this._vote('B','B8_CMO',0,'Insufficient data');
    let sU=0,sD=0;
    for (let i=closes.length-period;i<closes.length;i++) {
      const d=closes[i]-closes[i-1];
      if (d>0) sU+=d; else sD+=Math.abs(d);
    }
    const cmo=sD+sU!==0?((sU-sD)/(sU+sD))*100:0;
    return this._vote('B','B8_CMO',cmo>0?1:-1,`CMO: ${cmo.toFixed(1)}`);
  }

  // ── GROUP C: ICT ──
  _C1(ict,price) {
    const bOB=ict.orderBlocks.filter(o=>o.valid&&o.type==='bullish'&&price>=o.low&&price<=o.high);
    const sOB=ict.orderBlocks.filter(o=>o.valid&&o.type==='bearish'&&price>=o.low&&price<=o.high);
    if (bOB.length>0) return this._vote('C','C1_OrderBlock',1,`In bullish OB (${bOB.length})`);
    if (sOB.length>0) return this._vote('C','C1_OrderBlock',-1,`In bearish OB (${sOB.length})`);
    return this._vote('C','C1_OrderBlock',0,'Not in order block');
  }
  _C2(ict,price) {
    const bF=ict.fairValueGaps.filter(f=>f.valid&&f.type==='bullish'&&price>=f.low&&price<=f.high);
    const sF=ict.fairValueGaps.filter(f=>f.valid&&f.type==='bearish'&&price>=f.low&&price<=f.high);
    if (bF.length>0) return this._vote('C','C2_FVG',1,`In bullish FVG (${bF.length})`);
    if (sF.length>0) return this._vote('C','C2_FVG',-1,`In bearish FVG (${sF.length})`);
    return this._vote('C','C2_FVG',0,'Not in FVG');
  }
  _C3(ict) {
    const b=ict.bos;
    if (b.confirmed&&b.type==='bullish') return this._vote('C','C3_BOS',1,'Bullish BOS confirmed');
    if (b.confirmed&&b.type==='bearish') return this._vote('C','C3_BOS',-1,'Bearish BOS confirmed');
    return this._vote('C','C3_BOS',0,'No BOS');
  }
  _C4(ict) {
    const c=ict.choch;
    if (c.confirmed&&c.type==='bullish') return this._vote('C','C4_ChoCH',1,'Bullish CHoCH');
    if (c.confirmed&&c.type==='bearish') return this._vote('C','C4_ChoCH',-1,'Bearish CHoCH');
    return this._vote('C','C4_ChoCH',0,'No CHoCH');
  }
  _C5(ict) {
    const l=ict.liquidity;
    if (l.swept&&l.type==='bullish') return this._vote('C','C5_Liquidity',1,'Sell-side liquidity swept');
    if (l.swept&&l.type==='bearish') return this._vote('C','C5_Liquidity',-1,'Buy-side liquidity swept');
    return this._vote('C','C5_Liquidity',0,'No liquidity sweep');
  }
  _C6(ict) {
    const kz=ict.killZone;
    if (kz.inZone&&(kz.zone==='LONDON'||kz.zone==='NEWYORK')) return this._vote('C','C6_KillZone',0.5*kz.weight,`In ${kz.zone} kill zone`);
    return this._vote('C','C6_KillZone',0,'Outside kill zones');
  }
  _C7(ict) {
    const pd=ict.premiumDiscount;
    if (pd.isDiscount) return this._vote('C','C7_PremDiscount',1,`Discount zone (${(pd.percentInRange*100).toFixed(0)}%)`);
    if (pd.isPremium) return this._vote('C','C7_PremDiscount',-1,`Premium zone (${(pd.percentInRange*100).toFixed(0)}%)`);
    return this._vote('C','C7_PremDiscount',0,'At equilibrium');
  }
  _C8(c) {
    if (c.length<20) return this._vote('C','C8_MSB',0,'Insufficient data');
    const r=c.slice(-20), hh=Math.max(...r.map(x=>x.high)), ll=Math.min(...r.map(x=>x.low));
    const last=r[r.length-1];
    if (last.close>hh) return this._vote('C','C8_MSB',1,'MSB: broke above 20-candle high');
    if (last.close<ll) return this._vote('C','C8_MSB',-1,'MSB: broke below 20-candle low');
    return this._vote('C','C8_MSB',0,'No MSB');
  }

  // ── GROUP D: VOLATILITY ──
  _D1(c,ind) {
    const {atr}=ind, v=atr.filter(x=>x!==null);
    if (v.length<10) return this._vote('D','D1_ATR',0,'Insufficient data');
    const cur=v[v.length-1], avg=v.slice(-14).reduce((a,b)=>a+b,0)/Math.min(14,v.length);
    return this._vote('D','D1_ATR',cur>avg?0.5:-0.5,`ATR: ${cur.toFixed(3)} vs avg ${avg.toFixed(3)}`);
  }
  _D2(c,ind) {
    const {bb}=ind, v=bb.filter(b=>b.upper!==null);
    if (v.length<5) return this._vote('D','D2_BB',0,'Insufficient data');
    const last=v[v.length-1], prev=v[v.length-2], price=c[c.length-1].close;
    const squeeze=(last.upper-last.lower)<(prev.upper-prev.lower);
    if (squeeze&&price>last.middle) return this._vote('D','D2_BB',1,'BB squeeze release upward');
    return this._vote('D','D2_BB',0,'No BB squeeze');
  }
  _D3(c,ind) {
    const {bb}=ind, v=bb.filter(b=>b.upper!==null);
    if (v.length<3) return this._vote('D','D3_BBWidth',0,'Insufficient data');
    const w=v[v.length-1].upper-v[v.length-1].lower, pw=v[v.length-3].upper-v[v.length-3].lower;
    return this._vote('D','D3_BBWidth',w>pw?0.5:-0.5,`BB width ${w>pw?'expanding':'contracting'}`);
  }
  _D4(c,price) {
    const ema20=IndicatorEngine.ema(c.map(x=>x.close),20);
    const atr14=IndicatorEngine.atr(c,14), l=c.length-1;
    if (!atr14[l]) return this._vote('D','D4_Keltner',0,'Insufficient data');
    return this._vote('D','D4_Keltner',price>ema20[l]?1:-1,`Price ${price>ema20[l]?'above':'below'} Keltner midline`);
  }
  _D5(c,ind) {
    if (c.length<14) return this._vote('D','D5_ChaikinVol',0,'Insufficient data');
    const H=c.map(x=>x.high), L=c.map(x=>x.low);
    const curHL=H[H.length-1]-L[L.length-1];
    const avgHL=H.slice(-14).reduce((a,i)=>a+(H[i]-L[i]),0)/14;
    return this._vote('D','D5_ChaikinVol',curHL>avgHL?0.5:-0.5,`ChaikinVol ${curHL>avgHL?'rising':'falling'}`);
  }

  // ── GROUP E: VOLUME/FLOW ──
  _E1(c,ind,price) {
    const {vwap}=ind, l=vwap.length-1;
    return this._vote('E','E1_VWAP',price>vwap[l]?1:-1,`Price ${price>vwap[l]?'above':'below'} VWAP`);
  }
  _E2(c,ind,price) {
    const {vwap}=ind, l=vwap.length-1;
    const atr14=IndicatorEngine.atr(c,14), lastATR=atr14.filter(v=>v!==null).slice(-1)[0]||0.3;
    const lowerBand=vwap[l]-lastATR;
    if (price>=lowerBand&&price<=vwap[l]) return this._vote('E','E2_VWAPDev',1,'Bouncing off VWAP -1σ');
    return this._vote('E','E2_VWAPDev',price>vwap[l]?0.5:-0.5,'Above/below VWAP');
  }
  _E3(c,ind) {
    const {obv}=ind;
    if (obv.length<10) return this._vote('E','E3_OBV',0,'Insufficient data');
    const r=obv.slice(-10);
    const fH=r.slice(0,5).reduce((a,b)=>a+b,0)/5, fS=r.slice(5).reduce((a,b)=>a+b,0)/5;
    return this._vote('E','E3_OBV',fS>fH?1:-1,`OBV ${fS>fH?'trending up':'trending down'}`);
  }
  _E4(c,ind) {
    const {cmf}=ind, v=cmf.filter(x=>x!==null);
    if (v.length===0) return this._vote('E','E4_CMF',0,'Insufficient data');
    const val=v[v.length-1];
    if (val>0.1) return this._vote('E','E4_CMF',1,`CMF: ${val.toFixed(3)} (bullish)`);
    if (val<-0.1) return this._vote('E','E4_CMF',-1,`CMF: ${val.toFixed(3)} (bearish)`);
    return this._vote('E','E4_CMF',0,`CMF neutral: ${val.toFixed(3)}`);
  }
  _E5(c,ind) {
    const {mfi}=ind, v=mfi.filter(x=>x!==null);
    if (v.length===0) return this._vote('E','E5_MFI',0,'Insufficient data');
    const val=v[v.length-1];
    if (val>60) return this._vote('E','E5_MFI',1,`MFI: ${val.toFixed(1)} (bullish)`);
    if (val<40) return this._vote('E','E5_MFI',-1,`MFI: ${val.toFixed(1)} (bearish)`);
    return this._vote('E','E5_MFI',0,`MFI neutral: ${val.toFixed(1)}`);
  }

  // ── GROUP F: OSCILLATOR ──
  _F1(c,ind) {
    const {williamsR}=ind, v=williamsR.filter(x=>x!==null);
    if (v.length<2) return this._vote('F','F1_Williams',0,'Insufficient data');
    const cur=v[v.length-1], prev=v[v.length-2];
    if (prev<=-80&&cur>-80) return this._vote('F','F1_Williams',1,'Williams %R crossed above -80');
    if (prev>=-20&&cur<-20) return this._vote('F','F1_Williams',-1,'Williams %R crossed below -20');
    return this._vote('F','F1_Williams',cur>-50?0.5:-0.5,`Williams %R: ${cur.toFixed(1)}`);
  }
  _F2(c,ind) {
    const {cci}=ind, v=cci.filter(x=>x!==null);
    if (v.length<2) return this._vote('F','F2_CCI',0,'Insufficient data');
    const cur=v[v.length-1], prev=v[v.length-2];
    if (prev<=-100&&cur>-100) return this._vote('F','F2_CCI',1,'CCI crossed above -100');
    if (prev>=100&&cur<100) return this._vote('F','F2_CCI',-1,'CCI crossed below 100');
    return this._vote('F','F2_CCI',cur>0?0.5:-0.5,`CCI: ${cur.toFixed(1)}`);
  }
  _F3(c) {
    if (c.length<28) return this._vote('F','F3_UltOsc',0,'Insufficient data');
    const bp=c.map((x,i)=>i===0?0:x.close-Math.min(x.low,c[i-1].low));
    const tr=c.map((x,i)=>i===0?x.high-x.low:Math.max(x.high-x.low,Math.abs(x.high-c[i-1].close),Math.abs(x.low-c[i-1].close)));
    const avg=p => { const bS=bp.slice(-p).reduce((a,b)=>a+b,0), tS=tr.slice(-p).reduce((a,b)=>a+b,0); return tS!==0?bS/tS:0.5; };
    const uo=(avg(7)*4+avg(14)*2+avg(28))/7*100;
    return this._vote('F','F3_UltOsc',uo>50?1:-1,`Ultimate Osc: ${uo.toFixed(1)}`);
  }
  _F4(c) {
    const period=20;
    if (c.length<period) return this._vote('F','F4_DPO',0,'Insufficient data');
    const closes=c.map(x=>x.close), sma=closes.slice(-period).reduce((a,b)=>a+b,0)/period;
    const shift=Math.floor(period/2)+1, dpo=closes[closes.length-shift]-sma;
    return this._vote('F','F4_DPO',dpo>0?1:-1,`DPO: ${dpo.toFixed(3)}`);
  }
  _F5(c,ind) {
    const {aroon}=ind, l=aroon.length-1;
    if (!aroon[l]||aroon[l].up===null) return this._vote('F','F5_Aroon',0,'Insufficient data');
    return this._vote('F','F5_Aroon',aroon[l].up>aroon[l].down?1:-1,`Aroon: ${aroon[l].up.toFixed(0)}/${aroon[l].down.toFixed(0)}`);
  }

  // ── GROUP G: PATTERN ──
  _G1(c) {
    if (c.length<2) return this._vote('G','G1_Engulf',0,'Insufficient data');
    const prev=c[c.length-2], cur=c[c.length-1];
    const bE=prev.close<prev.open&&cur.close>cur.open&&cur.open<=prev.close&&cur.close>=prev.open;
    const sE=prev.close>prev.open&&cur.close<cur.open&&cur.open>=prev.close&&cur.close<=prev.open;
    if (bE) return this._vote('G','G1_Engulf',1,'Bullish engulfing');
    if (sE) return this._vote('G','G1_Engulf',-1,'Bearish engulfing');
    return this._vote('G','G1_Engulf',0,'No engulfing');
  }
  _G2(c) {
    if (c.length<1) return this._vote('G','G2_PinBar',0,'Insufficient data');
    const last=c[c.length-1], body=Math.abs(last.close-last.open), range=last.high-last.low;
    const uW=last.high-Math.max(last.close,last.open), lW=Math.min(last.close,last.open)-last.low;
    if (range===0) return this._vote('G','G2_PinBar',0,'No range');
    if (lW>body*2&&uW<body*0.5) return this._vote('G','G2_PinBar',1,'Bullish pin bar');
    if (uW>body*2&&lW<body*0.5) return this._vote('G','G2_PinBar',-1,'Bearish pin bar');
    return this._vote('G','G2_PinBar',0,'No pin bar');
  }
  _G3(c) {
    if (c.length<2) return this._vote('G','G3_InsideBar',0,'Insufficient data');
    const prev=c[c.length-2], cur=c[c.length-1];
    const iB=cur.high<prev.high&&cur.low>prev.low;
    if (iB&&cur.close>prev.high) return this._vote('G','G3_InsideBar',1,'Inside bar bullish breakout');
    if (iB&&cur.close<prev.low) return this._vote('G','G3_InsideBar',-1,'Inside bar bearish breakout');
    return this._vote('G','G3_InsideBar',0,'No inside bar');
  }
  _G4(c,ind) {
    const {donchian}=ind, l=donchian.length-1;
    if (!donchian[l]||donchian[l].upper===null) return this._vote('G','G4_Donchian',0,'Insufficient data');
    const price=c[c.length-1].close;
    if (price>=donchian[l].upper) return this._vote('G','G4_Donchian',1,'Donchian upper break');
    if (price<=donchian[l].lower) return this._vote('G','G4_Donchian',-1,'Donchian lower break');
    return this._vote('G','G4_Donchian',0,'Inside Donchian channel');
  }
  _G5(c,price) {
    if (c.length<30) return this._vote('G','G5_SR',0,'Insufficient data');
    const r=c.slice(-30);
    const res=Math.max(...r.map(x=>x.high)), sup=Math.min(...r.map(x=>x.low));
    if (price<=sup*1.002) return this._vote('G','G5_SR',1,'Bouncing off support');
    if (price>=res*0.998) return this._vote('G','G5_SR',-1,'Rejected at resistance');
    return this._vote('G','G5_SR',0,'Mid-range');
  }

  // ── GROUP H: ML/ADAPTIVE ──
  _H1(mlAdj,perf) {
    const h=DateTime.utc().hour, hs=perf.hourlyStats?.[h];
    if (hs&&hs.trades>=3&&hs.wins/hs.trades>0.6) return this._vote('H','H1_Hourly',1,`Hour ${h} WR: ${(hs.wins/hs.trades*100).toFixed(0)}%`);
    return this._vote('H','H1_Hourly',0,`Hour ${h} insufficient data`);
  }
  _H2(perf) {
    const d=DateTime.utc().weekday, ds=perf.dayOfWeekStats?.[d-1];
    if (ds&&ds.trades>=5&&ds.pnl>0) return this._vote('H','H2_DayOfWeek',1,`Day ${d} profitable`);
    return this._vote('H','H2_DayOfWeek',0,`Day ${d} no edge`);
  }
  _H3(perf) {
    if (perf.currentWinStreak>=3) return this._vote('H','H3_Streak',1,`${perf.currentWinStreak} win streak`);
    if (perf.currentLossStreak>=3) return this._vote('H','H3_Streak',-1,`${perf.currentLossStreak} loss streak`);
    return this._vote('H','H3_Streak',0,'No significant streak');
  }
  _H4(perf,curVotes) {
    const best=perf.bestAgents?.slice(0,5)||[];
    if (best.length<3) return this._vote('H','H4_AgentAcc',0,'Insufficient agent data');
    let agr=0,cnt=0;
    for (const a of best) { const v=curVotes.find(x=>x.name===a.name); if (v) { agr+=v.rawVote; cnt++; } }
    if (cnt>=3) {
      const avg=agr/cnt;
      if (avg>0.5) return this._vote('H','H4_AgentAcc',1,'Top agents agree bullish');
      if (avg<-0.5) return this._vote('H','H4_AgentAcc',-1,'Top agents agree bearish');
    }
    return this._vote('H','H4_AgentAcc',0,'Top agents split');
  }
  _H5(regime) {
    if (regime.type==='TREND'||regime.type==='STRONG_TREND') return this._vote('H','H5_Regime',0.5,`${regime.type} regime`);
    if (regime.type==='RANGE') return this._vote('H','H5_Regime',-0.5,'Range regime');
    return this._vote('H','H5_Regime',0,'Regime unclear');
  }

  logVotes() {
    const ts=new Date().toISOString();
    let log=`\n[${ts}] Agent Votes (Score: ${(this.agentVotes.scorePct*100).toFixed(1)}% → ${this.agentVotes.direction})\n`;
    log+='─'.repeat(80)+'\n';
    for (const g of ['A','B','C','D','E','F','G','H']) {
      const gv=this.agentVotes.votes.filter(v=>v.group===g);
      log+=`\nGroup ${g} (weight ${this.groupWeights[g]}x):\n`;
      for (const v of gv) {
        const st=v.disabled?colors.yellow('[DISABLED]'):v.rawVote>0?colors.green('[+1]'):v.rawVote<0?colors.red('[-1]'):colors.gray('[ 0]');
        log+=`  ${st} ${v.name.padEnd(20)} ${v.reason}\n`;
      }
    }
    log+='─'.repeat(80)+'\n';
    fse.appendFileSync(CONFIG.VOTES_LOG, log);
  }

  updateDisabledAgents(perf) {
    const aa=perf.agentAccuracy||{};
    for (const [name,stats] of Object.entries(aa)) {
      if (stats.total>=50&&stats.accuracy<0.40) this.disabledAgents.add(name);
      else if (stats.total>=100&&stats.accuracy<0.45) this.disabledAgents.add(name);
      else if (stats.total>=50&&stats.accuracy>=0.50) this.disabledAgents.delete(name);
    }
  }
}

// ───────────────────────────────────────────────────────────
// ML CONFIDENCE
// ───────────────────────────────────────────────────────────
class MLConfidence {
  constructor() { this.tradeHistory=[]; }

  logTrade(signal,entry,exit,pnl,agentVotes,reason) {
    const trade={id:`trade_${Date.now()}`,signal,entry,exit,pnl,result:pnl>0?'win':pnl===0?'breakeven':'loss',reason,agentScore:agentVotes?.scorePct||0,timestamp:new Date().toISOString(),hour:DateTime.utc().hour,day:DateTime.utc().weekday};
    this.tradeHistory.push(trade);
    if (this.tradeHistory.length>1000) this.tradeHistory=this.tradeHistory.slice(-1000);
    return trade;
  }

  getConfidenceAdjustment(perf) {
    if (this.tradeHistory.length<5) return {adjustment:0,note:'Learning phase'};
    const recent=this.tradeHistory.slice(-20), wins=recent.filter(t=>t.result==='win').length;
    const wr=wins/recent.length;
    let adj=0, note='';
    if (wr>0.65) { adj=0.1; note=`Recent WR ${wr.toFixed(0)}% — boosting confidence`; }
    else if (wr<0.40) { adj=-0.15; note=`Recent WR ${wr.toFixed(0)}% — reducing confidence`; }
    else { note=`Recent WR ${wr.toFixed(0)}% — neutral`; }
    return {adjustment:adj,note,recentWinRate:wr};
  }

  async save() { await fse.writeJson(CONFIG.TRADES_FILE, this.tradeHistory, {spaces:2}); }
  async load() { if (await fse.pathExists(CONFIG.TRADES_FILE)) this.tradeHistory=await fse.readJson(CONFIG.TRADES_FILE); }
}

// ───────────────────────────────────────────────────────────
// RISK MANAGER
// ───────────────────────────────────────────────────────────
class RiskManager {
  constructor(startingBalance) {
    this.startingBalance=startingBalance; this.currentBalance=startingBalance;
    this.dailyPnl=0; this.totalDrawdown=0; this.tradingHalted=false; this.haltReason='';
  }

  calculateLotSize(scorePct,balance,slDistance,atr) {
    // USOIL: 1 lot = 1000 barrels, $1 move = $1000 per lot, 0.01 lot = $10 per $1
    // Lot size = riskAmount / (slDistance * contractValuePerLot)
    const riskAmt=balance*CONFIG.RISK_PCT;
    const contractValue=1000; // $1000 per $1 move per 1 lot
    let lot=riskAmt/(slDistance*contractValue);
    if (scorePct>0.85) lot*=1.5;
    else if (scorePct>0.75) lot*=1.2;
    else lot*=0.8;
    if (atr&&atr>0.4) lot*=1.2;
    return roundLot(clamp(lot,CONFIG.MIN_LOT,CONFIG.MAX_LOT));
  }

  checkSpread(spread) {
    if (spread>CONFIG.MAX_SPREAD) return {passed:false,reason:`Spread ${spread.toFixed(3)} > max ${CONFIG.MAX_SPREAD}`};
    return {passed:true};
  }

  checkDailyLoss(dailyPnl,balance) {
    this.dailyPnl=dailyPnl; this.currentBalance=balance;
    const maxLoss=this.startingBalance*CONFIG.MAX_DAILY_LOSS_PCT;
    if (dailyPnl<=-maxLoss) { this.tradingHalted=true; this.haltReason=`Daily loss limit: $${dailyPnl.toFixed(2)} / -$${maxLoss.toFixed(2)}`; return {passed:false,reason:this.haltReason}; }
    return {passed:true};
  }

  checkDrawdown(balance) {
    const dd=(this.startingBalance-balance)/this.startingBalance;
    this.totalDrawdown=dd;
    if (dd>CONFIG.MAX_DRAWDOWN_PCT) { this.tradingHalted=true; this.haltReason=`Max drawdown: ${(dd*100).toFixed(1)}%`; return {passed:false,reason:this.haltReason}; }
    return {passed:true};
  }

  detectRegime(adxVal,atrVal,avgATR) {
    let type='RANGE';
    if (adxVal>=20&&adxVal<=35) type='TREND';
    else if (adxVal>35) type='STRONG_TREND';
    let vol='normal';
    if (atrVal<avgATR*0.7) vol='low';
    else if (atrVal>avgATR*2) vol='spike';
    return {type,adx:adxVal,atr:atrVal,avgATR,volStatus:vol,shouldTrade:type!=='RANGE'&&vol==='normal'};
  }

  resetDailyPnl() { this.dailyPnl=0; this.tradingHalted=false; this.haltReason=''; this.startingBalance=this.currentBalance; }
}

// ───────────────────────────────────────────────────────────
// POSITION MANAGER
// ───────────────────────────────────────────────────────────
class PositionManager {
  constructor(connection) { this.connection=connection; this.posStates=new Map(); }

  async managePositions(candles,currentPrice) {
    try {
      const positions=await retryWithBackoff(()=>this.connection.getPositions());
      const myPos=positions.filter(p=>p.symbol===CONFIG.SYMBOL);
      for (const pos of myPos) {
        const profit=pos.profit||0, pk=pos.id;
        if (!this.posStates.has(pk)) this.posStates.set(pk,{peakProfit:profit,breakevenSet:false,entryPrice:pos.openPrice,stage:1});
        const st=this.posStates.get(pk);
        if (profit>st.peakProfit) st.peakProfit=profit;

        // STAGE 1: Let it run freely — no closes until $2 profit
        // BREAKEVEN: close at +$2.00 (was $1.00 — much more patient)
        if (st.stage===1&&!st.breakevenSet&&profit>=2.00) {
          try {
            await this.connection.closePosition(pk);
            console.log(colors.yellow(`  🟡 BREAKEVEN: Closed ${pk.slice(0,8)} at +$${profit.toFixed(2)}`));
            this.posStates.delete(pk); continue;
          } catch(e) { console.log(colors.yellow(`  ⚠ BE failed: ${e.message}`)); }
        }

        // STAGE 2: Once past $2, trail loosely from $4 peak, $0.80 pullback
        if (st.stage===1&&profit>=2.00) st.stage=2;
        if (st.stage===2&&st.peakProfit>=4.00&&profit<st.peakProfit-0.80) {
          try {
            await this.connection.closePosition(pk);
            console.log(colors.green(`  🟢 LOOSE TRAIL: Closed ${pk.slice(0,8)} | Peak: +$${st.peakProfit.toFixed(2)} → $${profit.toFixed(2)}`));
            this.posStates.delete(pk); continue;
          } catch(e) {}
        }

        // STAGE 3: Past $5 peak, tighten trail to $1.00 pullback from peak
        if (st.stage===2&&st.peakProfit>=5.00) st.stage=3;
        if (st.stage===3&&st.peakProfit>=7.00&&profit<st.peakProfit-1.00) {
          try {
            await this.connection.closePosition(pk);
            console.log(colors.green(`  🟢 MEDIUM TRAIL: Closed ${pk.slice(0,8)} | Peak: +$${st.peakProfit.toFixed(2)} → $${profit.toFixed(2)}`));
            this.posStates.delete(pk); continue;
          } catch(e) {}
        }

        // STAGE 4: Past $10 peak, tight trail at $1.20 from peak, or hard cap at $15
        if (st.stage===3&&st.peakProfit>=10.00) st.stage=4;
        if (st.stage===4&&st.peakProfit>=12.00&&profit<st.peakProfit-1.20) {
          try {
            await this.connection.closePosition(pk);
            console.log(colors.green(`  🟢 TIGHT TRAIL: Closed ${pk.slice(0,8)} | Peak: +$${st.peakProfit.toFixed(2)} → $${profit.toFixed(2)}`));
            this.posStates.delete(pk); continue;
          } catch(e) {}
        }

        // HARD TP at $15 (was $8 — hold winners MUCH longer)
        if (profit>=15.00) {
          try {
            await this.connection.closePosition(pk);
            console.log(colors.green(`  🟢 MEGA TAKE PROFIT: Closed ${pk.slice(0,8)} at +$${profit.toFixed(2)} 🎉`));
            this.posStates.delete(pk); continue;
          } catch(e) {}
        }

        // Status messages — show stage and targets
        if (profit>=2.00) console.log(colors.green(`  🚀 STAGE ${st.stage} | ${pk.slice(0,8)} | +$${profit.toFixed(2)} | Peak: +$${st.peakProfit.toFixed(2)} | BE:$2 | Trail:$4/$0.80`));
        else if (profit>=0.50) console.log(colors.green(`  💪 HOLDING ${pk.slice(0,8)} | +$${profit.toFixed(2)} | Peak: +$${st.peakProfit.toFixed(2)} | No close until $2`));
        else if (profit>=0.10) console.log(colors.green(`  📈 HOLDING ${pk.slice(0,8)} | +$${profit.toFixed(2)}`));
      }
      return myPos.length;
    } catch(e) { console.log(colors.red(`  ❌ Pos manage error: ${e.message}`)); return 0; }
  }
}

// ───────────────────────────────────────────────────────────
// TRADE EXECUTOR
// ───────────────────────────────────────────────────────────
class TradeExecutor {
  constructor(connection) { this.connection=connection; this.lastTradeTime=0; }

  canTrade() {
    const since=Date.now()-this.lastTradeTime;
    if (since<CONFIG.TRADE_COOLDOWN) return {canTrade:false,reason:`Cooldown: ${Math.round((CONFIG.TRADE_COOLDOWN-since)/1000)}s`};
    return {canTrade:true};
  }

  async execute(signal,price,lotSize,slDist,tpDist) {
    const sl=signal==='BUY'?roundLot((price-slDist)*100)/100:roundLot((price+slDist)*100)/100;
    const tp=signal==='BUY'?roundLot((price+tpDist)*100)/100:roundLot((price-tpDist)*100)/100;
    console.log(colors.cyan(`  🎯 Executing ${signal}: ${lotSize} lots @ ${price.toFixed(3)}`));
    console.log(colors.cyan(`     SL: ${sl} ($${slDist.toFixed(2)}) | TP: ${tp} ($${tpDist.toFixed(2)})`));
    try {
      let result;
      if (signal==='BUY') result=await retryWithBackoff(()=>this.connection.createMarketBuyOrder(CONFIG.SYMBOL,lotSize,sl,undefined,{comment:'SHIVA_V3'}));
      else result=await retryWithBackoff(()=>this.connection.createMarketSellOrder(CONFIG.SYMBOL,lotSize,sl,undefined,{comment:'SHIVA_V3'}));
      const id=result.stringCode||result.id||'unknown';
      console.log(colors.green(`  ✅ Order filled: ${id}`));
      this.lastTradeTime=Date.now();
      return {success:true,id,lotSize,sl,tp};
    } catch(e) { console.log(colors.red(`  ❌ Order failed: ${e.message}`)); return {success:false,error:e.message}; }
  }
}

// ───────────────────────────────────────────────────────────
// PERFORMANCE TRACKER
// ───────────────────────────────────────────────────────────
class PerformanceTracker {
  constructor() {
    this.stats={totalTrades:0,wins:0,losses:0,breakevens:0,winRate:0,profitFactor:0,avgWin:0,avgLoss:0,expectancy:0,maxConsecLosses:0,currentWinStreak:0,currentLossStreak:0,totalPnl:0,bestTrade:0,worstTrade:0};
    this.hourlyStats={}; this.dayOfWeekStats={}; this.agentAccuracy={};
    this.bestAgents=[]; this.worstAgents=[];
  }

  recordTrade(trade) {
    this.stats.totalTrades++; this.stats.totalPnl+=trade.pnl;
    if (trade.pnl>0) { this.stats.wins++; this.stats.currentWinStreak++; this.stats.currentLossStreak=0; this.stats.maxConsecLosses=Math.max(this.stats.maxConsecLosses,this.stats.currentLossStreak); this.stats.bestTrade=Math.max(this.stats.bestTrade,trade.pnl); }
    else if (trade.pnl<0) { this.stats.losses++; this.stats.currentLossStreak++; this.stats.currentWinStreak=0; this.stats.worstTrade=Math.min(this.stats.worstTrade,trade.pnl); }
    else { this.stats.breakevens++; }

    const twl=this.stats.wins+this.stats.losses;
    this.stats.winRate=twl>0?this.stats.wins/twl:0;
    this.stats.avgWin=this.stats.wins>0?(this.stats.totalPnl>0?this.stats.totalPnl:0)/this.stats.wins:0;
    this.stats.avgLoss=this.stats.losses>0?(this.stats.totalPnl<0?Math.abs(this.stats.totalPnl):0)/this.stats.losses:0;
    this.stats.profitFactor=this.stats.avgLoss>0?this.stats.avgWin/this.stats.avgLoss:0;
    this.stats.expectancy=twl>0?(this.stats.winRate*this.stats.avgWin)-((1-this.stats.winRate)*Math.abs(this.stats.avgLoss)):0;

    const h=trade.hour;
    if (!this.hourlyStats[h]) this.hourlyStats[h]={trades:0,wins:0,losses:0,pnl:0};
    this.hourlyStats[h].trades++; this.hourlyStats[h].pnl+=trade.pnl;
    if (trade.pnl>0) this.hourlyStats[h].wins++; else if (trade.pnl<0) this.hourlyStats[h].losses++;

    const d=trade.day-1;
    if (!this.dayOfWeekStats[d]) this.dayOfWeekStats[d]={trades:0,wins:0,losses:0,pnl:0};
    this.dayOfWeekStats[d].trades++; this.dayOfWeekStats[d].pnl+=trade.pnl;
    if (trade.pnl>0) this.dayOfWeekStats[d].wins++; else if (trade.pnl<0) this.dayOfWeekStats[d].losses++;
  }

  updateAgentAccuracy(agentVotes,tradeResult) {
    const dir=tradeResult.pnl>0?1:tradeResult.pnl<0?-1:0;
    for (const v of agentVotes.votes) {
      if (v.disabled) continue;
      if (!this.agentAccuracy[v.name]) this.agentAccuracy[v.name]={correct:0,total:0,accuracy:0};
      this.agentAccuracy[v.name].total++;
      const correct=(v.rawVote>0&&dir>0)||(v.rawVote<0&&dir<0)||(v.rawVote===0);
      if (correct) this.agentAccuracy[v.name].correct++;
      this.agentAccuracy[v.name].accuracy=this.agentAccuracy[v.name].correct/this.agentAccuracy[v.name].total;
    }
    const sorted=Object.entries(this.agentAccuracy).map(([n,s])=>({name:n,...s})).filter(a=>a.total>=10).sort((a,b)=>b.accuracy-a.accuracy);
    this.bestAgents=sorted.slice(0,5); this.worstAgents=sorted.slice(-5).reverse();
  }

  async save() {
    await fse.writeJson(CONFIG.PERFORMANCE_FILE,{stats:this.stats,hourlyStats:this.hourlyStats,dayOfWeekStats:this.dayOfWeekStats,agentAccuracy:this.agentAccuracy,bestAgents:this.bestAgents,worstAgents:this.worstAgents,savedAt:new Date().toISOString()},{spaces:2});
  }
  async load() {
    if (await fse.pathExists(CONFIG.PERFORMANCE_FILE)) {
      const d=await fse.readJson(CONFIG.PERFORMANCE_FILE);
      Object.assign(this.stats,d.stats||{}); Object.assign(this.hourlyStats,d.hourlyStats||{});
      Object.assign(this.dayOfWeekStats,d.dayOfWeekStats||{}); Object.assign(this.agentAccuracy,d.agentAccuracy||{});
      this.bestAgents=d.bestAgents||[]; this.worstAgents=d.worstAgents||[];
    }
  }
}

// ───────────────────────────────────────────────────────────
// SHIVA BOT ORCHESTRATOR
// ───────────────────────────────────────────────────────────
class ShivaBot {
  constructor() {
    this.api=null; this.metaAccount=null; this.connection=null;
    this.marketData=null; this.indicators={};
    this.ictAnalyzer=new ICTAnalyzer();
    this.agentVoter=new AgentVoter();
    this.mlConfidence=new MLConfidence();
    this.riskManager=null; this.positionManager=null; this.tradeExecutor=null;
    this.performanceTracker=new PerformanceTracker();
    this.running=false; this.startingBalance=0; this.lastDayReset=null;
  }

  async initialize() {
    console.log(colors.bold.cyan('\n╔═══════════════════════════════════════════════════════╗'));
    console.log(colors.bold.cyan('║           SHIVA Bot V3 — Initializing...              ║'));
    console.log(colors.bold.cyan('╚═══════════════════════════════════════════════════════╝\n'));

    await this.performanceTracker.load();
    await this.mlConfidence.load();
    console.log(colors.green('✓ Performance data loaded'));

    try {
      const redis=new Redis({url:CONFIG.REDIS_URL,token:CONFIG.REDIS_TOKEN});
      await redis.set('shiva:v3:test',{ok:true,time:new Date().toISOString()});
      console.log(colors.green('✓ Redis connected'));
    } catch(e) { console.log(colors.yellow(`⚠ Redis: ${e.message}`)); }

    console.log(colors.cyan('Connecting to MetaAPI...'));
    this.api=new MetaApi(CONFIG.METAAPI_TOKEN,{
      provisioningUrl:'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai',
      mtUrl:'https://mt-client-api-v1.new-york.agiliumtrade.agiliumtrade.ai'
    });

    const account=await this.api.metatraderAccountApi.getAccount(CONFIG.ACCOUNT_ID);
    if (account.state!=='DEPLOYED') throw new Error(`Account not deployed: ${account.state}`);

    this.metaAccount=account;
    this.connection=account.getRPCConnection();
    await this.connection.connect();
    await this.connection.waitSynchronized();
    console.log(colors.green(`✓ Connected: ${account.name} (${account.type})`));

    this.marketData=new MarketData(this.metaAccount, this.connection, CONFIG.SYMBOL);
    await this.marketData.updatePrice();
    console.log(colors.green(`✓ Price: ${CONFIG.SYMBOL} bid=${this.marketData.price.bid} ask=${this.marketData.price.ask} spread=${this.marketData.price.spread.toFixed(3)}`));

    console.log(colors.cyan('Fetching M15 candles...'));
    await this.marketData.fetchCandles(300);

    const balance=(await this.connection.getAccountInformation()).balance||0;
    this.startingBalance=balance;
    this.riskManager=new RiskManager(balance);
    this.positionManager=new PositionManager(this.connection);
    this.tradeExecutor=new TradeExecutor(this.connection);
    console.log(colors.green(`✓ Balance: $${balance.toFixed(2)}`));

    this.agentVoter.updateDisabledAgents(this.performanceTracker);
    console.log(colors.green(`✓ All components initialized\n`));
  }

  calculateIndicators() {
    const c=this.marketData.candles, closes=c.map(x=>x.close);
    this.indicators={
      ema9:IndicatorEngine.ema(closes,9), ema21:IndicatorEngine.ema(closes,21),
      ema50:IndicatorEngine.ema(closes,50), ema200:IndicatorEngine.ema(closes,200),
      rsi:IndicatorEngine.rsi(closes,14), macd:IndicatorEngine.macd(closes),
      stoch:IndicatorEngine.stochastic(c), atr:IndicatorEngine.atr(c,14),
      bb:IndicatorEngine.bollingerBands(closes), vwap:IndicatorEngine.vwap(c),
      supertrend:IndicatorEngine.supertrend(c,10,3),
      adxData:IndicatorEngine.adx(c,14),
      williamsR:IndicatorEngine.williamsR(c,14), cci:IndicatorEngine.cci(c,20),
      roc:IndicatorEngine.roc(closes,10), cmf:IndicatorEngine.cmf(c,20),
      obv:IndicatorEngine.obv(c), mfi:IndicatorEngine.mfi(c,14),
      aroon:IndicatorEngine.aroon(c,25), donchian:IndicatorEngine.donchian(c,20),
    };
    return this.indicators;
  }

  async runCycle() {
    console.log(colors.bold.cyan('\n┌─────────────────────────────────────────────────┐'));
    console.log(colors.bold.cyan(`│ SHIVA V3 │ ${CONFIG.SYMBOL} M15 │ ${new Date().toISOString().slice(0,19).replace('T',' ')} │`));
    console.log(colors.bold.cyan('├──────────┬──────────┬──────────┬────────────────┤'));

    try {
      // Reconnect if needed
      try { await this.connection.getAccountInformation(); }
      catch(e) { console.log(colors.yellow('⚠ Reconnecting...')); await this.initialize(); }

      await this.marketData.updatePrice();

      // Daily reset
      const today=DateTime.utc().toFormat('yyyy-MM-dd');
      if (this.lastDayReset!==today) { this.riskManager.resetDailyPnl(); this.lastDayReset=today; console.log(colors.green('📅 New day — daily PnL reset')); }

      const ai=await this.connection.getAccountInformation();
      const balance=ai.balance||0, equity=ai.equity||0, openPnl=equity-balance;

      this.calculateIndicators();
      const ictAnalysis=this.ictAnalyzer.analyze(this.marketData.candles);

      // Regime
      const vADX=this.indicators.adxData.filter(d=>d&&d.adx!==null);
      const vATR=this.indicators.atr.filter(a=>a!==null);
      const curADX=vADX.length>0?vADX[vADX.length-1].adx:0;
      const curATR=vATR.length>0?vATR[vATR.length-1]:0;
      const avgATR=vATR.length>14?vATR.slice(-14).reduce((a,b)=>a+b,0)/14:curATR;
      const regime=this.riskManager.detectRegime(curADX,curATR,avgATR);

      // Voting
      this.agentVoter.updateDisabledAgents(this.performanceTracker);
      const mlAdj=this.mlConfidence.getConfidenceAdjustment(this.performanceTracker);
      const voteResult=this.agentVoter.vote(this.marketData.candles,this.indicators,ictAnalysis,regime,mlAdj,this.performanceTracker);

      // Risk checks
      const spreadChk=this.riskManager.checkSpread(this.marketData.price.spread);
      const dailyChk=this.riskManager.checkDailyLoss(this.riskManager.dailyPnl,balance);
      const ddChk=this.riskManager.checkDrawdown(balance);

      // Manage positions
      const openPos=await this.positionManager.managePositions(this.marketData.candles,this.marketData.price.bid);

      // Dashboard
      this.displayDashboard({balance,equity,openPnl,dailyPnl:this.riskManager.dailyPnl,voteResult,regime,curADX,curATR,ictAnalysis,spread:this.marketData.price.spread,openPos});

      // Log votes
      this.agentVoter.logVotes();

      // Trade decision
      if (voteResult.shouldTrade&&spreadChk.passed&&dailyChk.passed&&ddChk.passed) {
        const cd=this.tradeExecutor.canTrade();
        if (cd.canTrade&&openPos<CONFIG.MAX_POSITIONS) {
          const price=this.marketData.price.bid, signal=voteResult.direction;
          const lastCandle=this.marketData.getLatestCandle();
          
          // Tighter SL: use the wick only (bottom wick for BUY, top wick for SELL)
          let slDist;
          if (lastCandle) {
            const close=lastCandle.close;
            if (signal==='BUY') {
              // BUY: SL below the bottom wick (close - low), add small buffer
              const bottomWick = close - lastCandle.low;
              slDist = Math.max(0.10, Math.min(bottomWick + 0.05, 1.50));
              console.log(colors.cyan(`  📏 Wick SL: bottom wick=$${bottomWick.toFixed(3)} → SL dist=$${slDist.toFixed(3)}`));
            } else {
              // SELL: SL above the top wick (high - close), add small buffer
              const topWick = lastCandle.high - close;
              slDist = Math.max(0.10, Math.min(topWick + 0.05, 1.50));
              console.log(colors.cyan(`  📏 Wick SL: top wick=$${topWick.toFixed(3)} → SL dist=$${slDist.toFixed(3)}`));
            }
          } else {
            slDist = 0.50;
          }
          
          const tpDist=slDist*CONFIG.RR_RATIO;
          const lotSize=this.riskManager.calculateLotSize(voteResult.score,balance,slDist,curATR);

          const result=await this.tradeExecutor.execute(signal,price,lotSize,slDist,tpDist);
          if (result.success) {
            const trade=this.mlConfidence.logTrade(signal,price,price,0,{scorePct:voteResult.score},'entry');
            this.performanceTracker.recordTrade(trade);
            this.performanceTracker.updateAgentAccuracy(this.agentVoter.agentVotes,trade);
            await this.performanceTracker.save();
            await this.mlConfidence.save();
          }
        } else { console.log(colors.yellow(`⏸️ Skip: ${cd.reason||`Max pos (${openPos}/${CONFIG.MAX_POSITIONS})`}`)); }
      } else {
        const reason=!voteResult.shouldTrade?`Score ${(voteResult.score*100).toFixed(1)}% < ${(CONFIG.MIN_SCORE_THRESHOLD*100).toFixed(0)}%`:!spreadChk.passed?spreadChk.reason:!dailyChk.passed?dailyChk.reason:ddChk.reason;
        console.log(colors.yellow(`⏸️ HOLD: ${reason}`));
      }

      // Redis push
      await this.pushToRedis({balance,equity,openPnl,voteResult,regime,curADX,curATR,openPos});

    } catch(e) { console.log(colors.red(`❌ Cycle error: ${e.message}`)); }
  }

  displayDashboard(data) {
    const {balance,equity,openPnl,dailyPnl,voteResult,regime,curADX,curATR,ictAnalysis,spread,openPos}=data;
    console.log(colors.bold.cyan('├──────────┼──────────┼──────────┼────────────────┤'));
    console.log(colors.bold.cyan('│ Balance  │ Equity   │ Open P&L │ Today P&L      │'));
    console.log(colors.bold.cyan('├──────────┼──────────┼──────────┼────────────────┤'));
    console.log(colors.cyan(`│ ${colors.bold(balance.toFixed(2).padStart(8))} │ ${colors.bold(equity.toFixed(2).padStart(8))} │ ${openPnl>=0?colors.green('+'+openPnl.toFixed(2).padStart(8)):colors.red(openPnl.toFixed(2).padStart(9))} │ ${dailyPnl>=0?colors.green('+'+dailyPnl.toFixed(2).padStart(12)):colors.red(dailyPnl.toFixed(2).padStart(13))} │`));
    console.log(colors.bold.cyan('├──────────┴──────────┴──────────┴────────────────┤'));
    const wr=this.performanceTracker.stats.winRate;
    const streak=this.performanceTracker.stats.currentWinStreak>0?colors.green('W'+this.performanceTracker.stats.currentWinStreak):colors.red('L'+this.performanceTracker.stats.currentLossStreak);
    console.log(colors.cyan(`│ Win Rate: ${(wr*100).toFixed(1).padStart(5)}% │ Streak: ${streak} │ Trades: ${this.performanceTracker.stats.totalTrades.toString().padStart(5)} │ PF: ${this.performanceTracker.stats.profitFactor.toFixed(2).padStart(6)} │`));
    console.log(colors.bold.cyan('├─────────────────────────────────────────────────┤'));
    const scoreBar='█'.repeat(Math.round(voteResult.score*10));
    const sc=voteResult.direction==='BUY'?colors.green:voteResult.direction==='SELL'?colors.red:colors.yellow;
    console.log(sc(`│ Agent Score: [${scoreBar.padEnd(10)}] ${(voteResult.score*100).toFixed(1).padStart(4)}% → ${voteResult.direction.padEnd(4)} │`));
    console.log(colors.cyan(`│ Regime: ${regime.type.padEnd(12)} │ ADX: ${curADX.toFixed(1).padStart(5)} │ ATR: ${curATR.toFixed(2).padStart(5)} │`));
    console.log(colors.cyan(`│ Kill Zone: ${ictAnalysis.killZone.zone.padEnd(10)} ${ictAnalysis.killZone.inZone?'✓':'✗'} │ Spread: ${spread.toFixed(3).padStart(6)} │ Pos: ${openPos}/${CONFIG.MAX_POSITIONS} │`));
    console.log(colors.bold.cyan('├─────────────────────────────────────────────────┤'));
    if (voteResult.shouldTrade) console.log(colors.green(`│ Decision: ${'TRADE'.padEnd(8)} ${voteResult.direction.padEnd(4)} │`));
    else console.log(colors.yellow(`│ Decision: ${'HOLD'.padEnd(15)} │`));
    console.log(colors.bold.cyan('└─────────────────────────────────────────────────┘'));
  }

  async pushToRedis(data) {
    try {
      const redis=new Redis({url:CONFIG.REDIS_URL,token:CONFIG.REDIS_TOKEN});
      await redis.set('shiva:v3:status',{balance:data.balance,equity:data.equity,score:data.voteResult.score,direction:data.voteResult.direction,regime:data.regime.type,positions:data.openPos,timestamp:new Date().toISOString()});
    } catch(e) {}
  }

  async start() {
    await this.initialize();
    this.running=true;
    console.log(colors.bold.green('\n🚀 SHIVA V3 is LIVE — M15 synced trading\n'));

    // Position monitor (30s)
    const posMon=setInterval(async ()=>{
      if (this.running&&this.connection) await this.positionManager.managePositions(this.marketData.candles,this.marketData.price.bid);
    },CONFIG.POSITION_MONITOR_INTERVAL);

    // Spread check (60s)
    const sprMon=setInterval(async ()=>{
      if (this.running&&this.marketData) {
        await this.marketData.updatePrice();
        const sc=this.riskManager.checkSpread(this.marketData.price.spread);
        if (!sc.passed) console.log(colors.yellow(`⚠️ Wide spread: ${sc.reason}`));
      }
    },CONFIG.SPREAD_CHECK_INTERVAL);

    // 9-minute cycle loop (takes trades every 9 minutes)
    const runCycle=async ()=>{
      if (!this.running) return;
      await this.runCycle();
      const interval=9*60*1000;
      console.log(colors.gray(`  ⏳ Next cycle in 9 min`));
      setTimeout(runCycle,interval);
    };

    await runCycle();

    // Graceful shutdown
    process.on('SIGINT',async ()=>{
      console.log(colors.bold.yellow('\n\n🛑 SHIVA V3 shutting down...'));
      this.running=false;
      clearInterval(posMon); clearInterval(sprMon);
      await this.performanceTracker.save();
      await this.mlConfidence.save();
      console.log(colors.green(`\n📊 Final: ${this.performanceTracker.stats.totalTrades} trades | WR: ${(this.performanceTracker.stats.winRate*100).toFixed(1)}% | PnL: $${this.performanceTracker.stats.totalPnl.toFixed(2)}`));
      process.exit(0);
    });
  }
}

// ───────────────────────────────────────────────────────────
// ENTRY POINT
// ───────────────────────────────────────────────────────────
const bot=new ShivaBot();
bot.start().catch(err=>{ console.error(colors.red('💀 Fatal:'),err.message); process.exit(1); });
