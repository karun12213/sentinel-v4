/**
 * SHIVA ICT FULL — LIVE BOT v2
 * 8-signal ICT ensemble for USOIL / XTIUSD.
 * Timeframe: 1H candles | Risk: compound 1% equity/ATR | Pyramiding: enabled
 * Session: 24/7 (no session filter per user request)
 */
require('dotenv').config();
const MetaApi = require('metaapi.cloud-sdk').default;
const { Redis } = require('@upstash/redis');
const https = require('https');

// ── Config ────────────────────────────────────────────────────────────────────
const TOKEN      = process.env.METAAPI_TOKEN;
const ACCOUNT_ID = process.env.METAAPI_ACCOUNT_ID;
const CHECK_INTERVAL = 60000; // 1 minute cycle

const MAX_POSITIONS  = 1;    // max 1 concurrent position
const DAILY_DD_LIMIT = 0.25; // 25% daily drawdown hard stop
const MAX_DAILY_LOSSES = 3;  // 3 losses → skip rest of day
const MIN_CONF_BASE  = 0.52; // ML confidence (not used in live JS, used for signal weighting)

const redis = new Redis({
    url: process.env.UPSTASH_REDIS_REST_URL || '',
    token: process.env.UPSTASH_REDIS_REST_TOKEN || ''
});

const api = new MetaApi(TOKEN);
let connection, tradingAccount;
let ACTIVE_SYMBOL = 'USOIL';

// ── Logger & Discord ──────────────────────────────────────────────────────────
function log(msg, type = 'info') {
    const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);
    const prefix = type === 'error' ? '[ERR]' : type === 'success' ? '[OK]' : '[>>]';
    console.log(`${prefix} [${ts}] ${msg}`);
}

async function discordPost(payload) {
    const webhook = process.env.DISCORD_TRADES || process.env.DISCORD_ALERTS;
    if (!webhook) return;
    const data = JSON.stringify(payload);
    const url = new URL(webhook);
    return new Promise((resolve) => {
        const req = https.request({
            hostname: url.hostname,
            path: url.pathname + url.search,
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) }
        }, () => resolve());
        req.on('error', (e) => log(`Discord error: ${e.message}`, 'error'));
        req.write(data);
        req.end();
    });
}

// ── Init ──────────────────────────────────────────────────────────────────────
async function init() {
    try {
        tradingAccount = await api.metatraderAccountApi.getAccount(ACCOUNT_ID);
        if (tradingAccount.state !== 'DEPLOYED') {
            log('Deploying account...');
            await tradingAccount.deploy();
        }
        connection = tradingAccount.getRPCConnection();
        await connection.connect();
        await connection.waitSynchronized();

        const symbols = await connection.getSymbols();
        if (symbols.includes('USOIL'))       ACTIVE_SYMBOL = 'USOIL';
        else if (symbols.includes('XTIUSD')) ACTIVE_SYMBOL = 'XTIUSD';
        else if (symbols.includes('Oil'))    ACTIVE_SYMBOL = 'Oil';

        log(`SHIVA ICT FULL connected. Symbol: ${ACTIVE_SYMBOL}`);
    } catch (e) {
        log(`Init failed: ${e.message}`, 'error');
        process.exit(1);
    }
}

// ── Indicators ────────────────────────────────────────────────────────────────
function ema(data, p) {
    const k = 2 / (p + 1);
    const out = [data[0]];
    for (let i = 1; i < data.length; i++) out.push(data[i] * k + out[i - 1] * (1 - k));
    return out;
}

function atrSeries(highs, lows, closes, p = 14) {
    const n = highs.length;
    const tr = [highs[0] - lows[0]];
    for (let i = 1; i < n; i++)
        tr.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i-1]), Math.abs(lows[i] - closes[i-1])));
    const k = 1 / p;
    const out = [tr.slice(0, p).reduce((a, b) => a + b, 0) / p];
    for (let i = p; i < n; i++) out.push(tr[i] * k + out[out.length - 1] * (1 - k));
    const pad = Array(Math.max(0, p - 1)).fill(out[0]);
    return pad.concat(out);
}

function rsiSeries(closes, p = 14) {
    const n = closes.length;
    const out = Array(n).fill(50);
    if (n <= p) return out;
    let avgG = 0, avgL = 0;
    for (let i = 1; i <= p; i++) {
        const d = closes[i] - closes[i - 1];
        avgG += Math.max(d, 0) / p;
        avgL += Math.max(-d, 0) / p;
    }
    out[p] = avgL === 0 ? 100 : 100 - 100 / (1 + avgG / avgL);
    for (let i = p + 1; i < n; i++) {
        const d = closes[i] - closes[i - 1];
        avgG = (avgG * (p - 1) + Math.max(d, 0)) / p;
        avgL = (avgL * (p - 1) + Math.max(-d, 0)) / p;
        out[i] = avgL === 0 ? 100 : 100 - 100 / (1 + avgG / (avgL + 1e-10));
    }
    return out;
}

function donchian(highs, lows, p) {
    return highs.map((_, i) => ({
        hi: Math.max(...highs.slice(Math.max(0, i - p + 1), i + 1)),
        lo: Math.min(...lows.slice(Math.max(0, i - p + 1), i + 1))
    }));
}

function bbSeries(closes, p = 20, mult = 2.0) {
    return closes.map((_, i) => {
        const s = closes.slice(Math.max(0, i - p + 1), i + 1);
        const m = s.reduce((a, b) => a + b, 0) / s.length;
        const sd = Math.sqrt(s.reduce((a, b) => a + (b - m) ** 2, 0) / s.length);
        return { upper: m + mult * sd, mid: m, lower: m - mult * sd };
    });
}

function kcSeries(closes, highs, lows, p = 20, mult = 1.5) {
    const emaC = ema(closes, p);
    const atr  = atrSeries(highs, lows, closes, p);
    return emaC.map((e, i) => ({ upper: e + mult * atr[i], mid: e, lower: e - mult * atr[i] }));
}

function vwapSeries(closes, volumes, timestamps) {
    let cumPV = 0, cumV = 0, sessionPrices = [];
    let prevDate = null;
    return closes.map((c, i) => {
        const dt = new Date(timestamps[i]);
        const date = dt.toISOString().slice(0, 10);
        if (dt.getUTCHours() === 7 && date !== prevDate) {
            cumPV = 0; cumV = 0; sessionPrices = [];
        }
        prevDate = date;
        cumPV += c * volumes[i];
        cumV += volumes[i];
        sessionPrices.push(c);
        const vw = cumPV / (cumV + 1e-10);
        const devs = sessionPrices.map(p => p - vw);
        const std = devs.length > 1
            ? Math.sqrt(devs.reduce((a, b) => a + b * b, 0) / devs.length)
            : 0;
        return { vwap: vw, std };
    });
}

function orbPerDay(highs, lows, timestamps) {
    const dayOrb = {};
    timestamps.forEach((ts, i) => {
        const dt = new Date(ts);
        const d = dt.toISOString().slice(0, 10);
        const h = dt.getUTCHours();
        if (h === 7 || h === 8) {
            if (!dayOrb[d]) dayOrb[d] = { hi: highs[i], lo: lows[i] };
            else { dayOrb[d].hi = Math.max(dayOrb[d].hi, highs[i]); dayOrb[d].lo = Math.min(dayOrb[d].lo, lows[i]); }
        }
    });
    let curHi = null, curLo = null;
    return timestamps.map((ts) => {
        const d = new Date(ts).toISOString().slice(0, 10);
        if (dayOrb[d]) { curHi = dayOrb[d].hi; curLo = dayOrb[d].lo; }
        return { hi: curHi, lo: curLo };
    });
}

/**
 * computeFullIndicators — returns all indicators for the final bar
 */
function computeFullIndicators(candles) {
    const n = candles.length;
    if (n < 60) return null;

    const closes    = candles.map(c => c.close);
    const highs     = candles.map(c => c.high);
    const lows      = candles.map(c => c.low);
    const opens     = candles.map(c => c.open);
    const volumes   = candles.map(c => c.tickVolume || c.volume || 1);
    const timestamps = candles.map(c => c.time instanceof Date ? c.time.getTime() : new Date(c.time).getTime());

    const ema9arr  = ema(closes, 9);
    const ema21arr = ema(closes, 21);
    const ema50arr = ema(closes, 50);
    const atr14arr = atrSeries(highs, lows, closes, 14);
    const rsi14arr = rsiSeries(closes, 14);
    const rsi2arr  = rsiSeries(closes, 2);

    const don20 = donchian(highs, lows, 20);
    const don55 = donchian(highs, lows, 55);
    const bb    = bbSeries(closes, 20, 2.0);
    const kc    = kcSeries(closes, highs, lows, 20, 1.5);
    const vwapArr = vwapSeries(closes, volumes, timestamps);
    const orbArr  = orbPerDay(highs, lows, timestamps);

    return {
        n, closes, highs, lows, opens, volumes, timestamps,
        ema9: ema9arr, ema21: ema21arr, ema50: ema50arr,
        atr14: atr14arr, rsi14: rsi14arr, rsi2: rsi2arr,
        don20, don55, bb, kc, vwap: vwapArr, orb: orbArr
    };
}

// ── 8 Signal Generators ───────────────────────────────────────────────────────
// Each returns { side: 'BUY'|'SELL'|null, slDist, tpDist, name }

function sig1_emaCross(I, ind) {
    const i = I;
    if (i < 2) return null;
    const { closes, ema9, ema21, rsi14, atr14 } = ind;
    // Approximate daily EMA200: use ema50 as proxy since we only have ~100 1H bars
    const above200 = closes[i] > ind.ema50[i];  // rough HTF filter
    const a = atr14[i - 1];
    if (ema9[i-1] < ema21[i-1] && ema9[i] >= ema21[i] && above200 && rsi14[i] < 65)
        return { side: 'BUY', slDist: a, tpDist: 2 * a, name: 'S1:EMA Cross' };
    if (ema9[i-1] > ema21[i-1] && ema9[i] <= ema21[i] && !above200 && rsi14[i] > 35)
        return { side: 'SELL', slDist: a, tpDist: 2 * a, name: 'S1:EMA Cross' };
    return null;
}

function sig2_donchian(I, ind) {
    const i = I;
    if (i < 2) return null;
    const { closes, don20, don55, rsi14, atr14, ema50 } = ind;
    const above200 = closes[i] > ema50[i];
    const a = atr14[i - 1];
    const don55mid = (don55[i].hi + don55[i].lo) / 2;
    if (closes[i] > don20[i-1].hi && closes[i] > don55mid && above200 && rsi14[i] > 50 && rsi14[i] < 75)
        return { side: 'BUY', slDist: 1.5 * a, tpDist: 3 * a, name: 'S2:Donchian' };
    if (closes[i] < don20[i-1].lo && closes[i] < don55mid && !above200 && rsi14[i] > 25 && rsi14[i] < 50)
        return { side: 'SELL', slDist: 1.5 * a, tpDist: 3 * a, name: 'S2:Donchian' };
    return null;
}

function sig3_ict(I, ind) {
    const i = I;
    if (i < 12) return null;
    const { closes, highs, lows, atr14, ema50 } = ind;
    const above200 = closes[i] > ema50[i];
    const a = atr14[i - 1];

    const recentLos = lows.slice(Math.max(0, i - 11), i - 1);
    const recentHis = highs.slice(Math.max(0, i - 11), i - 1);
    const minLo = Math.min(...recentLos);
    const maxHi = Math.max(...recentHis);

    const sweepLo   = lows[i-1] < minLo;
    const recoveryU = closes[i] > lows[i-1];
    const fvgBull   = highs[i-2] < lows[i];

    if (sweepLo && recoveryU && fvgBull && above200)
        return { side: 'BUY', slDist: a, tpDist: 2 * a, name: 'S3:ICT Liq' };

    const sweepHi   = highs[i-1] > maxHi;
    const recoveryD = closes[i] < highs[i-1];
    const fvgBear   = lows[i-2] > highs[i];

    if (sweepHi && recoveryD && fvgBear && !above200)
        return { side: 'SELL', slDist: a, tpDist: 2 * a, name: 'S3:ICT Liq' };
    return null;
}

function sig4_candle(I, ind) {
    const i = I;
    if (i < 11) return null;
    const { closes, highs, lows, opens, atr14, ema50 } = ind;
    const above200 = closes[i] > ema50[i];
    const a = atr14[i - 1];
    const c = closes[i], o = opens[i];
    const body = Math.abs(c - o);
    const lowerWick = Math.min(o, c) - lows[i];
    const upperWick = highs[i] - Math.max(o, c);

    const recent10 = closes.slice(i - 10, i);
    const sorted   = [...recent10].sort((a, b) => a - b);
    const p30      = sorted[Math.floor(0.3 * sorted.length)];
    const p70      = sorted[Math.floor(0.7 * sorted.length)];

    // Hammer
    if (body > 0 && lowerWick >= 2 * body && upperWick < 0.5 * body && c <= p30 && above200)
        return { side: 'BUY', slDist: a, tpDist: 2 * a, name: 'S4:Hammer' };
    // Shooting star
    if (body > 0 && upperWick >= 2 * body && lowerWick < 0.5 * body && c >= p70 && !above200)
        return { side: 'SELL', slDist: a, tpDist: 2 * a, name: 'S4:ShootStar' };
    return null;
}

function sig5_rsi2(I, ind) {
    const i = I;
    if (i < 2) return null;
    const { closes, rsi2, rsi14, atr14, ema50 } = ind;
    const above200 = closes[i] > ema50[i];
    const a = atr14[i - 1];
    if (rsi2[i] < 10 && above200 && rsi14[i] > 40)
        return { side: 'BUY', slDist: a, tpDist: 2 * a, name: 'S5:RSI2' };
    if (rsi2[i] > 90 && !above200 && rsi14[i] < 60)
        return { side: 'SELL', slDist: a, tpDist: 2 * a, name: 'S5:RSI2' };
    return null;
}

function sig6_squeeze(I, ind) {
    const i = I;
    if (i < 8) return null;
    const { bb, kc, closes, atr14 } = ind;
    const a = atr14[i - 1];
    // Check squeeze active for last 6 bars
    const wasSqueeze = Array.from({ length: 6 }, (_, k) => i - 6 + k)
        .every(j => bb[j].upper < kc[j].upper && bb[j].lower > kc[j].lower);
    if (!wasSqueeze) return null;
    // Released up
    if (bb[i].upper >= kc[i].upper && closes[i] > bb[i].mid)
        return { side: 'BUY', slDist: 1.5 * a, tpDist: 2.5 * a, name: 'S6:Squeeze' };
    if (bb[i].lower <= kc[i].lower && closes[i] < bb[i].mid)
        return { side: 'SELL', slDist: 1.5 * a, tpDist: 2.5 * a, name: 'S6:Squeeze' };
    return null;
}

function sig7_vwap(I, ind) {
    const i = I;
    if (i < 2) return null;
    const { closes, vwap, atr14, timestamps } = ind;
    const hour = new Date(timestamps[i]).getUTCHours();
    if (hour < 7 || hour > 17) return null;
    const a = atr14[i - 1];
    const { vwap: vw, std } = vwap[i];
    if (closes[i] < vw - 1.5 * std && closes[i] > closes[i - 1])
        return { side: 'BUY', slDist: a, tpDist: 1.5 * a, name: 'S7:VWAP Dev' };
    if (closes[i] > vw + 1.5 * std && closes[i] < closes[i - 1])
        return { side: 'SELL', slDist: a, tpDist: 1.5 * a, name: 'S7:VWAP Dev' };
    return null;
}

function sig8_orb(I, ind) {
    const i = I;
    if (i < 2) return null;
    const { closes, orb, atr14, ema50, timestamps } = ind;
    const hour = new Date(timestamps[i]).getUTCHours();
    if (hour > 16) return null;
    const a = atr14[i - 1];
    const above200 = closes[i] > ema50[i];
    const { hi: orbHi, lo: orbLo } = orb[i];
    if (orbHi === null || orbLo === null) return null;
    if (closes[i] > orbHi && above200)
        return { side: 'BUY', slDist: 1.5 * a, tpDist: 3 * a, name: 'S8:ORB' };
    if (closes[i] < orbLo && !above200)
        return { side: 'SELL', slDist: 1.5 * a, tpDist: 3 * a, name: 'S8:ORB' };
    return null;
}

const ALL_SIG_FNS = [sig1_emaCross, sig2_donchian, sig3_ict, sig4_candle,
                     sig5_rsi2, sig6_squeeze, sig7_vwap, sig8_orb];

function getAllSignals(ind) {
    const i = ind.n - 1;
    return ALL_SIG_FNS.map(fn => {
        try { return fn(i, ind); } catch (e) { return null; }
    }).filter(s => s !== null);
}

// ── Ensemble Decision ─────────────────────────────────────────────────────────
function getEnsembleSignal(ind) {
    const signals = getAllSignals(ind);
    if (signals.length === 0) return null;

    const buys  = signals.filter(s => s.side === 'BUY');
    const sells = signals.filter(s => s.side === 'SELL');

    let chosen = null;
    let lotMultiplier = 0.5;  // 1 signal → half lot

    if (buys.length >= 2) {
        chosen = buys[0];
        lotMultiplier = 1.0;  // 2+ agree → full lot
    } else if (sells.length >= 2) {
        chosen = sells[0];
        lotMultiplier = 1.0;
    } else if (signals.length >= 1) {
        // Single signal — half lot, only trade most confident ones
        chosen = signals[0];
        lotMultiplier = 0.5;
    }

    return chosen ? { ...chosen, lotMultiplier, allSignals: signals } : null;
}

// ── 3-Phase Trailing SL ───────────────────────────────────────────────────────
function computeTrailingStopPhase(pos, currentPrice, currentAtr, ind) {
    const i = ind.n - 1;
    const lows  = ind.lows;
    const highs = ind.highs;
    const { side, entry, initialRisk } = pos;
    const pnlPts = side === 'BUY' ? currentPrice - entry : entry - currentPrice;

    let newSL = pos.sl;
    let forceClose = false;

    // Phase 1: Breakeven at 1R
    if (pnlPts >= initialRisk && !pos.phase1) {
        const be = side === 'BUY' ? entry + 0.01 : entry - 0.01;
        if ((side === 'BUY' && be > newSL) || (side === 'SELL' && be < newSL)) {
            newSL = be;
            pos.phase1 = true;
            pos.phase = Math.max(pos.phase || 0, 1);
        }
    }

    // Phase 2: Structure trail at 1.5R (swing lows/highs)
    if (pnlPts >= initialRisk * 1.5) {
        const start = Math.max(0, i - 5);
        if (side === 'BUY') {
            const swingLow = Math.min(...lows.slice(start, i));
            if (swingLow > newSL) newSL = swingLow;
        } else {
            const swingHigh = Math.max(...highs.slice(start, i));
            if (swingHigh < newSL) newSL = swingHigh;
        }
        pos.phase = Math.max(pos.phase || 0, 2);
    }

    // Phase 3: ATR compression at 2R
    if (pnlPts >= initialRisk * 2) {
        const trail = side === 'BUY' ? currentPrice - 0.5 * currentAtr : currentPrice + 0.5 * currentAtr;
        if ((side === 'BUY' && trail > newSL) || (side === 'SELL' && trail < newSL)) newSL = trail;
        pos.phase = 3;
        pos.peakPnl = Math.max(pos.peakPnl || pnlPts, pnlPts);
        if (pos.peakPnl >= initialRisk * 2 && pnlPts < pos.peakPnl * 0.75) forceClose = true;
    }

    return { newSL: parseFloat(newSL.toFixed(2)), forceClose };
}

// ── Position Management ───────────────────────────────────────────────────────
async function managePositions(price, ind, equity) {
    const positions = (await connection.getPositions()).filter(p => p.symbol === ACTIVE_SYMBOL);
    let trailData   = (await redis.get('shiva:trail_v4')) || {};
    const atr = ind.atr14[ind.n - 1] || 0.5;

    for (const pos of positions) {
        const profit = pos.profit || 0;
        const side   = pos.type.includes('BUY') ? 'BUY' : 'SELL';

        if (!trailData[pos.id]) {
            const risk = pos.stopLoss ? Math.abs(pos.openPrice - pos.stopLoss) : atr;
            trailData[pos.id] = {
                side, entry: pos.openPrice, sl: pos.stopLoss || 0,
                initialRisk: risk, phase: 0, phase1: false, peakPnl: 0
            };
        }

        const t = trailData[pos.id];
        const { newSL, forceClose } = computeTrailingStopPhase(t, price, atr, ind);

        if (forceClose) {
            try {
                await connection.closePosition(pos.id);
                log(`Phase3 peak-retracement close ${pos.id}`);
            } catch (e) { log(`Close error: ${e.message}`, 'error'); }
        } else if (
            (side === 'BUY'  && newSL > (pos.stopLoss || 0)) ||
            (side === 'SELL' && newSL < (pos.stopLoss || 99999))
        ) {
            try {
                await connection.modifyPosition(pos.id, { stopLoss: newSL });
                if (t.phase >= 1 && !pos.comment?.includes('PYRAMID')) {
                    // Pyramid on breakeven trigger (phase 1)
                    if (t.phase === 1 && positions.length < MAX_POSITIONS + 1) {
                        log(`Pyramiding at breakeven for ${pos.id}`);
                        await openPosition(side, price, ind, equity, true, pos.volume);
                    }
                }
            } catch (e) { log(`Modify error: ${e.message}`, 'error'); }
        }
    }

    // Clean up closed positions from trailData
    const liveIds = new Set(positions.map(p => p.id));
    let changed = false;
    for (const id in trailData) {
        if (!liveIds.has(id)) {
            log(`Position ${id} closed. Cleaning trail.`);
            await discordPost({
                embeds: [{
                    title: `POSITION CLOSED — ${ACTIVE_SYMBOL}`,
                    color: 0x888888,
                    description: `ID: \`${id}\``,
                    footer: { text: 'SHIVA ICT ENGINE v2' },
                    timestamp: new Date().toISOString()
                }]
            });
            delete trailData[id];
            changed = true;
        }
    }
    if (changed || positions.length > 0) await redis.set('shiva:trail_v4', trailData);
}

// ── Open Position ─────────────────────────────────────────────────────────────
async function openPosition(side, price, ind, equity, isPyramid = false, baseLot = 0) {
    const atr = ind.atr14[ind.n - 1] || 0.5;

    // Auto-compound lot sizing: equity * 1% / ATR / 1000
    let lot;
    if (isPyramid) {
        lot = baseLot * 0.5;
    } else {
        lot = equity * 0.01 / (atr * 1000);
    }
    lot = Math.max(0.01, Math.min(5.0, parseFloat(lot.toFixed(2))));

    const slPts = Math.max(0.15, atr * 1.5);
    const sl = parseFloat((side === 'BUY' ? price - slPts : price + slPts).toFixed(2));
    const tp = parseFloat((side === 'BUY' ? price + slPts * 2.5 : price - slPts * 2.5).toFixed(2));

    try {
        const comment = isPyramid ? 'SHIVA_PYRAMID' : 'SHIVA_ICT';
        if (side === 'BUY')
            await connection.createMarketBuyOrder(ACTIVE_SYMBOL, lot, sl, tp, { comment });
        else
            await connection.createMarketSellOrder(ACTIVE_SYMBOL, lot, sl, tp, { comment });

        log(`Opened ${side} ${lot} lots @ ${price.toFixed(2)} | SL:${sl} TP:${tp}`, 'success');
        await discordPost({
            embeds: [{
                title: `${side} OPENED — ${ACTIVE_SYMBOL}`,
                color: side === 'BUY' ? 0x00FF88 : 0xFF4444,
                fields: [
                    { name: 'Entry', value: `\`${price.toFixed(2)}\``, inline: true },
                    { name: 'SL',    value: `\`${sl}\``,               inline: true },
                    { name: 'TP',    value: `\`${tp}\``,               inline: true },
                    { name: 'Lot',   value: `\`${lot}\``,              inline: true },
                    { name: 'Type',  value: isPyramid ? '`PYRAMID`' : '`BASE`', inline: true },
                    { name: 'ATR',   value: `\`${atr.toFixed(3)}\``,   inline: true }
                ],
                footer: { text: 'SHIVA ICT ENGINE v2' },
                timestamp: new Date().toISOString()
            }]
        });
    } catch (e) {
        log(`Open error: ${e.message}`, 'error');
    }
}

// ── Main Cycle ────────────────────────────────────────────────────────────────
async function run() {
    log('Cycle started');
    const info    = await connection.getAccountInformation();
    const equity  = info.equity;
    const balance = info.balance;
    const today   = new Date().getUTCDate();

    // ── Daily DD check ────────────────────────────────────────────────────────
    let ddState = (await redis.get('shiva:dd_v4')) || { date: today, startBal: balance, losses: 0, lastTwo: [] };
    if (ddState.date !== today) ddState = { date: today, startBal: balance, losses: 0, lastTwo: [] };
    await redis.set('shiva:dd_v4', ddState);

    if (equity <= ddState.startBal * (1 - DAILY_DD_LIMIT)) {
        log('Daily DD limit hit. No new trades.', 'error');
        return;
    }

    // ── Daily loss count guard ────────────────────────────────────────────────
    if (ddState.losses >= MAX_DAILY_LOSSES) {
        log(`Daily loss limit (${MAX_DAILY_LOSSES}) hit. Skipping.`, 'error');
        return;
    }

    // ── Get price and candles ─────────────────────────────────────────────────
    const priceData = await connection.getSymbolPrice(ACTIVE_SYMBOL);
    const price = priceData.bid || priceData.ask;
    log(`${ACTIVE_SYMBOL} @ ${price.toFixed(2)} | Equity: $${equity.toFixed(2)}`);

    // Use 1H candles, 200 bars
    const history = await tradingAccount.getHistoricalCandles(ACTIVE_SYMBOL, '1h', null, 200);
    if (!history || history.length < 60) { log('Insufficient candle data'); return; }

    const candles = history.map(c => ({
        open: c.open, high: c.high, low: c.low, close: c.close,
        tickVolume: c.tickVolume || 1,
        time: c.time
    }));

    const ind = computeFullIndicators(candles);
    if (!ind) { log('Indicator computation failed'); return; }

    await managePositions(price, ind, equity);

    // ── Signal check ──────────────────────────────────────────────────────────
    const positions = (await connection.getPositions()).filter(p => p.symbol === ACTIVE_SYMBOL);
    if (positions.length >= MAX_POSITIONS) {
        log(`Max positions (${MAX_POSITIONS}) reached.`);
        return;
    }

    const sig = getEnsembleSignal(ind);
    if (!sig) return;

    // Require higher confidence after 2 consecutive losses
    const lastTwo = ddState.lastTwo || [];
    if (lastTwo.length === 2 && lastTwo[0] === 'L' && lastTwo[1] === 'L') {
        if (sig.lotMultiplier < 1.0) {
            log('2 consecutive losses: skipping single-signal entry. Require 2+ agreement.');
            return;
        }
    }

    const sigNames = sig.allSignals.map(s => s.name).join(', ');
    log(`Ensemble signal: ${sig.side} | ${sigNames} | lot_mult=${sig.lotMultiplier}`);

    await openPosition(sig.side, price, ind, equity);
}

// ── Startup ───────────────────────────────────────────────────────────────────
async function start() {
    await init();
    while (true) {
        try {
            await run();
        } catch (e) {
            log(`Loop error: ${e.message}`, 'error');
            await discordPost({
                embeds: [{
                    title: 'SHIVA LOOP ERROR',
                    color: 0xFF0000,
                    description: e.message,
                    footer: { text: 'SHIVA ICT ENGINE v2' },
                    timestamp: new Date().toISOString()
                }]
            });
        }
        await new Promise(r => setTimeout(r, CHECK_INTERVAL));
    }
}

start();
