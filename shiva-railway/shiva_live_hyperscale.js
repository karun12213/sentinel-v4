/**
 * SHIVA HYPER-SCALE LIVE BOT
 * Continuous execution version of the aggressive USOIL strategy.
 * Timeframe: 5m/15m | Risk: 10% (Fractional Kelly) | Pyramiding: enabled
 */
require('dotenv').config();
const MetaApi = require('metaapi.cloud-sdk').default;
const { Redis } = require('@upstash/redis');
const https = require('https');

// Config
const TOKEN = process.env.METAAPI_TOKEN;
const ACCOUNT_ID = process.env.METAAPI_ACCOUNT_ID;
const SYMBOL = process.env.SYMBOL || 'USOIL';
const CHECK_INTERVAL = 60000; // 1 minute cycle

const RISK_PCT = 0.10;
const MAX_POSITIONS = 2; 
const DAILY_DD_LIMIT = 0.25;
const MIN_CONFIDENCE = 65;

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL || '',
  token: process.env.UPSTASH_REDIS_REST_TOKEN || ''
});

const api = new MetaApi(TOKEN);
let connection, tradingAccount;

async function log(msg, type = 'info') {
    const timestamp = new Date().toISOString().replace('T', ' ').slice(0, 19);
    const emoji = type === 'error' ? '❌' : (type === 'success' ? '✅' : '🔹');
    console.log(`${emoji} [${timestamp}] ${msg}`);
}

async function discordPost(payload) {
    const webhook = process.env.DISCORD_TRADES || process.env.DISCORD_ALERTS;
    if (!webhook) return;
    const data = JSON.stringify(payload);
    const url = new URL(webhook);
    const options = {
        hostname: url.hostname,
        path: url.pathname,
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': data.length,
        },
    };
    return new Promise((resolve) => {
        const req = https.request(options, (res) => resolve());
        req.on('error', (e) => console.error(`Discord error: ${e.message}`));
        req.write(data);
        req.end();
    });
}

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
        log(`SHIVA HYPER-SCALE connected. Available methods: ${Object.keys(connection).filter(k => typeof connection[k] === 'function').join(', ')}`);
        log('SHIVA HYPER-SCALE connected and synchronized');
    } catch (e) {
        log(`Init failed: ${e.message}`, 'error');
        process.exit(1);
    }
}

// ── Technical Indicators ─────────────────────────────────────────────────────
function computeIndicators(candles) {
    const n = candles.length;
    if (n < 2) return { n, ema50: [], atr14: [], don10Hi: [], don10Lo: [], don20Hi: [] };
    
    const closes = candles.map(c => c.close);
    const highs = candles.map(c => c.high);
    const lows = candles.map(c => c.low);

    const ema = (data, p) => {
        const k = 2/(p+1);
        let res = [data[0]];
        for(let i=1; i<data.length; i++) res.push(data[i]*k + res[i-1]*(1-k));
        return res;
    };

    const atr = (h, l, c, p) => {
        let tr = [h[0]-l[0]];
        for(let i=1; i<h.length; i++) {
            tr.push(Math.max(h[i]-l[i], Math.abs(h[i]-c[i-1]), Math.abs(l[i]-c[i-1])));
        }
        let res = [tr.slice(0,p).reduce((a,b)=>a+b,0)/p];
        const k = 1/p;
        for(let i=p; i<tr.length; i++) res.push(tr[i]*k + res[res.length-1]*(1-k));
        const padding = Array(Math.max(0, p-1)).fill(res[0]);
        return padding.concat(res);
    };

    return {
        n,
        ema50: ema(closes, 50),
        atr14: atr(highs, lows, closes, 14),
        don10Hi: highs.map((_, i) => Math.max(...highs.slice(Math.max(0, i-9), i+1))),
        don10Lo: lows.map((_, i) => Math.min(...lows.slice(Math.max(0, i-9), i+1))),
        don20Hi: highs.map((_, i) => Math.max(...highs.slice(Math.max(0, i-19), i+1)))
    };
}

// ── Signal Logic ─────────────────────────────────────────────────────────────
function getSignal(candles, ind, price, now) {
    const i = ind.n - 1;
    if (i < 20) return null;
    const hr = now.getUTCHours();

    // Session filter: London (7-12) or NY (13-17)
    if (!((hr >= 7 && hr <= 12) || (hr >= 13 && hr <= 17))) return null;

    // ORB Breakout (Simplified)
    if (hr === 13 && price > ind.don10Hi[i-1]) return { signal: 'BUY', name: 'ORB ↑', conf: 72 };
    if (hr === 13 && price < ind.don10Lo[i-1]) return { signal: 'SELL', name: 'ORB ↓', conf: 72 };

    // Donchian Breakout
    if (price > ind.don20Hi[i-1]) return { signal: 'BUY', name: 'Donchian ↑', conf: 70 };
    if (price < ind.don10Lo[i-1]) return { signal: 'SELL', name: 'Donchian ↓', conf: 70 };

    return null;
}

// ── Position Management ──────────────────────────────────────────────────────
async function managePositions(price, ind, equity) {
    const positions = (await connection.getPositions()).filter(p => p.symbol === SYMBOL);
    let trailData = await redis.get('shiva:trail_v2') || {};
    const atr = ind.atr14[ind.n-1] || 0.5;

    for (const pos of positions) {
        const profit = pos.profit || 0;
        if (!trailData[pos.id]) {
            trailData[pos.id] = { peak: profit, phase: 0, risk: Math.abs(pos.openPrice - pos.stopLoss) || 0.5 };
        }
        const t = trailData[pos.id];
        t.peak = Math.max(t.peak, profit);
        const side = pos.type.includes('BUY') ? 'BUY' : 'SELL';
        const risk = t.risk;

        // Phase 1: Breakeven + Pyramid
        if (t.phase < 1 && profit / (pos.volume * 100) >= risk) {
            const newSL = side === 'BUY' ? pos.openPrice + 0.10 : pos.openPrice - 0.10;
            try {
                await connection.modifyPosition(pos.id, { stopLoss: newSL });
                t.phase = 1;
                log(`🛡️ Breakeven for ${pos.id}`);
                if (positions.length < MAX_POSITIONS && !pos.comment?.includes('PYRAMID')) {
                    log('🚀 Pyramiding scale-in...');
                    await openPosition(side, price, ind, equity, true, pos.volume);
                }
            } catch (e) { log(`BE error: ${e.message}`, 'error'); }
        }

        // Phase 3: ATR Trail
        if (t.phase >= 1 && profit / (pos.volume * 100) >= 2 * risk) {
            const trailSL = side === 'BUY' ? price - 0.5*atr : price + 0.5*atr;
            if ((side === 'BUY' && trailSL > (pos.stopLoss || 0)) || (side === 'SELL' && trailSL < (pos.stopLoss || 9999))) {
                try {
                    await connection.modifyPosition(pos.id, { stopLoss: parseFloat(trailSL.toFixed(2)) });
                } catch (e) {}
            }
        }
    }
    
    const liveIds = positions.map(p => p.id);
    let changed = false;
    for (const id in trailData) {
        if (!liveIds.includes(id)) {
            log(`Position ${id} closed. Cleaning trail data.`);
            await discordPost({
                embeds: [{
                    title: `🏁 POSITION CLOSED — ${SYMBOL}`,
                    color: 0x888888,
                    description: `ID: \`${id}\``,
                    footer: { text: 'SHIVA HYPER-SCALE ENGINE' },
                    timestamp: new Date().toISOString()
                }]
            });
            delete trailData[id];
            changed = true;
        }
    }
    if (changed || positions.length > 0) await redis.set('shiva:trail_v2', trailData);
}

async function openPosition(side, price, ind, equity, isPyramid = false, baseLot = 0) {
    const atr = ind.atr14[ind.n-1] || 0.5;
    const slPts = Math.max(0.15, atr * 1.5);
    const lot = isPyramid ? baseLot * 0.5 : (equity * RISK_PCT) / (slPts * 1000);
    const finalLot = Math.min(5.0, Math.max(0.01, Math.round(lot * 100) / 100));

    const sl = side === 'BUY' ? price - slPts : price + slPts;
    const tp = side === 'BUY' ? price + slPts * 2.5 : price - slPts * 2.5;

    try {
        const comment = isPyramid ? 'PYRAMID' : 'SHIVA_HYPER';
        const res = side === 'BUY' 
            ? await connection.createMarketBuyOrder(SYMBOL, finalLot, sl, tp, { comment })
            : await connection.createMarketSellOrder(SYMBOL, finalLot, sl, tp, { comment });
        
        log(`✅ Opened ${side} ${finalLot} lots | SL: ${sl.toFixed(2)} | TP: ${tp.toFixed(2)}`);
        
        await discordPost({
            embeds: [{
                title: `${side === 'BUY' ? '🚀' : '📉'} ${side} OPENED — ${SYMBOL}`,
                color: side === 'BUY' ? 0x00FF88 : 0xFF4444,
                fields: [
                    { name: 'Entry', value: `\`${price.toFixed(2)}\``, inline: true },
                    { name: 'SL', value: `\`${sl.toFixed(2)}\``, inline: true },
                    { name: 'TP', value: `\`${tp.toFixed(2)}\``, inline: true },
                    { name: 'Lot', value: `\`${finalLot.toFixed(2)}\``, inline: true },
                    { name: 'Type', value: isPyramid ? '`PYRAMID`' : '`BASE`', inline: true }
                ],
                footer: { text: 'SHIVA HYPER-SCALE ENGINE' },
                timestamp: new Date().toISOString()
            }]
        });
    } catch (e) {
        log(`Open error: ${e.message}`, 'error');
    }
}

async function run() {
    log('Cycle started');
    const info = await connection.getAccountInformation();
    const equity = info.equity;
    const balance = info.balance;

    // Daily DD Check
    const today = new Date().getUTCDate();
    let ddState = await redis.get('shiva:dd_state') || { date: today, startBal: balance };
    if (ddState.date !== today) ddState = { date: today, startBal: balance };
    await redis.set('shiva:dd_state', ddState);

    if (equity <= ddState.startBal * (1 - DAILY_DD_LIMIT)) {
        log('🛑 Daily DD Limit hit. No new trades.', 'error');
        return;
    }

    const priceData = await connection.getSymbolPrice(SYMBOL);
    const price = priceData.bid || priceData.ask;

    const history = await connection.getHistoricalCandles(SYMBOL, '5m', new Date(Date.now() - 100 * 5 * 60 * 1000), new Date());
    const candles = history.map(c => ({ open: c.open, high: c.high, low: c.low, close: c.close }));
    const ind = computeIndicators(candles);

    await managePositions(price, ind, equity);

    const positions = (await connection.getPositions()).filter(p => p.symbol === SYMBOL);
    if (positions.length < MAX_POSITIONS) {
        const sig = getSignal(candles, ind, price, new Date());
        if (sig && sig.conf >= MIN_CONFIDENCE) {
            log(`🎯 Signal: ${sig.name} (${sig.conf}%)`);
            await openPosition(sig.signal, price, ind, equity);
        }
    }
}

async function start() {
    await init();
    while (true) {
        try {
            await run();
        } catch (e) {
            log(`Loop error: ${e.message}`, 'error');
        }
        await new Promise(r => setTimeout(r, CHECK_INTERVAL));
    }
}

start();
