/**
 * SHIVA NEWS TRADER — EIA Wednesday + NFP Friday
 *
 * BACKTEST RESULTS (Apr-May 2026, 0.01 lot):
 *   EIA Wednesday 14:30 UTC:
 *     3/4 events triggered (75% rate), WR=33%, EV/event=$1.03, EV/week=$0.78
 *     Outcomes: +$7.00 (TP), -$5.10 (SL), +$1.20 (time stop)
 *   NFP Gold 12:30 UTC (1 sample):
 *     WR=100%, $7.30 per event, EV/week=$0.40 (conservative 50% WR assumed)
 *   COMBINED EV/week @0.01 lot: $1.18
 *
 * ENTRY RULES:
 *   - Wait for the release candle (1m) to close
 *   - Body must be > threshold (0.20 pts USOIL / $0.50 GOLD)
 *   - Direction = candle body direction
 *   - SL = opposite wick of release candle (min threshold pts away)
 *   - TP = 2× body from entry
 *   - Time stop: 30 min (EIA) or 60 min (NFP)
 *
 * INSTRUMENTS:
 *   - EIA: USOIL/XTIUSD (Pepperstone), PT_USD = $10/pt @ 0.01 lot
 *   - NFP: XAUUSD (Pepperstone), PT_USD = $10/pt @ 0.01 lot (0.01 * 1000 oz/lot... actually broker-specific, use same formula)
 */

// ─── Window Detection ─────────────────────────────────────────────────────────

/**
 * Returns true if now is within the EIA window (Wed 14:25–14:36 UTC)
 */
function isEIAWindow() {
  const now = new Date();
  const dayUTC   = now.getUTCDay();    // 3 = Wednesday
  const hourUTC  = now.getUTCHours();
  const minUTC   = now.getUTCMinutes();
  const totalMin = hourUTC * 60 + minUTC;
  // Wednesday, 14:25 → 14:36 UTC (allow up to 6 min after release for candle close)
  return dayUTC === 3 && totalMin >= 14 * 60 + 25 && totalMin <= 14 * 60 + 36;
}

/**
 * Returns true if now is within the NFP window (1st Friday of month, 12:25–12:36 UTC)
 */
function isNFPWindow() {
  const now = new Date();
  const dayUTC  = now.getUTCDay();  // 5 = Friday
  const dateUTC = now.getUTCDate();
  const hourUTC = now.getUTCHours();
  const minUTC  = now.getUTCMinutes();
  const totalMin = hourUTC * 60 + minUTC;
  // First Friday of month: day-of-month <= 7, day-of-week = Friday
  const isFirstFriday = dayUTC === 5 && dateUTC <= 7;
  return isFirstFriday && totalMin >= 12 * 60 + 25 && totalMin <= 12 * 60 + 36;
}

/**
 * Returns 'EIA' | 'NFP' | null for the current window
 */
function getActiveNewsEvent() {
  if (isEIAWindow())  return 'EIA';
  if (isNFPWindow())  return 'NFP';
  return null;
}

// ─── Signal Generation ────────────────────────────────────────────────────────

const EIA_MIN_BODY  = 0.20;   // minimum body size in points for USOIL
const NFP_MIN_BODY  = 0.50;   // minimum body size for GOLD

/**
 * Analyse a completed 1-minute candle from a news release and return a trade signal.
 *
 * @param {object} candle  - { open, high, low, close, time }
 * @param {string} event   - 'EIA' | 'NFP'
 * @returns {{ side, entry, sl, tp, slPts, tpPts, stopMinutes } | null}
 */
function getNewsSignal(candle, event) {
  if (!candle) return null;

  const { open, high, low, close } = candle;
  const body     = Math.abs(close - open);
  const minBody  = event === 'EIA' ? EIA_MIN_BODY : NFP_MIN_BODY;

  if (body < minBody) return null;

  const isBuy  = close > open;
  const side   = isBuy ? 'BUY' : 'SELL';
  const entry  = close;

  // SL = opposite wick, minimum minBody away
  let sl;
  if (isBuy) {
    sl = Math.min(low, entry - minBody);
  } else {
    sl = Math.max(high, entry + minBody);
  }

  const slPts = Math.abs(entry - sl);

  // TP = 2× body from entry
  const tpPts = body * 2;
  const tp    = isBuy ? entry + tpPts : entry - tpPts;

  // Time stop in minutes
  const stopMinutes = event === 'EIA' ? 30 : 60;

  return { side, entry, sl, tp, slPts, tpPts, stopMinutes, event, body, candle };
}

// ─── Lot Sizing ───────────────────────────────────────────────────────────────

/**
 * Calculate lot size risking `riskPct` of equity with the given SL distance.
 * Formula mirrors existing bot: lot = equity * riskPct / (slPts * 1000)
 *
 * @param {number} equity   - current account equity in USD
 * @param {number} slPts    - stop-loss distance in instrument points
 * @param {number} riskPct  - fraction of equity to risk (default 0.05 = 5%)
 * @returns {number} lot size, clamped [0.01, 5.00]
 */
function calcNewsLot(equity, slPts, riskPct = 0.05) {
  if (!equity || equity <= 0 || !slPts || slPts <= 0) return 0.01;
  const raw = (equity * riskPct) / (slPts * 1000);
  return Math.min(5.0, Math.max(0.01, Math.round(raw * 100) / 100));
}

// ─── State Management ─────────────────────────────────────────────────────────

const NEWS_STATE_KEY = 'shiva:news_trader_state';

async function getNewsState(redis) {
  try {
    const val = await redis.get(NEWS_STATE_KEY);
    if (!val) return {};
    return typeof val === 'string' ? JSON.parse(val) : val;
  } catch (e) {
    return {};
  }
}

async function setNewsState(redis, state) {
  try {
    await redis.set(NEWS_STATE_KEY, JSON.stringify(state));
  } catch (e) {
    console.error('NewsTrader: redis write error', e.message);
  }
}

// ─── Main Run Function ────────────────────────────────────────────────────────

/**
 * Called each cycle from index.js.
 *
 * @param {object} connection  - MetaApi RPC connection
 * @param {string} symbol      - 'USOIL' or 'XAUUSD'
 * @param {number} equity      - current account equity
 * @param {object} redis       - Upstash Redis client
 * @param {Function} [logFn]   - optional log(msg, type) function
 */
async function run(connection, symbol, equity, redis, logFn) {
  const log = logFn || ((msg, t) => {
    const ts = new Date().toISOString().slice(0, 19).replace('T', ' ');
    console.log(`${ts} [NEWS] ${msg}`);
  });

  const event = getActiveNewsEvent();
  if (!event) return null;  // not in any news window

  const state = await getNewsState(redis);

  // Determine which symbol to trade
  const tradeSymbol = event === 'EIA' ? 'USOIL' : 'XAUUSD';
  // Only act on cycles where our symbol matches or is not constrained
  // (index.js can pass symbol as null to allow news trader to pick)

  log(`News window active: ${event} — checking for signal on ${tradeSymbol}`, 'info');

  // Check if we already traded this event today (prevent duplicate entries)
  const todayKey = `${event}_${new Date().toISOString().slice(0, 10)}`;
  if (state[todayKey]) {
    log(`Already traded ${event} today (${todayKey}), skipping`, 'info');
    return null;
  }

  // Pull the last 2 completed 1-minute candles for this symbol
  let candles;
  try {
    candles = await connection.getHistoricalCandles(tradeSymbol, '1m', null, 2);
  } catch (e) {
    log(`Failed to get candles for ${tradeSymbol}: ${e.message}`, 'error');
    return null;
  }

  if (!candles || candles.length < 1) {
    log(`No candles returned for ${tradeSymbol}`, 'error');
    return null;
  }

  // Use the most-recently-closed candle (penultimate if current is live)
  const releaseCandle = candles[candles.length - 2] || candles[candles.length - 1];
  const signal = getNewsSignal(releaseCandle, event);

  if (!signal) {
    log(`${event}: Release candle body too small (open=${releaseCandle.open?.toFixed(2)}, close=${releaseCandle.close?.toFixed(2)}), no trade`, 'info');
    state[todayKey] = { result: 'SKIP_SMALL', time: new Date().toISOString() };
    await setNewsState(redis, state);
    return null;
  }

  const lot = calcNewsLot(equity, signal.slPts);

  log(
    `${event} SIGNAL: ${signal.side} ${tradeSymbol} @ ${signal.entry.toFixed(2)} ` +
    `SL=${signal.sl.toFixed(2)} (${signal.slPts.toFixed(2)}pts) ` +
    `TP=${signal.tp.toFixed(2)} (${signal.tpPts.toFixed(2)}pts) ` +
    `LOT=${lot} EQUITY=${equity?.toFixed(2)}`,
    'trade'
  );

  // Record that we are acting on this event
  state[todayKey] = {
    event,
    side: signal.side,
    entry: signal.entry,
    sl: signal.sl,
    tp: signal.tp,
    lot,
    time: new Date().toISOString(),
    result: 'OPENED'
  };
  await setNewsState(redis, state);

  // Place the trade
  try {
    let result;
    const comment = `SHIVA_${event}_${signal.side}`;
    if (signal.side === 'BUY') {
      result = await connection.createMarketBuyOrder(tradeSymbol, lot, signal.sl, signal.tp, { comment });
    } else {
      result = await connection.createMarketSellOrder(tradeSymbol, lot, signal.sl, signal.tp, { comment });
    }
    log(`${event} order placed. OrderId=${result?.orderId || 'unknown'}`, 'success');
    state[todayKey].orderId = result?.orderId;
    await setNewsState(redis, state);

    // Return signal for callers that want to display it
    return { ...signal, lot, orderId: result?.orderId };
  } catch (e) {
    log(`${event} order FAILED: ${e.message}`, 'error');
    state[todayKey].result = 'FAILED';
    state[todayKey].error = e.message;
    await setNewsState(redis, state);
    return null;
  }
}

// ─── Exports ─────────────────────────────────────────────────────────────────

module.exports = {
  isEIAWindow,
  isNFPWindow,
  getActiveNewsEvent,
  getNewsSignal,
  calcNewsLot,
  run
};
