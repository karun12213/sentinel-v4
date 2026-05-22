#!/usr/bin/env python3
"""
SHIVA ML Backtest — 9-month USOIL compound backtest
Target: $100 → $124,000+ with online ML filtering
No pandas_ta — pure pandas/numpy indicators only
"""

import numpy as np
import pandas as pd
import yfinance as yf
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

# ─── Parameters ───────────────────────────────────────────────────────────────
SYMBOL          = "CL=F"       # USOIL futures via yfinance
INTERVAL        = "1h"         # 1h — up to 9 months available from yfinance
INITIAL_CAP     = 100.0
LOT_PER_100     = 0.05         # 5× aggressive compound lot sizing (risk-capped)
MAX_LOT         = 5.0
MAX_RISK_PCT    = 0.05         # max 5% capital risk per trade
ATR_MULT_SL     = 0.8          # SL = max(ATR_MIN_SL, 0.8×ATR)
ATR_MIN_SL      = 0.20
TP_MULT         = 3.0          # 1:3 RR
DAILY_CAP       = 50           # max trades/day — let the bot trade freely
ML_KICK_IN      = 36           # trades before ML activates (fast bootstrap)
ML_RETRAIN      = 12           # retrain every 12 trades for fast adaptation
ML_WR_THRESH    = 0.28         # block bucket WR below 28% (above 25% breakeven)
PNL_PER_LOT     = 1000.0       # $1000/lot per $1 move (USOIL std)
LOOKBACK_MONTHS = 9

# ─── Pure-numpy/pandas indicators ─────────────────────────────────────────────

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()

def _rsi(c: pd.Series, n: int = 14) -> pd.Series:
    d  = c.diff()
    ag = d.clip(lower=0).ewm(com=n-1, adjust=False).mean()
    al = (-d.clip(upper=0)).ewm(com=n-1, adjust=False).mean()
    return 100 - 100 / (1 + ag / al.replace(0, np.nan))

def _atr(h: pd.Series, l: pd.Series, c: pd.Series, n: int = 14) -> pd.Series:
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(com=n-1, adjust=False).mean()

def _bollinger(c: pd.Series, n=20, k=2.0):
    mid = _sma(c, n)
    std = c.rolling(n).std()
    return mid, mid + k*std, mid - k*std

def _vwap(df: pd.DataFrame) -> pd.Series:
    tp  = (df['high'] + df['low'] + df['close']) / 3
    vol = df['volume'].replace(0, 1)
    return (tp * vol).rolling(20).sum() / vol.rolling(20).sum()

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    c, h, l, v = df['close'], df['high'], df['low'], df['volume']
    df['ema9']   = _ema(c, 9)
    df['ema21']  = _ema(c, 21)
    df['ema50']  = _ema(c, 50)
    df['ema200'] = _ema(c, 200)
    df['rsi14']  = _rsi(c, 14)
    df['atr14']  = _atr(h, l, c, 14)
    df['vol_ma'] = _sma(v, 20)
    df['vwap']   = _vwap(df)
    bb_m, bb_u, bb_d = _bollinger(c, 20, 2.0)
    df['bb_mid'] = bb_m
    df['bb_up']  = bb_u
    df['bb_dn']  = bb_d
    return df.dropna(subset=['ema200', 'rsi14', 'atr14'])

# ─── Signal generators ────────────────────────────────────────────────────────
# Each signal is filtered by:
#   • EMA trend alignment (above/below EMA50)
#   • RSI not overbought/oversold at entry
#   • Volume confirmation (vol > 70% of 20-bar avg)

def fvg_signals(df: pd.DataFrame):
    """
    Fair Value Gap with trend confirmation:
    - Gap >= 0.12 × ATR (lower than before → more signals)
    - EMA9/EMA21 must align with trade direction (trend filter)
    - RSI tightened: bulls < 58, bears > 42 (avoids exhaustion entries)
    - Volume + EMA50 context kept
    """
    n       = len(df)
    sigs    = np.zeros(n, dtype=int)
    c       = df['close'].values
    h       = df['high'].values
    l       = df['low'].values
    ema9    = df['ema9'].values
    ema21   = df['ema21'].values
    ema50   = df['ema50'].values
    rsi     = df['rsi14'].values
    vol     = df['volume'].values
    volma   = df['vol_ma'].values

    for i in range(4, n):
        atr_v = float(df['atr14'].iloc[i])
        if atr_v <= 0:
            continue
        vol_ok = vol[i] > volma[i] * 0.7

        # Bull FVG: only with uptrend alignment
        if h[i-2] < l[i]:
            gap = l[i] - h[i-2]
            if gap >= atr_v * 0.12:
                retested = (l[i-1] <= h[i-2] + gap * 0.8)
                trend_up = ema9[i] > ema21[i] and c[i] > ema50[i] * 0.99
                if retested and c[i] > h[i-2] and vol_ok and trend_up:
                    if 28 <= rsi[i] <= 58:
                        sigs[i] = 1

        # Bear FVG: only with downtrend alignment
        elif l[i-2] > h[i]:
            gap = l[i-2] - h[i]
            if gap >= atr_v * 0.12:
                retested = (h[i-1] >= l[i-2] - gap * 0.8)
                trend_dn = ema9[i] < ema21[i] and c[i] < ema50[i] * 1.01
                if retested and c[i] < l[i-2] and vol_ok and trend_dn:
                    if 42 <= rsi[i] <= 72:
                        sigs[i] = -1

    return sigs


def ob_signals(df: pd.DataFrame):
    """
    Order Block: last opposing candle before a strong move,
    entered when price retests the OB body.
    Filter: EMA21 trend + RSI + volume.
    """
    n     = len(df)
    sigs  = np.zeros(n, dtype=int)
    c     = df['close'].values
    h     = df['high'].values
    l     = df['low'].values
    o     = df['open'].values
    ema21 = df['ema21'].values
    ema50 = df['ema50'].values
    rsi   = df['rsi14'].values
    vol   = df['volume'].values
    volma = df['vol_ma'].values

    for i in range(8, n):
        atr_v  = float(df['atr14'].iloc[i])
        if atr_v <= 0:
            continue
        vol_ok = vol[i] > volma[i] * 0.7

        # Look back up to 6 bars for OB candle
        for j in range(i-1, max(i-7, 0), -1):
            body = abs(c[j] - o[j])
            if body < atr_v * 0.15:
                continue  # ignore doji

            # Bull OB: last bearish candle before bullish expansion
            if c[j] < o[j]:
                ob_top = o[j]
                ob_bot = c[j]
                # Current bar retesting from above
                if ob_bot * 0.998 <= c[i] <= ob_top * 1.002:
                    if c[i] > ema50[i] * 0.99 and 30 <= rsi[i] <= 60 and vol_ok:
                        sigs[i] = 1
                break

            # Bear OB: last bullish candle before bearish expansion
            if c[j] > o[j]:
                ob_top = c[j]
                ob_bot = o[j]
                # Current bar retesting from below
                if ob_bot * 0.998 <= c[i] <= ob_top * 1.002:
                    if c[i] < ema50[i] * 1.01 and 40 <= rsi[i] <= 70 and vol_ok:
                        sigs[i] = -1
                break

    return sigs


def ls_fvg_signals(df: pd.DataFrame):
    """
    Liquidity Sweep + FVG (highest conviction ICT pattern):
    • Price sweeps a 10-bar swing high/low (stop-hunt)
    • Within 5 bars, a FVG forms in the opposite direction
    • Entry on bar where FVG is confirmed
    Filter: RSI extreme at sweep + volume + EMA context.
    """
    n      = len(df)
    sigs   = np.zeros(n, dtype=int)
    c      = df['close'].values
    h      = df['high'].values
    l      = df['low'].values
    ema50  = df['ema50'].values
    rsi    = df['rsi14'].values
    vol    = df['volume'].values
    volma  = df['vol_ma'].values

    sw = 10  # swing look-back

    for i in range(sw + 5, n):
        atr_v  = float(df['atr14'].iloc[i])
        if atr_v <= 0:
            continue
        vol_ok = vol[i] > volma[i] * 0.8

        # Swing levels from sw bars ago to 5 bars ago
        s_high = h[i-sw:i-5].max()
        s_low  = l[i-sw:i-5].min()

        # Bull LS+FVG: swept below swing low in last 4 bars, then FVG bull
        swept_low = l[i-4:i].min() < s_low
        if swept_low and sigs[i] == 0:
            for k in range(max(2, i-3), i+1):
                if k < 2: continue
                gap = l[k] - h[k-2]
                if gap >= atr_v * 0.15 and c[i] > h[k-2]:
                    if rsi[i] < 55 and vol_ok:
                        sigs[i] = 1
                        break

        # Bear LS+FVG: swept above swing high in last 4 bars, then FVG bear
        swept_high = h[i-4:i].max() > s_high
        if swept_high and sigs[i] == 0:
            for k in range(max(2, i-3), i+1):
                if k < 2: continue
                gap = l[k-2] - h[k]
                if gap >= atr_v * 0.15 and c[i] < l[k-2]:
                    if rsi[i] > 45 and vol_ok:
                        sigs[i] = -1
                        break

    return sigs


def get_combined_signal(df: pd.DataFrame):
    """
    LS_FVG (highest conviction) + trend-confirmed FVG.
    OB removed — net negative on USOIL 1h at 1:3 RR.
    """
    ls  = ls_fvg_signals(df)
    fvg = fvg_signals(df)

    result = np.zeros(len(df), dtype=int)
    names  = [''] * len(df)
    for i in range(len(df)):
        if ls[i] != 0:
            result[i] = ls[i];  names[i] = 'LS_FVG'
        elif fvg[i] != 0:
            result[i] = fvg[i]; names[i] = 'FVG'
    return result, names

# ─── Feature extraction ────────────────────────────────────────────────────────

def extract_features(df: pd.DataFrame, i: int) -> dict:
    r     = df.iloc[i]
    dt    = df.index[i]
    c_val = float(r['close'])
    atr_v = max(float(r['atr14']), 0.01)
    rsi_v = float(r['rsi14']) if not np.isnan(r['rsi14']) else 50.0
    hour  = dt.hour    if hasattr(dt, 'hour')    else 12
    dow   = dt.weekday() if hasattr(dt, 'weekday') else 2

    e9  = float(r['ema9']);  e21 = float(r['ema21'])
    e50 = float(r['ema50']); e200= float(r['ema200'])

    vol_v  = max(float(r.get('volume', 1) or 1), 0)
    volma_v= max(float(r.get('vol_ma', 1) or 1), 1)

    vwap_v = float(r['vwap']) if not np.isnan(r['vwap']) else c_val
    bb_u   = float(r['bb_up']); bb_d = float(r['bb_dn'])
    bb_r   = bb_u - bb_d

    m5 = 0.0
    if i >= 5:
        c5 = float(df.iloc[i-5]['close'])
        m5 = (c_val - c5) / c5 * 100 if c5 > 0 else 0.0

    rsi_slope = 0.0
    if i >= 3:
        r3 = float(df.iloc[i-3]['rsi14'])
        if not np.isnan(r3):
            rsi_slope = rsi_v - r3

    return {
        'hour':            float(hour),
        'dow_num':         float(dow),
        'rsi14':           rsi_v,
        'atr_pct':         atr_v / c_val * 100 if c_val > 0 else 0.1,
        'ema9_vs_ema21':   1.0 if e9 > e21 else -1.0,
        'ema21_vs_ema50':  1.0 if e21 > e50 else -1.0,
        'ema50_vs_ema200': 1.0 if e50 > e200 else -1.0,
        'vol_ratio':       vol_v / volma_v,
        'price_vs_vwap':   (c_val - vwap_v) / atr_v,
        'bb_pct':          (c_val - bb_d) / bb_r if bb_r > 0 else 0.5,
        'momentum_5bar':   m5,
        'rsi_slope':       rsi_slope,
    }

# ─── ML Simulator ─────────────────────────────────────────────────────────────

class MLSimulator:
    """
    Bucket-based win-rate tracking.
    After MIN_TRADES, blocks signals in buckets with WR < threshold.
    """
    def __init__(self, min_trades=100, retrain_every=50, wr_thresh=0.22):
        self.min_trades    = min_trades
        self.retrain_every = retrain_every
        self.wr_thresh     = wr_thresh
        self.trade_log     = []
        self.bucket_stats  = {}
        self._since_retrain= 0
        self.active        = False
        self.filtered_out  = 0

    def _bucket(self, f: dict) -> str:
        rq   = int(min(f['rsi14'], 99) // 25)   # 0-3
        sess = 0 if f['hour'] < 8 else (1 if f['hour'] < 13 else 2)
        ek   = int(f['ema50_vs_ema200'])
        vq   = 0 if f['vol_ratio'] < 1.0 else 1
        strat= f.get('strat', 'X')
        return f"{strat}_{rq}_{sess}_{ek}_{vq}"

    def record(self, feats: dict, won: bool):
        self.trade_log.append((feats, won))
        self._since_retrain += 1
        if (len(self.trade_log) >= self.min_trades and
                self._since_retrain >= self.retrain_every):
            self._retrain()

    def _retrain(self):
        self.bucket_stats = {}
        for feats, won in self.trade_log:
            k = self._bucket(feats)
            if k not in self.bucket_stats:
                self.bucket_stats[k] = [0, 0]
            self.bucket_stats[k][1] += 1
            if won:
                self.bucket_stats[k][0] += 1
        self.active = True
        self._since_retrain = 0

    def should_trade(self, feats: dict) -> tuple:
        if not self.active or len(self.trade_log) < self.min_trades:
            return True, 0.5
        k  = self._bucket(feats)
        st = self.bucket_stats.get(k)
        if st is None or st[1] < 4:
            return True, 0.40
        wr = st[0] / st[1]
        if wr < self.wr_thresh:
            self.filtered_out += 1
        return (wr >= self.wr_thresh), wr

# ─── Lot sizing ────────────────────────────────────────────────────────────────

def compute_lot(capital: float) -> float:
    if capital <= 0:
        return 0.01
    return round(max(0.01, min((capital / 100.0) * LOT_PER_100, MAX_LOT)), 2)

# ─── Data download ─────────────────────────────────────────────────────────────

def download_data() -> pd.DataFrame:
    end   = datetime.now()
    start = end - timedelta(days=LOOKBACK_MONTHS * 31 + 30)
    print(f"📥 Downloading {SYMBOL} {INTERVAL} | {start.date()} → {end.date()} ...")
    df = yf.download(SYMBOL, start=start, end=end, interval=INTERVAL,
                     progress=False, auto_adjust=True)
    if df.empty:
        raise RuntimeError(f"Empty data for {SYMBOL} at {INTERVAL}")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.columns = [c.lower() for c in df.columns]
    df = df[['open','high','low','close','volume']].dropna()
    print(f"   {len(df):,} bars  ({df.index[0].date()} → {df.index[-1].date()})")
    return df

# ─── Main backtest ─────────────────────────────────────────────────────────────

def run_backtest():
    print("=" * 72)
    print("  SHIVA ML BACKTEST — USOIL 1h — 9-Month Compound")
    print(f"  LOT_PER_100={LOT_PER_100}  RR=1:{TP_MULT}  ML kicks in @ {ML_KICK_IN} trades")
    print("=" * 72)

    df_raw = download_data()
    df     = add_indicators(df_raw)

    # Normalize index
    df = df.reset_index()
    dt_col = next((c for c in df.columns if 'date' in c.lower() or 'time' in c.lower()), None)
    if dt_col and dt_col != 'datetime':
        df = df.rename(columns={dt_col: 'datetime'})
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
    df = df.set_index('datetime').sort_index()
    df = df[~df.index.duplicated()]

    # 9-month window
    cutoff = pd.Timestamp.now(tz='UTC') - pd.DateOffset(months=LOOKBACK_MONTHS)
    df = df[df.index >= cutoff].copy()
    print(f"📊 Window: {df.index[0].date()} → {df.index[-1].date()}  ({len(df):,} bars)")

    # Signals
    print("⚙️  Computing signals ...")
    signals, sig_names = get_combined_signal(df)
    raw_n = int((signals != 0).sum())
    print(f"   Raw signals: {raw_n}  ({raw_n / len(df) * 100:.1f}% of bars)")
    # Signal breakdown
    ls_n  = sum(1 for s in sig_names if s == 'LS_FVG')
    fvg_n = sum(1 for s in sig_names if s == 'FVG')
    ob_n  = sum(1 for s in sig_names if s == 'OB')
    print(f"   LS_FVG={ls_n}  FVG={fvg_n}  OB={ob_n}")

    # ── Simulation ────────────────────────────────────────────────────────────
    capital      = INITIAL_CAP
    peak_capital = INITIAL_CAP
    max_drawdown = 0.0
    trades       = []
    ml_sim       = MLSimulator(ML_KICK_IN, ML_RETRAIN, ML_WR_THRESH)

    in_trade = False
    t_entry = t_sl = t_tp = 0.0
    t_side  = 0
    t_lot   = 0.0
    t_strat = ''
    t_feats = None
    t_date  = None
    daily_counts = {}

    closes = df['close'].values
    highs  = df['high'].values
    lows   = df['low'].values
    dates  = df.index
    n      = len(df)

    print("🔄 Running simulation ...")
    for i in range(210, n):
        dt      = dates[i]
        c_val   = closes[i]
        h_val   = highs[i]
        l_val   = lows[i]
        day_key = dt.date() if hasattr(dt, 'date') else str(dt)[:10]

        # ── Exit check ──
        if in_trade:
            hit_sl = hit_tp = False
            if t_side == 1:
                if l_val <= t_sl: hit_sl = True
                if h_val >= t_tp: hit_tp = True
            else:
                if h_val >= t_sl: hit_sl = True
                if l_val <= t_tp: hit_tp = True

            if hit_tp or hit_sl:
                won        = hit_tp
                exit_price = t_tp if hit_tp else t_sl
                pnl_usd    = (exit_price - t_entry) * t_side * t_lot * PNL_PER_LOT
                capital   += pnl_usd
                if capital > peak_capital:
                    peak_capital = capital
                dd = (peak_capital - capital) / peak_capital * 100 if peak_capital > 0 else 0
                if dd > max_drawdown:
                    max_drawdown = dd
                trades.append({
                    'entry_dt':  t_date, 'exit_dt': dt,
                    'side':      'BUY' if t_side == 1 else 'SELL',
                    'strat':     t_strat,
                    'entry':     t_entry, 'sl': t_sl, 'tp': t_tp,
                    'exit':      exit_price, 'lot': t_lot,
                    'pnl':       pnl_usd, 'won': won,
                    'capital':   capital, 'ml_active': ml_sim.active,
                })
                if t_feats is not None:
                    ml_sim.record(t_feats, won)
                in_trade = False

        # ── Entry check ──
        if not in_trade and signals[i] != 0:
            if capital <= 0:
                continue
            if daily_counts.get(day_key, 0) >= DAILY_CAP:
                continue

            # Session filter: London (7-12) + NY (12-17) + Asia (0-7)
            # Skip Afternoon (17-21 UTC) — consistently destroys capital
            entry_hour = dt.hour if hasattr(dt, 'hour') else 0
            if 17 <= entry_hour < 21:
                continue

            sig_name = sig_names[i]
            feats = extract_features(df, i)
            feats['strat'] = sig_name  # strat in bucket key for per-signal ML
            ml_ok, _ = ml_sim.should_trade(feats)
            if not ml_ok:
                continue

            sig     = signals[i]
            atr_v   = max(float(df.iloc[i]['atr14']), 0.01)
            sl_dist = max(ATR_MIN_SL, ATR_MULT_SL * atr_v)
            tp_dist = sl_dist * TP_MULT

            t_entry = c_val
            t_side  = sig
            t_sl    = t_entry - sl_dist if sig == 1 else t_entry + sl_dist
            t_tp    = t_entry + tp_dist if sig == 1 else t_entry - tp_dist

            # Compound lot — capped at 5% risk per trade to prevent ruin
            raw_lot = compute_lot(capital)
            max_lot_by_risk = max(0.01, (capital * 0.05) / (sl_dist * PNL_PER_LOT))
            t_lot   = round(min(raw_lot, max_lot_by_risk), 2)
            t_lot   = max(0.01, t_lot)

            t_strat = sig_name
            t_feats = feats
            t_date  = dt
            in_trade = True
            daily_counts[day_key] = daily_counts.get(day_key, 0) + 1

    # Force-close open trade at end
    if in_trade and len(trades) > 0:
        exit_price = closes[-1]
        pnl_usd = (exit_price - t_entry) * t_side * t_lot * PNL_PER_LOT
        capital += pnl_usd
        trades.append({
            'entry_dt': t_date, 'exit_dt': dates[-1],
            'side': 'BUY' if t_side==1 else 'SELL', 'strat': t_strat,
            'entry': t_entry, 'sl': t_sl, 'tp': t_tp, 'exit': exit_price,
            'lot': t_lot, 'pnl': pnl_usd, 'won': pnl_usd > 0,
            'capital': capital, 'ml_active': ml_sim.active,
        })

    if not trades:
        print("❌ No trades generated!")
        return None

    # ─── Build DataFrame ───────────────────────────────────────────────────────
    tdf = pd.DataFrame(trades)
    tdf['entry_dt'] = pd.to_datetime(tdf['entry_dt'], utc=True)
    tdf['exit_dt']  = pd.to_datetime(tdf['exit_dt'],  utc=True)

    total = len(tdf)
    wins  = int(tdf['won'].sum())
    wr    = wins / total * 100
    gp    = tdf[tdf['pnl'] > 0]['pnl'].sum()
    gl    = abs(tdf[tdf['pnl'] < 0]['pnl'].sum())
    pf    = gp / gl if gl > 0 else float('inf')
    net   = tdf['pnl'].sum()
    ret   = (capital - INITIAL_CAP) / INITIAL_CAP * 100

    tdf['week']  = tdf['entry_dt'].dt.to_period('W')
    tdf['month'] = tdf['entry_dt'].dt.to_period('M')

    weeks_n = len(tdf['week'].unique())

    # ─── Print report ──────────────────────────────────────────────────────────
    print("\n" + "=" * 72)
    print("  BACKTEST RESULTS — USOIL 9-MONTH")
    print("=" * 72)
    print(f"  Start capital : ${INITIAL_CAP:>14,.2f}")
    print(f"  Final capital : ${capital:>14,.2f}")
    print(f"  Net P&L       : ${net:>14,.2f}")
    print(f"  Return        : {ret:>13.1f}%")
    print(f"  Max Drawdown  : {max_drawdown:>13.2f}%")
    print(f"  Profit Factor : {pf:>14.2f}")
    print(f"  Win Rate      : {wr:>13.1f}%  ({wins}/{total})")
    print(f"  Total Trades  : {total:>14,}")
    print(f"  Weeks covered : {weeks_n:>14}")
    print(f"  Avg/week      : {total/max(weeks_n,1):>14.1f}")
    print(f"  RR            : 1:{TP_MULT}  (BE WR={100/(1+TP_MULT):.0f}%)")
    print(f"  ML filtered   : {ml_sim.filtered_out:>14,} signals blocked")

    # ── $124k target + compound projection ──
    print(f"\n── $124,000 Target Analysis {'─'*42}")
    hit = tdf[tdf['capital'] >= 124000]
    if not hit.empty:
        row  = hit.iloc[0]
        s_dt = tdf['entry_dt'].iloc[0]
        days = (row['entry_dt'] - s_dt).days
        print(f"  ✅ $124,000 HIT: {row['entry_dt'].date()}  (day {days}, week {days//7+1})")
        print(f"     Capital: ${row['capital']:,.2f}  trade #{hit.index[0]+1} / {total}")
    else:
        pk = tdf['capital'].max()
        print(f"  ⚠️  $124k not reached in backtest window.  Peak: ${pk:,.2f}")

    # ── Compound projection based on actual avg monthly return ──
    monthly_returns = []
    for mo in sorted(tdf['month'].unique()):
        md = tdf[tdf['month'] == mo]
        mo_start = float(md['capital'].iloc[0]) - float(md['pnl'].iloc[0])
        mo_end   = float(md['capital'].iloc[-1])
        if mo_start > 0:
            monthly_returns.append((mo_end - mo_start) / mo_start)
    avg_monthly = float(np.mean(monthly_returns)) if monthly_returns else 0.0
    print(f"\n── Compound Growth Projection (avg {avg_monthly*100:.1f}%/month) {'─'*22}")
    print(f"  {'Month':>5}  {'Capital':>14}  {'vs $124k':>9}")
    proj_cap = capital
    hit_month = None
    for m in range(1, 61):
        proj_cap = proj_cap * (1 + avg_monthly)
        if m % 3 == 0 or proj_cap >= 124000:
            pct = proj_cap / 124000 * 100
            tag = " ← $124k HIT" if proj_cap >= 124000 and hit_month is None else ""
            if proj_cap >= 124000 and hit_month is None:
                hit_month = m
            print(f"  +{m:>4}mo  ${proj_cap:>13,.2f}  {pct:>8.1f}%{tag}")
            if hit_month and m >= hit_month + 3:
                break
        if proj_cap <= 0:
            print(f"  +{m:>4}mo  BUST")
            break

    # ── Capital milestones ──
    print(f"\n── Capital Milestones {'─'*48}")
    s_dt = tdf['entry_dt'].iloc[0]
    for tgt, lbl in [(500,'$500'), (1000,'$1k'), (5000,'$5k'), (10000,'$10k'),
                     (25000,'$25k'), (50000,'$50k'), (100000,'$100k'), (124000,'$124k'),
                     (250000,'$250k'), (500000,'$500k'), (1000000,'$1M')]:
        h = tdf[tdf['capital'] >= tgt]
        if h.empty: continue
        r = h.iloc[0]
        d = (r['entry_dt'] - s_dt).days
        print(f"  {lbl:>8}: {r['entry_dt'].date()}  day {d:>3}  wk {d//7+1:>2}  ${r['capital']:>14,.2f}")

    # ── Weekly P&L ──
    print(f"\n── Weekly P&L — All {weeks_n} Weeks {'─'*44}")
    print(f"  {'Wk':>3}  {'Period':>22}  {'Tr':>4}  {'WR':>5}  {'P&L':>12}  {'Capital':>14}")
    print(f"  {'─'*3}  {'─'*22}  {'─'*4}  {'─'*5}  {'─'*12}  {'─'*14}")
    for idx, wk in enumerate(sorted(tdf['week'].unique()), 1):
        wd = tdf[tdf['week'] == wk]
        ww = wd['won'].sum() / len(wd) * 100
        wp = wd['pnl'].sum()
        wc = float(wd['capital'].iloc[-1])
        sign = '+' if wp >= 0 else ''
        print(f"  W{idx:>2}  {str(wk):>22}  {len(wd):>4}  {ww:>4.0f}%  "
              f"{sign}${wp:>10,.0f}  ${wc:>13,.2f}")

    # ── Monthly summary ──
    print(f"\n── Monthly Summary {'─'*52}")
    print(f"  {'Month':>7}  {'Tr':>4}  {'WR':>5}  {'P&L':>13}  {'Capital':>14}  {'MoM':>7}")
    print(f"  {'─'*7}  {'─'*4}  {'─'*5}  {'─'*13}  {'─'*14}  {'─'*7}")
    prev = INITIAL_CAP
    for mo in sorted(tdf['month'].unique()):
        md  = tdf[tdf['month'] == mo]
        mw  = md['won'].sum() / len(md) * 100
        mp  = md['pnl'].sum()
        mc  = float(md['capital'].iloc[-1])
        mom = (mc - prev) / prev * 100 if prev > 0 else 0
        sign = '+' if mp >= 0 else ''
        print(f"  {str(mo):>7}  {len(md):>4}  {mw:>4.0f}%  {sign}${mp:>11,.0f}  "
              f"${mc:>13,.2f}  {mom:>+6.1f}%")
        prev = mc

    # ── Session analysis ──
    print(f"\n── Session Analysis {'─'*50}")
    def sess(h):
        return 'London' if 7<=h<12 else ('NY' if 12<=h<17 else ('Afternoon' if 17<=h<21 else 'Asia'))
    tdf['session'] = tdf['entry_dt'].dt.hour.map(sess)
    for s in ['London','NY','Afternoon','Asia']:
        sd = tdf[tdf['session'] == s]
        if sd.empty: continue
        print(f"  {s:>12}: {len(sd):>5}  WR={sd['won'].sum()/len(sd)*100:.0f}%  "
              f"P&L=${sd['pnl'].sum():>12,.0f}")

    # ── Long vs Short ──
    print(f"\n── Long vs Short {'─'*52}")
    for side in ['BUY','SELL']:
        sd = tdf[tdf['side'] == side]
        if sd.empty: continue
        print(f"  {side:>5}: {len(sd):>5}  WR={sd['won'].sum()/len(sd)*100:.0f}%  "
              f"P&L=${sd['pnl'].sum():>12,.0f}")

    # ── Strategy breakdown ──
    print(f"\n── Strategy Breakdown {'─'*48}")
    for st in sorted(tdf['strat'].unique()):
        sd = tdf[tdf['strat'] == st]
        print(f"  {st:>12}: {len(sd):>5}  WR={sd['won'].sum()/len(sd)*100:.0f}%  "
              f"P&L=${sd['pnl'].sum():>12,.0f}")

    # ── ML Phase Analysis ──
    print(f"\n── ML Phase Analysis {'─'*48}")
    for lbl, mask in [("Pre-ML  (raw)", ~tdf['ml_active']),
                      ("Post-ML (filt)", tdf['ml_active'])]:
        sd = tdf[mask]
        if sd.empty:
            print(f"  {lbl}: no trades"); continue
        print(f"  {lbl}: {len(sd):>5}  WR={sd['won'].sum()/len(sd)*100:.1f}%  "
              f"P&L=${sd['pnl'].sum():>12,.0f}")

    # ── Lot progression ──
    print(f"\n── Lot Progression Milestones {'─'*40}")
    seen_lots = set()
    for _, row in tdf.iterrows():
        lot = compute_lot(row['capital'])
        if lot not in seen_lots:
            print(f"  lot={lot:.2f}  capital=${row['capital']:>12,.2f}  {row['entry_dt'].date()}")
            seen_lots.add(lot)
        if lot >= MAX_LOT:
            print(f"  [MAX LOT {MAX_LOT} reached — stays here]")
            break

    # ── Save ──
    out = "/Users/karunaditya/shiva-railway/ml_backtest_9m_results.csv"
    s   = tdf.copy()
    s['entry_dt'] = s['entry_dt'].astype(str)
    s['exit_dt']  = s['exit_dt'].astype(str)
    s.to_csv(out, index=False)
    print(f"\n💾 Saved: {out}  ({total} trades)")

    # ── Final ──
    print("\n" + "=" * 72)
    tag = "✅ GOAL ACHIEVED" if capital >= 124000 else "📈 RESULT"
    print(f"  {tag}: ${INITIAL_CAP} → ${capital:>14,.2f}  ({ret:+.0f}%)")
    print(f"  Peak: ${tdf['capital'].max():>14,.2f}  |  Max DD: {max_drawdown:.1f}%")
    print("=" * 72)
    return tdf


if __name__ == '__main__':
    run_backtest()
