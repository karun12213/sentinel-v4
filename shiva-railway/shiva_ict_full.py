"""
SHIVA ICT Full Backtest Engine — USOIL / CL=F
Simulated results. No lookahead bias.
"""

import warnings
warnings.filterwarnings('ignore')

import os, sys, pickle, math
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# ─── Dependency check ────────────────────────────────────────────────────────
try:
    import yfinance as yf
except ImportError:
    os.system(f'{sys.executable} -m pip install yfinance -q')
    import yfinance as yf

try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.neural_network import MLPClassifier
    from sklearn.preprocessing import StandardScaler
except ImportError:
    os.system(f'{sys.executable} -m pip install scikit-learn --break-system-packages -q')
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.neural_network import MLPClassifier
    from sklearn.preprocessing import StandardScaler


# ─── 1. DATA DOWNLOAD ────────────────────────────────────────────────────────
def download_data():
    print("Downloading CL=F hourly data (9 months)...")
    h_path = '/tmp/cl_1h.csv'
    d_path = '/tmp/cl_daily.csv'

    end = datetime.today()
    start_h = end - timedelta(days=274)   # ~9 months
    start_d = end - timedelta(days=600)

    df_h = yf.download('CL=F', start=start_h, end=end, interval='1h', auto_adjust=True, progress=False)
    df_d = yf.download('CL=F', start=start_d, end=end, interval='1d', auto_adjust=True, progress=False)

    df_h.to_csv(h_path)
    df_d.to_csv(d_path)
    print(f"  Hourly bars: {len(df_h)} | Daily bars: {len(df_d)}")
    return df_h, df_d


# ─── 2. INDICATORS ───────────────────────────────────────────────────────────
def ema_series(arr, p):
    k = 2.0/(p+1)
    out = np.empty(len(arr))
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i]*k + out[i-1]*(1-k)
    return out

def compute_atr(high, low, close, p=14):
    n = len(close)
    tr = np.empty(n)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        tr[i] = max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))
    atr = np.empty(n)
    atr[0] = tr[0]
    k = 1.0/p
    for i in range(1, n):
        atr[i] = tr[i]*k + atr[i-1]*(1-k)
    return atr

def compute_rsi(close, p=14):
    n = len(close)
    gains = np.zeros(n)
    losses = np.zeros(n)
    for i in range(1, n):
        d = close[i] - close[i-1]
        gains[i] = max(d, 0)
        losses[i] = max(-d, 0)
    avg_g = np.zeros(n)
    avg_l = np.zeros(n)
    avg_g[p] = np.mean(gains[1:p+1])
    avg_l[p] = np.mean(losses[1:p+1])
    for i in range(p+1, n):
        avg_g[i] = (avg_g[i-1]*(p-1) + gains[i]) / p
        avg_l[i] = (avg_l[i-1]*(p-1) + losses[i]) / p
    rsi = np.where(avg_l == 0, 100, 100 - 100/(1 + avg_g/(avg_l+1e-10)))
    rsi[:p] = 50
    return rsi

def compute_macd(close, fast=12, slow=26, sig=9):
    ema_f = ema_series(close, fast)
    ema_s = ema_series(close, slow)
    line = ema_f - ema_s
    signal = ema_series(line, sig)
    hist = line - signal
    return line, signal, hist

def compute_donchian(high, low, p):
    n = len(high)
    hi = np.empty(n)
    lo = np.empty(n)
    for i in range(n):
        s = max(0, i-p+1)
        hi[i] = np.max(high[s:i+1])
        lo[i] = np.min(low[s:i+1])
    mid = (hi + lo) / 2
    return hi, lo, mid

def compute_bb(close, p=20, std_mult=2.0):
    n = len(close)
    mid = np.empty(n)
    upper = np.empty(n)
    lower = np.empty(n)
    for i in range(n):
        s = max(0, i-p+1)
        m = np.mean(close[s:i+1])
        sd = np.std(close[s:i+1])
        mid[i] = m
        upper[i] = m + std_mult*sd
        lower[i] = m - std_mult*sd
    return upper, mid, lower

def compute_keltner(close, high, low, p=20, mult=1.5):
    ema_c = ema_series(close, p)
    atr = compute_atr(high, low, close, p)
    upper = ema_c + mult*atr
    lower = ema_c - mult*atr
    return upper, ema_c, lower

def compute_vwap(close, volume, timestamps):
    """Session VWAP reset at 7 UTC each day"""
    n = len(close)
    vwap = np.empty(n)
    vwap_std = np.empty(n)
    cum_pv = 0.0
    cum_v = 0.0
    session_prices = []
    prev_day = None
    for i in range(n):
        dt = pd.Timestamp(timestamps[i])
        day_hour = (dt.day, dt.hour)
        # Reset at 7 UTC
        if prev_day is None or (dt.hour == 7 and (prev_day is None or dt.date() != pd.Timestamp(timestamps[i-1]).date())):
            cum_pv = 0.0
            cum_v = 0.0
            session_prices = []
        cum_pv += close[i] * volume[i]
        cum_v += volume[i]
        session_prices.append(close[i])
        vwap[i] = cum_pv / (cum_v + 1e-10)
        dev = [p - vwap[i] for p in session_prices]
        vwap_std[i] = np.std(dev) if len(dev) > 1 else 0.0
        prev_day = dt.date()
    return vwap, vwap_std

def compute_orb(high, low, timestamps):
    """ORB: high/low of first 2 bars at 7-8 UTC each day"""
    n = len(timestamps)
    orb_high = np.full(n, np.nan)
    orb_low = np.full(n, np.nan)
    orb_map = {}  # date -> (h, l)
    for i in range(n):
        dt = pd.Timestamp(timestamps[i])
        d = dt.date()
        h = dt.hour
        if h in (7, 8):
            if d not in orb_map:
                orb_map[d] = {'h': high[i], 'l': low[i]}
            else:
                orb_map[d]['h'] = max(orb_map[d]['h'], high[i])
                orb_map[d]['l'] = min(orb_map[d]['l'], low[i])
    # Forward fill
    cur_h, cur_l = np.nan, np.nan
    for i in range(n):
        dt = pd.Timestamp(timestamps[i])
        d = dt.date()
        if d in orb_map:
            cur_h = orb_map[d]['h']
            cur_l = orb_map[d]['l']
        orb_high[i] = cur_h
        orb_low[i] = cur_l
    return orb_high, orb_low

def compute_indicators(close, high, low, open_, volume, timestamps, daily_ema200_map):
    n = len(close)
    ema9   = ema_series(close, 9)
    ema21  = ema_series(close, 21)
    ema50  = ema_series(close, 50)
    atr14  = compute_atr(high, low, close, 14)
    rsi14  = compute_rsi(close, 14)
    rsi2   = compute_rsi(close, 2)
    macd_line, macd_sig, macd_hist = compute_macd(close)
    don20_hi, don20_lo, don20_mid  = compute_donchian(high, low, 20)
    don55_hi, don55_lo, don55_mid  = compute_donchian(high, low, 55)
    bb_upper, bb_mid, bb_lower     = compute_bb(close, 20, 2.0)
    kc_upper, kc_mid, kc_lower     = compute_keltner(close, high, low, 20, 1.5)
    vwap, vwap_std = compute_vwap(close, volume, timestamps)
    orb_high, orb_low = compute_orb(high, low, timestamps)

    # Daily EMA200 mapped to hourly
    daily_ema200 = np.empty(n)
    for i in range(n):
        dt = pd.Timestamp(timestamps[i]).date()
        if dt in daily_ema200_map:
            daily_ema200[i] = daily_ema200_map[dt]
        else:
            # Use nearest previous
            daily_ema200[i] = daily_ema200[i-1] if i > 0 else close[i]

    # Don20 %position
    don20_pct = np.where((don20_hi - don20_lo) > 0,
                         (close - don20_lo) / (don20_hi - don20_lo), 0.5)
    # BB %B
    bb_pct_b = np.where((bb_upper - bb_lower) > 0,
                        (close - bb_lower) / (bb_upper - bb_lower), 0.5)
    # Keltner squeeze
    kc_squeeze = (bb_upper < kc_upper) & (bb_lower > kc_lower)

    return dict(
        ema9=ema9, ema21=ema21, ema50=ema50, atr14=atr14,
        rsi14=rsi14, rsi2=rsi2,
        macd_line=macd_line, macd_sig=macd_sig, macd_hist=macd_hist,
        don20_hi=don20_hi, don20_lo=don20_lo, don20_mid=don20_mid,
        don55_hi=don55_hi, don55_lo=don55_lo, don55_mid=don55_mid,
        bb_upper=bb_upper, bb_mid=bb_mid, bb_lower=bb_lower,
        kc_upper=kc_upper, kc_mid=kc_mid, kc_lower=kc_lower,
        vwap=vwap, vwap_std=vwap_std,
        orb_high=orb_high, orb_low=orb_low,
        daily_ema200=daily_ema200,
        don20_pct=don20_pct, bb_pct_b=bb_pct_b, kc_squeeze=kc_squeeze
    )


# ─── 3. SIGNAL GENERATORS ────────────────────────────────────────────────────
def sig1_ema_cross(i, close, ema9, ema21, daily_ema200, rsi14, atr14):
    if i < 2: return None, 0, 0
    if ema9[i-1] < ema21[i-1] and ema9[i] >= ema21[i] and close[i] > daily_ema200[i] and rsi14[i] < 65:
        return 'BUY', 1*atr14[i-1], 2*atr14[i-1]
    if ema9[i-1] > ema21[i-1] and ema9[i] <= ema21[i] and close[i] < daily_ema200[i] and rsi14[i] > 35:
        return 'SELL', 1*atr14[i-1], 2*atr14[i-1]
    return None, 0, 0

def sig2_donchian(i, close, don20_hi, don20_lo, don55_mid, daily_ema200, rsi14, atr14):
    if i < 2: return None, 0, 0
    if close[i] > don20_hi[i-1] and close[i] > don55_mid[i] and close[i] > daily_ema200[i] and 50 < rsi14[i] < 75:
        return 'BUY', 1.5*atr14[i-1], 3*atr14[i-1]
    if close[i] < don20_lo[i-1] and close[i] < don55_mid[i] and close[i] < daily_ema200[i] and 25 < rsi14[i] < 50:
        return 'SELL', 1.5*atr14[i-1], 3*atr14[i-1]
    return None, 0, 0

def sig3_ict(i, close, high, low, daily_ema200, atr14):
    if i < 12: return None, 0, 0
    sweep_lo = low[i-1] < np.min(low[max(0,i-11):i-1])
    recovery_up = close[i] > low[i-1]
    fvg_bull = high[i-2] < low[i]   # gap between bar i-2 top and bar i bottom
    if sweep_lo and recovery_up and fvg_bull and close[i] > daily_ema200[i]:
        return 'BUY', 1*atr14[i-1], 2*atr14[i-1]

    sweep_hi = high[i-1] > np.max(high[max(0,i-11):i-1])
    recovery_dn = close[i] < high[i-1]
    fvg_bear = low[i-2] > high[i]
    if sweep_hi and recovery_dn and fvg_bear and close[i] < daily_ema200[i]:
        return 'SELL', 1*atr14[i-1], 2*atr14[i-1]
    return None, 0, 0

def sig4_candle(i, close, high, low, open_, daily_ema200, atr14):
    if i < 11: return None, 0, 0
    c, o = close[i], open_[i]
    body = abs(c - o)
    lower_wick = min(o,c) - low[i]
    upper_wick = high[i] - max(o,c)
    # Hammer
    if lower_wick >= 2*body and upper_wick < 0.5*body and body > 0:
        if c <= np.percentile(close[i-10:i], 30) and c > daily_ema200[i]:
            return 'BUY', 1*atr14[i-1], 2*atr14[i-1]
    # Shooting star
    if upper_wick >= 2*body and lower_wick < 0.5*body and body > 0:
        if c >= np.percentile(close[i-10:i], 70) and c < daily_ema200[i]:
            return 'SELL', 1*atr14[i-1], 2*atr14[i-1]
    return None, 0, 0

def sig5_rsi2(i, close, rsi2, rsi14, daily_ema200, atr14):
    if i < 2: return None, 0, 0
    if rsi2[i] < 10 and close[i] > daily_ema200[i] and rsi14[i] > 40:
        return 'BUY', 1*atr14[i-1], 2*atr14[i-1]
    if rsi2[i] > 90 and close[i] < daily_ema200[i] and rsi14[i] < 60:
        return 'SELL', 1*atr14[i-1], 2*atr14[i-1]
    return None, 0, 0

def sig6_squeeze(i, close, bb_upper, bb_lower, bb_mid, kc_upper, kc_lower, kc_squeeze, atr14):
    if i < 8: return None, 0, 0
    # Check squeeze was active for last 6 bars
    was_squeeze = all(kc_squeeze[j] for j in range(i-6, i))
    if not was_squeeze: return None, 0, 0
    # Released up
    if bb_upper[i] >= kc_upper[i] and close[i] > bb_mid[i]:
        return 'BUY', 1.5*atr14[i-1], 2.5*atr14[i-1]
    if bb_lower[i] <= kc_lower[i] and close[i] < bb_mid[i]:
        return 'SELL', 1.5*atr14[i-1], 2.5*atr14[i-1]
    return None, 0, 0

def sig7_vwap(i, close, vwap, vwap_std, hour, atr14):
    if i < 2: return None, 0, 0
    if not (7 <= hour <= 17): return None, 0, 0
    if close[i] < vwap[i] - 1.5*vwap_std[i] and close[i] > close[i-1]:
        return 'BUY', 1*atr14[i-1], 1.5*atr14[i-1]
    if close[i] > vwap[i] + 1.5*vwap_std[i] and close[i] < close[i-1]:
        return 'SELL', 1*atr14[i-1], 1.5*atr14[i-1]
    return None, 0, 0

def sig8_orb(i, close, orb_high, orb_low, daily_ema200, hour, atr14):
    if i < 2: return None, 0, 0
    if hour > 16: return None, 0, 0
    if np.isnan(orb_high[i]) or np.isnan(orb_low[i]): return None, 0, 0
    if close[i] > orb_high[i] and close[i] > daily_ema200[i]:
        return 'BUY', 1.5*atr14[i-1], 3*atr14[i-1]
    if close[i] < orb_low[i] and close[i] < daily_ema200[i]:
        return 'SELL', 1.5*atr14[i-1], 3*atr14[i-1]
    return None, 0, 0

ALL_SIGNALS = [sig1_ema_cross, sig2_donchian, sig3_ict, sig4_candle,
               sig5_rsi2, sig6_squeeze, sig7_vwap, sig8_orb]
SIGNAL_NAMES = ['S1:EMA Cross', 'S2:Donchian', 'S3:ICT Liq', 'S4:Hammer/SS',
                 'S5:RSI2 Rev', 'S6:Squeeze', 'S7:VWAP Dev', 'S8:ORB']

def get_all_signals(i, close, high, low, open_, volume, ind, hour, timestamps):
    results = []
    for fn in ALL_SIGNALS:
        if fn == sig1_ema_cross:
            r = fn(i, close, ind['ema9'], ind['ema21'], ind['daily_ema200'], ind['rsi14'], ind['atr14'])
        elif fn == sig2_donchian:
            r = fn(i, close, ind['don20_hi'], ind['don20_lo'], ind['don55_mid'], ind['daily_ema200'], ind['rsi14'], ind['atr14'])
        elif fn == sig3_ict:
            r = fn(i, close, high, low, ind['daily_ema200'], ind['atr14'])
        elif fn == sig4_candle:
            r = fn(i, close, high, low, open_, ind['daily_ema200'], ind['atr14'])
        elif fn == sig5_rsi2:
            r = fn(i, close, ind['rsi2'], ind['rsi14'], ind['daily_ema200'], ind['atr14'])
        elif fn == sig6_squeeze:
            r = fn(i, close, ind['bb_upper'], ind['bb_lower'], ind['bb_mid'], ind['kc_upper'], ind['kc_lower'], ind['kc_squeeze'], ind['atr14'])
        elif fn == sig7_vwap:
            r = fn(i, close, ind['vwap'], ind['vwap_std'], hour, ind['atr14'])
        elif fn == sig8_orb:
            r = fn(i, close, ind['orb_high'], ind['orb_low'], ind['daily_ema200'], hour, ind['atr14'])
        else:
            r = (None, 0, 0)
        results.append(r)
    return results


# ─── 4. TRAILING SL ──────────────────────────────────────────────────────────
def update_trailing_sl(pos, current_price, current_atr, lows, highs, i):
    side = pos['side']
    entry = pos['entry']
    initial_risk = pos['initial_risk']
    pnl_pts = (current_price - entry) if side == 'BUY' else (entry - current_price)

    # Phase 1: Breakeven at 1R
    if pnl_pts >= initial_risk and not pos.get('phase1'):
        new_sl = entry + 0.01 if side == 'BUY' else entry - 0.01
        if (side == 'BUY' and new_sl > pos['sl']) or (side == 'SELL' and new_sl < pos['sl']):
            pos['sl'] = new_sl
            pos['phase1'] = True

    # Phase 2: Structure trail at 1.5R
    if pnl_pts >= initial_risk * 1.5:
        if side == 'BUY':
            swing_low = min(lows[max(0,i-5):i])
            if swing_low > pos['sl']:
                pos['sl'] = swing_low
        else:
            swing_high = max(highs[max(0,i-5):i])
            if swing_high < pos['sl']:
                pos['sl'] = swing_high
        pos['phase'] = max(pos.get('phase', 0), 2)

    # Phase 3: ATR compression at 2R
    if pnl_pts >= initial_risk * 2:
        trail = current_price - 0.5*current_atr if side == 'BUY' else current_price + 0.5*current_atr
        if (side == 'BUY' and trail > pos['sl']) or (side == 'SELL' and trail < pos['sl']):
            pos['sl'] = trail
        pos['phase'] = 3
        pos['peak_pnl'] = max(pos.get('peak_pnl', pnl_pts), pnl_pts)
        if pos['peak_pnl'] >= initial_risk * 2 and pnl_pts < pos['peak_pnl'] * 0.75:
            return True  # Force close
    return False


# ─── 5. FEATURE BUILDER ──────────────────────────────────────────────────────
def build_features(i, close, high, low, open_, volume, ind, hour, dow):
    if i < 21: return None
    c = close[i]
    vol_mean = float(np.mean(volume[max(0,i-20):i])) + 1e-9
    feats = [
        c / close[i-1] - 1,
        open_[i] / c - 1,
        (high[i] - low[i]) / c,
        ind['atr14'][i] / c,
        (c - ind['ema9'][i]) / c,
        (c - ind['ema21'][i]) / c,
        (c - ind['ema50'][i]) / c,
        (c - ind['daily_ema200'][i]) / c,
        ind['ema9'][i] / ind['ema21'][i] - 1,
        ind['rsi14'][i] / 100,
        ind['rsi2'][i] / 100,
        ind['macd_line'][i] / c,
        ind['macd_sig'][i] / c,
        ind['macd_hist'][i] / c,
        ind['don20_pct'][i],
        ind['bb_pct_b'][i],
        float(ind['kc_squeeze'][i]),
        float(volume[i]) / vol_mean,
        np.sin(2 * np.pi * hour / 24),
        np.cos(2 * np.pi * hour / 24),
        dow / 6.0,
        (high[i] - max(open_[i], c)) / (high[i] - low[i] + 1e-9),
        (min(open_[i], c) - low[i]) / (high[i] - low[i] + 1e-9),
        abs(c - open_[i]) / (high[i] - low[i] + 1e-9),
    ]
    return feats


# ─── 6. BACKTEST CORE ────────────────────────────────────────────────────────
def run_backtest(close, high, low, open_, volume, timestamps, ind,
                 signal_mask=None,       # list of signal indices to use
                 require_agreement=False, # 2+ signals agree
                 use_rf=False,
                 use_mlp=False,
                 use_trailing=False,
                 use_muting=False,
                 use_compound=False,
                 session_filter=False,
                 initial_equity=10000.0,
                 lot_size=0.01):

    n = len(close)
    if signal_mask is None:
        signal_mask = list(range(8))

    equity = initial_equity
    trades = []
    pos = None
    daily_losses = {}
    weekly_start = equity
    weekly_num = 0

    # ML state
    rf_model = None
    mlp_model = None
    scaler_rf = StandardScaler()
    scaler_mlp = StandardScaler()
    ml_X = []
    ml_y = []
    ml_retrain_counter = 0
    TRAIN_DAYS = 180
    train_cutoff = None

    # Muting state
    signal_stats = {k: [] for k in range(8)}  # last 30 trade outcomes per signal
    MUTE_THRESHOLD = 0.30
    MUTE_WINDOW = 30

    # Correlation block
    last_two = []

    for i in range(60, n):
        dt = pd.Timestamp(timestamps[i])
        hour = dt.hour
        dow = dt.dayofweek
        date = dt.date()

        # Weekly equity tracking
        wk = dt.isocalendar()[1]
        if wk != weekly_num:
            weekly_num = wk
            weekly_start = equity

        # Weekly DD guard
        if equity < weekly_start * 0.90:
            continue

        # Set train cutoff at 180 days
        if train_cutoff is None:
            first_dt = pd.Timestamp(timestamps[60]).date()
            train_cutoff = first_dt + timedelta(days=TRAIN_DAYS)

        cur_atr = ind['atr14'][i]
        if cur_atr < 0.01: continue

        # ─── Manage open position ─────────────────────────────────────────
        if pos is not None:
            cur_price = close[i]
            force_close = False

            if use_trailing:
                force_close = update_trailing_sl(pos, cur_price, cur_atr, low, high, i)

            # Check SL/TP hit (using high/low of bar)
            closed = False
            if pos['side'] == 'BUY':
                if low[i] <= pos['sl']:
                    exit_p = pos['sl']
                    closed = True
                elif high[i] >= pos['tp']:
                    exit_p = pos['tp']
                    closed = True
                elif force_close:
                    exit_p = cur_price
                    closed = True
            else:
                if high[i] >= pos['sl']:
                    exit_p = pos['sl']
                    closed = True
                elif low[i] <= pos['tp']:
                    exit_p = pos['tp']
                    closed = True
                elif force_close:
                    exit_p = cur_price
                    closed = True

            if closed:
                pnl_pts = (exit_p - pos['entry']) if pos['side'] == 'BUY' else (pos['entry'] - exit_p)
                # $10/pt per 0.01 lot for crude oil (1 lot = 1000 barrels, ~$10/tick per 0.01)
                pnl_usd = pnl_pts * pos['lot'] * 1000
                equity += pnl_usd
                outcome = 1 if pnl_usd > 0 else 0
                trades.append({
                    'entry_dt': pos['entry_dt'], 'exit_dt': dt,
                    'side': pos['side'], 'signal': pos['signal'],
                    'entry': pos['entry'], 'exit': exit_p,
                    'pnl_pts': pnl_pts, 'pnl_usd': pnl_usd,
                    'lot': pos['lot'], 'equity': equity
                })
                # Update daily losses
                if pnl_usd < 0:
                    daily_losses[date] = daily_losses.get(date, 0) + 1

                # Update signal stats for muting
                sig_idx = pos.get('sig_idx', 0)
                signal_stats[sig_idx].append(outcome)
                if len(signal_stats[sig_idx]) > MUTE_WINDOW:
                    signal_stats[sig_idx] = signal_stats[sig_idx][-MUTE_WINDOW:]

                # Update ML labels
                feats = pos.get('features')
                if feats is not None:
                    ml_X.append(feats)
                    ml_y.append(outcome)
                    ml_retrain_counter += 1

                # Retrain ML every 20 trades after warmup
                if ml_retrain_counter >= 20 and date > train_cutoff:
                    if len(ml_X) >= 40:
                        X_arr = np.array(ml_X, dtype=float)
                        y_arr = np.array(ml_y)
                        try:
                            scaler_rf.fit(X_arr)
                            Xs = scaler_rf.transform(X_arr)
                            rf_model = RandomForestClassifier(n_estimators=50, max_depth=6,
                                                               min_samples_leaf=5, random_state=42)
                            rf_model.fit(Xs, y_arr)
                            scaler_mlp.fit(X_arr)
                            Xs_m = scaler_mlp.transform(X_arr)
                            mlp_model = MLPClassifier(hidden_layer_sizes=(32, 16), max_iter=200,
                                                       random_state=42)
                            mlp_model.fit(Xs_m, y_arr)
                            ml_retrain_counter = 0
                        except Exception as e:
                            pass

                # Correlation block tracking
                last_two.append(outcome)
                if len(last_two) > 2: last_two = last_two[-2:]

                pos = None
                continue

        # ─── Daily loss limit ─────────────────────────────────────────────
        if daily_losses.get(date, 0) >= 3:
            continue

        # ─── No concurrent position ───────────────────────────────────────
        if pos is not None:
            continue

        # ─── Get signals ─────────────────────────────────────────────────
        sigs = get_all_signals(i, close, high, low, open_, volume, ind, hour, timestamps)

        # Apply muting
        if use_muting:
            for si in range(8):
                stats = signal_stats[si]
                if len(stats) >= MUTE_WINDOW:
                    wr = sum(stats) / len(stats)
                    if wr < MUTE_THRESHOLD:
                        sigs[si] = (None, 0, 0)

        # Filter to active signals
        active_sigs = [(sigs[si], si) for si in signal_mask if sigs[si][0] is not None]
        if not active_sigs:
            continue

        # Agreement logic
        buy_sigs  = [(s,si) for s,si in active_sigs if s[0] == 'BUY']
        sell_sigs = [(s,si) for s,si in active_sigs if s[0] == 'SELL']

        if require_agreement:
            chosen_dir = None
            if len(buy_sigs) >= 2:
                chosen_dir = 'BUY'
                chosen_list = buy_sigs
            elif len(sell_sigs) >= 2:
                chosen_dir = 'SELL'
                chosen_list = sell_sigs
            if chosen_dir is None:
                continue
            # Use first agreeing pair
            chosen_sig, chosen_si = chosen_list[0]
        else:
            # Use first active signal
            chosen_sig, chosen_si = active_sigs[0]

        direction, sl_dist, tp_dist = chosen_sig
        if direction is None or sl_dist <= 0: continue

        # ─── ML Filter ────────────────────────────────────────────────────
        feats = build_features(i, close, high, low, open_, volume, ind, hour, dow)
        min_conf = 0.52
        if len(last_two) == 2 and sum(last_two) == 0:
            min_conf = 0.60  # Stricter after 2 consecutive losses

        if use_rf and rf_model is not None and feats is not None:
            try:
                Xf = scaler_rf.transform([feats])
                prob = rf_model.predict_proba(Xf)[0][1]
                if prob < min_conf:
                    continue
            except:
                pass

        if use_mlp and mlp_model is not None and feats is not None:
            try:
                Xf = scaler_mlp.transform([feats])
                prob = mlp_model.predict_proba(Xf)[0][1]
                if prob < min_conf:
                    continue
            except:
                pass

        # ─── Lot sizing ───────────────────────────────────────────────────
        if use_compound:
            lot = equity * 0.01 / (cur_atr * 1000)
            lot = max(0.01, min(5.0, round(lot, 2)))
        else:
            lot = lot_size

        # Ensemble lot boost: if 2+ signals agree, full lot; else half lot
        n_agree = len(buy_sigs) if direction == 'BUY' else len(sell_sigs)

        entry_price = close[i]
        sl = entry_price - sl_dist if direction == 'BUY' else entry_price + sl_dist
        tp = entry_price + tp_dist if direction == 'BUY' else entry_price - tp_dist

        pos = {
            'side': direction, 'entry': entry_price, 'sl': sl, 'tp': tp,
            'initial_risk': sl_dist, 'lot': lot,
            'signal': SIGNAL_NAMES[chosen_si], 'sig_idx': chosen_si,
            'entry_dt': dt, 'phase1': False, 'phase': 0,
            'features': feats
        }

    # Close any open position at last price
    if pos is not None:
        exit_p = close[-1]
        pnl_pts = (exit_p - pos['entry']) if pos['side'] == 'BUY' else (pos['entry'] - exit_p)
        pnl_usd = pnl_pts * pos['lot'] * 1000
        equity += pnl_usd
        trades.append({
            'entry_dt': pos['entry_dt'], 'exit_dt': pd.Timestamp(timestamps[-1]),
            'side': pos['side'], 'signal': pos['signal'],
            'entry': pos['entry'], 'exit': exit_p,
            'pnl_pts': pnl_pts, 'pnl_usd': pnl_usd,
            'lot': pos['lot'], 'equity': equity
        })

    return pd.DataFrame(trades), equity


# ─── 7. STATISTICS ───────────────────────────────────────────────────────────
def compute_stats(trades_df, initial_equity=10000.0, total_weeks=None):
    if trades_df is None or len(trades_df) == 0:
        return {'trades': 0, 'wr': 0, 'pf': 0, 'pnl': 0, 'ev_week': 0,
                'max_dd_pct': 0, 'max_consec_loss': 0, 'trades_week': 0}

    df = trades_df.copy()
    n = len(df)
    wins = df[df['pnl_usd'] > 0]['pnl_usd'].sum()
    losses = abs(df[df['pnl_usd'] <= 0]['pnl_usd'].sum())
    pf = wins / (losses + 1e-9)
    wr = (df['pnl_usd'] > 0).mean() * 100
    pnl = df['pnl_usd'].sum()

    # Max DD
    eq = initial_equity + df['pnl_usd'].cumsum()
    peak = eq.cummax()
    dd = (eq - peak) / peak
    max_dd = abs(dd.min()) * 100

    # Max consecutive losses
    consec = 0
    max_consec = 0
    for p in df['pnl_usd']:
        if p <= 0:
            consec += 1
            max_consec = max(max_consec, consec)
        else:
            consec = 0

    if total_weeks is None:
        if len(df) > 0:
            span_days = (df['exit_dt'].max() - df['entry_dt'].min()).days
            total_weeks = max(1, span_days / 7)
        else:
            total_weeks = 1

    trades_week = n / total_weeks
    ev_week = pnl / total_weeks

    return {
        'trades': n, 'wr': wr, 'pf': pf, 'pnl': pnl,
        'ev_week': ev_week, 'max_dd_pct': max_dd,
        'max_consec_loss': max_consec, 'trades_week': trades_week
    }

def monthly_pnl(trades_df):
    if trades_df is None or len(trades_df) == 0:
        return {}
    df = trades_df.copy()
    df['month'] = df['exit_dt'].dt.to_period('M')
    return df.groupby('month')['pnl_usd'].sum().to_dict()

def lot_table(stats, initial_equity=10000.0):
    """Return (lot_for_400, capital_for_400)"""
    ev_week = stats['ev_week']
    if ev_week <= 0:
        return 'N/A', 'N/A'
    lot_needed = 400 / (ev_week / 0.01)
    capital_needed = initial_equity * (400 / (ev_week + 1e-9))
    return round(lot_needed, 3), round(capital_needed, 0)


# ─── 8. MAIN ─────────────────────────────────────────────────────────────────
def main():
    print("=" * 70)
    print("SHIVA ICT FULL BACKTEST — USOIL / CL=F  [SIMULATED RESULTS]")
    print("=" * 70)

    df_h, df_d = download_data()

    # Flatten MultiIndex columns if needed
    if isinstance(df_h.columns, pd.MultiIndex):
        df_h.columns = [c[0] for c in df_h.columns]
    if isinstance(df_d.columns, pd.MultiIndex):
        df_d.columns = [c[0] for c in df_d.columns]

    df_h = df_h.dropna()
    df_d = df_d.dropna()

    close    = df_h['Close'].values.astype(float)
    high     = df_h['High'].values.astype(float)
    low      = df_h['Low'].values.astype(float)
    open_    = df_h['Open'].values.astype(float)
    volume   = df_h['Volume'].values.astype(float)
    timestamps = df_h.index.tolist()

    # Daily EMA200 map
    d_close = df_d['Close'].values.astype(float)
    d_ema200 = ema_series(d_close, 200)
    daily_ema200_map = {}
    for j, dt in enumerate(df_d.index):
        daily_ema200_map[pd.Timestamp(dt).date()] = d_ema200[j]

    print("\nComputing indicators (no lookahead)...")
    ind = compute_indicators(close, high, low, open_, volume, timestamps, daily_ema200_map)
    print("  Done.")

    n = len(close)
    span_days = (pd.Timestamp(timestamps[-1]) - pd.Timestamp(timestamps[0])).days
    total_weeks = max(1, span_days / 7)
    print(f"  Data span: {span_days} days ({total_weeks:.1f} weeks) | Bars: {n}")
    print(f"  Price range: ${close.min():.2f} – ${close.max():.2f}")

    INITIAL_EQUITY = 10000.0
    results = {}

    print("\n" + "─"*70)
    print("ITERATION 1: Individual Signals")
    print("─"*70)

    for si in range(8):
        tr, eq = run_backtest(close, high, low, open_, volume, timestamps, ind,
                              signal_mask=[si], initial_equity=INITIAL_EQUITY)
        st = compute_stats(tr, INITIAL_EQUITY, total_weeks)
        lt = lot_table(st, INITIAL_EQUITY)
        results[SIGNAL_NAMES[si]] = st
        mp = monthly_pnl(tr)
        print(f"  {SIGNAL_NAMES[si]:20s} | Trades:{st['trades']:4d} | WR:{st['wr']:5.1f}% | "
              f"PF:{st['pf']:5.2f} | PnL:${st['pnl']:8.2f} | EV/wk:${st['ev_week']:7.2f} | "
              f"DD:{st['max_dd_pct']:5.1f}% | ConsecL:{st['max_consec_loss']}")

    print("\n" + "─"*70)
    print("ITERATION 2: All 8 Signals Combined (OR)")
    print("─"*70)
    tr, eq = run_backtest(close, high, low, open_, volume, timestamps, ind,
                          signal_mask=list(range(8)), initial_equity=INITIAL_EQUITY)
    st = compute_stats(tr, INITIAL_EQUITY, total_weeks)
    results['Combined OR'] = st
    lt = lot_table(st, INITIAL_EQUITY)
    print(f"  {'Combined OR':20s} | Trades:{st['trades']:4d} | WR:{st['wr']:5.1f}% | "
          f"PF:{st['pf']:5.2f} | PnL:${st['pnl']:8.2f} | EV/wk:${st['ev_week']:7.2f} | "
          f"DD:{st['max_dd_pct']:5.1f}% | ConsecL:{st['max_consec_loss']}")
    mp_combined = monthly_pnl(tr)

    print("\n" + "─"*70)
    print("ITERATION 3: 2-Signal Agreement Required")
    print("─"*70)
    tr, eq = run_backtest(close, high, low, open_, volume, timestamps, ind,
                          signal_mask=list(range(8)), require_agreement=True,
                          initial_equity=INITIAL_EQUITY)
    st = compute_stats(tr, INITIAL_EQUITY, total_weeks)
    results['2-Signal Agree'] = st
    print(f"  {'2-Signal Agree':20s} | Trades:{st['trades']:4d} | WR:{st['wr']:5.1f}% | "
          f"PF:{st['pf']:5.2f} | PnL:${st['pnl']:8.2f} | EV/wk:${st['ev_week']:7.2f} | "
          f"DD:{st['max_dd_pct']:5.1f}% | ConsecL:{st['max_consec_loss']}")

    print("\n" + "─"*70)
    print("ITERATION 4: + ML Random Forest Filter (P>=0.52)")
    print("─"*70)
    tr, eq = run_backtest(close, high, low, open_, volume, timestamps, ind,
                          signal_mask=list(range(8)), use_rf=True,
                          initial_equity=INITIAL_EQUITY)
    st = compute_stats(tr, INITIAL_EQUITY, total_weeks)
    results['+ RF Filter'] = st
    print(f"  {'+ RF Filter':20s} | Trades:{st['trades']:4d} | WR:{st['wr']:5.1f}% | "
          f"PF:{st['pf']:5.2f} | PnL:${st['pnl']:8.2f} | EV/wk:${st['ev_week']:7.2f} | "
          f"DD:{st['max_dd_pct']:5.1f}% | ConsecL:{st['max_consec_loss']}")

    print("\n" + "─"*70)
    print("ITERATION 5: + MLP Neural Network Filter")
    print("─"*70)
    tr, eq = run_backtest(close, high, low, open_, volume, timestamps, ind,
                          signal_mask=list(range(8)), use_rf=True, use_mlp=True,
                          initial_equity=INITIAL_EQUITY)
    st = compute_stats(tr, INITIAL_EQUITY, total_weeks)
    results['+ MLP Filter'] = st
    print(f"  {'+ MLP Filter':20s} | Trades:{st['trades']:4d} | WR:{st['wr']:5.1f}% | "
          f"PF:{st['pf']:5.2f} | PnL:${st['pnl']:8.2f} | EV/wk:${st['ev_week']:7.2f} | "
          f"DD:{st['max_dd_pct']:5.1f}% | ConsecL:{st['max_consec_loss']}")

    print("\n" + "─"*70)
    print("ITERATION 6: + 3-Phase Trailing SL")
    print("─"*70)
    tr, eq = run_backtest(close, high, low, open_, volume, timestamps, ind,
                          signal_mask=list(range(8)), use_rf=True, use_mlp=True,
                          use_trailing=True, initial_equity=INITIAL_EQUITY)
    st = compute_stats(tr, INITIAL_EQUITY, total_weeks)
    results['+ Trailing SL'] = st
    print(f"  {'+ Trailing SL':20s} | Trades:{st['trades']:4d} | WR:{st['wr']:5.1f}% | "
          f"PF:{st['pf']:5.2f} | PnL:${st['pnl']:8.2f} | EV/wk:${st['ev_week']:7.2f} | "
          f"DD:{st['max_dd_pct']:5.1f}% | ConsecL:{st['max_consec_loss']}")

    print("\n" + "─"*70)
    print("ITERATION 7: + Adaptive Signal Muting (30-trade WR < 30%)")
    print("─"*70)
    tr, eq = run_backtest(close, high, low, open_, volume, timestamps, ind,
                          signal_mask=list(range(8)), use_rf=True, use_mlp=True,
                          use_trailing=True, use_muting=True, initial_equity=INITIAL_EQUITY)
    st = compute_stats(tr, INITIAL_EQUITY, total_weeks)
    results['+ Muting'] = st
    print(f"  {'+ Muting':20s} | Trades:{st['trades']:4d} | WR:{st['wr']:5.1f}% | "
          f"PF:{st['pf']:5.2f} | PnL:${st['pnl']:8.2f} | EV/wk:${st['ev_week']:7.2f} | "
          f"DD:{st['max_dd_pct']:5.1f}% | ConsecL:{st['max_consec_loss']}")

    print("\n" + "─"*70)
    print("ITERATION 8: + Compound Lot Sizing (equity*1%/ATR/1000)")
    print("─"*70)
    tr, eq = run_backtest(close, high, low, open_, volume, timestamps, ind,
                          signal_mask=list(range(8)), use_rf=True, use_mlp=True,
                          use_trailing=True, use_muting=True, use_compound=True,
                          initial_equity=INITIAL_EQUITY)
    st = compute_stats(tr, INITIAL_EQUITY, total_weeks)
    results['FINAL compound'] = st
    print(f"  {'FINAL compound':20s} | Trades:{st['trades']:4d} | WR:{st['wr']:5.1f}% | "
          f"PF:{st['pf']:5.2f} | PnL:${st['pnl']:8.2f} | EV/wk:${st['ev_week']:7.2f} | "
          f"DD:{st['max_dd_pct']:5.1f}% | ConsecL:{st['max_consec_loss']}")

    # ─── Monthly P&L for combined ──────────────────────────────────────────
    print("\n" + "─"*70)
    print("MONTHLY P&L — Combined OR (0.01 lot):")
    for k, v in sorted(mp_combined.items(), key=lambda x: str(x[0])):
        print(f"  {str(k):8s}  ${v:8.2f}")

    # ─── Final Summary Table ──────────────────────────────────────────────
    print("\n")
    print("┌" + "─"*66 + "┐")
    print("│ STRATEGY              WR%    PF     PnL($)   EV/wk($)   DD%   │")
    print("├" + "─"*66 + "┤")
    order = (SIGNAL_NAMES + ['Combined OR', '2-Signal Agree', '+ RF Filter',
                              '+ MLP Filter', '+ Trailing SL', '+ Muting', 'FINAL compound'])
    for name in order:
        if name not in results: continue
        s = results[name]
        lt = lot_table(s, INITIAL_EQUITY)
        print(f"│ {name:22s} {s['wr']:5.1f}% {s['pf']:5.2f} {s['pnl']:8.2f}  {s['ev_week']:8.2f}  {s['max_dd_pct']:5.1f}% │")
    print("└" + "─"*66 + "┘")

    # ─── Lot sizing table ─────────────────────────────────────────────────
    print("\nLOT SIZING TABLE (target $400/week):")
    print(f"  {'Strategy':22s}  Lot@$400/wk  Capital needed")
    print("  " + "─"*50)
    for name in order:
        if name not in results: continue
        lt = lot_table(results[name], INITIAL_EQUITY)
        print(f"  {name:22s}  {str(lt[0]):12s}  ${str(lt[1])}")

    print("\n[SIMULATED RESULTS — Not financial advice]")
    print("=" * 70)

if __name__ == '__main__':
    main()
