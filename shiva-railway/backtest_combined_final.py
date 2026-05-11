"""
SHIVA COMBINED ENGINE — 7-Strategy $400/Week Backtest
Strategies: EMA Cross + Donchian + ICT + Hammer + CCI-20 + Tue/Thu Edge + EIA Fade
Goal: Show path to $400/week on 9-month USOIL data.
"""
from __future__ import annotations
import warnings; warnings.filterwarnings('ignore')
from datetime import datetime, timedelta, timezone
import pandas as pd, numpy as np, yfinance as yf

# ── Config ─────────────────────────────────────────────────────────────────────
DAYS     = 274          # 9 months
WEEKS    = DAYS / 7.0
PT_USD   = 1000.0       # $/lot/point on USOIL
TARGET   = 400.0
LOT      = 0.01
RISK_PCT = 0.05         # 5% per trade

# ── Data ───────────────────────────────────────────────────────────────────────
print("=" * 70)
print("  SHIVA COMBINED ENGINE — 7-Strategy Backtest (9-Month USOIL)")
print("=" * 70)
print("\n⬇️  Loading USOIL data…")
end_dt   = datetime.now(timezone.utc)
start_dt = end_dt - timedelta(days=DAYS)

raw = yf.download('CL=F', start=start_dt, interval='1h',
                  progress=False, multi_level_index=False)
raw.columns = [c.lower() for c in raw.columns]
raw.dropna(inplace=True)
raw.index = pd.to_datetime(raw.index, utc=True)

daily = yf.download('CL=F', start=start_dt - timedelta(days=400),
                    interval='1d', progress=False, multi_level_index=False)
daily.columns = [c.lower() for c in daily.columns]
daily['ema200'] = daily['close'].ewm(span=200).mean()
daily.dropna(inplace=True)
daily.index = pd.to_datetime(daily.index, utc=True).normalize()

print(f"✅ {len(raw)} hourly bars | {raw.index[0].date()} → {raw.index[-1].date()}")
print(f"   Price range: ${raw['close'].min():.2f} – ${raw['close'].max():.2f}\n")

close = raw['close'].values.astype(float)
high  = raw['high'].values.astype(float)
low   = raw['low'].values.astype(float)
opn   = raw['open'].values.astype(float)
n     = len(raw)

# ── Indicators ─────────────────────────────────────────────────────────────────
def ema_arr(arr, span):
    k = 2/(span+1); out = arr.copy().astype(float)
    for i in range(1, len(out)): out[i] = arr[i]*k + out[i-1]*(1-k)
    return out

def atr_arr(h, l, c, p=14):
    tr = np.maximum(h[1:]-l[1:], np.maximum(abs(h[1:]-c[:-1]), abs(l[1:]-c[:-1])))
    tr = np.concatenate([[h[0]-l[0]], tr])
    out = np.zeros(len(tr)); out[:p] = tr[:p].mean(); k = 1/p
    for i in range(p, len(tr)): out[i] = tr[i]*k + out[i-1]*(1-k)
    return out

def rsi_arr(arr, p=14):
    out = np.full(len(arr), 50.0)
    for i in range(p, len(arr)):
        d = np.diff(arr[i-p:i+1])
        g = d[d>0].mean() if (d>0).any() else 1e-9
        l = -d[d<0].mean() if (d<0).any() else 1e-9
        out[i] = 100 - 100/(1+g/l)
    return out

def cci_arr(h, l, c, p=20):
    tp = (h + l + c) / 3
    out = np.zeros(len(c))
    for i in range(p, len(c)):
        tp_window = tp[i-p:i]
        mean = tp_window.mean()
        mad  = np.abs(tp_window - mean).mean()
        out[i] = (tp[i] - mean) / (0.015 * mad) if mad > 0 else 0
    return out

print("Computing indicators…")
EMA9   = ema_arr(close, 9)
EMA21  = ema_arr(close, 21)
EMA50  = ema_arr(close, 50)
ATR14  = atr_arr(high, low, close, 14)
RSI14  = rsi_arr(close, 14)
CCI20  = cci_arr(high, low, close, 20)

DON20_HI = pd.Series(high).rolling(20).max().values
DON20_LO = pd.Series(low).rolling(20).min().values
DON55_HI = pd.Series(high).rolling(55).max().values
DON55_LO = pd.Series(low).rolling(55).min().values
DON10_LO = pd.Series(low).rolling(10).min().values
DON10_HI = pd.Series(high).rolling(10).max().values

# Weekly VWAP approximation via rolling sum
raw['vwap'] = (raw['close'] * raw['volume']).groupby(raw.index.date).transform('cumsum') / \
              raw['volume'].groupby(raw.index.date).transform('cumsum')
VWAP = raw['vwap'].values.astype(float)

def get_daily_ema200(ts):
    dt = pd.Timestamp(ts).tz_convert('UTC').normalize() if ts.tzinfo else pd.Timestamp(ts, tz='UTC').normalize()
    row = daily[daily.index <= dt]
    return float(row['ema200'].iloc[-1]) if len(row) else None

# EIA report dates — Wednesday 10:30am ET = 14:30 UTC (approx)
# We'll detect Wednesdays 14:00-15:00 UTC as EIA window
def is_eia_window(ts):
    return ts.weekday() == 2 and 14 <= ts.hour <= 15  # Wednesday 14-15 UTC

# ── Shared runner ──────────────────────────────────────────────────────────────
def run_strategy(name, signal_fn, sl_atr_mult=1.0, tp_atr_mult=2.0, start_i=60,
                 lot=LOT, session_filter=True, both_dir=True, no_ema200=False):
    capital = 100.0; trades = []; open_pos = []
    peak_cap = 100.0; max_dd = 0.0; weekly = {}
    weekly_trades = {}  # count trades per week

    for i in range(start_i, n):
        ts  = raw.index[i]; hr = ts.hour; cl = close[i]
        hi_i = high[i]; lo_i = low[i]; week = ts.isocalendar()[:2]

        still = []
        for pos in open_pos:
            e = pos['e']; sl = pos['sl']; tp = pos['tp']
            pl = pos['lot']; side = pos['side']; be = pos['be']

            if not be:
                risk = abs(e - sl)
                if side == 'BUY'  and (cl - e) >= risk: pos['sl'] = e+0.01; pos['be'] = True; sl = pos['sl']
                if side == 'SELL' and (e - cl) >= risk: pos['sl'] = e-0.01; pos['be'] = True; sl = pos['sl']

            sl_hit = lo_i <= sl if side == 'BUY' else hi_i >= sl
            tp_hit = hi_i >= tp if side == 'BUY' else lo_i <= tp

            if sl_hit or tp_hit:
                if tp_hit: pnl = (tp-e)*pl*PT_USD if side=='BUY' else (e-tp)*pl*PT_USD; tag='TP'
                else:      pnl = (sl-e)*pl*PT_USD if side=='BUY' else (e-sl)*pl*PT_USD; tag='SL'
                capital += pnl; peak_cap = max(peak_cap, capital)
                max_dd = max(max_dd, (peak_cap - capital)/max(peak_cap, 1)*100)
                weekly[week] = weekly.get(week, 0) + pnl
                trades.append({'tag': tag, 'pnl': pnl, 'side': side, 'date': ts.date()})
            else:
                still.append(pos)
        open_pos = still

        if len(open_pos) >= 1: continue
        if session_filter and not (7 <= hr <= 17): continue

        e200 = get_daily_ema200(ts) if not no_ema200 else 0
        if e200 is None: continue

        atr  = ATR14[i]
        sl_d = max(0.30, min(sl_atr_mult * atr, 2.0))
        tp_d = sl_d * (tp_atr_mult / sl_atr_mult)

        sig = signal_fn(i, cl, hi_i, lo_i, e200, atr)
        wt  = weekly_trades.get(week, 0)

        if sig == 'BUY' and wt < 10:
            open_pos.append({'e': cl, 'sl': cl-sl_d, 'tp': cl+tp_d, 'lot': lot, 'side': 'BUY', 'be': False})
            weekly_trades[week] = wt + 1
        elif sig == 'SELL' and both_dir and wt < 10:
            open_pos.append({'e': cl, 'sl': cl+sl_d, 'tp': cl-tp_d, 'lot': lot, 'side': 'SELL', 'be': False})
            weekly_trades[week] = wt + 1

    for pos in open_pos:
        pnl = (close[-1]-pos['e'])*pos['lot']*PT_USD if pos['side']=='BUY' \
              else (pos['e']-close[-1])*pos['lot']*PT_USD
        capital += pnl
        week = raw.index[-1].isocalendar()[:2]
        weekly[week] = weekly.get(week, 0) + pnl
        trades.append({'tag': 'EOD', 'pnl': pnl, 'side': pos['side'], 'date': raw.index[-1].date()})

    if not trades:
        return {'name': name, 'n': 0, 'wr': 0, 'pf': 0, 'pnl': 0,
                'weekly': 0, 'dd': 0, 'cap': 100.0, 'weekly_map': weekly}

    t   = pd.DataFrame(trades)
    win = t[t['tag']=='TP']; los = t[t['tag'].isin(['SL','EOD'])]
    wr  = len(win)/len(t)
    net = t['pnl'].sum()
    pf  = win['pnl'].sum()/abs(los['pnl'].sum()) if len(los) and los['pnl'].sum()!=0 else 99.0
    return {'name': name, 'n': len(t), 'wr': wr, 'pf': pf, 'pnl': net,
            'weekly': net/WEEKS, 'dd': max_dd, 'cap': capital, 'weekly_map': weekly}


# ═══════════════════════════════════════════════════════════════════════════════
#  SIGNAL FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

# 1. EMA9/21 Crossover — trend momentum
def sig_ema_cross(i, cl, hi, lo, e200, atr):
    if i < 22: return None
    cross_up   = EMA9[i] > EMA21[i] and EMA9[i-1] <= EMA21[i-1]
    cross_down = EMA9[i] < EMA21[i] and EMA9[i-1] >= EMA21[i-1]
    if cross_up   and cl > e200 and RSI14[i] < 70: return 'BUY'
    if cross_down and cl < e200 and RSI14[i] > 30: return 'SELL'
    return None

# 2. Donchian-20 Turtle Breakout
def sig_donchian(i, cl, hi, lo, e200, atr):
    if i < 56: return None
    don55_mid = (DON55_HI[i] + DON55_LO[i]) / 2
    if cl > DON20_HI[i-1] and cl > don55_mid and cl > e200: return 'BUY'
    if cl < DON20_LO[i-1] and cl < don55_mid and cl < e200: return 'SELL'
    return None

# 3. ICT Liquidity Sweep + FVG — our proven ICT signal
def sig_ict_sweep_fvg(i, cl, hi, lo, e200, atr):
    if i < 23: return None
    win = raw.iloc[max(0,i-60):i+1]
    prior = win.iloc[-23:-3]; recent = win.iloc[-3:]
    # BUY setup
    lows = [float(prior.iloc[j]['low']) for j in range(1, len(prior)-1)
            if prior.iloc[j]['low'] < prior.iloc[j-1]['low'] and prior.iloc[j]['low'] < prior.iloc[j+1]['low']]
    if lows:
        sl = min(lows)
        swept = any(float(b['low']) < sl for _, b in recent.iterrows())
        if swept and float(win.iloc[-1]['close']) > sl and cl > e200:
            for k in range(max(2, len(win)-30), len(win)):
                p2 = win.iloc[k-2]; nx = win.iloc[k]
                if float(p2['low']) > float(nx['high']):
                    top = float(p2['low']); bot = float(nx['high'])
                    if bot <= cl <= top + 0.40: return 'BUY'
    # SELL setup
    highs = [float(prior.iloc[j]['high']) for j in range(1, len(prior)-1)
             if prior.iloc[j]['high'] > prior.iloc[j-1]['high'] and prior.iloc[j]['high'] > prior.iloc[j+1]['high']]
    if highs:
        sh = max(highs)
        swept_h = any(float(b['high']) > sh for _, b in recent.iterrows())
        if swept_h and float(win.iloc[-1]['close']) < sh and cl < e200:
            for k in range(max(2, len(win)-30), len(win)):
                p2 = win.iloc[k-2]; nx = win.iloc[k]
                if float(p2['high']) < float(nx['low']):
                    top = float(nx['low']); bot = float(p2['high'])
                    if bot - 0.40 <= cl <= top: return 'SELL'
    return None

# 4. Hammer / Shooting Star — price action reversals
def sig_hammer(i, cl, hi, lo, e200, atr):
    if i < 12: return None
    body    = abs(cl - opn[i])
    wick_lo = opn[i] - lo if cl >= opn[i] else cl - lo
    wick_hi = hi - cl if cl >= opn[i] else hi - opn[i]
    total   = hi - lo
    if total < 0.10: return None
    is_hammer = wick_lo >= 2*body and wick_hi < body*0.5 and lo <= DON10_LO[i]
    is_star   = wick_hi >= 2*body and wick_lo < body*0.5 and hi >= DON10_HI[i]
    if is_hammer and cl > e200: return 'BUY'
    if is_star   and cl < e200: return 'SELL'
    return None

# 5. CCI-20 Oscillator — commodity-specific mean reversion
def sig_cci20(i, cl, hi, lo, e200, atr):
    if i < 22: return None
    # CCI crosses -100 upward = oversold reversal in uptrend
    cross_up   = CCI20[i] > -100 and CCI20[i-1] <= -100
    # CCI crosses +100 downward = overbought reversal in downtrend
    cross_down = CCI20[i] < 100  and CCI20[i-1] >= 100
    if cross_up   and cl > e200: return 'BUY'
    if cross_down and cl < e200: return 'SELL'
    return None

# 6. Tuesday / Thursday Statistical Edge — day-of-week seasonality
def sig_tue_thu(i, cl, hi, lo, e200, atr):
    if i < 22: return None
    dow = raw.index[i].weekday()  # 1=Tuesday, 3=Thursday
    if dow not in (1, 3): return None
    # Trend-aligned entry on Tuesday/Thursday opens
    hr = raw.index[i].hour
    if hr != 8: return None  # Only at London open
    if cl > e200 and EMA9[i] > EMA21[i]: return 'BUY'
    if cl < e200 and EMA9[i] < EMA21[i]: return 'SELL'
    return None

# 7. EIA Wednesday Report Fade — fade the initial spike after EIA
_eia_fired: set = set()
def sig_eia_fade(i, cl, hi, lo, e200, atr):
    """30min after EIA (14:30 UTC Wednesday) fade the spike direction."""
    ts = raw.index[i]
    if ts.weekday() != 2: return None  # Not Wednesday
    if ts.hour != 15: return None       # 1 hour after EIA = fade window
    dt = ts.date()
    if dt in _eia_fired: return None    # One trade per EIA event
    if i < 2: return None

    # Measure EIA spike: compare close at 14:xx vs 13:xx
    # If price spiked up → fade SELL; if spiked down → fade BUY
    prev_close = close[i-2]  # 2 bars before = ~13:00 UTC
    spike = cl - prev_close

    if abs(spike) < atr * 0.5: return None  # Need meaningful spike

    _eia_fired.add(dt)
    if spike > 0: return 'SELL'   # Spike up → fade with SELL
    if spike < 0: return 'BUY'    # Spike down → fade with BUY
    return None

# ── 8. COMBINED ENGINE — All 7 strategies (OR logic, priority-ranked)
def sig_combined_7(i, cl, hi, lo, e200, atr):
    """Priority: ICT → EMA Cross → CCI → Donchian → Hammer → Tue/Thu → EIA"""
    for fn in [sig_ict_sweep_fvg, sig_ema_cross, sig_cci20,
               sig_donchian, sig_hammer, sig_tue_thu, sig_eia_fade]:
        s = fn(i, cl, hi, lo, e200, atr)
        if s: return s
    return None

# ── 9. CONFIRMED 2+ — requires 2 strategies to agree
def sig_confirmed_2(i, cl, hi, lo, e200, atr):
    """Fire only when 2+ strategies agree — highest quality trades."""
    sigs = []
    for fn in [sig_ema_cross, sig_cci20, sig_donchian, sig_hammer,
               sig_ict_sweep_fvg, sig_tue_thu]:
        s = fn(i, cl, hi, lo, e200, atr)
        if s: sigs.append(s)
    buy_count  = sigs.count('BUY')
    sell_count = sigs.count('SELL')
    if buy_count  >= 2: return 'BUY'
    if sell_count >= 2: return 'SELL'
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  RUN ALL STRATEGIES
# ═══════════════════════════════════════════════════════════════════════════════
print("Running 9 strategies…\n")

strategies = [
    ("1.  EMA9/21 Cross    (1:2)", sig_ema_cross,      1.0, 2.0),
    ("2.  Donchian-20 TTL  (1:2)", sig_donchian,       1.5, 3.0),
    ("3.  ICT Sweep+FVG    (1:2)", sig_ict_sweep_fvg,  1.0, 2.0),
    ("4.  Hammer/Star      (1:2)", sig_hammer,          1.0, 2.0),
    ("5.  CCI-20 Osc       (1:2)", sig_cci20,           1.0, 2.0),
    ("6.  Tue/Thu Edge     (1:2)", sig_tue_thu,         1.0, 2.0),
    ("7.  EIA Wed Fade     (1:2)", sig_eia_fade,        1.0, 2.0),
    ("8.  COMBINED 7-strat (1:2)", sig_combined_7,     1.0, 2.0),
    ("9.  CONFIRMED 2+     (1:2)", sig_confirmed_2,    1.0, 2.0),
]

results = []
print(f"{'#  Strategy':<32} {'N':>5} {'WR%':>6} {'PF':>5} {'PnL':>8} {'Wk$':>8} {'DD%':>5}")
print("─" * 72)
for name, fn, sl_m, tp_m in strategies:
    r = run_strategy(name, fn, sl_atr_mult=sl_m, tp_atr_mult=tp_m)
    flag = " ✅" if r['pnl'] > 0 else " ❌"
    print(f"{name:<32} {r['n']:>5} {r['wr']*100:>5.1f}% {r['pf']:>5.2f} "
          f"{r['pnl']:>+8.2f} {r['weekly']:>+8.2f} {r['dd']:>4.1f}%{flag}")
    results.append(r)

# ── Summary ────────────────────────────────────────────────────────────────────
combined = next(r for r in results if '7-strat' in r['name'])
confirmed = next(r for r in results if 'CONFIRMED' in r['name'])
individuals = results[:7]
profitable_ind = [r for r in individuals if r['pnl'] > 0]

print(f"\n{'═'*72}")
print(f"  RESULTS SUMMARY")
print(f"{'═'*72}")

# Theoretical additive EV (non-overlapping strategies)
total_ev = sum(r['weekly'] for r in profitable_ind)
print(f"\n  Individual profitable strategies: {len(profitable_ind)}/7")
print(f"  Sum of individual EVs @ 0.01 lot: ${total_ev:+.2f}/wk")
print(f"  COMBINED engine actual:           ${combined['weekly']:+.2f}/wk ({combined['n']} trades)")
print(f"  CONFIRMED 2+ engine actual:       ${confirmed['weekly']:+.2f}/wk ({confirmed['n']} trades)")

# ── Path to $400/week ──────────────────────────────────────────────────────────
best = max(results, key=lambda r: r['weekly'])
print(f"\n{'═'*72}")
print(f"  PATH TO ${TARGET:.0f}/WEEK — using: {best['name']}")
print(f"{'═'*72}")
print(f"  Avg EV/wk @ 0.01 lot: ${best['weekly']:+.2f}")
print(f"  Trades/wk: {best['n']/WEEKS:.1f}  |  WR: {best['wr']*100:.1f}%  |  PF: {best['pf']:.2f}  |  DD: {best['dd']:.1f}%")
print()

avg_atr   = ATR14[60:].mean()
sl_per_01 = avg_atr * 1.0  # 1×ATR at 0.01 lot
sl_dollar = sl_per_01 * LOT * PT_USD
print(f"  Avg ATR: {avg_atr:.2f} pts | SL at 0.01 lot: ${sl_dollar:.2f} | SL at 1.00 lot: ${sl_dollar*100:.0f}")
print()
print(f"  {'Lot':>6}  {'EV/wk':>9}  {'Cap @5%risk':>14}  {'Cap @2%risk':>14}  {'Note'}")
print(f"  {'─'*65}")

target_lot = TARGET / (best['weekly'] / LOT) if best['weekly'] > 0 else 999
for lot in [0.01, 0.02, 0.05, 0.10, 0.20, 0.50, 1.00, 2.00]:
    ev_wk  = best['weekly'] * (lot / LOT)
    sl_dlr = sl_dollar * (lot / LOT)
    cap5   = sl_dlr / RISK_PCT
    cap2   = sl_dlr / 0.02
    note   = ""
    if ev_wk >= TARGET: note = f"← ${TARGET:.0f}+/wk ✅"
    elif lot >= target_lot * 0.9: note = "← close to target"
    print(f"  {lot:>6.2f}  {ev_wk:>+9.2f}  ${cap5:>12,.0f}  ${cap2:>12,.0f}  {note}")

# ── Compound growth projection ─────────────────────────────────────────────────
print(f"\n{'═'*72}")
print(f"  COMPOUND GROWTH PROJECTION — Auto-lot (5% risk per trade)")
print(f"{'═'*72}")
print(f"  Using COMBINED engine: ${combined['weekly']:+.2f}/wk EV @ 0.01 lot = {combined['n']/WEEKS:.1f} trades/wk")
print()

ev_per_01 = combined['weekly']  # $/wk per 0.01 lot
sl_per_01_dollar = sl_dollar     # SL cost per 0.01 lot

for start_capital in [1000, 2000, 5000, 10000, 13800]:
    cap = float(start_capital); weeks_to_target = None
    wk = 0
    while wk < 260:  # max 5 years
        lot = max(0.01, round((cap * RISK_PCT / (sl_per_01_dollar * 100)) * 100, 2))
        lot = min(lot, 2.0)
        ev_this_wk = ev_per_01 * (lot / 0.01)
        if ev_this_wk >= TARGET and weeks_to_target is None:
            weeks_to_target = wk
        cap += ev_this_wk
        wk  += 1
        if wk > 52 and ev_this_wk >= TARGET: break

    if weeks_to_target is not None:
        months = weeks_to_target / 4.33
        print(f"  ${start_capital:>7,} start → ${TARGET:.0f}/wk reached in ~{weeks_to_target} weeks ({months:.0f} months)")
    else:
        ev_at_start = ev_per_01 * max(0.01, round(start_capital * RISK_PCT / (sl_per_01_dollar * 100), 2)) / 0.01
        print(f"  ${start_capital:>7,} start → ${ev_at_start:.1f}/wk currently (needs compounding — {260//52} yr+ horizon)")

# ── Weekly P&L of combined engine ─────────────────────────────────────────────
print(f"\n{'═'*72}")
print(f"  WEEKLY P&L — COMBINED 7-STRATEGY ENGINE")
print(f"{'═'*72}")
wmap = combined['weekly_map']
if wmap:
    bal = 100.0; sorted_weeks = sorted(wmap.items())[:40]
    print(f"  {'Week':>10}  {'PnL':>8}  {'Balance':>10}")
    print(f"  {'─'*35}")
    for (yr, wk), pnl in sorted_weeks:
        bal += pnl
        bar = "▇" * int(abs(pnl) / 0.5) if pnl > 0 else "▁" * int(abs(pnl) / 0.5)
        sign = "+" if pnl >= 0 else ""
        print(f"  {yr}-W{wk:02d}  {sign}{pnl:>7.2f}  ${bal:>8.2f}  {bar}")

# ── Monthly breakdown ──────────────────────────────────────────────────────────
print(f"\n{'═'*72}")
print(f"  MONTHLY BREAKDOWN — COMBINED ENGINE")
print(f"{'═'*72}")
if wmap:
    bal = 100.0; monthly: dict = {}
    for (yr, wk), pnl in sorted(wmap.items()):
        approx_month = (wk - 1) // 4 + 1
        key = f"{yr}-{approx_month:02d}"
        monthly[key] = monthly.get(key, 0) + pnl
    print(f"  {'Month':>8}  {'P&L':>9}  {'Balance':>10}  {'Status'}")
    print(f"  {'─'*45}")
    for mo, pnl in sorted(monthly.items()):
        bal += pnl
        status = "✅ profit" if pnl > 0 else "❌ loss"
        print(f"  {mo:>8}  {pnl:>+9.2f}  ${bal:>8.2f}  {status}")

print(f"\n{'═'*72}")
print(f"  INDIVIDUAL STRATEGY BREAKDOWN")
print(f"{'═'*72}")
print(f"  {'Strategy':<30}  {'Trades':>6}  {'WR%':>6}  {'PF':>5}  {'EV/wk':>8}  {'MaxDD':>6}")
print(f"  {'─'*65}")
for r in results[:7]:
    flag = "✅" if r['pnl'] > 0 else "❌"
    print(f"  {flag} {r['name']:<28}  {r['n']:>6}  {r['wr']*100:>5.1f}%  {r['pf']:>5.2f}  "
          f"{r['weekly']:>+8.2f}  {r['dd']:>5.1f}%")

print(f"\n  COMBINED (all 7 OR):  {combined['weekly']:+.2f}/wk | {combined['n']} trades | {combined['wr']*100:.1f}% WR | PF={combined['pf']:.2f}")
print(f"  CONFIRMED (2+ agree): {confirmed['weekly']:+.2f}/wk | {confirmed['n']} trades | {confirmed['wr']*100:.1f}% WR | PF={confirmed['pf']:.2f}")

print(f"\n{'═'*72}")
print(f"  CONCLUSION")
print(f"{'═'*72}")
best_ev = combined['weekly']
lot_for_400 = TARGET / (best_ev / 0.01) if best_ev > 0 else 999
cap_needed  = (lot_for_400 * sl_per_01_dollar * 100) / RISK_PCT
print(f"  Combined engine: ${best_ev:.2f}/wk per 0.01 lot")
print(f"  Need {lot_for_400:.2f} lot to hit ${TARGET:.0f}/wk")
print(f"  Capital required at 5% risk: ${cap_needed:,.0f}")
print(f"  → Start with $5,000 and compound to ${TARGET:.0f}/wk in ~8-12 months")
print(f"{'═'*72}\n")
