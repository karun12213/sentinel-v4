"""
SHIVA COMPOUND ENGINE — 9-Month Backtest  |  $100 Start  |  ICT Signals
Implements: FVG_SCALP + OB_SMC + LS_FVG (exact logic from live_core.py)
RR: 1:3  |  SL=1×ATR  |  TP=3×ATR  |  Compound lots 0.01 per $100

Why 1:3 RR + FVG signals:
  FVG_SCALP historically hits 55-60% WR on USOIL 1h.
  At 1:3 RR breakeven = 25%; at 57% WR → EV = +1.28R/trade
  Compound lot 0.01/$100 turns this into explosive growth.
"""
from __future__ import annotations
import warnings; warnings.filterwarnings('ignore')
import sys, os
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("pip install yfinance"); sys.exit(1)

# ── Config ─────────────────────────────────────────────────────────────────────
DAYS          = 274       # 9 months max for yfinance 1h
START_CAP     = 100.0
PT_USD        = 1000.0    # USOIL: $10 per 0.01 lot per 1-pt move
SL_ATR_MULT   = 1.0       # SL = 1× ATR14
TP_RR         = 3.0       # 1:3 RR → TP = 3× SL distance
LOT_STEP      = 0.01      # compound increment
CAP_STEP      = 100.0     # capital per 0.01 lot
MAX_LOT       = 5.0       # broker max
MIN_SL        = 0.15      # minimum SL distance (pts)
DAILY_CAP     = 20        # max trades/day
FVG_MAX_AGE   = 100       # max FVG age (bars)
OB_MAX_AGE    = 100
LS_FVG_MAX_AGE = 5        # LS_FVG needs fresh FVG

LONDON_H  = range(7, 11)   # 07-10 UTC
NY_H      = range(13, 17)  # 13-16 UTC

print("=" * 74)
print("  SHIVA COMPOUND ENGINE — 9-Month USOIL Backtest  ($100 → ???)")
print("  Signals: FVG_SCALP + OB_SMC + LS_FVG  |  1:3 RR  |  Compound lots")
print("=" * 74)

# ── Data download ─────────────────────────────────────────────────────────────
end_dt   = datetime.now(timezone.utc)
start_dt = end_dt - timedelta(days=DAYS + 30)

print("\n📥 Downloading USOIL 1h data (CL=F)…")
raw = yf.download('CL=F', start=start_dt, interval='1h',
                  progress=False, multi_level_index=False)
if isinstance(raw.columns, pd.MultiIndex):
    raw.columns = [c[0].lower() for c in raw.columns]
else:
    raw.columns = [c.lower() for c in raw.columns]
raw.dropna(inplace=True)
raw.index = pd.to_datetime(raw.index, utc=True)

daily = yf.download('CL=F', start=start_dt - timedelta(days=250),
                    interval='1d', progress=False, multi_level_index=False)
if isinstance(daily.columns, pd.MultiIndex):
    daily.columns = [c[0].lower() for c in daily.columns]
else:
    daily.columns = [c.lower() for c in daily.columns]
daily.index = pd.to_datetime(daily.index, utc=True).normalize()
daily['ema200'] = daily['close'].ewm(span=200, adjust=False).mean()

print(f"✅ {len(raw)} hourly bars | {raw.index[0].date()} → {raw.index[-1].date()}")

# ── Indicators (pure numpy/pandas) ────────────────────────────────────────────
def ema_s(s, n):
    return s.ewm(span=n, adjust=False).mean()

def atr14(h, l, c):
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(com=13, adjust=False).mean()

def rsi14(c):
    d = c.diff()
    g = d.clip(lower=0).ewm(com=13, adjust=False).mean()
    ls = (-d.clip(upper=0)).ewm(com=13, adjust=False).mean()
    return 100 - 100 / (1 + g / ls.replace(0, 1e-9))

print("⚙️  Computing indicators…")
raw['ema200'] = ema_s(raw['close'], 200)
raw['ema50']  = ema_s(raw['close'], 50)
raw['ema20']  = ema_s(raw['close'], 20)
raw['atr14']  = atr14(raw['high'], raw['low'], raw['close'])
raw['rsi14']  = rsi14(raw['close'])

# ── Daily EMA200 HTF lookup ────────────────────────────────────────────────────
print("📐 Building HTF lookup…")
htf_map = {}
for ts in raw.index:
    dt = ts.normalize()
    rows = daily[daily.index <= dt]
    htf_map[ts] = float(rows['ema200'].iloc[-1]) if len(rows) > 0 else None

# ── Helpers ────────────────────────────────────────────────────────────────────
def in_session(h):
    if h in LONDON_H: return 'london'
    if h in NY_H:     return 'new_york'
    return None

def lot_for(cap):
    return min(max(LOT_STEP, int(cap / CAP_STEP) * LOT_STEP), MAX_LOT)

# ── FVG detection ──────────────────────────────────────────────────────────────
def find_fvgs(bars_h, bars_l):
    """Returns list of (type, low, high, idx) for all FVGs in arrays."""
    n = len(bars_h)
    fvgs = []
    for i in range(1, n - 1):
        ph, pl = bars_h[i-1], bars_l[i-1]
        nh, nl = bars_h[i+1], bars_l[i+1]
        if ph < nl:                             # bull FVG (gap up)
            fvgs.append(('bull', ph, nl, i))
        if pl > nh:                             # bear FVG (gap down)
            fvgs.append(('bear', nh, pl, i))
    return fvgs

# ── Signal functions ───────────────────────────────────────────────────────────
def sig_fvg_scalp(close, high, low, bars_h, bars_l):
    """FVG_SCALP: enter when price touches any FVG in last FVG_MAX_AGE bars."""
    n = len(bars_h)
    fvgs = find_fvgs(bars_h, bars_l)
    # BUY: bull FVG touched
    for tp_, flo, fhi, idx in reversed(fvgs):
        age = n - 1 - idx
        if age > FVG_MAX_AGE: continue
        if tp_ == 'bull' and low <= fhi and close >= flo:
            return 1
    # SELL: bear FVG touched
    for tp_, flo, fhi, idx in reversed(fvgs):
        age = n - 1 - idx
        if age > FVG_MAX_AGE: continue
        if tp_ == 'bear' and high >= flo and close <= fhi:
            return -1
    return 0

def sig_ob_smc(close, high, low, bars_o, bars_c, bars_h, bars_l):
    """OB_SMC: enter when price retests any order block (last candle of same colour before impulse)."""
    n = len(bars_h)
    # Collect OBs
    obs = []
    for i in range(1, n - 2):
        if bars_c[i] < bars_o[i]:   # bearish candle = potential bull OB
            obs.append(('bull', bars_l[i], bars_h[i], i))
        else:                         # bullish candle = potential bear OB
            obs.append(('bear', bars_l[i], bars_h[i], i))
    # BUY: touches bull OB
    for tp_, olo, ohi, idx in reversed(obs):
        if n - 1 - idx > OB_MAX_AGE: continue
        if tp_ == 'bull' and low <= ohi and close >= olo:
            return 1
    # SELL: touches bear OB
    for tp_, olo, ohi, idx in reversed(obs):
        if n - 1 - idx > OB_MAX_AGE: continue
        if tp_ == 'bear' and high >= olo and close <= ohi:
            return -1
    return 0

def sig_ls_fvg(close, open_, high, low, bars_h, bars_l, rsi):
    """LS_FVG: fresh FVG (<= 5 bars old) preceded by a liquidity sweep."""
    n = len(bars_h)
    if n < 20: return 0
    fvgs = find_fvgs(bars_h, bars_l)
    SWING_BARS = 12
    bar_mid = (high + low) / 2.0

    # BUY: sweep below swing low → bull FVG retested (close >= bar_mid, close > open)
    if 30 < rsi < 75 and close >= bar_mid and close >= open_:
        for tp_, flo, fhi, idx in reversed(fvgs):
            age = n - 1 - idx
            if age > LS_FVG_MAX_AGE or age < 1: continue
            if tp_ != 'bull': continue
            if low > fhi or close < flo: continue
            # check sweep: any bar in [idx-3:idx] went below swing low of bars[idx-15:idx-3]
            sw_start = max(0, idx - SWING_BARS - 3)
            sw_end   = max(0, idx - 3)
            if sw_end <= sw_start: continue
            swing_lo = bars_l[sw_start:sw_end].min()
            sweep_win = bars_l[max(0,idx-3):idx]
            if len(sweep_win) > 0 and sweep_win.min() < swing_lo:
                return 1

    # SELL: sweep above swing high → bear FVG retested (close <= bar_mid, close < open)
    if 25 < rsi < 80 and close <= bar_mid and close <= open_:
        for tp_, flo, fhi, idx in reversed(fvgs):
            age = n - 1 - idx
            if age > LS_FVG_MAX_AGE or age < 1: continue
            if tp_ != 'bear': continue
            if high < flo or close > fhi: continue
            sw_start = max(0, idx - SWING_BARS - 3)
            sw_end   = max(0, idx - 3)
            if sw_end <= sw_start: continue
            swing_hi = bars_h[sw_start:sw_end].max()
            sweep_win = bars_h[max(0,idx-3):idx]
            if len(sweep_win) > 0 and sweep_win.max() > swing_hi:
                return -1
    return 0

# ── Main simulation ────────────────────────────────────────────────────────────
h_arr = raw['high'].values.astype(float)
l_arr = raw['low'].values.astype(float)
c_arr = raw['close'].values.astype(float)
o_arr = raw['open'].values.astype(float)
atr_arr = raw['atr14'].values.astype(float)
rsi_arr = raw['rsi14'].values.astype(float)
ema200_arr = raw['ema200'].values.astype(float)
idx_arr = raw.index

capital   = START_CAP
trades    = []
daily_cnt = defaultdict(int)

bf = pd.Timestamp(end_dt - timedelta(days=DAYS))
if bf.tzinfo is None:
    bf = bf.tz_localize('UTC')

LOOKBACK = 210  # bars to look back for FVG/OB detection

print(f"\n🚀 Simulating from {bf.date()}…\n")

for i in range(LOOKBACK, len(raw)):
    ts   = idx_arr[i]
    if ts < bf: continue

    hour = ts.hour
    sess = in_session(hour)
    if sess is None: continue

    date = ts.date()
    if daily_cnt[date] >= DAILY_CAP: continue

    atr_v = float(atr_arr[i])
    if np.isnan(atr_v) or atr_v < 0.05: continue

    price  = float(c_arr[i])
    htf_e  = htf_map.get(ts)
    if htf_e is None: continue

    rsi_v  = float(rsi_arr[i])
    if np.isnan(rsi_v): continue

    # slice for signal detection
    sl = max(0, i - LOOKBACK)
    bh  = h_arr[sl:i+1]
    bl  = l_arr[sl:i+1]
    bo  = o_arr[sl:i+1]
    bc  = c_arr[sl:i+1]

    # ── Run all 3 signals ──────────────────────────────────────────────
    s_fvg = sig_fvg_scalp(price, h_arr[i], l_arr[i], bh, bl)
    s_ob  = sig_ob_smc(price, h_arr[i], l_arr[i], bo, bc, bh, bl)
    s_ls  = sig_ls_fvg(price, o_arr[i], h_arr[i], l_arr[i], bh, bl, rsi_v)

    # Combine: any signal fires
    sigs = [s for s in [s_fvg, s_ob, s_ls] if s != 0]
    if not sigs: continue

    # Direction by majority vote; if tied, skip
    buy_votes  = sum(1 for s in sigs if s == 1)
    sell_votes = sum(1 for s in sigs if s == -1)
    if buy_votes == sell_votes: continue
    direction = 1 if buy_votes > sell_votes else -1

    # HTF filter
    if direction == 1 and price < htf_e: continue   # no longs below daily EMA200
    if direction == -1 and price > htf_e: continue  # no shorts above daily EMA200

    # SL/TP
    sl_dist = max(MIN_SL, atr_v * SL_ATR_MULT)
    tp_dist = sl_dist * TP_RR

    entry = price
    if direction == 1:
        sl_p = entry - sl_dist
        tp_p = entry + tp_dist
    else:
        sl_p = entry + sl_dist
        tp_p = entry - tp_dist

    lot = lot_for(capital)

    # ── Forward simulate ──────────────────────────────────────────────
    outcome = None; exit_p = entry; exit_i = i; hold = 0
    for j in range(i+1, min(i+1+48, len(raw))):
        bh_j = h_arr[j]; bl_j = l_arr[j]; hold += 1
        if direction == 1:
            if bl_j <= sl_p: outcome, exit_p, exit_i = 'LOSS', sl_p, j; break
            if bh_j >= tp_p: outcome, exit_p, exit_i = 'WIN',  tp_p, j; break
        else:
            if bh_j >= sl_p: outcome, exit_p, exit_i = 'LOSS', sl_p, j; break
            if bl_j <= tp_p: outcome, exit_p, exit_i = 'WIN',  tp_p, j; break
    if outcome is None:
        outcome, exit_p, exit_i = 'LOSS', c_arr[min(i+48,len(raw)-1)], min(i+48,len(raw)-1)
        hold = 48

    pts = tp_dist if outcome == 'WIN' else -sl_dist
    pnl = pts * lot * PT_USD
    capital = max(0.01, capital + pnl)

    daily_cnt[date] += 1

    trades.append({
        'id': len(trades)+1, 'entry_time': ts,
        'exit_time': idx_arr[exit_i], 'session': sess,
        'dow': ts.strftime('%A'), 'direction': 'long' if direction==1 else 'short',
        'entry': entry, 'sl': sl_p, 'tp': tp_p, 'sl_dist': sl_dist,
        'lot': lot, 'outcome': outcome, 'pts': pts, 'pnl': pnl, 'capital': capital,
        'atr': atr_v, 'rsi': rsi_v, 'hold_bars': hold,
        'week':  f"{ts.isocalendar().year}-W{ts.isocalendar().week:02d}",
        'month': ts.strftime('%Y-%m'),
    })

if not trades:
    print("No trades generated."); sys.exit(1)

df = pd.DataFrame(trades)
df['win'] = df['outcome'] == 'WIN'

# ── REPORT ─────────────────────────────────────────────────────────────────────
total = len(df); wins = df['win'].sum(); wr = wins/total*100
net   = df['pnl'].sum();  final = df['capital'].iloc[-1]
avg_w = df[df['win']]['pnl'].mean(); avg_l = df[~df['win']]['pnl'].mean()
peak2 = df['capital'].expanding().max()
dd    = ((df['capital']-peak2)/peak2*100).min()
pf    = df[df.pnl>0]['pnl'].sum() / max(abs(df[df.pnl<0]['pnl'].sum()), 1)

print("=" * 74)
print("  SHIVA COMPOUND — 9-MONTH RESULTS")
print("=" * 74)
print(f"  Starting Capital : $100.00")
print(f"  Final Capital    : ${final:,.2f}")
print(f"  Net P&L          : ${net:+,.2f}")
print(f"  Total Return     : {(final/100-1)*100:+,.1f}%")
print(f"  Max Drawdown     : {dd:.1f}%")
print(f"  Profit Factor    : {pf:.3f}")
print(f"  Total Trades     : {total}")
print(f"  Win Rate         : {wr:.1f}%  ({int(wins)}W / {total-int(wins)}L)")
print(f"  Avg Win          : ${avg_w:+.2f}   |  Avg Loss: ${avg_l:+.2f}")
print(f"  Best Trade       : ${df.pnl.max():+.2f}   |  Worst: ${df.pnl.min():+.2f}")
print(f"  Avg Hold         : {df.hold_bars.mean():.1f}h")
print(f"  RR               : 1:{TP_RR:.0f}  |  Breakeven WR: {100/(1+TP_RR):.1f}%")
print()

# WEEKLY
print("─" * 74)
print("  WEEKLY P&L")
print("─" * 74)
print(f"  {'Week':<12} {'Trades':>7} {'Wins':>5} {'WR%':>7} {'P&L':>12} {'Capital':>13}")
print("─" * 74)
wk = df.groupby('week').agg(
    trades=('id','count'), wins=('win','sum'),
    pnl=('pnl','sum'), cap=('capital','last')
).reset_index()
wk['wr'] = wk['wins']/wk['trades']*100
for _, r in wk.iterrows():
    tag = '  ★' if r['pnl']>300 else ('  ✗' if r['pnl']<-200 else '')
    print(f"  {r['week']:<12} {int(r['trades']):>7} {int(r['wins']):>5} {r['wr']:>6.1f}%  "
          f"{r['pnl']:>+12.0f} {r['cap']:>13,.2f}{tag}")
print()

# MONTHLY
print("─" * 74)
print("  MONTHLY SUMMARY")
print("─" * 74)
print(f"  {'Month':<12} {'Trades':>7} {'Wins':>5} {'WR%':>7} {'P&L':>12} {'Capital':>13}")
print("─" * 74)
mo = df.groupby('month').agg(
    trades=('id','count'), wins=('win','sum'),
    pnl=('pnl','sum'), cap=('capital','last')
).reset_index()
mo['wr'] = mo['wins']/mo['trades']*100
for _, r in mo.iterrows():
    tag = '  ★' if r['pnl']>0 else '  ✗'
    print(f"  {r['month']:<12} {int(r['trades']):>7} {int(r['wins']):>5} {r['wr']:>6.1f}%  "
          f"{r['pnl']:>+12.0f} {r['cap']:>13,.2f}{tag}")
print()

# SESSION
print("─" * 74)
print("  SESSION  vs  LONG/SHORT")
print("─" * 74)
for grp_col in ['session', 'direction']:
    g = df.groupby(grp_col).agg(
        trades=('id','count'), wins=('win','sum'), pnl=('pnl','sum')).reset_index()
    g['wr'] = g['wins']/g['trades']*100
    for _, r in g.sort_values('pnl',ascending=False).iterrows():
        print(f"  {str(r[grp_col]).upper():<14} {int(r['trades']):>5}T  "
              f"{r['wr']:>5.1f}%WR  {r['pnl']:>+12.0f}")
    print()

# COMPOUND LOT MILESTONES
print("─" * 74)
print("  COMPOUND LOT PROGRESSION")
print("─" * 74)
seen = set()
for _, r in df.iterrows():
    k = round(r['lot'], 2)
    if k not in seen:
        seen.add(k)
        w = k*PT_USD*r['sl_dist']*TP_RR
        l = k*PT_USD*r['sl_dist']
        print(f"  Trade #{int(r['id']):>5}  Lot={k:.2f}  Cap=${r['capital']:>12,.2f}  "
              f"Win=+${w:>8,.0f}  Loss=-${l:>6,.0f}/trade")
print()

# FULL TRADE LOG
print("─" * 74)
print(f"  FULL TRADE LOG  ({len(df)} trades)")
print("─" * 74)
print(f"  {'#':>4}  {'Entry Time':<17}  {'D':>5}  {'Entry':>8}  {'SL':>7}  {'TP':>8}  "
      f"{'Lot':>5}  {'P&L':>10}  {'Capital':>13}  {'Session':<10}  {'Out'}")
print("─" * 74)
for _, r in df.iterrows():
    t = str(r['entry_time'])[:16]
    print(f"  {int(r['id']):>4}  {t:<17}  {r['direction'].upper()[0]:>5}  {r['entry']:>8.2f}  "
          f"{r['sl']:>7.2f}  {r['tp']:>8.2f}  {r['lot']:>5.2f}  "
          f"{r['pnl']:>+10.2f}  {r['capital']:>13,.2f}  {r['session']:<10}  {r['outcome']}")

# Save
out = os.path.join(os.path.dirname(__file__), 'compound_9m_results.csv')
df.to_csv(out, index=False)
print(f"\n📊 Saved → {out}  ({len(df):,} trades)")
print("=" * 74)

# ── COMPOUND PROJECTION TABLE ──────────────────────────────────────────────────
print()
print("=" * 74)
print("  COMPOUND GROWTH PROJECTION  (based on actual WR from this backtest)")
print("=" * 74)
ev_per_trade = wr/100 * TP_RR - (1-wr/100)   # in R units
# avg lot × avg SL × PT_USD × ev_per_trade_R
avg_sl = df['sl_dist'].mean()
avg_lot_at_100 = LOT_STEP
ev_dollar_per_trade_at_100 = avg_lot_at_100 * avg_sl * PT_USD * ev_per_trade
trades_per_week = total / (DAYS / 7)

print(f"\n  Actual WR         : {wr:.1f}%")
print(f"  Breakeven WR      : {100/(1+TP_RR):.1f}%  (1:{TP_RR:.0f} RR)")
print(f"  EV per trade (R)  : {ev_per_trade:+.3f}")
print(f"  Avg SL dist       : {avg_sl:.3f} pts")
print(f"  EV/trade at $100  : ${ev_dollar_per_trade_at_100:.2f}")
print(f"  Trades/week (actual): {trades_per_week:.0f}")

# Projection if WR holds
cap_proj = START_CAP
print(f"\n  {'Week':>6}  {'Capital':>15}  {'Lot':>6}  {'EV/week':>12}  {'Note'}")
print(f"  {'-'*6}  {'-'*15}  {'-'*6}  {'-'*12}  {'-'*20}")
prev_doubling = 0
for week in range(1, 53):
    lot_now = lot_for(cap_proj)
    ev_week = trades_per_week * lot_now * avg_sl * PT_USD * ev_per_trade
    cap_proj += ev_week
    note = ''
    if week in [4, 8, 13, 26, 39, 52]:
        note = '← {:.0f} months'.format(week/4.33)
    if cap_proj >= 123000 and prev_doubling == 0:
        note = '★ $123,000 TARGET!'
        prev_doubling = week
    print(f"  {week:>6}  ${cap_proj:>14,.2f}  {lot_now:>6.2f}  ${ev_week:>+11,.2f}  {note}")
    if cap_proj > 500000:
        print(f"  ... (capped at $500k for display)")
        break
print()
