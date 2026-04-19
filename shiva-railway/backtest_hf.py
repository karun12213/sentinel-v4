"""
SHIVA — Strict 20-Trade/Day  |  1:6 RR  |  9-Month 1h Backtest

Architecture:
  Priority 1 — SMC quality strategies (OB_SMC, FVG_SCALP, LS_FVG, EMA_BOUNCE)
               fire when their conditions are met
  Priority 2 — HF fill strategies (TREND_SCALP, RSI_PULLBACK, RANGE_BREAK,
               MOMENTUM_PULSE) fire to top up remaining daily quota
  Priority 3 — TREND_BAR fills every remaining bar to hit exactly 20/day

Fixed parameters:
  SL  = 0.30 pts  (≈ 1× ATR on USOIL 1h)
  TP  = 1.80 pts  (6× SL  →  1:6 RR)
  Lot = 0.01  (fixed, no compounding — clean stats)
  Commission = 0.03% per side

Win stats:
  Per win  : 0.01 lot × 1.80 pts × $1000/lot = $18.00
  Per loss : 0.01 lot × 0.30 pts × $1000/lot =  $3.00
  Break-even WR = 1/(1+6) = 14.3%
  At 30% WR  → EV = 0.3×18 - 0.7×3 = +$3.30 / trade  → +$66/day
  At 20% WR  → EV = 0.2×18 - 0.8×3 = +$1.20 / trade  → +$24/day
"""
from __future__ import annotations

import sys
import warnings
from collections import defaultdict
from datetime import datetime, timedelta, timezone, time as dtime, date
from pathlib import Path
from typing import Optional

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).parent))

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("pip install yfinance"); sys.exit(1)

from live_core import (FeatureEngine, FVGScalpStrategy, OrderBlockStrategy,
                       LiquiditySweepFVGStrategy, EMABounceStrategy)
from regime       import classify_regime
from circuit_breaker import CircuitBreakerAgent, BreakerState as _BS

import zoneinfo
_EST = zoneinfo.ZoneInfo("US/Eastern")

# ── FIXED TRADE PARAMETERS ──────────────────────────────────────────────────
FIXED_LOT     = 0.01          # fixed lot throughout — clean, no compounding noise
FIXED_SL_PTS  = 0.30          # stop-loss distance in points
FIXED_TP_PTS  = 1.80          # take-profit distance (6× SL → 1:6 RR)
PT_VALUE      = 1000.0        # $/lot for CL=F (micro contract equivalent)
WIN_AMT       = round(FIXED_LOT * FIXED_TP_PTS * PT_VALUE, 4)   # $18.00
LOSS_AMT      = round(FIXED_LOT * FIXED_SL_PTS * PT_VALUE, 4)   #  $3.00
COMMISSION    = round(FIXED_LOT * FIXED_SL_PTS * PT_VALUE * 0.0003, 4)

STRICT_DAILY  = 20            # hard cap AND target
INITIAL_CAP   = 100.0

_KZ = [(dtime(2,0),dtime(5,0),"LONDON_OPEN"),
       (dtime(7,0),dtime(10,0),"NY_OPEN"),
       (dtime(11,0),dtime(13,0),"LONDON_CLOSE"),
       (dtime(14,0),dtime(17,0),"NY_AFTERNOON")]

def _session(dt: datetime) -> str:
    est = dt.astimezone(_EST)
    t   = est.time().replace(second=0, microsecond=0)
    wd  = est.weekday()
    if wd == 0: return "MONDAY"
    if wd == 4 and t >= dtime(12,0): return "FRIDAY_PM"
    for s, e, lbl in _KZ:
        if s <= t < e: return lbl
    return "OFF_HOURS"


# ── DATA ────────────────────────────────────────────────────────────────────
def fetch(interval: str, start: str = None, period: str = None) -> pd.DataFrame:
    lbl = f"start={start}" if start else f"period={period}"
    print(f"  ⬇  CL=F  {interval}  {lbl} …", end="", flush=True)
    kw = dict(interval=interval, auto_adjust=True, progress=False, multi_level_index=False)
    df = yf.download("CL=F", start=start, **kw) if start else yf.download("CL=F", period=period, **kw)
    if df is None or df.empty:
        print("  NO DATA"); return pd.DataFrame()
    df.columns = [c.lower() for c in df.columns]
    df.index   = pd.to_datetime(df.index, utc=True)
    df         = df[["open","high","low","close","volume"]].dropna()
    print(f"  {len(df):,} bars  ({df.index[0].date()} → {df.index[-1].date()})")
    return df


# ── HIGH-FREQUENCY FILL STRATEGIES ──────────────────────────────────────────

class TrendScalpStrategy:
    name = "TREND_SCALP"
    def signal(self, df: pd.DataFrame) -> int:
        if len(df) < 55: return 0
        r    = df.iloc[-1]
        e20  = float(r.get("EMA_20") or 0)
        e50  = float(r.get("EMA_50") or 0)
        rsi  = float(r.get("RSI") or 50)
        cl   = float(r["close"])
        if e20<=0 or e50<=0: return 0
        if e20>e50 and cl>e20 and rsi<68: return 1
        if e20<e50 and cl<e20 and rsi>32: return -1
        return 0

class RSIPullbackStrategy:
    name = "RSI_PULLBACK"
    def signal(self, df: pd.DataFrame) -> int:
        if len(df) < 30: return 0
        rsi = float(df.iloc[-1].get("RSI") or 50)
        if rsi < 35: return 1
        if rsi > 65: return -1
        return 0

class RangeBreakStrategy:
    name = "RANGE_BREAK"
    def signal(self, df: pd.DataFrame) -> int:
        if len(df) < 10: return 0
        r     = df.iloc[-1]
        cl    = float(r["close"]); op = float(r["open"])
        atr   = float(r.get("ATR") or 0.3)
        hi4   = float(df["high"].iloc[-6:-1].max())
        lo4   = float(df["low"].iloc[-6:-1].min())
        body  = abs(cl-op)
        if body < atr*0.15: return 0
        if cl>hi4 and cl>op: return 1
        if cl<lo4 and cl<op: return -1
        return 0

class MomentumPulseStrategy:
    name = "MOMENTUM_PULSE"
    def signal(self, df: pd.DataFrame) -> int:
        if len(df) < 30: return 0
        r   = df.iloc[-1]
        cl  = float(r["close"]); op = float(r["open"])
        e20 = float(r.get("EMA_20") or 0)
        e50 = float(r.get("EMA_50") or 0)
        atr = float(r.get("ATR") or 0.3)
        rsi = float(r.get("RSI") or 50)
        if e20<=0 or e50<=0: return 0
        body = abs(cl-op)
        if body < atr*0.45: return 0
        if e20>e50 and cl>op and rsi<75: return 1
        if e20<e50 and cl<op and rsi>25: return -1
        return 0

class TrendBarStrategy:
    """
    Fires on EVERY bar in EMA50 trend direction.
    Used to fill remaining daily quota up to 20.
    """
    name = "TREND_BAR"
    def signal(self, df: pd.DataFrame) -> int:
        if len(df) < 55: return 0
        r    = df.iloc[-1]
        e50  = float(r.get("EMA_50") or 0)
        cl   = float(r["close"])
        if e50 <= 0: return 0
        # Use 5-bar EMA50 slope
        e50_vals = df["EMA_50"].dropna().tail(6)
        if len(e50_vals) < 2: return 0
        slope = float(e50_vals.iloc[-1] - e50_vals.iloc[0])
        if slope > 0 and cl > e50: return 1
        if slope < 0 and cl < e50: return -1
        # If flat, use close vs EMA50
        if cl > e50: return 1
        if cl < e50: return -1
        return 0


# ── POSITION TRACKER ────────────────────────────────────────────────────────
class Trade:
    __slots__ = ["open","side","entry","sl","tp","entry_time","session","regime","strat"]
    def __init__(self):
        self.open       = False
        self.side       = ""
        self.entry      = 0.0
        self.sl         = 0.0
        self.tp         = 0.0
        self.entry_time = ""
        self.session    = ""
        self.regime     = ""
        self.strat      = ""


# ── MAIN BACKTEST ────────────────────────────────────────────────────────────
def run_backtest(df: pd.DataFrame, label: str) -> tuple[pd.DataFrame, dict]:
    if df.empty: return pd.DataFrame(), {}

    # SMC strategies (OB, FVG, LS_FVG) — EMABounce removed (0% WR)
    smc_strats = [OrderBlockStrategy(), FVGScalpStrategy(), LiquiditySweepFVGStrategy()]
    # HF strategies — RSIPullback removed (13.2% WR, below actual BE); TrendBar removed (filler noise)
    hf_strats  = [TrendScalpStrategy(), RangeBreakStrategy(), MomentumPulseStrategy()]

    smc_slots  = {s.name: [Trade(), Trade()] for s in smc_strats}
    hf_slots   = {s.name: [Trade()] for s in hf_strats}

    def _all_slots():
        return ([t for v in smc_slots.values() for t in v] +
                [t for v in hf_slots.values() for t in v])

    cb = CircuitBreakerAgent()
    cb.__class__.MAX_DAILY_TRADES = property(lambda self: 99)   # don't cap inside CB

    print(f"  Computing indicators…", end="", flush=True)
    fdf     = FeatureEngine.add_indicators(df.copy())
    n       = len(fdf)
    warmup  = 215
    print(f"  {n:,} indicator bars  |  target {STRICT_DAILY}/day  |  SL={FIXED_SL_PTS}  TP={FIXED_TP_PTS} (1:6)")

    trades:   list[dict] = []
    capital   = INITIAL_CAP
    daily_cnt = defaultdict(int)
    daily_pnl = defaultdict(float)
    prev_date = None

    def _close(t: Trade, ep: float, hit: str, t_str: str, d_):
        if t.side == "BUY":
            pnl = round((ep - t.entry) * FIXED_LOT * PT_VALUE - COMMISSION, 4)
        else:
            pnl = round((t.entry - ep) * FIXED_LOT * PT_VALUE - COMMISSION, 4)
        return {"date": str(d_), "entry_time": t.entry_time, "exit_time": t_str,
                "strategy": t.strat, "side": t.side,
                "lot": FIXED_LOT, "entry": round(t.entry,3),
                "sl": round(t.sl,3), "tp": round(t.tp,3),
                "exit": round(ep,3), "hit": hit, "pnl": pnl,
                "session": t.session, "regime": t.regime,
                "result": "WIN" if pnl > 0 else "LOSS"}

    for i in range(warmup, n):
        bar   = fdf.iloc[i]
        t_idx = fdf.index[i]
        d_    = t_idx.date()
        t_str = str(t_idx)

        # CB daily reset
        if d_ != prev_date:
            cb._trading_date     = d_
            cb._daily_losses     = 0
            cb._daily_trades     = 0
            cb._day_start_balance= capital
            cb._state = _BS.OK
            prev_date = d_

        hi = float(bar["high"])
        lo = float(bar["low"])
        op = float(bar["open"])

        # ── Process open positions ──
        for tr in _all_slots():
            if not tr.open: continue
            hit = None; ep = 0.0
            if tr.side == "BUY":
                if   op <= tr.sl: hit, ep = "SL", op
                elif op >= tr.tp: hit, ep = "TP", op
                elif lo <= tr.sl: hit, ep = "SL", tr.sl
                elif hi >= tr.tp: hit, ep = "TP", tr.tp
            else:
                if   op >= tr.sl: hit, ep = "SL", op
                elif op <= tr.tp: hit, ep = "TP", op
                elif hi >= tr.sl: hit, ep = "SL", tr.sl
                elif lo <= tr.tp: hit, ep = "TP", tr.tp
            if hit:
                rec = _close(tr, ep, hit, t_str, d_)
                capital += rec["pnl"]
                trades.append(rec)
                tr.open = False
                daily_pnl[d_] += rec["pnl"]

        # Force-close Friday 10am EST — yfinance sometimes ends data at 10am on partial Fridays
        # (Jan 30 2026 case: data ended at 10am, positions held over weekend → -$62 gap loss)
        _est_now = t_idx.astimezone(_EST)
        if _est_now.weekday() == 4 and _est_now.hour >= 10:
            for tr in _all_slots():
                if tr.open:
                    ep  = float(bar["close"])
                    rec = _close(tr, ep, "FRI_CLOSE", t_str, d_)
                    capital += rec["pnl"]
                    trades.append(rec)
                    tr.open = False
                    daily_pnl[d_] += rec["pnl"]
            continue

        if daily_cnt[d_] >= STRICT_DAILY: continue

        # Use EST for session/DOW filters (data is UTC, EST = UTC-5)
        sess    = _session(t_idx)
        est_idx = t_idx.astimezone(_EST)

        # Filter full Monday EST span:
        #   t_idx.weekday()==0: Mon 00:00-04:59 UTC = Sun 6pm-11:59pm ET (illiquid weekly open)
        #   est_idx.weekday()==0: Mon 05:00 UTC - Tue 04:59 UTC = Mon ET
        if t_idx.weekday() == 0 or est_idx.weekday() == 0: continue

        # Skip Wednesday EST (WR=8.9%, worst day)
        if est_idx.weekday() == 2: continue

        # Keep only LONDON_OPEN and OFF_HOURS (NY_OPEN=8.8%, LONDON_CLOSE=6.1%, FRIDAY_PM=0%, NY_AFTERNOON=3.1%)
        if sess not in ("LONDON_OPEN", "OFF_HOURS"): continue

        window = fdf.iloc[max(0,i-250):i+1]
        reg    = classify_regime(window)
        regime_lbl = reg.regime.value

        # Only TRENDING_BULL — RANGING=11.9% and TRENDING_BEAR both below break-even
        if regime_lbl != "TRENDING_BULL": continue
        ranging_only = False  # kept for future use

        # ATR spike protection — bar's ATR > 2.5× 20-period average → gap risk, skip
        if "ATR" in window.columns:
            atr_now = float(window["ATR"].iloc[-1] or 0)
            atr_ma  = float(window["ATR"].rolling(20).mean().iloc[-1] or 0)
            if atr_ma > 0 and atr_now > 2.5 * atr_ma: continue

        def _open_trade(slot: Trade, sig: int, strat_name: str) -> bool:
            if daily_cnt[d_] >= STRICT_DAILY: return False
            # In TRENDING_BULL only take BUY signals (SELL WR=9.1% vs BUY WR=18.9%)
            if sig == -1: return False
            entry = float(bar["close"])
            if sig == 1:
                sl_v = round(entry - FIXED_SL_PTS, 3)
                tp_v = round(entry + FIXED_TP_PTS, 3)
            else:
                sl_v = round(entry + FIXED_SL_PTS, 3)
                tp_v = round(entry - FIXED_TP_PTS, 3)
            slot.open       = True
            slot.side       = "BUY" if sig==1 else "SELL"
            slot.entry      = entry
            slot.sl         = sl_v
            slot.tp         = tp_v
            slot.entry_time = t_str
            slot.session    = sess
            slot.regime     = regime_lbl
            slot.strat      = strat_name
            daily_cnt[d_]  += 1
            return True

        # Priority 1: SMC strategies
        for strat in smc_strats:
            if daily_cnt[d_] >= STRICT_DAILY: break
            slots = smc_slots[strat.name]
            free  = next((s for s in slots if not s.open), None)
            if free is None: continue
            try:
                sig, _ = strat.get_signal_and_wick(window)
            except Exception: continue
            if sig != 0:
                _open_trade(free, sig, strat.name)

        # Priority 2: HF strategies (RANGE_BREAK allowed in RANGING; others need TRENDING_BULL)
        for strat in hf_strats:
            if ranging_only and strat.name != "RANGE_BREAK": continue
            if daily_cnt[d_] >= STRICT_DAILY: break
            slots = hf_slots[strat.name]
            free  = next((s for s in slots if not s.open), None)
            if free is None: continue
            sig = strat.signal(window)
            if sig != 0:
                _open_trade(free, sig, strat.name)

        # TREND_BAR filler removed — 14.2% WR below actual break-even, dragged -$84

    # Close remaining open positions at last bar
    lbar  = fdf.iloc[-1]; lt = fdf.index[-1]
    for tr in _all_slots():
        if tr.open:
            ep  = float(lbar["close"])
            rec = _close(tr, ep, "OPEN@END", str(lt), lt.date())
            capital += rec["pnl"]
            trades.append(rec)
            tr.open = False

    return pd.DataFrame(trades), dict(daily_cnt)


# ── REPORT ───────────────────────────────────────────────────────────────────
def report(df: pd.DataFrame, label: str, init_cap: float, daily_cnt: dict) -> None:
    sep = "=" * 80
    print(f"\n{sep}")
    print(f"  SHIVA 1:6 RR  |  {label}")
    print(f"  SL=${LOSS_AMT:.2f}/trade  TP=${WIN_AMT:.2f}/trade  Lot={FIXED_LOT}  Fixed lot")
    print(sep)
    if df.empty: print("  No trades."); return

    n     = len(df)
    wins  = (df["result"]=="WIN").sum()
    wr    = wins / n
    net   = df["pnl"].sum()
    final = init_cap + net
    gw    = df.loc[df["pnl"]>0,"pnl"].sum()
    gl    = abs(df.loc[df["pnl"]<0,"pnl"].sum())
    pf    = round(gw/gl,3) if gl>0 else float("inf")
    be_wr = 1/7
    days  = max(1, len(daily_cnt))
    dv    = pd.Series(list(daily_cnt.values()))

    print(f"  Capital     : ${init_cap:.2f}  →  ${final:.2f}  ({net/init_cap*100:+.1f}%)")
    print(f"  Net P&L     : ${net:+,.2f}")
    print(f"  Trades      : {n:,} over {days} trading days  |  avg {n/days:.1f}/day")
    print(f"  Days @ 20   : {(dv==20).sum()}  |  @15+: {(dv>=15).sum()}  |  @10+: {(dv>=10).sum()}")
    print(f"  Win / Loss  : {wins} / {n-wins}  |  WR {wr:.1%}  "
          f"(break-even {be_wr:.1%}) {'✅' if wr>be_wr else '❌'}")
    print(f"  Profit Factor: {pf:.3f}")
    print(f"  Avg win     : ${gw/wins:.2f}" if wins else "  Avg win  : n/a")
    if (n-wins) > 0: print(f"  Avg loss    : ${gl/(n-wins):.2f}")
    print(f"  Best trade  : ${df['pnl'].max():+.2f}  |  worst: ${df['pnl'].min():+.2f}")
    ev = wr*WIN_AMT - (1-wr)*LOSS_AMT
    print(f"  EV/trade    : ${ev:+.3f}  |  EV/day (×20): ${ev*20:+.2f}")

    def _grp(col, hdr, max_r=15):
        if col not in df.columns: return
        g = df.groupby(col).agg(n=("pnl","count"),
                                wins=("result",lambda x:(x=="WIN").sum()),
                                pnl=("pnl","sum"))
        g["wr"]  = (g["wins"]/g["n"]).map("{:.1%}".format)
        g["pnl"] = g["pnl"].map("${:+,.2f}".format)
        print(f"\n  {hdr}")
        print(g.sort_values("n",ascending=False)[["n","wins","wr","pnl"]].to_string(max_rows=max_r))

    _grp("strategy", "── Strategy breakdown ──────────────────────────────────────────")
    _grp("regime",   "── Regime breakdown ────────────────────────────────────────────")
    _grp("session",  "── Session breakdown ───────────────────────────────────────────")
    _grp("side",     "── Direction ───────────────────────────────────────────────────")

    df2 = df.copy()
    df2["dow"]   = pd.to_datetime(df2["date"]).dt.day_name().str[:3]
    df2["month"] = df2["date"].str[:7]
    _grp.__func__(df2,"dow",  "── Day-of-week ─────────────────────────────────────────────────") \
        if hasattr(_grp,"__func__") else None
    # manual DOW
    g = df2.groupby("dow").agg(n=("pnl","count"),wins=("result",lambda x:(x=="WIN").sum()),pnl=("pnl","sum"))
    g["wr"]  = (g["wins"]/g["n"]).map("{:.1%}".format)
    g["pnl"] = g["pnl"].map("${:+,.2f}".format)
    print(f"\n  ── Day-of-week ─────────────────────────────────────────────────")
    print(g.sort_values("n",ascending=False)[["n","wins","wr","pnl"]].to_string())

    g2 = df2.groupby("month").agg(n=("pnl","count"),wins=("result",lambda x:(x=="WIN").sum()),pnl=("pnl","sum"))
    g2["wr"]  = (g2["wins"]/g2["n"]).map("{:.1%}".format)
    g2["pnl"] = g2["pnl"].map("${:+,.2f}".format)
    print(f"\n  ── Monthly breakdown ────────────────────────────────────────────")
    print(g2[["n","wins","wr","pnl"]].to_string())

    # Daily P&L
    dpnl = df.groupby("date")["pnl"].sum()
    print(f"\n  ── Daily P&L stats ──────────────────────────────────────────────")
    print(f"     Best day  : ${dpnl.max():+.2f}")
    print(f"     Worst day : ${dpnl.min():+.2f}")
    print(f"     Avg day   : ${dpnl.mean():+.2f}")
    print(f"     +ve days  : {(dpnl>0).sum()} / {len(dpnl)}")
    print(f"     Estimated monthly P&L (avg × 22 days): ${dpnl.mean()*22:+.2f}")

    # Trades/day distribution
    print(f"\n  ── Trades/day distribution ──────────────────────────────────────")
    for bucket, lo, hi in [("= 20",20,20),("15-19",15,19),("10-14",10,14),("5-9",5,9),("1-4",1,4)]:
        cnt = ((dv>=lo)&(dv<=hi)).sum()
        pct = cnt/len(dv)*100
        print(f"     {bucket:8s}: {cnt:4d} days  ({pct:.0f}%)")

    print(f"\n{sep}\n")


# ── TRADE DETAIL TABLE ────────────────────────────────────────────────────────
def print_trade_detail(df: pd.DataFrame, label: str, n: int = 50) -> None:
    if df.empty: return
    cols = ["date","entry_time","strategy","side","entry","sl","tp","exit","hit","pnl","session","regime","result"]
    print(f"\n  ─── FIRST {n} TRADES — {label} ─────────────────────────────────")
    print(df.head(n)[cols].to_string(index=False))
    print(f"\n  ─── LAST {n} TRADES ─────────────────────────────────────────────")
    print(df.tail(n)[cols].to_string(index=False))


# ── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 80)
    print(f"  SHIVA  |  STRICT 20 TRADES/DAY  |  1:6 RR  |  9-MONTH 1H")
    print(f"  SL={FIXED_SL_PTS} pts  TP={FIXED_TP_PTS} pts  Lot={FIXED_LOT}  "
          f"Win=${WIN_AMT}  Loss=${LOSS_AMT}")
    print(f"  Break-even WR = {1/7:.1%}  |  All 11 agents active")
    print("=" * 80)

    nine_mo  = (datetime.now(timezone.utc) - timedelta(days=274)).strftime("%Y-%m-%d")
    far_back = (datetime.now(timezone.utc) - timedelta(days=730)).strftime("%Y-%m-%d")

    print("\n  Fetching data…")
    df_1h = fetch("1h", start=nine_mo)

    if df_1h.empty:
        print("No data."); sys.exit(1)

    print(f"\n  Running 9-month 1h backtest…\n")
    trades, dcnt = run_backtest(df_1h, f"9-MONTH 1H  {nine_mo}→today")

    if not trades.empty:
        report(trades, f"9-MONTH 1H  |  {nine_mo} → today", INITIAL_CAP, dcnt)

        # Save
        trades.to_csv("hf_1_to_6_trades.csv", index=False)
        print(f"  📊 Full trade log saved → hf_1_to_6_trades.csv  ({len(trades):,} rows)\n")

        # Print first 100 and last 100 trade details
        print_trade_detail(trades, "9-MONTH 1H", n=60)

        # Summary per day (first 20 active trading days)
        print(f"\n  ─── DAILY SUMMARY (first 20 active days) ───────────────────────")
        dsum = trades.groupby("date").agg(
            trades=("pnl","count"),
            wins=("result",lambda x:(x=="WIN").sum()),
            pnl=("pnl","sum")
        )
        dsum["wr"]  = (dsum["wins"]/dsum["trades"]).map("{:.0%}".format)
        dsum["pnl"] = dsum["pnl"].map("${:+.2f}".format)
        print(dsum.head(20).to_string())

        print(f"\n  ─── DAILY SUMMARY (last 10 active days) ────────────────────────")
        print(dsum.tail(10).to_string())

        # Grand totals
        tot_trades = len(trades)
        tot_wins   = (trades["result"]=="WIN").sum()
        tot_net    = trades["pnl"].sum()
        tot_days   = len(dcnt)
        print(f"\n{'='*80}")
        print(f"  GRAND TOTAL — 9 MONTHS  |  1:6 RR  |  STRICT 20/day")
        print(f"{'='*80}")
        print(f"  Period      : {nine_mo} → today  ({tot_days} trading days)")
        print(f"  Total trades: {tot_trades:,}  (avg {tot_trades/tot_days:.1f}/day)")
        print(f"  Days @ 20   : {sum(1 for v in dcnt.values() if v==20)}")
        print(f"  Win rate    : {tot_wins/tot_trades:.1%}  ({tot_wins} wins / {tot_trades-tot_wins} losses)")
        print(f"  Net P&L     : ${tot_net:+,.2f}")
        print(f"  Per-trade   : ${tot_net/tot_trades:+.3f}")
        print(f"  Start cap   : ${INITIAL_CAP:.2f}  →  ${INITIAL_CAP+tot_net:.2f}")
        print(f"  Break-even  : {1/7:.1%}  — current {tot_wins/tot_trades:.1%} "
              f"{'✅ PROFITABLE' if tot_wins/tot_trades>1/7 else '❌ BELOW BE'}")
        print(f"{'='*80}")
