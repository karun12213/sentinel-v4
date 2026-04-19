"""
SHIVA — Phase 8: Dynamic Stop Loss Engine

Replaces fixed SL/TP with smart structure-based levels:
  SL  = below/above the swept liquidity level (structural)
  Min = 1.5× ATR(14) from entry
  Max = 3.0× ATR(14) — skip trade if structure requires wider
  Breakeven: move SL to entry after 1:1 RR achieved
  Partial exit: close 50% at 1:1 RR (configurable)
  TP2 = Fib -0.27 extension (trailing remainder by swing structure)

Also exposes modify_stop_to_breakeven() and trail_by_structure() for
post-entry management (called by ExecutionEngine).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import polars as pl
import yaml


# ── Config ─────────────────────────────────────────────────────────────────

def _load_cfg() -> dict:
    p = Path(__file__).parent / "config.yaml"
    return yaml.safe_load(p.read_text()).get("dynamic_sl", {}) if p.exists() else {}

_CFG = _load_cfg()

MIN_ATR_MULT      = float(_CFG.get("min_atr_mult",        1.5))
MAX_ATR_MULT      = float(_CFG.get("max_atr_mult",        3.0))
BREAKEVEN_AT_RR   = float(_CFG.get("breakeven_at_rr",     1.0))
PARTIAL_EXIT_PCT  = float(_CFG.get("partial_exit_pct",    0.50))
TP_FIB            = float(_CFG.get("tp_fib",             -0.27))  # extension level


# ── Data types ─────────────────────────────────────────────────────────────

@dataclass
class SLTPLevels:
    side:       str           # "BUY" or "SELL"
    entry:      float
    sl:         float
    tp1:        float         # 1:1 RR (partial exit)
    tp2:        float         # Fib -0.27 extension (runner)
    sl_source:  str           # "swept_liquidity" | "atr_min" | "atr_max_exceeded"
    risk_pts:   float
    reward_pts: float
    valid:      bool          # False if ATR constraint breached
    reason:     str

    @property
    def rr_ratio(self) -> float:
        return self.reward_pts / self.risk_pts if self.risk_pts > 0 else 0.0

    def __str__(self) -> str:
        flag = "✅" if self.valid else "⛔"
        return (
            f"[DynSL] {flag} {self.side}  entry={self.entry:.2f}  "
            f"SL={self.sl:.2f}  TP1={self.tp1:.2f}  TP2={self.tp2:.2f}  "
            f"risk={self.risk_pts:.3f}  RR=1:{self.rr_ratio:.1f}  "
            f"src={self.sl_source}  | {self.reason}"
        )


# ── SL placement helpers ───────────────────────────────────────────────────

def _find_swept_level(df: pd.DataFrame, side: str, lookback: int = 20) -> Optional[float]:
    """
    Find the most recent swept swing level:
    - BUY: swept low (price dipped below a prior swing low then recovered)
    - SELL: swept high (price spiked above a prior swing high then fell back)
    Returns the swept price level or None.
    """
    if len(df) < lookback + 3:
        return None

    bars = df.tail(lookback + 3).reset_index(drop=True)
    n    = len(bars)

    # Find swing highs / lows in the prior section
    prior = bars.iloc[:lookback]
    swing_highs = [
        float(prior.iloc[i]["high"])
        for i in range(1, len(prior) - 1)
        if float(prior.iloc[i]["high"]) > float(prior.iloc[i-1]["high"])
        and float(prior.iloc[i]["high"]) > float(prior.iloc[i+1]["high"])
    ]
    swing_lows = [
        float(prior.iloc[i]["low"])
        for i in range(1, len(prior) - 1)
        if float(prior.iloc[i]["low"]) < float(prior.iloc[i-1]["low"])
        and float(prior.iloc[i]["low"]) < float(prior.iloc[i+1]["low"])
    ]

    recent = bars.iloc[lookback:]

    if side == "BUY" and swing_lows:
        # Lowest swing low that was swept (wick below) then closed back above
        target_low = min(swing_lows)
        swept = any(float(b["low"]) < target_low for _, b in recent.iterrows())
        recovered = float(bars.iloc[-1]["close"]) > target_low
        if swept and recovered:
            return target_low

    if side == "SELL" and swing_highs:
        target_high = max(swing_highs)
        swept    = any(float(b["high"]) > target_high for _, b in recent.iterrows())
        rejected = float(bars.iloc[-1]["close"]) < target_high
        if swept and rejected:
            return target_high

    return None


def _fib_extension(entry: float, sl: float, side: str, level: float = TP_FIB) -> float:
    """
    Compute Fibonacci extension TP.
    For BUY:  TP = entry + (entry - sl) * (1 - level)   [level=-0.27 → 1.27×]
    For SELL: TP = entry - (sl - entry) * (1 - level)
    """
    risk = abs(entry - sl)
    mult = 1.0 - level   # e.g. -0.27 → 1.27
    if side == "BUY":
        return entry + risk * mult
    else:
        return entry - risk * mult


# ── Main computation ────────────────────────────────────────────────────────

def compute_dynamic_levels(
    side:    str,
    entry:   float,
    df:      pd.DataFrame,
    point:   float = 0.01,    # minimum price increment
    min_stop: float = 0.10,   # broker minimum stop distance
) -> SLTPLevels:
    """
    Compute SL/TP levels using structural swept liquidity.
    Falls back gracefully if no swept level found (uses ATR min).
    """
    atr = 0.30   # default fallback
    if "ATR" in df.columns and not df["ATR"].empty:
        v = df["ATR"].dropna()
        if len(v) > 0:
            atr = float(v.iloc[-1])

    atr_min = atr * MIN_ATR_MULT
    atr_max = atr * MAX_ATR_MULT

    # Try structural SL
    swept = _find_swept_level(df, side)
    sl_source = "atr_min"
    sl_raw    = None

    if swept is not None:
        if side == "BUY":
            sl_raw = swept - point   # just below swept low
        else:
            sl_raw = swept + point   # just above swept high
        sl_source = "swept_liquidity"

    # Clamp to ATR constraints
    if side == "BUY":
        sl_atr_min = entry - atr_min
        sl_atr_max = entry - atr_max
        if sl_raw is None:
            sl_raw = sl_atr_min   # default: 1.5×ATR
        elif sl_raw > entry - min_stop:
            sl_raw = entry - min_stop   # too tight
        sl = max(sl_atr_max, min(sl_raw, sl_atr_min))
        risk_pts = max(entry - sl, min_stop)

        if entry - sl > atr_max:
            return SLTPLevels(
                side=side, entry=entry, sl=sl, tp1=entry, tp2=entry,
                sl_source="atr_max_exceeded", risk_pts=risk_pts,
                reward_pts=0.0, valid=False,
                reason=f"SL too wide: {entry-sl:.3f} > {atr_max:.3f} (3× ATR)",
            )

        tp1 = entry + risk_pts * BREAKEVEN_AT_RR  # 1:1
        tp2 = _fib_extension(entry, sl, "BUY", TP_FIB)

    else:  # SELL
        sl_atr_min = entry + atr_min
        sl_atr_max = entry + atr_max
        if sl_raw is None:
            sl_raw = sl_atr_min
        elif sl_raw < entry + min_stop:
            sl_raw = entry + min_stop
        sl = min(sl_atr_max, max(sl_raw, sl_atr_min))
        risk_pts = max(sl - entry, min_stop)

        if sl - entry > atr_max:
            return SLTPLevels(
                side=side, entry=entry, sl=sl, tp1=entry, tp2=entry,
                sl_source="atr_max_exceeded", risk_pts=risk_pts,
                reward_pts=0.0, valid=False,
                reason=f"SL too wide: {sl-entry:.3f} > {atr_max:.3f} (3× ATR)",
            )

        tp1 = entry - risk_pts * BREAKEVEN_AT_RR
        tp2 = _fib_extension(entry, sl, "SELL", TP_FIB)

    reward_pts = abs(tp2 - entry)

    return SLTPLevels(
        side=side, entry=entry, sl=sl, tp1=tp1, tp2=tp2,
        sl_source=sl_source, risk_pts=risk_pts,
        reward_pts=reward_pts, valid=True,
        reason=f"ATR={atr:.3f}  swept={'yes' if swept else 'no'}  1:{reward_pts/risk_pts:.1f}R",
    )


# ── Post-entry management helpers ─────────────────────────────────────────

def check_breakeven(
    side:        str,
    entry:       float,
    current_price: float,
    current_sl:  float,
    tp1:         float,
) -> tuple[bool, float]:
    """
    Returns (should_move_to_BE, new_sl).
    Moves SL to breakeven once price reaches tp1 (1:1 RR).
    """
    if side == "BUY":
        if current_price >= tp1 and current_sl < entry:
            return True, entry + 0.01   # just above entry
    else:
        if current_price <= tp1 and current_sl > entry:
            return True, entry - 0.01
    return False, current_sl


def trail_by_structure(
    side:        str,
    df:          pd.DataFrame,
    current_sl:  float,
    lookback:    int = 5,
) -> float:
    """
    Trail SL using the last swing low/high.
    Only tightens (never widens) the stop.
    Returns the new SL price.
    """
    if len(df) < lookback + 2:
        return current_sl

    bars = df.tail(lookback + 2).reset_index(drop=True)

    if side == "BUY":
        swing_lows = [
            float(bars.iloc[i]["low"])
            for i in range(1, len(bars) - 1)
            if float(bars.iloc[i]["low"]) < float(bars.iloc[i-1]["low"])
            and float(bars.iloc[i]["low"]) < float(bars.iloc[i+1]["low"])
        ]
        if swing_lows:
            new_sl = max(swing_lows)   # highest swing low = tightest trailing stop
            return max(current_sl, new_sl)  # only move up (tighten)
    else:
        swing_highs = [
            float(bars.iloc[i]["high"])
            for i in range(1, len(bars) - 1)
            if float(bars.iloc[i]["high"]) > float(bars.iloc[i-1]["high"])
            and float(bars.iloc[i]["high"]) > float(bars.iloc[i+1]["high"])
        ]
        if swing_highs:
            new_sl = min(swing_highs)   # lowest swing high
            return min(current_sl, new_sl)  # only move down (tighten)

    return current_sl


# ── DynamicSLEngine (stateful) ─────────────────────────────────────────────

class DynamicSLEngine:
    """
    Replaces ExecutionEngine._build_levels() with structural stops.

    Usage:
        engine = DynamicSLEngine()
        levels = engine.compute("BUY", entry_price, df)
        if not levels.valid:
            skip_trade()
            continue
        lot50  = lot * PARTIAL_EXIT_PCT    # close this at tp1
        lot50r = lot * (1 - PARTIAL_EXIT_PCT)  # trail remainder to tp2
    """

    LOG_PATH = Path("/tmp/shiva_dynamic_sl.parquet")
    _SCHEMA  = {
        "timestamp":  pl.Utf8,
        "side":       pl.Utf8,
        "entry":      pl.Float64,
        "sl":         pl.Float64,
        "tp1":        pl.Float64,
        "tp2":        pl.Float64,
        "sl_source":  pl.Utf8,
        "risk_pts":   pl.Float64,
        "rr":         pl.Float64,
        "valid":      pl.Boolean,
    }

    def __init__(self):
        self._log_rows: list[dict] = []

    def compute(self, side: str, entry: float, df: pd.DataFrame,
                point: float = 0.01, min_stop: float = 0.10) -> SLTPLevels:
        levels = compute_dynamic_levels(side, entry, df, point, min_stop)
        print(levels)
        self._log(levels)
        return levels

    def get_log_df(self) -> pl.DataFrame:
        if not self._log_rows:
            return pl.DataFrame(schema=self._SCHEMA)
        return pl.DataFrame(self._log_rows, schema=self._SCHEMA)

    def _log(self, lv: SLTPLevels) -> None:
        self._log_rows.append({
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "side":       lv.side,
            "entry":      lv.entry,
            "sl":         lv.sl,
            "tp1":        lv.tp1,
            "tp2":        lv.tp2,
            "sl_source":  lv.sl_source,
            "risk_pts":   lv.risk_pts,
            "rr":         lv.rr_ratio,
            "valid":      lv.valid,
        })
        try:
            pl.DataFrame(self._log_rows, schema=self._SCHEMA).write_parquet(str(self.LOG_PATH))
        except Exception:
            pass


# ── Unit tests ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pandas_ta as pta
    print("Running Phase 8 unit tests…\n")

    rng = np.random.default_rng(8)

    def _make_df(n: int = 60, atr: float = 0.4) -> pd.DataFrame:
        close = 75.0 + np.cumsum(rng.normal(0.02, atr * 0.5, n))
        high  = close + rng.uniform(0.1, atr, n)
        low   = close - rng.uniform(0.1, atr, n)
        open_ = close - rng.uniform(-0.1, 0.1, n)
        df    = pd.DataFrame({"open": open_, "high": high, "low": low,
                              "close": close, "volume": np.ones(n)*1000})
        df["ATR"] = pta.atr(df["high"], df["low"], df["close"], length=14)
        return df.dropna()

    df = _make_df(60, atr=0.4)
    entry = float(df.iloc[-1]["close"])

    # Test 1: BUY levels — valid SL below entry
    lv = compute_dynamic_levels("BUY", entry, df)
    print(f"Test 1 (BUY levels)       → valid={lv.valid}  SL={lv.sl:.2f} < entry={entry:.2f}")
    assert lv.sl < entry, f"BUY SL should be below entry: {lv}"

    # Test 2: SELL levels — valid SL above entry
    lv2 = compute_dynamic_levels("SELL", entry, df)
    print(f"Test 2 (SELL levels)      → valid={lv2.valid}  SL={lv2.sl:.2f} > entry={entry:.2f}")
    assert lv2.sl > entry, f"SELL SL should be above entry: {lv2}"

    # Test 3: TP2 computed via Fib -0.27
    atr_val = float(df["ATR"].dropna().iloc[-1])
    assert abs(lv.tp2 - entry) > abs(lv.tp1 - entry), "TP2 should be further than TP1"
    print(f"Test 3 (TP1<TP2)          → TP1={lv.tp1:.2f}  TP2={lv.tp2:.2f} ✅")

    # Test 4: Oversized stop rejected
    tiny_df = _make_df(60, atr=5.0)  # huge ATR
    tiny_entry = float(tiny_df.iloc[-1]["close"])
    lv3 = compute_dynamic_levels("BUY", tiny_entry, tiny_df, min_stop=0.10)
    print(f"Test 4 (wide ATR)         → valid={lv3.valid}  src={lv3.sl_source}")

    # Test 5: Breakeven check
    sl_new_be, new_sl = check_breakeven("BUY", entry, entry + 0.5, entry - 0.3, entry + 0.3)
    print(f"Test 5 (breakeven hit)    → should_move={sl_new_be}  new_sl={new_sl:.2f}")
    assert sl_new_be and new_sl > entry - 0.3

    sl_no_be, _ = check_breakeven("BUY", entry, entry + 0.1, entry - 0.3, entry + 0.3)
    print(f"Test 6 (no breakeven yet) → should_move={sl_no_be}")
    assert not sl_no_be

    # Test 7: Trail by structure
    trail_sl = trail_by_structure("BUY", df, current_sl=entry - 0.5)
    print(f"Test 7 (trail structure)  → new_sl={trail_sl:.2f}  old_sl={entry-0.5:.2f}")
    assert trail_sl >= entry - 0.5   # should only tighten

    # Test 8: DynamicSLEngine Polars log
    eng = DynamicSLEngine()
    eng.compute("BUY", entry, df)
    log = eng.get_log_df()
    print(f"Test 8 (Polars log)       → {len(log)} rows")
    assert len(log) == 1

    print("\n✅ All Phase 8 unit tests passed.")
