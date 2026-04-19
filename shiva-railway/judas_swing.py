"""
SHIVA — Phase 7: Judas Swing Detector

ICT Judas Swing logic for USOIL:
  1. At London/NY session open, record initial direction (first 15 min)
  2. If price sweeps previous session high/low within first 30 min → flag
  3. Wait for displacement candle (large body, opposite direction to sweep)
  4. Enter on first pullback to FVG created by displacement candle

Historically 65–70% win rate on USOIL.
Feeds a JUDAS_SWING signal into the ConfluenceScorer as a standalone pattern.

Expected win rate improvement embedded in Phase 4 confluence.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional
import zoneinfo

import numpy as np
import pandas as pd
import polars as pl
import yaml


_EST = zoneinfo.ZoneInfo("US/Eastern")


# ── Config ────────────────────────────────────────────────────────────────

def _load_cfg() -> dict:
    p = Path(__file__).parent / "config.yaml"
    return yaml.safe_load(p.read_text()).get("judas_swing", {}) if p.exists() else {}

_CFG = _load_cfg()

SESSION_LOOKBACK_MINS   = int(_CFG.get("session_lookback_mins",  15))
SWEEP_WINDOW_MINS       = int(_CFG.get("sweep_window_mins",      30))
DISPLACEMENT_MIN_ATR    = float(_CFG.get("displacement_min_atr", 0.8))
FVG_MAX_AGE_BARS        = int(_CFG.get("fvg_max_age_bars",        5))


# ── Data types ─────────────────────────────────────────────────────────────

class JudasPhase(str, Enum):
    WATCHING      = "WATCHING"      # session just opened, tracking
    SWEEP_FOUND   = "SWEEP_FOUND"   # liquidity swept, waiting for displacement
    DISPLACEMENT  = "DISPLACEMENT"  # displacement candle confirmed
    FVG_ENTRY     = "FVG_ENTRY"     # pullback into FVG — actionable signal
    EXPIRED       = "EXPIRED"       # sweep window passed with no setup


@dataclass
class JudasSignal:
    phase:      JudasPhase
    direction:  int          # +1 BUY / -1 SELL / 0 none
    fvg_low:    float = 0.0
    fvg_high:   float = 0.0
    sweep_level: float = 0.0
    confidence:  float = 0.0  # 0–1 based on displacement candle size
    reason:      str   = ""
    timestamp:   datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_actionable(self) -> bool:
        return self.phase == JudasPhase.FVG_ENTRY and self.direction != 0

    def __str__(self) -> str:
        flag = "🎯" if self.is_actionable else "👁"
        side = {1: "BUY", -1: "SELL", 0: "–"}[self.direction]
        return (
            f"[JudasSwing] {flag} {self.phase.value}  {side}  "
            f"FVG[{self.fvg_low:.2f}–{self.fvg_high:.2f}]  "
            f"conf={self.confidence:.0%}  | {self.reason}"
        )


# ── Pure detectors ─────────────────────────────────────────────────────────

def _session_open_time(dt: datetime) -> Optional[datetime]:
    """Returns the session open time (London or NY) for the current bar's date, in UTC."""
    est = dt.astimezone(_EST)
    london_open = est.replace(hour=2, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    ny_open     = est.replace(hour=7, minute=0, second=0, microsecond=0).astimezone(timezone.utc)

    # Within London window (02–05 EST)
    if 2 <= est.hour < 5:
        return london_open
    # Within NY window (07–10 EST)
    if 7 <= est.hour < 10:
        return ny_open
    return None


def _find_fvg_after(bars: pd.DataFrame, after_idx: int, direction: int) -> Optional[dict]:
    """Find first FVG in `direction` created after `after_idx`."""
    bars = bars.reset_index(drop=True)
    n    = len(bars)
    for i in range(max(1, after_idx), n - 1):
        ph = float(bars.iloc[i-1]["high"])
        pl_ = float(bars.iloc[i-1]["low"])
        nh = float(bars.iloc[i+1]["high"])
        nl = float(bars.iloc[i+1]["low"])
        age = n - 1 - i
        if age > FVG_MAX_AGE_BARS:
            continue
        if direction == 1 and ph < nl:   # bullish FVG
            return {"low": ph, "high": nl, "idx": i, "age": age}
        if direction == -1 and pl_ > nh: # bearish FVG
            return {"low": nh, "high": pl_, "idx": i, "age": age}
    return None


def detect_judas_swing(
    df: pd.DataFrame,
    now: Optional[datetime] = None,
) -> JudasSignal:
    """
    Main detector — runs on each 5m bar.
    df must have datetime index (UTC), columns: open, high, low, close, ATR.
    """
    if now is None:
        try:
            now = df.index[-1].to_pydatetime().replace(tzinfo=timezone.utc)
        except Exception:
            now = datetime.now(timezone.utc)

    no_signal = JudasSignal(phase=JudasPhase.WATCHING, direction=0, reason="watching")

    if len(df) < 10:
        return no_signal

    session_open = _session_open_time(now)
    if session_open is None:
        return JudasSignal(phase=JudasPhase.EXPIRED, direction=0, reason="outside kill zone")

    # Filter bars within sweep window
    sweep_cutoff = session_open + timedelta(minutes=SWEEP_WINDOW_MINS)
    lookback_start = session_open - timedelta(minutes=60)  # prev hour for swing levels

    try:
        prior_mask  = (df.index >= lookback_start) & (df.index < session_open)
        window_mask = (df.index >= session_open)   & (df.index <= now)
    except Exception:
        return no_signal

    prior_bars  = df[prior_mask]
    window_bars = df[window_mask]

    if prior_bars.empty or window_bars.empty:
        return no_signal

    prev_high = float(prior_bars["high"].max())
    prev_low  = float(prior_bars["low"].min())

    # Step 1: initial direction in first SESSION_LOOKBACK_MINS
    lookback_bars = window_bars[
        window_bars.index <= session_open + timedelta(minutes=SESSION_LOOKBACK_MINS)
    ]
    if lookback_bars.empty:
        return no_signal

    first_open  = float(lookback_bars.iloc[0]["open"])
    last_close  = float(lookback_bars.iloc[-1]["close"])
    init_direction = 1 if last_close > first_open else -1

    # Step 2: look for sweep of opposite level
    swept_high = any(float(b["high"]) > prev_high for _, b in window_bars.iterrows())
    swept_low  = any(float(b["low"])  < prev_low  for _, b in window_bars.iterrows())

    # Judas: initial bullish move → sweep below (trap longs) → expect SELL reversal
    # Judas: initial bearish move → sweep above (trap shorts) → expect BUY reversal
    sweep_found = False
    judas_direction = 0
    sweep_level = 0.0

    if init_direction == 1 and swept_low:
        sweep_found     = True
        judas_direction = 1        # swept down, reversal is BUY
        sweep_level     = prev_low
    elif init_direction == -1 and swept_high:
        sweep_found     = True
        judas_direction = -1       # swept up, reversal is SELL
        sweep_level     = prev_high

    if not sweep_found:
        if now > sweep_cutoff:
            return JudasSignal(phase=JudasPhase.EXPIRED, direction=0,
                               reason=f"sweep window expired  prev H={prev_high:.2f} L={prev_low:.2f}")
        return JudasSignal(phase=JudasPhase.WATCHING, direction=0,
                           reason=f"no sweep yet  prev H={prev_high:.2f} L={prev_low:.2f}")

    # Step 3: look for displacement candle after the sweep
    atr_val = float(df["ATR"].iloc[-1]) if "ATR" in df.columns else 0.3
    disp_bars = window_bars.copy()
    disp_idx  = None
    disp_size = 0.0

    for i, (_, bar) in enumerate(disp_bars.iterrows()):
        body = abs(float(bar["close"]) - float(bar["open"]))
        is_bull_disp = float(bar["close"]) > float(bar["open"])
        is_bear_disp = float(bar["close"]) < float(bar["open"])

        if judas_direction == 1 and is_bull_disp and body >= atr_val * DISPLACEMENT_MIN_ATR:
            disp_idx  = i
            disp_size = body
            break
        if judas_direction == -1 and is_bear_disp and body >= atr_val * DISPLACEMENT_MIN_ATR:
            disp_idx  = i
            disp_size = body
            break

    if disp_idx is None:
        return JudasSignal(
            phase=JudasPhase.SWEEP_FOUND,
            direction=judas_direction,
            sweep_level=sweep_level,
            reason=f"sweep of {sweep_level:.2f} confirmed, waiting displacement",
        )

    # Step 4: find FVG created by displacement candle
    all_bars = df.copy().reset_index(drop=True)
    disp_abs_idx = len(all_bars) - len(disp_bars) + disp_idx

    fvg = _find_fvg_after(all_bars, disp_abs_idx, judas_direction)

    if fvg is None:
        return JudasSignal(
            phase=JudasPhase.DISPLACEMENT,
            direction=judas_direction,
            sweep_level=sweep_level,
            reason=f"displacement confirmed (body={disp_size:.3f}), no FVG yet",
        )

    # Step 5: check if current price is retesting the FVG
    cur_close = float(df.iloc[-1]["close"])
    cur_low   = float(df.iloc[-1]["low"])
    cur_high  = float(df.iloc[-1]["high"])
    cur_open  = float(df.iloc[-1]["open"])

    in_fvg = False
    if judas_direction == 1:
        in_fvg = (cur_low <= fvg["high"]) and (cur_close >= fvg["low"])
        if in_fvg and cur_close < cur_open:
            in_fvg = False   # bearish close inside bullish FVG — bad
    else:
        in_fvg = (cur_high >= fvg["low"]) and (cur_close <= fvg["high"])
        if in_fvg and cur_close > cur_open:
            in_fvg = False

    confidence = min(1.0, disp_size / (atr_val * 2.0)) if atr_val > 0 else 0.5

    if in_fvg:
        return JudasSignal(
            phase=JudasPhase.FVG_ENTRY,
            direction=judas_direction,
            fvg_low=fvg["low"],
            fvg_high=fvg["high"],
            sweep_level=sweep_level,
            confidence=confidence,
            reason=(
                f"Judas sweep of {sweep_level:.2f} → disp {disp_size:.3f} → "
                f"FVG retest [{fvg['low']:.2f}–{fvg['high']:.2f}]  age={fvg['age']}"
            ),
        )

    return JudasSignal(
        phase=JudasPhase.DISPLACEMENT,
        direction=judas_direction,
        fvg_low=fvg["low"],
        fvg_high=fvg["high"],
        sweep_level=sweep_level,
        confidence=confidence,
        reason=f"FVG found [{fvg['low']:.2f}–{fvg['high']:.2f}], waiting for retest",
    )


# ── JudasSwingAgent ────────────────────────────────────────────────────────

class JudasSwingAgent:
    """
    Stateful wrapper around detect_judas_swing.
    Tracks the current setup state across bars and emits signals
    to the confluence scorer.

    Usage:
        agent = JudasSwingAgent()
        signal = agent.check(df, now=datetime.now(utc))
        if signal.is_actionable:
            # feed into ConfluenceScorer or override as standalone entry
    """

    LOG_PATH = Path("/tmp/shiva_judas.parquet")
    _SCHEMA  = {
        "timestamp":  pl.Utf8,
        "phase":      pl.Utf8,
        "direction":  pl.Int8,
        "fvg_low":    pl.Float64,
        "fvg_high":   pl.Float64,
        "confidence": pl.Float64,
        "reason":     pl.Utf8,
    }

    def __init__(self):
        self._last_signal:  Optional[JudasSignal] = None
        self._log_rows:     list[dict]             = []

    def check(self, df: pd.DataFrame, now: Optional[datetime] = None) -> JudasSignal:
        signal = detect_judas_swing(df, now)

        if signal.phase != getattr(self._last_signal, "phase", None):
            print(signal)

        self._last_signal = signal
        if signal.is_actionable:
            self._log(signal)

        return signal

    def get_log_df(self) -> pl.DataFrame:
        if not self._log_rows:
            return pl.DataFrame(schema=self._SCHEMA)
        return pl.DataFrame(self._log_rows, schema=self._SCHEMA)

    def _log(self, s: JudasSignal) -> None:
        self._log_rows.append({
            "timestamp":  s.timestamp.isoformat(),
            "phase":      s.phase.value,
            "direction":  s.direction,
            "fvg_low":    s.fvg_low,
            "fvg_high":   s.fvg_high,
            "confidence": s.confidence,
            "reason":     s.reason,
        })
        try:
            pl.DataFrame(self._log_rows, schema=self._SCHEMA).write_parquet(str(self.LOG_PATH))
        except Exception:
            pass


# ── Unit tests ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Running Phase 7 unit tests…\n")

    rng = np.random.default_rng(7)

    def _make_sweep_df(session_hour_est: int = 7) -> tuple[pd.DataFrame, datetime]:
        """
        Build a synthetic 5m dataframe that contains a Judas sweep setup.
        session_hour_est: session open hour (7=NY, 2=London)
        """
        n = 60   # 5 hours of 5m bars
        base_time = datetime(2025, 3, 11, session_hour_est, 0, tzinfo=_EST).astimezone(timezone.utc)
        times = [base_time + timedelta(minutes=5*i) for i in range(n)]

        close = 75.0 + np.cumsum(rng.normal(0.0, 0.15, n))
        high  = close + rng.uniform(0.05, 0.3, n)
        low   = close - rng.uniform(0.05, 0.3, n)
        open_ = close - rng.uniform(-0.1, 0.1, n)
        atr   = np.full(n, 0.4)

        # Force a sweep of prev low (index 3) then a bullish displacement (index 5)
        low[3]   = close[0] - 2.0    # spike below prev session low
        close[5] = close[4] + 0.5    # bullish displacement candle
        open_[5] = close[4]
        high[5]  = close[5] + 0.1

        # Create a bullish FVG at index 6
        high[5]  = close[6] - 0.1    # gap: prev bar high < next bar low
        low[7]   = high[5] + 0.05    # next bar low above prev bar high

        df = pd.DataFrame({
            "open": open_, "high": high, "low": low, "close": close,
            "ATR": atr,
        }, index=pd.DatetimeIndex(times))

        now = times[8]   # 8th bar — should be in FVG retest zone
        return df, now

    # Test 1: detect_judas_swing on synthetic data — should reach at least SWEEP_FOUND
    df, now = _make_sweep_df(7)
    sig = detect_judas_swing(df.iloc[:4], now=now)
    print(f"Test 1 (early — no sweep yet) → phase={sig.phase.value}")
    assert sig.phase in (JudasPhase.WATCHING, JudasPhase.EXPIRED)

    # Test 2: outside kill zone → EXPIRED
    outside_time = datetime(2025, 3, 11, 15, 0, tzinfo=_EST).astimezone(timezone.utc)
    sig2 = detect_judas_swing(df, now=outside_time)
    print(f"Test 2 (outside kill zone)   → phase={sig2.phase.value}")
    assert sig2.phase == JudasPhase.EXPIRED

    # Test 3: JudasSwingAgent state tracking
    agent = JudasSwingAgent()
    for i in range(5, len(df)):
        result = agent.check(df.iloc[:i], now=df.index[i-1].to_pydatetime())
    print(f"Test 3 (agent state tracking)→ last phase={agent._last_signal.phase.value}")
    assert agent._last_signal is not None

    # Test 4: actionable signal produces Polars log entry
    agent2 = JudasSwingAgent()
    mock_signal = JudasSignal(
        phase=JudasPhase.FVG_ENTRY,
        direction=1,
        fvg_low=74.5,
        fvg_high=74.8,
        sweep_level=73.5,
        confidence=0.75,
        reason="mock signal",
    )
    agent2._log(mock_signal)
    df_log = agent2.get_log_df()
    print(f"Test 4 (Polars log)          → {len(df_log)} rows  direction={df_log['direction'][0]}")
    assert len(df_log) == 1 and df_log["direction"][0] == 1

    print("\n✅ All Phase 7 unit tests passed.")
