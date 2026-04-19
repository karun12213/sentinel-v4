"""
SHIVA — Phase 4: Multi-Confluence Scoring Engine

Seven signals scored independently then summed:
  1. HTF Bias aligned               — 25 pts
  2. Kill Zone active               — 15 pts
  3. BOS or CHoCH on M15/H1        — 20 pts
  4. Liquidity sweep before entry   — 20 pts
  5. FVG present + price returning  — 15 pts
  6. CNN ensemble confidence ≥ 70%  — 20 pts  (Phase 6)
  7. RSI divergence or OB           — 10 pts

Total: 125 pts possible
  ≥ 90 → FULL entry (1% risk)
  70–89 → HALF entry (0.5% risk)
  < 70 → NO TRADE

All decisions logged to Polars DataFrame.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional, TYPE_CHECKING

import numpy as np
import pandas as pd
import polars as pl
import yaml

if TYPE_CHECKING:
    from htf_bias import BiasResult
    from kill_zone import KillZoneResult


# ── Config ────────────────────────────────────────────────────────────────

def _load_cfg() -> dict:
    p = Path(__file__).parent / "config.yaml"
    return yaml.safe_load(p.read_text()).get("confluence", {}) if p.exists() else {}

_CFG = _load_cfg()
_SC  = _CFG.get("scores", {})

FULL_THRESHOLD = int(_CFG.get("full_entry_threshold",  90))
HALF_THRESHOLD = int(_CFG.get("half_entry_threshold",  70))

S_HTF_BIAS    = int(_SC.get("htf_bias_aligned",  25))
S_KILL_ZONE   = int(_SC.get("kill_zone_active",  15))
S_BOS_CHOCH   = int(_SC.get("bos_or_choch",      20))
S_LIQ_SWEEP   = int(_SC.get("liquidity_sweep",   20))
S_FVG         = int(_SC.get("fvg_present",       15))
S_CNN         = int(_SC.get("cnn_confidence",    20))
S_RSI_OB      = int(_SC.get("rsi_div_or_ob",     10))
MAX_SCORE     = S_HTF_BIAS + S_KILL_ZONE + S_BOS_CHOCH + S_LIQ_SWEEP + S_FVG + S_CNN + S_RSI_OB


# ── Data types ─────────────────────────────────────────────────────────────

class EntrySize(str, Enum):
    FULL   = "FULL"    # 1% risk
    HALF   = "HALF"    # 0.5% risk
    NO_TRADE = "NO_TRADE"


@dataclass
class MarketState:
    """Snapshot passed to ConfluenceScorer on each bar."""
    signal:          int          # +1 BUY / -1 SELL / 0 HOLD
    df:              pd.DataFrame # indicator-enriched OHLCV (pandas, from existing FeatureEngine)

    # Phase 1
    htf_bias_score:  int    = 0   # raw score from HTFBiasAgent (-100 to +100)
    htf_bias_label:  str    = ""  # "BULLISH" / "BEARISH" / "NEUTRAL"

    # Phase 2
    kill_zone_active: bool  = False
    session_name:     str   = ""

    # Phase 6 (optional — 0 if CNN not running)
    cnn_confidence:   float = 0.0
    cnn_direction:    int   = 0   # +1 / -1 / 0

    # Phase 5 (optional)
    regime:           str   = ""  # TRENDING_BULL / TRENDING_BEAR / RANGING / HIGH_VOLATILITY


@dataclass
class ConfluenceResult:
    score:       int
    entry_size:  EntrySize
    breakdown:   dict[str, int]
    reasons:     list[str]
    timestamp:   datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def allows_entry(self) -> bool:
        return self.entry_size != EntrySize.NO_TRADE

    @property
    def risk_multiplier(self) -> float:
        return {"FULL": 1.0, "HALF": 0.5, "NO_TRADE": 0.0}[self.entry_size.value]

    def __str__(self) -> str:
        parts = ", ".join(f"{k}:{v}" for k, v in self.breakdown.items() if v > 0)
        return (
            f"[Confluence] {self.entry_size.value}  score={self.score}/{MAX_SCORE}  "
            f"[{parts}]"
        )


# ── Signal detectors (pure, testable) ─────────────────────────────────────

def detect_bos_choch(df: pd.DataFrame, signal: int) -> tuple[bool, str]:
    """
    Break of Structure (BOS): latest close breaks the most recent swing high/low.
    Change of Character (CHoCH): first BOS after a series of opposite structure.
    Uses last 20 bars.
    """
    if len(df) < 20:
        return False, "insufficient bars"

    recent = df.tail(20)
    closes = recent["close"].values
    highs  = recent["high"].values
    lows   = recent["low"].values

    # Swing high/low: bar whose high/low exceeds both neighbours
    swing_highs = [
        highs[i] for i in range(1, len(highs) - 1)
        if highs[i] > highs[i-1] and highs[i] > highs[i+1]
    ]
    swing_lows = [
        lows[i] for i in range(1, len(lows) - 1)
        if lows[i] < lows[i-1] and lows[i] < lows[i+1]
    ]

    last_close = float(closes[-1])

    if signal == 1 and swing_highs:
        prev_swing_high = max(swing_highs[:-1]) if len(swing_highs) > 1 else swing_highs[0]
        if last_close > prev_swing_high:
            return True, f"BOS: close {last_close:.2f} > swing high {prev_swing_high:.2f}"

    if signal == -1 and swing_lows:
        prev_swing_low = min(swing_lows[:-1]) if len(swing_lows) > 1 else swing_lows[0]
        if last_close < prev_swing_low:
            return True, f"BOS: close {last_close:.2f} < swing low {prev_swing_low:.2f}"

    return False, "no BOS/CHoCH"


def detect_liquidity_sweep(df: pd.DataFrame, signal: int, lookback: int = 15) -> tuple[bool, str]:
    """
    Detects if price swept a swing high/low in the recent lookback window
    before reversing in the signal direction.
    Same logic as LiquiditySweepFVGStrategy._swept_liquidity but standalone.
    """
    if len(df) < lookback + 5:
        return False, "insufficient bars"

    prior  = df.iloc[-(lookback+5): -5]
    recent = df.tail(5)

    if prior.empty or recent.empty:
        return False, "insufficient bars"

    swing_high = float(prior["high"].max())
    swing_low  = float(prior["low"].min())

    swept_high = any(float(r["high"]) > swing_high for _, r in recent.iterrows())
    swept_low  = any(float(r["low"])  < swing_low  for _, r in recent.iterrows())

    if signal == 1 and swept_low:
        return True, f"Liq sweep below {swing_low:.2f}"
    if signal == -1 and swept_high:
        return True, f"Liq sweep above {swing_high:.2f}"

    return False, "no liquidity sweep"


def detect_fvg(df: pd.DataFrame, signal: int, max_age: int = 8) -> tuple[bool, str]:
    """
    Checks if there is a fresh unmitigated FVG in the signal direction
    that current price is returning into.
    """
    if len(df) < 10:
        return False, "insufficient bars"

    bars = df.tail(max_age + 3).reset_index(drop=True)
    n    = len(bars)
    last = bars.iloc[-1]

    for i in range(1, n - 1):
        prev_high = float(bars.iloc[i-1]["high"])
        prev_low  = float(bars.iloc[i-1]["low"])
        next_high = float(bars.iloc[i+1]["high"])
        next_low  = float(bars.iloc[i+1]["low"])
        age       = n - 1 - i

        if signal == 1 and prev_high < next_low:
            fvg_low, fvg_high = prev_high, next_low
            if float(last["low"]) <= fvg_high and float(last["close"]) >= fvg_low:
                return True, f"Bull FVG [{fvg_low:.2f}–{fvg_high:.2f}] age={age}"

        if signal == -1 and prev_low > next_high:
            fvg_low, fvg_high = next_high, prev_low
            if float(last["high"]) >= fvg_low and float(last["close"]) <= fvg_high:
                return True, f"Bear FVG [{fvg_low:.2f}–{fvg_high:.2f}] age={age}"

    return False, "no FVG"


def detect_rsi_divergence_or_ob(df: pd.DataFrame, signal: int) -> tuple[bool, str]:
    """
    RSI divergence: price makes new high/low but RSI does not → divergence.
    OR: order block present (simplified: last opposing candle before impulse).
    """
    if len(df) < 14:
        return False, "insufficient bars"

    rsi_col = next((c for c in df.columns if c.upper().startswith("RSI")), None)
    if rsi_col is None:
        return False, "no RSI column"

    recent = df.tail(10)
    closes = recent["close"].values
    rsis   = recent[rsi_col].values

    valid_mask = ~np.isnan(rsis)
    if valid_mask.sum() < 4:
        return False, "RSI NaN"

    closes_v = closes[valid_mask]
    rsis_v   = rsis[valid_mask]

    # Bullish RSI divergence: lower low in price, higher low in RSI
    if signal == 1:
        price_ll = closes_v[-1] < closes_v[0]
        rsi_hl   = rsis_v[-1] > rsis_v[0]
        if price_ll and rsi_hl:
            return True, f"Bullish RSI divergence  RSI {rsis_v[0]:.0f}→{rsis_v[-1]:.0f}"

    # Bearish RSI divergence: higher high in price, lower high in RSI
    if signal == -1:
        price_hh = closes_v[-1] > closes_v[0]
        rsi_lh   = rsis_v[-1] < rsis_v[0]
        if price_hh and rsi_lh:
            return True, f"Bearish RSI divergence  RSI {rsis_v[0]:.0f}→{rsis_v[-1]:.0f}"

    # Order Block: last opposite-direction candle before strong move
    bars = df.tail(15).reset_index(drop=True)
    n    = len(bars)
    atr  = float(df["ATR"].iloc[-1]) if "ATR" in df.columns else 0.3

    for i in range(2, n - 2):
        bar = bars.iloc[i]
        is_bearish = float(bar["close"]) < float(bar["open"])
        is_bullish = float(bar["close"]) > float(bar["open"])
        future     = bars.iloc[i+1: min(i+4, n)]

        if signal == 1 and is_bearish and not future.empty:
            impulse = float(future["high"].max()) - float(bar["high"])
            if impulse >= atr * 0.8:
                return True, f"Bullish OB at {bar['low']:.2f}–{bar['high']:.2f}"

        if signal == -1 and is_bullish and not future.empty:
            impulse = float(bar["low"]) - float(future["low"].min())
            if impulse >= atr * 0.8:
                return True, f"Bearish OB at {bar['low']:.2f}–{bar['high']:.2f}"

    return False, "no RSI div / OB"


# ── ConfluenceScorer ───────────────────────────────────────────────────────

class ConfluenceScorer:
    """
    Stateless scorer — call score(state) on each potential entry.

    Usage:
        scorer = ConfluenceScorer()
        result = scorer.score(state)
        if result.allows_entry:
            lot = base_lot * result.risk_multiplier
    """

    LOG_PATH = Path("/tmp/shiva_confluence.parquet")
    _SCHEMA  = {
        "timestamp":   pl.Utf8,
        "signal":      pl.Int8,
        "score":       pl.Int32,
        "entry_size":  pl.Utf8,
        "htf":         pl.Int32,
        "kill_zone":   pl.Int32,
        "bos_choch":   pl.Int32,
        "liq_sweep":   pl.Int32,
        "fvg":         pl.Int32,
        "cnn":         pl.Int32,
        "rsi_ob":      pl.Int32,
        "reasons":     pl.Utf8,
    }

    def __init__(self):
        self._log_rows: list[dict] = []

    def score(self, state: MarketState) -> ConfluenceResult:
        sig = state.signal
        breakdown: dict[str, int] = {}
        reasons:   list[str]      = []

        # ── 1. HTF Bias ──
        htf_pts = 0
        if state.htf_bias_label == "BULLISH" and sig == 1:
            htf_pts = S_HTF_BIAS
            reasons.append(f"HTF BULLISH ({state.htf_bias_score:+d})")
        elif state.htf_bias_label == "BEARISH" and sig == -1:
            htf_pts = S_HTF_BIAS
            reasons.append(f"HTF BEARISH ({state.htf_bias_score:+d})")
        elif state.htf_bias_label == "NEUTRAL":
            htf_pts = S_HTF_BIAS // 2   # partial credit for neutral
            reasons.append("HTF NEUTRAL (partial)")
        breakdown["htf_bias"] = htf_pts

        # ── 2. Kill Zone ──
        kz_pts = S_KILL_ZONE if state.kill_zone_active else 0
        if kz_pts:
            reasons.append(f"Kill zone: {state.session_name}")
        breakdown["kill_zone"] = kz_pts

        # ── 3. BOS / CHoCH ──
        bos_ok, bos_reason = detect_bos_choch(state.df, sig)
        bos_pts = S_BOS_CHOCH if bos_ok else 0
        if bos_ok:
            reasons.append(bos_reason)
        breakdown["bos_choch"] = bos_pts

        # ── 4. Liquidity Sweep ──
        ls_ok, ls_reason = detect_liquidity_sweep(state.df, sig)
        ls_pts = S_LIQ_SWEEP if ls_ok else 0
        if ls_ok:
            reasons.append(ls_reason)
        breakdown["liq_sweep"] = ls_pts

        # ── 5. FVG ──
        fvg_ok, fvg_reason = detect_fvg(state.df, sig)
        fvg_pts = S_FVG if fvg_ok else 0
        if fvg_ok:
            reasons.append(fvg_reason)
        breakdown["fvg"] = fvg_pts

        # ── 6. CNN Ensemble ──
        cnn_pts = 0
        if state.cnn_confidence >= 0.70 and state.cnn_direction == sig:
            cnn_pts = S_CNN
            reasons.append(f"CNN {state.cnn_confidence:.0%} aligned")
        elif state.cnn_confidence >= 0.65 and state.cnn_direction == sig:
            cnn_pts = S_CNN // 2
            reasons.append(f"CNN {state.cnn_confidence:.0%} partial")
        breakdown["cnn"] = cnn_pts

        # ── 7. RSI Divergence / OB ──
        rsi_ok, rsi_reason = detect_rsi_divergence_or_ob(state.df, sig)
        rsi_pts = S_RSI_OB if rsi_ok else 0
        if rsi_ok:
            reasons.append(rsi_reason)
        breakdown["rsi_ob"] = rsi_pts

        total = sum(breakdown.values())

        if total >= FULL_THRESHOLD:
            entry_size = EntrySize.FULL
        elif total >= HALF_THRESHOLD:
            entry_size = EntrySize.HALF
        else:
            entry_size = EntrySize.NO_TRADE
            reasons.append(f"Score {total} < {HALF_THRESHOLD} threshold — no trade")

        result = ConfluenceResult(
            score=total,
            entry_size=entry_size,
            breakdown=breakdown,
            reasons=reasons,
        )

        if entry_size != EntrySize.NO_TRADE:
            print(result)

        self._log(state.signal, result)
        return result

    def get_log_df(self) -> pl.DataFrame:
        if not self._log_rows:
            return pl.DataFrame(schema=self._SCHEMA)
        return pl.DataFrame(self._log_rows, schema=self._SCHEMA)

    def _log(self, signal: int, result: ConfluenceResult) -> None:
        bd = result.breakdown
        self._log_rows.append({
            "timestamp":  result.timestamp.isoformat(),
            "signal":     signal,
            "score":      result.score,
            "entry_size": result.entry_size.value,
            "htf":        bd.get("htf_bias", 0),
            "kill_zone":  bd.get("kill_zone", 0),
            "bos_choch":  bd.get("bos_choch", 0),
            "liq_sweep":  bd.get("liq_sweep", 0),
            "fvg":        bd.get("fvg", 0),
            "cnn":        bd.get("cnn", 0),
            "rsi_ob":     bd.get("rsi_ob", 0),
            "reasons":    " | ".join(result.reasons),
        })
        if len(self._log_rows) % 50 == 0:
            try:
                pl.DataFrame(self._log_rows, schema=self._SCHEMA).write_parquet(str(self.LOG_PATH))
            except Exception:
                pass


# ── Unit tests ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pandas_ta as pta
    print("Running Phase 4 unit tests…\n")

    rng = np.random.default_rng(99)

    def _make_df(n: int = 100, trend: str = "up") -> pd.DataFrame:
        base  = 75.0
        drift = 0.08 if trend == "up" else -0.08
        close = base + np.cumsum(rng.normal(drift, 0.3, n))
        high  = close + rng.uniform(0.1, 0.5, n)
        low   = close - rng.uniform(0.1, 0.5, n)
        open_ = close - rng.uniform(-0.2, 0.2, n)
        df    = pd.DataFrame({"open": open_, "high": high, "low": low,
                              "close": close, "volume": np.ones(n) * 1000})
        df["RSI"] = pta.rsi(df["close"], length=14)
        df["ATR"] = pta.atr(df["high"], df["low"], df["close"], length=14)
        df["EMA_200"] = pta.ema(df["close"], length=min(200, n))
        return df.dropna()

    scorer = ConfluenceScorer()

    # Test 1: HTF aligned + kill zone + FVG → should score well
    df_up = _make_df(120, "up")
    state = MarketState(
        signal=1,
        df=df_up,
        htf_bias_score=75,
        htf_bias_label="BULLISH",
        kill_zone_active=True,
        session_name="NY_OPEN",
        cnn_confidence=0.72,
        cnn_direction=1,
    )
    r = scorer.score(state)
    print(f"Test 1 (aligned BUY)  → score={r.score}  size={r.entry_size.value}")
    assert r.score >= 40, f"Expected score >= 40, got {r.score}"

    # Test 2: wrong direction CNN → CNN pts = 0
    state2 = MarketState(
        signal=1,
        df=df_up,
        htf_bias_label="BULLISH",
        htf_bias_score=70,
        kill_zone_active=False,
        cnn_confidence=0.80,
        cnn_direction=-1,  # CNN says SELL but signal is BUY
    )
    r2 = scorer.score(state2)
    assert r2.breakdown["cnn"] == 0, "CNN should not add points if direction mismatched"
    print(f"Test 2 (CNN mismatch)  → cnn_pts={r2.breakdown['cnn']}  score={r2.score}")

    # Test 3: NO TRADE path (low score)
    state3 = MarketState(
        signal=1,
        df=_make_df(30, "up"),  # short df, most detectors will return False
        htf_bias_label="NEUTRAL",
        htf_bias_score=10,
        kill_zone_active=False,
        cnn_confidence=0.0,
        cnn_direction=0,
    )
    r3 = scorer.score(state3)
    print(f"Test 3 (no trade)      → score={r3.score}  size={r3.entry_size.value}")
    assert r3.entry_size == EntrySize.NO_TRADE

    # Test 4: Polars log
    df_log = scorer.get_log_df()
    print(f"Test 4 (Polars log)    → {len(df_log)} rows")
    assert len(df_log) == 3

    # Test 5: breakdown dict keys present
    assert all(k in r.breakdown for k in ["htf_bias","kill_zone","bos_choch","liq_sweep","fvg","cnn","rsi_ob"])
    print("Test 5 (breakdown keys)→ ✅")

    print("\n✅ All Phase 4 unit tests passed.")
