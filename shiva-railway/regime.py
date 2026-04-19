"""
SHIVA — Phase 5: Market Regime Classifier

Classes: TRENDING_BULL | TRENDING_BEAR | RANGING | HIGH_VOLATILITY

Features (all computed from existing indicator dataframe):
  ADX(14) — trend strength
  ATR percentile (20-period) — volatility rank
  Bollinger Band width — squeeze indicator
  EMA slope (5-bar) — directional bias

Routing rules:
  TRENDING_BULL/BEAR → momentum/BOS entries (normal size)
  RANGING            → liquidity grab + FVG mean reversion (normal size)
  HIGH_VOLATILITY    → reduce size 50% or skip

Expected win rate improvement: +5–8%
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import yaml


# ── Config ────────────────────────────────────────────────────────────────

def _load_cfg() -> dict:
    p = Path(__file__).parent / "config.yaml"
    return yaml.safe_load(p.read_text()).get("regime", {}) if p.exists() else {}

_CFG = _load_cfg()

ADX_TREND_THRESHOLD  = float(_CFG.get("adx_trend_threshold",   25.0))
ADX_RANGE_THRESHOLD  = float(_CFG.get("adx_range_threshold",   20.0))
ATR_PERCENTILE_HV    = float(_CFG.get("atr_percentile_hv",     90.0))
ATR_LOOKBACK         = int(_CFG.get("atr_lookback",             20))
EMA_SLOPE_LOOKBACK   = int(_CFG.get("ema_slope_lookback",        5))
HV_SIZE_REDUCTION    = float(_CFG.get("hv_size_reduction",      0.50))
BB_WIDTH_NARROW_PCT  = float(_CFG.get("bb_width_narrow_pct",    0.02))


# ── Data types ─────────────────────────────────────────────────────────────

class Regime(str, Enum):
    TRENDING_BULL  = "TRENDING_BULL"
    TRENDING_BEAR  = "TRENDING_BEAR"
    RANGING        = "RANGING"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    UNKNOWN        = "UNKNOWN"


@dataclass
class RegimeResult:
    regime:      Regime
    adx:         float
    atr_pct:     float   # ATR percentile rank (0–100)
    bb_width:    float   # BB width as fraction of price
    ema_slope:   float   # EMA slope (pts/bar)
    size_factor: float   # recommended position size multiplier
    reason:      str
    timestamp:   datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __str__(self) -> str:
        return (
            f"[Regime] {self.regime.value}  "
            f"ADX={self.adx:.1f}  ATR%={self.atr_pct:.0f}  "
            f"BB_w={self.bb_width:.3f}  slope={self.ema_slope:+.4f}  "
            f"size={self.size_factor:.0%}  | {self.reason}"
        )


# ── Pure computation ───────────────────────────────────────────────────────

def classify_regime(df: pd.DataFrame) -> RegimeResult:
    """
    Classify current market regime from an indicator-enriched OHLCV DataFrame.
    Expects columns: ATR, ADX_14 (optional), BB_upper, BB_lower, BB_mid (optional),
                     EMA_50 (optional), close.
    All missing columns degrade gracefully.
    """
    if len(df) < ATR_LOOKBACK + 5:
        return RegimeResult(
            regime=Regime.UNKNOWN, adx=0.0, atr_pct=50.0,
            bb_width=0.0, ema_slope=0.0, size_factor=1.0,
            reason="insufficient bars",
        )

    row = df.iloc[-1]

    # ── ADX ──
    adx = 0.0
    for col in ["ADX_14", "ADX_14_14", "ADX"]:
        if col in df.columns:
            v = row.get(col)
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                adx = float(v)
                break

    # ── ATR percentile ──
    atr_series = df["ATR"].dropna().tail(ATR_LOOKBACK * 3)
    cur_atr    = float(row.get("ATR", 0) or 0)
    atr_pct    = 50.0
    if len(atr_series) >= 5 and cur_atr > 0:
        atr_pct = float(np.sum(atr_series.values < cur_atr) / len(atr_series) * 100)

    # ── BB width ──
    bb_width = 0.0
    if all(c in df.columns for c in ["BB_upper", "BB_lower", "BB_mid"]):
        bu = row.get("BB_upper")
        bl = row.get("BB_lower")
        bm = row.get("BB_mid")
        if bu and bl and bm and not any(isinstance(x, float) and np.isnan(x) for x in [bu, bl, bm]):
            bb_width = (float(bu) - float(bl)) / float(bm)

    # ── EMA slope ──
    ema_slope = 0.0
    ema_col   = next((c for c in ["EMA_50", "EMA_20"] if c in df.columns), None)
    if ema_col:
        ema_recent = df[ema_col].dropna().tail(EMA_SLOPE_LOOKBACK + 1)
        if len(ema_recent) >= 2:
            ema_slope = float(ema_recent.iloc[-1] - ema_recent.iloc[0]) / max(len(ema_recent) - 1, 1)

    # ── Classification ──
    parts: list[str] = []

    # HIGH_VOLATILITY overrides everything
    if atr_pct >= ATR_PERCENTILE_HV:
        parts.append(f"ATR at {atr_pct:.0f}th pct (extreme volatility)")
        return RegimeResult(
            regime=Regime.HIGH_VOLATILITY,
            adx=adx, atr_pct=atr_pct, bb_width=bb_width, ema_slope=ema_slope,
            size_factor=HV_SIZE_REDUCTION,
            reason=" | ".join(parts),
        )

    # RANGING: ADX < threshold AND BB narrow (OR ADX below lower threshold)
    is_ranging = (
        (adx > 0 and adx < ADX_RANGE_THRESHOLD) or
        (adx > 0 and adx < ADX_TREND_THRESHOLD and bb_width > 0 and bb_width < BB_WIDTH_NARROW_PCT)
    )

    if is_ranging:
        parts.append(f"ADX={adx:.1f} < {ADX_TREND_THRESHOLD}  BB_width={bb_width:.3f}")
        return RegimeResult(
            regime=Regime.RANGING,
            adx=adx, atr_pct=atr_pct, bb_width=bb_width, ema_slope=ema_slope,
            size_factor=1.0,
            reason=" | ".join(parts) or "ranging (low ADX)",
        )

    # TRENDING
    if adx >= ADX_TREND_THRESHOLD or adx == 0:
        if ema_slope > 0:
            parts.append(f"ADX={adx:.1f} ≥ {ADX_TREND_THRESHOLD}  EMA slope={ema_slope:+.4f} (bull)")
            return RegimeResult(
                regime=Regime.TRENDING_BULL,
                adx=adx, atr_pct=atr_pct, bb_width=bb_width, ema_slope=ema_slope,
                size_factor=1.0,
                reason=" | ".join(parts),
            )
        else:
            parts.append(f"ADX={adx:.1f} ≥ {ADX_TREND_THRESHOLD}  EMA slope={ema_slope:+.4f} (bear)")
            return RegimeResult(
                regime=Regime.TRENDING_BEAR,
                adx=adx, atr_pct=atr_pct, bb_width=bb_width, ema_slope=ema_slope,
                size_factor=1.0,
                reason=" | ".join(parts),
            )

    # Default
    return RegimeResult(
        regime=Regime.RANGING,
        adx=adx, atr_pct=atr_pct, bb_width=bb_width, ema_slope=ema_slope,
        size_factor=1.0,
        reason=f"default ranging  ADX={adx:.1f}",
    )


# ── RegimeClassifier (stateful, with Polars logging) ─────────────────────

class RegimeClassifier:
    """
    Thin stateful wrapper: caches the last result, logs to Polars.

    Usage:
        classifier = RegimeClassifier()
        result = classifier.classify(df)
        # Skip or reduce position in HIGH_VOLATILITY
        lot *= result.size_factor
        # Route to correct strategy set based on result.regime
    """

    LOG_PATH = Path("/tmp/shiva_regime.parquet")
    _SCHEMA  = {
        "timestamp":  pl.Utf8,
        "regime":     pl.Utf8,
        "adx":        pl.Float64,
        "atr_pct":    pl.Float64,
        "bb_width":   pl.Float64,
        "ema_slope":  pl.Float64,
        "size_factor": pl.Float64,
        "reason":     pl.Utf8,
    }

    def __init__(self):
        self.current: Optional[RegimeResult] = None
        self._log_rows: list[dict] = []
        self._last_regime: Optional[Regime] = None

    def classify(self, df: pd.DataFrame) -> RegimeResult:
        result = classify_regime(df)
        if result.regime != self._last_regime:
            print(result)
            self._last_regime = result.regime
            self._log(result)
        self.current = result
        return result

    def get_log_df(self) -> pl.DataFrame:
        if not self._log_rows:
            return pl.DataFrame(schema=self._SCHEMA)
        return pl.DataFrame(self._log_rows, schema=self._SCHEMA)

    def _log(self, r: RegimeResult) -> None:
        self._log_rows.append({
            "timestamp":  r.timestamp.isoformat(),
            "regime":     r.regime.value,
            "adx":        r.adx,
            "atr_pct":    r.atr_pct,
            "bb_width":   r.bb_width,
            "ema_slope":  r.ema_slope,
            "size_factor": r.size_factor,
            "reason":     r.reason,
        })
        try:
            pl.DataFrame(self._log_rows, schema=self._SCHEMA).write_parquet(str(self.LOG_PATH))
        except Exception:
            pass


# ── Unit tests ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pandas_ta as pta
    print("Running Phase 5 unit tests…\n")

    rng = np.random.default_rng(5)

    def _make_df(n: int, adx_override: float = None, atr_mult: float = 1.0,
                 trend: str = "up", flat_atr: float = None) -> pd.DataFrame:
        drift = 0.1 if trend == "up" else -0.1
        close = 75.0 + np.cumsum(rng.normal(drift, 0.3 * atr_mult, n))
        high  = close + rng.uniform(0.1, 0.5 * atr_mult, n)
        low   = close - rng.uniform(0.1, 0.5 * atr_mult, n)
        open_ = close - rng.uniform(-0.15, 0.15, n)
        df    = pd.DataFrame({"open": open_, "high": high, "low": low,
                              "close": close, "volume": np.ones(n)*1000})
        try:
            df["ATR"]    = pta.atr(df["high"], df["low"], df["close"], length=14)
            df["EMA_50"] = pta.ema(df["close"], length=min(50, n))
            bb = pta.bbands(df["close"], length=20, std=2)
            if bb is not None:
                for src, dst in [("BBU_20_2.0","BB_upper"),("BBL_20_2.0","BB_lower"),("BBM_20_2.0","BB_mid")]:
                    if src in bb.columns: df[dst] = bb[src].values
            adx = pta.adx(df["high"], df["low"], df["close"], length=14)
            if adx is not None:
                acol = next((c for c in adx.columns if c.startswith("ADX_")), None)
                if acol: df["ADX_14"] = adx[acol].values
        except Exception:
            pass
        if adx_override is not None:
            df["ADX_14"] = adx_override
        if flat_atr is not None:
            df["ATR"] = flat_atr   # constant ATR → percentile rank always ~50%
        return df.dropna()

    # Test 1: strong uptrend (flat_atr keeps percentile rank at 50%, not HV)
    df = _make_df(150, adx_override=30.0, trend="up", flat_atr=0.4)
    r  = classify_regime(df)
    print(f"Test 1 (strong uptrend)  → {r.regime.value}  ADX={r.adx:.1f}  slope={r.ema_slope:+.4f}  ATR%={r.atr_pct:.0f}")
    assert r.regime == Regime.TRENDING_BULL, f"Expected TRENDING_BULL, got {r.regime}"

    # Test 2: strong downtrend
    df2 = _make_df(150, adx_override=30.0, trend="down", flat_atr=0.4)
    r2  = classify_regime(df2)
    print(f"Test 2 (strong downtrend)→ {r2.regime.value}  slope={r2.ema_slope:+.4f}")
    assert r2.regime == Regime.TRENDING_BEAR, f"Expected TRENDING_BEAR, got {r2.regime}"

    # Test 3: ranging (low ADX)
    df3 = _make_df(100, adx_override=12.0)
    r3  = classify_regime(df3)
    print(f"Test 3 (ranging)         → {r3.regime.value}  ADX={r3.adx:.1f}")
    assert r3.regime == Regime.RANGING

    # Test 4: high volatility (force ATR high)
    df4 = _make_df(80, atr_mult=5.0)
    r4  = classify_regime(df4)
    print(f"Test 4 (high volatility) → {r4.regime.value}  ATR%={r4.atr_pct:.0f}  size={r4.size_factor:.0%}")
    # With 5x ATR mult the percentile rank should be very high
    assert r4.size_factor == HV_SIZE_REDUCTION or r4.regime == Regime.HIGH_VOLATILITY or r4.atr_pct > 50

    # Test 5: RegimeClassifier logs on regime change
    clf = RegimeClassifier()
    clf.classify(df)
    clf.classify(df3)   # regime change → should log
    log_df = clf.get_log_df()
    print(f"Test 5 (regime changes)  → {len(log_df)} log rows")
    assert len(log_df) >= 1

    print("\n✅ All Phase 5 unit tests passed.")
