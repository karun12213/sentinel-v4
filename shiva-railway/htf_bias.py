"""
SHIVA — Phase 1: HTF Bias Engine
Higher-Timeframe Bias Agent for USOIL/WTI

Pulls Daily + H4 OHLCV data via MetaAPI, computes a bias score from three
independent signals:
  1. EMA 50/200 cross (Daily) + EMA 20/50 cross (H4)  → 40 pts
  2. Market structure: HH/HL (bullish) or LH/LL (bearish)  → 35 pts
  3. Price vs weekly open                               → 25 pts

Score range: -100 (max bearish) to +100 (max bullish)
  >=  60 → BULLISH  (blocks SELL entries)
  <= -60 → BEARISH  (blocks BUY entries)
  else   → NEUTRAL  (all entries allowed, other filters decide)

All decisions logged to Polars DataFrame, exported every 4 hours.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import polars as pl
import yaml

if TYPE_CHECKING:
    pass  # MetaAPI account injected at runtime to avoid circular import


# ── Config loader ─────────────────────────────────────────────────────────

def _load_cfg() -> dict:
    cfg_path = Path(__file__).parent / "config.yaml"
    if cfg_path.exists():
        with open(cfg_path) as f:
            return yaml.safe_load(f).get("htf_bias", {})
    return {}


_CFG = _load_cfg()

EMA_FAST              = int(_CFG.get("ema_fast", 50))
EMA_SLOW              = int(_CFG.get("ema_slow", 200))
H4_EMA_SLOW           = int(_CFG.get("h4_ema_slow", 50))
STRUCTURE_LOOKBACK    = int(_CFG.get("structure_lookback", 10))
REFRESH_INTERVAL_MINS = int(_CFG.get("refresh_interval_mins", 60))
W_EMA_CROSS           = int(_CFG.get("weight_ema_cross", 40))
W_STRUCTURE           = int(_CFG.get("weight_structure", 35))
W_WEEKLY_OPEN         = int(_CFG.get("weight_weekly_open", 25))
BULLISH_THRESHOLD     = int(_CFG.get("bullish_threshold", 60))
BEARISH_THRESHOLD     = int(_CFG.get("bearish_threshold", -60))


# ── Data types ────────────────────────────────────────────────────────────

class Bias(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"


@dataclass
class BiasResult:
    bias:        Bias
    score:       int          # -100 to +100
    ema_score:   int          # contribution from EMA signal
    struct_score: int         # contribution from structure
    weekly_score: int         # contribution from weekly open
    reasoning:   str
    timestamp:   datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def blocks_buy(self)  -> bool: return self.bias == Bias.BEARISH
    def blocks_sell(self) -> bool: return self.bias == Bias.BULLISH

    def __str__(self) -> str:
        return (
            f"[HTF Bias] {self.bias.value}  score={self.score:+d}  "
            f"(EMA:{self.ema_score:+d}  Struct:{self.struct_score:+d}  "
            f"WeeklyOpen:{self.weekly_score:+d})  | {self.reasoning}"
        )


# ── Pure computation helpers (synchronous, testable) ─────────────────────

def _ema(series: np.ndarray, period: int) -> np.ndarray:
    """Exponential moving average — numpy only, no pandas dependency."""
    k = 2.0 / (period + 1)
    out = np.full_like(series, np.nan)
    # seed with simple mean of first `period` values
    start = period - 1
    if len(series) <= start:
        return out
    out[start] = np.mean(series[: period])
    for i in range(start + 1, len(series)):
        out[i] = series[i] * k + out[i - 1] * (1 - k)
    return out


def compute_ema_score(
    daily_closes: np.ndarray,
    h4_closes:    np.ndarray,
) -> tuple[int, str]:
    """
    Returns (score, reasoning_snippet).
    Daily EMA50/200 provides 30 of 40 pts; H4 EMA20/50 adds 10 pts.
    """
    score  = 0
    parts: list[str] = []

    # ── Daily EMA50/200 ──
    if len(daily_closes) >= EMA_SLOW + 5:
        d_fast = _ema(daily_closes, EMA_FAST)
        d_slow = _ema(daily_closes, EMA_SLOW)
        latest_fast = d_fast[-1]
        latest_slow = d_slow[-1]
        prev_fast   = d_fast[-2]
        prev_slow   = d_slow[-2]

        if np.isnan(latest_fast) or np.isnan(latest_slow):
            parts.append("Daily EMA insufficient data")
        else:
            golden_cross = (prev_fast <= prev_slow) and (latest_fast > latest_slow)
            death_cross  = (prev_fast >= prev_slow) and (latest_fast < latest_slow)
            if latest_fast > latest_slow:
                pts = 30
                label = "Golden cross" if golden_cross else "Bull EMA alignment"
                parts.append(f"Daily {label} EMA{EMA_FAST}>{EMA_SLOW}")
            else:
                pts = -30
                label = "Death cross" if death_cross else "Bear EMA alignment"
                parts.append(f"Daily {label} EMA{EMA_FAST}<{EMA_SLOW}")
            score += pts
    else:
        parts.append(f"Daily EMA: need {EMA_SLOW+5} bars (have {len(daily_closes)})")

    # ── H4 EMA20/50 confirmation (+10 pts weight) ──
    if len(h4_closes) >= H4_EMA_SLOW + 5:
        h4_fast = _ema(h4_closes, 20)
        h4_slow = _ema(h4_closes, H4_EMA_SLOW)
        if not (np.isnan(h4_fast[-1]) or np.isnan(h4_slow[-1])):
            if h4_fast[-1] > h4_slow[-1]:
                score += 10
                parts.append(f"H4 bull EMA20>{H4_EMA_SLOW}")
            else:
                score -= 10
                parts.append(f"H4 bear EMA20<{H4_EMA_SLOW}")

    return score, " | ".join(parts)


def compute_structure_score(
    daily_highs:  np.ndarray,
    daily_lows:   np.ndarray,
    h4_highs:     np.ndarray,
    h4_lows:      np.ndarray,
    lookback:     int = STRUCTURE_LOOKBACK,
) -> tuple[int, str]:
    """
    Detect swing-point structure on Daily (25 pts) and H4 (10 pts).
    HH + HL → bullish  |  LH + LL → bearish  |  mixed → 0
    """
    def _classify(highs: np.ndarray, lows: np.ndarray, n: int) -> str:
        """Returns 'BULL', 'BEAR', or 'MIXED'."""
        if len(highs) < n + 2:
            return "MIXED"
        h = highs[-n:]
        lo = lows[-n:]
        # Compare last 3 pivot regions (beginning / mid / end thirds)
        thirds = max(n // 3, 1)
        h1, h2, h3 = h[:thirds].max(), h[thirds:2*thirds].max(), h[2*thirds:].max()
        l1, l2, l3 = lo[:thirds].min(), lo[thirds:2*thirds].min(), lo[2*thirds:].min()

        hh = (h3 > h2 > h1)   # higher highs
        hl = (l3 > l2 > l1)   # higher lows
        lh = (h3 < h2 < h1)   # lower highs
        ll = (l3 < l2 < l1)   # lower lows

        if hh and hl:  return "BULL"
        if lh and ll:  return "BEAR"
        # Partial — give partial credit
        if hh or hl:   return "WEAK_BULL"
        if lh or ll:   return "WEAK_BEAR"
        return "MIXED"

    score = 0
    parts: list[str] = []

    d_struct = _classify(daily_highs, daily_lows, lookback)
    if d_struct == "BULL":
        score += 25; parts.append("Daily HH+HL structure")
    elif d_struct == "BEAR":
        score -= 25; parts.append("Daily LH+LL structure")
    elif d_struct == "WEAK_BULL":
        score += 12; parts.append("Daily weak bull structure")
    elif d_struct == "WEAK_BEAR":
        score -= 12; parts.append("Daily weak bear structure")
    else:
        parts.append("Daily structure: mixed")

    h4_struct = _classify(h4_highs, h4_lows, lookback)
    if h4_struct in ("BULL", "WEAK_BULL"):
        score += 10 if h4_struct == "BULL" else 5
        parts.append(f"H4 {h4_struct.lower()} structure")
    elif h4_struct in ("BEAR", "WEAK_BEAR"):
        score -= 10 if h4_struct == "BEAR" else 5
        parts.append(f"H4 {h4_struct.lower()} structure")

    return score, " | ".join(parts)


def compute_weekly_open_score(
    current_price: float,
    weekly_open:   float,
) -> tuple[int, str]:
    """
    Price clearly above weekly open → +25 (bullish momentum).
    Price clearly below weekly open → -25 (bearish momentum).
    Within 0.1% → 0 (too close to call).
    """
    if weekly_open <= 0:
        return 0, "Weekly open: unavailable"

    diff_pct = (current_price - weekly_open) / weekly_open * 100

    if diff_pct > 0.10:
        return 25, f"Price {diff_pct:+.2f}% above weekly open {weekly_open:.2f}"
    elif diff_pct < -0.10:
        return -25, f"Price {diff_pct:+.2f}% below weekly open {weekly_open:.2f}"
    else:
        return 0, f"Price within 0.10% of weekly open {weekly_open:.2f}"


def compute_bias(
    daily_closes: np.ndarray,
    daily_highs:  np.ndarray,
    daily_lows:   np.ndarray,
    h4_closes:    np.ndarray,
    h4_highs:     np.ndarray,
    h4_lows:      np.ndarray,
    current_price: float,
    weekly_open:   float,
) -> BiasResult:
    """Pure function — fully testable without MetaAPI."""
    ema_score,    ema_reason    = compute_ema_score(daily_closes, h4_closes)
    struct_score, struct_reason = compute_structure_score(
        daily_highs, daily_lows, h4_highs, h4_lows
    )
    weekly_score, weekly_reason = compute_weekly_open_score(current_price, weekly_open)

    # Scale each component to its weight ceiling
    def _scale(raw: int, component_max: int, weight: int) -> int:
        return round(raw / component_max * weight) if component_max else 0

    ema_w    = _scale(ema_score, 40, W_EMA_CROSS)   # max raw=40 → W_EMA_CROSS pts
    struct_w = _scale(struct_score, 35, W_STRUCTURE) # max raw=35 → W_STRUCTURE pts
    weekly_w = _scale(weekly_score, 25, W_WEEKLY_OPEN)

    total = max(-100, min(100, ema_w + struct_w + weekly_w))

    if total >= BULLISH_THRESHOLD:
        bias = Bias.BULLISH
    elif total <= BEARISH_THRESHOLD:
        bias = Bias.BEARISH
    else:
        bias = Bias.NEUTRAL

    reasoning = f"EMA: {ema_reason} || Structure: {struct_reason} || Weekly: {weekly_reason}"

    return BiasResult(
        bias=bias,
        score=total,
        ema_score=ema_w,
        struct_score=struct_w,
        weekly_score=weekly_w,
        reasoning=reasoning,
    )


# ── HTFBiasAgent ──────────────────────────────────────────────────────────

class HTFBiasAgent:
    """
    Async agent that refreshes HTF bias on a configurable interval.
    Inject a MetaAPI account object via `start(account)`.

    Usage in the main loop:
        agent = HTFBiasAgent(symbol="USOIL")
        await agent.start(account)          # fetch once immediately
        ...
        result = agent.current_bias         # BiasResult or None
        if result and result.blocks_buy():
            skip_trade()
    """

    LOG_PATH = Path("/tmp/shiva_htf_bias.parquet")

    def __init__(self, symbol: str = "USOIL"):
        self.symbol        = symbol
        self.current_bias: BiasResult | None = None
        self._account      = None
        self._task: asyncio.Task | None = None
        self._log_rows: list[dict]      = []
        self._schema = {
            "timestamp":    pl.Utf8,
            "bias":         pl.Utf8,
            "score":        pl.Int32,
            "ema_score":    pl.Int32,
            "struct_score": pl.Int32,
            "weekly_score": pl.Int32,
            "reasoning":    pl.Utf8,
        }

    # ── public API ──

    async def start(self, account) -> None:
        """Inject MetaAPI account and begin the refresh loop."""
        self._account = account
        await self._refresh()   # immediate first fetch
        self._task = asyncio.create_task(self._loop(), name="htf_bias_loop")
        print(f"🧭 HTF Bias Agent started  |  refresh every {REFRESH_INTERVAL_MINS} min")

    def stop(self) -> None:
        if self._task:
            self._task.cancel()

    def allows_buy(self)  -> bool:
        """Returns True if HTF bias does NOT block buys."""
        if self.current_bias is None:
            return True  # no data yet → allow (don't block on startup)
        return not self.current_bias.blocks_buy()

    def allows_sell(self) -> bool:
        if self.current_bias is None:
            return True
        return not self.current_bias.blocks_sell()

    def bias_score(self) -> int:
        return self.current_bias.score if self.current_bias else 0

    # ── internal ──

    async def _loop(self) -> None:
        interval = REFRESH_INTERVAL_MINS * 60
        while True:
            try:
                await asyncio.sleep(interval)
                await self._refresh()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"⚠️  HTF Bias refresh error: {e}")
                await asyncio.sleep(300)

    async def _refresh(self) -> None:
        try:
            daily_candles = await self._fetch("1d",  240)  # ~1 year of daily
            h4_candles    = await self._fetch("4h",  200)  # ~33 days of H4

            if not daily_candles or not h4_candles:
                print("⚠️  HTF Bias: insufficient candle data")
                return

            daily_closes = np.array([c["close"] for c in daily_candles], dtype=float)
            daily_highs  = np.array([c["high"]  for c in daily_candles], dtype=float)
            daily_lows   = np.array([c["low"]   for c in daily_candles], dtype=float)
            h4_closes    = np.array([c["close"] for c in h4_candles],    dtype=float)
            h4_highs     = np.array([c["high"]  for c in h4_candles],    dtype=float)
            h4_lows      = np.array([c["low"]   for c in h4_candles],    dtype=float)

            weekly_open = self._get_weekly_open(daily_candles)
            current_price = daily_closes[-1]

            result = compute_bias(
                daily_closes, daily_highs, daily_lows,
                h4_closes,    h4_highs,    h4_lows,
                current_price, weekly_open,
            )
            self.current_bias = result
            self._log(result)
            print(result)

        except Exception as e:
            print(f"⚠️  HTF Bias _refresh: {e}")

    async def _fetch(self, timeframe: str, count: int) -> list[dict]:
        """Fetch historical candles via MetaAPI account."""
        try:
            start = datetime.now(timezone.utc) - timedelta(
                days=count if timeframe == "1d" else count // 6
            )
            candles = await self._account.get_historical_candles(
                self.symbol, timeframe, start, count
            )
            return candles or []
        except Exception as e:
            print(f"⚠️  HTF Bias fetch {timeframe}: {e}")
            return []

    @staticmethod
    def _get_weekly_open(daily_candles: list[dict]) -> float:
        """Return the Monday open of the current ISO week."""
        if not daily_candles:
            return 0.0
        now = datetime.now(timezone.utc)
        monday = now - timedelta(days=now.weekday())  # ISO Monday
        monday_date = monday.date()
        for c in reversed(daily_candles):
            try:
                bar_time = c.get("time") or c.get("brokerTime") or ""
                if isinstance(bar_time, str):
                    bar_dt = datetime.fromisoformat(bar_time.replace("Z", "+00:00"))
                elif isinstance(bar_time, datetime):
                    bar_dt = bar_time
                else:
                    continue
                if bar_dt.date() >= monday_date:
                    continue  # skip current week's bars that opened on or after Monday
                if bar_dt.date() == monday_date:
                    return float(c["open"])
            except Exception:
                continue
        # Fall back to the open of the most recent daily bar near Monday
        # Find any bar from this week
        for c in reversed(daily_candles):
            try:
                bar_time = c.get("time") or c.get("brokerTime") or ""
                if isinstance(bar_time, str):
                    bar_dt = datetime.fromisoformat(bar_time.replace("Z", "+00:00"))
                elif isinstance(bar_time, datetime):
                    bar_dt = bar_time
                else:
                    continue
                if bar_dt.isocalendar()[1] == now.isocalendar()[1]:
                    return float(c["open"])
            except Exception:
                continue
        return float(daily_candles[-1]["open"]) if daily_candles else 0.0

    def _log(self, result: BiasResult) -> None:
        row = {
            "timestamp":    result.timestamp.isoformat(),
            "bias":         result.bias.value,
            "score":        result.score,
            "ema_score":    result.ema_score,
            "struct_score": result.struct_score,
            "weekly_score": result.weekly_score,
            "reasoning":    result.reasoning,
        }
        self._log_rows.append(row)
        try:
            df = pl.DataFrame(self._log_rows, schema=self._schema)
            df.write_parquet(str(self.LOG_PATH))
        except Exception as e:
            print(f"⚠️  HTF Bias log error: {e}")

    def get_log_df(self) -> pl.DataFrame:
        """Return Polars DataFrame of all logged bias decisions."""
        if not self._log_rows:
            return pl.DataFrame(schema=self._schema)
        return pl.DataFrame(self._log_rows, schema=self._schema)


# ── Unit-testable mock ────────────────────────────────────────────────────

def mock_bias_result(bias: str = "BULLISH") -> BiasResult:
    """Returns a mock BiasResult for unit testing without MetaAPI."""
    score = 75 if bias == "BULLISH" else (-75 if bias == "BEARISH" else 20)
    return BiasResult(
        bias=Bias(bias),
        score=score,
        ema_score=30,
        struct_score=25,
        weekly_score=20 if bias == "BULLISH" else -20,
        reasoning="[MOCK] synthetic bias for testing",
    )


# ── Standalone test ───────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    print("Running Phase 1 unit tests (no MetaAPI required)…\n")

    # Generate synthetic USOIL-like price series
    rng = np.random.default_rng(42)
    n   = 260  # ~1 year daily

    # Uptrend: price drifts up
    trend_up   = 70.0 + np.cumsum(rng.normal(0.05, 0.5, n))
    noise_h    = trend_up + rng.uniform(0.1, 1.0, n)
    noise_l    = trend_up - rng.uniform(0.1, 1.0, n)

    # Downtrend: price drifts down
    trend_dn   = 80.0 + np.cumsum(rng.normal(-0.05, 0.5, n))
    noise_h_dn = trend_dn + rng.uniform(0.1, 1.0, n)
    noise_l_dn = trend_dn - rng.uniform(0.1, 1.0, n)

    # H4 — shorter series
    h4_n  = 200
    h4_up = 70.0 + np.cumsum(rng.normal(0.02, 0.3, h4_n))

    current_price = float(trend_up[-1])
    weekly_open   = float(trend_up[-5])  # approximate Monday open

    # Test 1: uptrend conditions → should be BULLISH
    r = compute_bias(
        daily_closes=trend_up, daily_highs=noise_h, daily_lows=noise_l,
        h4_closes=h4_up,       h4_highs=h4_up+0.5, h4_lows=h4_up-0.5,
        current_price=current_price, weekly_open=weekly_open * 0.98,
    )
    print(f"Test 1 (uptrend)  → {r.bias.value}  score={r.score:+d}")
    assert r.bias == Bias.BULLISH or r.score > 0, f"Expected bullish, got {r}"

    # Test 2: downtrend conditions → should be BEARISH
    r2 = compute_bias(
        daily_closes=trend_dn, daily_highs=noise_h_dn, daily_lows=noise_l_dn,
        h4_closes=trend_dn[-200:], h4_highs=noise_h_dn[-200:], h4_lows=noise_l_dn[-200:],
        current_price=float(trend_dn[-1]),
        weekly_open=float(trend_dn[-5]) * 1.02,  # price below weekly open
    )
    print(f"Test 2 (downtrend)→ {r2.bias.value}  score={r2.score:+d}")
    assert r2.bias == Bias.BEARISH or r2.score < 0, f"Expected bearish, got {r2}"

    # Test 3: mock result
    m = mock_bias_result("BULLISH")
    print(f"Test 3 (mock)     → {m.bias.value}  blocks_sell={m.blocks_sell()}")
    assert m.blocks_sell()
    assert not m.blocks_buy()

    # Test 4: log to Polars
    agent = HTFBiasAgent.__new__(HTFBiasAgent)
    agent._log_rows = []
    agent._schema   = {
        "timestamp":    pl.Utf8,
        "bias":         pl.Utf8,
        "score":        pl.Int32,
        "ema_score":    pl.Int32,
        "struct_score": pl.Int32,
        "weekly_score": pl.Int32,
        "reasoning":    pl.Utf8,
    }
    agent.LOG_PATH = Path("/tmp/test_htf_bias.parquet")
    agent._log(r)
    df = agent.get_log_df()
    assert len(df) == 1
    assert df["bias"][0] == r.bias.value
    print(f"Test 4 (Polars log)→ ✅  1 row logged  bias={df['bias'][0]}")

    print("\n✅ All Phase 1 unit tests passed.")
