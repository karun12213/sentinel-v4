"""
SHIVA — Phase 9: Daily Circuit Breaker

Rules (all config-driven):
  1. Max 2 losses in one trading day  → pause until next session
  2. Max 2% daily drawdown of account → full shutdown + alert
  3. Max 4 trades per day             → hard cap
  4. Rolling 20-trade win rate < 50%  → reduce size 50% + alert

All circuit breaker events logged to Polars DataFrame.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone, date
from enum import Enum
from pathlib import Path
from typing import Optional, Callable

import polars as pl
import yaml


# ── Config ────────────────────────────────────────────────────────────────

def _load_cfg() -> dict:
    p = Path(__file__).parent / "config.yaml"
    return yaml.safe_load(p.read_text()).get("circuit_breaker", {}) if p.exists() else {}

_CFG = _load_cfg()

MAX_DAILY_LOSSES   = int(_CFG.get("max_daily_losses",    2))
MAX_DAILY_DD_PCT   = float(_CFG.get("max_daily_drawdown_pct", 2.0))
MAX_DAILY_TRADES   = int(_CFG.get("max_daily_trades",    4))
WR_REVIEW_WINDOW   = int(_CFG.get("wr_review_window",    20))
WR_ALERT_THRESHOLD = float(_CFG.get("wr_alert_threshold", 0.50))
SIZE_REDUCTION     = float(_CFG.get("size_reduction_on_alert", 0.50))


# ── Data types ─────────────────────────────────────────────────────────────

class BreakerState(str, Enum):
    OK           = "OK"
    DAILY_PAUSED = "DAILY_PAUSED"    # max losses or max trades hit
    SHUTDOWN     = "SHUTDOWN"        # max drawdown hit
    SIZE_REDUCED = "SIZE_REDUCED"    # rolling WR alert


@dataclass
class BreakerResult:
    state:       BreakerState
    allows_trade: bool
    size_factor:  float        # 1.0 = full, 0.5 = halved
    reason:       str
    timestamp:    datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __str__(self) -> str:
        flag = "✅" if self.allows_trade else "⛔"
        return f"[CircuitBreaker] {flag} {self.state.value}  size={self.size_factor:.0%}  | {self.reason}"


# ── CircuitBreakerAgent ───────────────────────────────────────────────────

class CircuitBreakerAgent:
    """
    Tracks daily losses, drawdown, and rolling win-rate.
    Call on_trade_result() after every close; call check() before entry.

    Replaces the ad-hoc circuit breaker in ExecutionEngine.
    """

    LOG_PATH = Path("/tmp/shiva_circuit_breaker.parquet")
    _SCHEMA  = {
        "timestamp":    pl.Utf8,
        "event_type":   pl.Utf8,
        "state":        pl.Utf8,
        "daily_losses": pl.Int32,
        "daily_trades": pl.Int32,
        "daily_dd_pct": pl.Float64,
        "rolling_wr":   pl.Float64,
        "reason":       pl.Utf8,
    }

    def __init__(
        self,
        on_shutdown: Optional[Callable[[str], None]] = None,
        on_alert:    Optional[Callable[[str], None]] = None,
    ):
        self._on_shutdown = on_shutdown or (lambda r: print(f"🚨 SHUTDOWN: {r}"))
        self._on_alert    = on_alert    or (lambda r: print(f"⚠️  ALERT: {r}"))

        # Daily state
        self._trading_date:  date  = datetime.now(timezone.utc).date()
        self._daily_losses:  int   = 0
        self._daily_trades:  int   = 0
        self._day_start_balance: float = 0.0
        self._state: BreakerState  = BreakerState.OK
        self._size_factor: float   = 1.0

        # Rolling trade history (pnl values)
        self._trade_history: list[float] = []
        self._log_rows: list[dict]       = []

    # ── Public API ──

    def set_balance(self, balance: float) -> None:
        """Call once at session start to anchor daily drawdown calculation."""
        today = datetime.now(timezone.utc).date()
        if today != self._trading_date:
            self._reset_day(today, balance)
        elif self._day_start_balance == 0.0:
            self._day_start_balance = balance

    def check(self) -> BreakerResult:
        """Returns BreakerResult — call before every potential entry."""
        today = datetime.now(timezone.utc).date()
        if today != self._trading_date:
            self._reset_day(today, self._day_start_balance)

        if self._state == BreakerState.SHUTDOWN:
            return BreakerResult(
                state=BreakerState.SHUTDOWN,
                allows_trade=False,
                size_factor=0.0,
                reason=f"Daily drawdown shutdown — manual reset required",
            )

        if self._state == BreakerState.DAILY_PAUSED:
            return BreakerResult(
                state=BreakerState.DAILY_PAUSED,
                allows_trade=False,
                size_factor=0.0,
                reason=f"Daily paused: {self._daily_losses} losses / {self._daily_trades} trades",
            )

        if self._daily_trades >= MAX_DAILY_TRADES:
            self._set_state(BreakerState.DAILY_PAUSED, "max daily trades reached")
            return BreakerResult(
                state=BreakerState.DAILY_PAUSED,
                allows_trade=False,
                size_factor=0.0,
                reason=f"Max daily trades ({MAX_DAILY_TRADES}) reached",
            )

        return BreakerResult(
            state=self._state,
            allows_trade=True,
            size_factor=self._size_factor,
            reason=f"OK  losses={self._daily_losses}/{MAX_DAILY_LOSSES}  trades={self._daily_trades}/{MAX_DAILY_TRADES}",
        )

    def on_trade_result(self, pnl: float, account_balance: float) -> BreakerResult:
        """
        Call after every trade close.
        Returns the resulting BreakerResult (may trigger pause/shutdown).
        """
        today = datetime.now(timezone.utc).date()
        if today != self._trading_date:
            self._reset_day(today, account_balance)

        self._daily_trades += 1
        self._trade_history.append(pnl)
        if len(self._trade_history) > WR_REVIEW_WINDOW * 2:
            self._trade_history = self._trade_history[-WR_REVIEW_WINDOW * 2:]

        if pnl < 0:
            self._daily_losses += 1
            self._log_event("loss", f"loss #{self._daily_losses}  pnl={pnl:.2f}")

        # Daily drawdown check
        if self._day_start_balance > 0:
            dd_pct = (self._day_start_balance - account_balance) / self._day_start_balance * 100
            if dd_pct >= MAX_DAILY_DD_PCT:
                reason = f"Daily drawdown {dd_pct:.2f}% >= {MAX_DAILY_DD_PCT}%"
                self._set_state(BreakerState.SHUTDOWN, reason)
                self._on_shutdown(reason)
                return BreakerResult(
                    state=BreakerState.SHUTDOWN,
                    allows_trade=False,
                    size_factor=0.0,
                    reason=reason,
                )

        # Daily loss limit
        if self._daily_losses >= MAX_DAILY_LOSSES:
            reason = f"{self._daily_losses} losses today >= limit {MAX_DAILY_LOSSES}"
            self._set_state(BreakerState.DAILY_PAUSED, reason)
            return BreakerResult(
                state=BreakerState.DAILY_PAUSED,
                allows_trade=False,
                size_factor=0.0,
                reason=reason,
            )

        # Rolling WR check
        rolling_wr = self._rolling_wr()
        if rolling_wr is not None and rolling_wr < WR_ALERT_THRESHOLD:
            if self._size_factor > SIZE_REDUCTION:
                reason = f"Rolling {WR_REVIEW_WINDOW}-trade WR {rolling_wr:.1%} < {WR_ALERT_THRESHOLD:.0%}"
                self._size_factor = SIZE_REDUCTION
                self._set_state(BreakerState.SIZE_REDUCED, reason)
                self._on_alert(reason)
        elif rolling_wr is not None and rolling_wr >= WR_ALERT_THRESHOLD + 0.10:
            if self._state == BreakerState.SIZE_REDUCED:
                self._size_factor = 1.0
                self._set_state(BreakerState.OK, "WR recovered")

        result = BreakerResult(
            state=self._state,
            allows_trade=self._state not in (BreakerState.DAILY_PAUSED, BreakerState.SHUTDOWN),
            size_factor=self._size_factor,
            reason=f"pnl={pnl:+.2f}  daily_losses={self._daily_losses}  rolling_wr={rolling_wr or 'n/a'}",
        )
        print(result)
        return result

    def daily_summary(self) -> dict:
        return {
            "date":         str(self._trading_date),
            "trades":       self._daily_trades,
            "losses":       self._daily_losses,
            "state":        self._state.value,
            "size_factor":  self._size_factor,
            "rolling_wr":   self._rolling_wr(),
        }

    def get_log_df(self) -> pl.DataFrame:
        if not self._log_rows:
            return pl.DataFrame(schema=self._SCHEMA)
        return pl.DataFrame(self._log_rows, schema=self._SCHEMA)

    def reset_shutdown(self) -> None:
        """Manual reset after operator review of SHUTDOWN state."""
        self._set_state(BreakerState.OK, "manual reset")
        print("🔓 Circuit breaker reset by operator")

    # ── Internal ──

    def _rolling_wr(self) -> Optional[float]:
        if len(self._trade_history) < WR_REVIEW_WINDOW:
            return None
        window = self._trade_history[-WR_REVIEW_WINDOW:]
        wins   = sum(1 for p in window if p > 0)
        return wins / WR_REVIEW_WINDOW

    def _reset_day(self, today: date, balance: float) -> None:
        prev_state = self._state
        self._trading_date       = today
        self._daily_losses       = 0
        self._daily_trades       = 0
        self._day_start_balance  = balance
        if self._state != BreakerState.SHUTDOWN:
            self._state = BreakerState.SIZE_REDUCED if self._size_factor < 1.0 else BreakerState.OK
        print(f"📅 Circuit breaker reset for {today}  |  prev_state={prev_state.value}")

    def _set_state(self, state: BreakerState, reason: str) -> None:
        if state != self._state:
            self._state = state
            self._log_event(f"state→{state.value}", reason)

    def _log_event(self, event_type: str, reason: str) -> None:
        self._log_rows.append({
            "timestamp":    datetime.now(timezone.utc).isoformat(),
            "event_type":   event_type,
            "state":        self._state.value,
            "daily_losses": self._daily_losses,
            "daily_trades": self._daily_trades,
            "daily_dd_pct": 0.0,
            "rolling_wr":   self._rolling_wr() or 0.0,
            "reason":       reason,
        })
        try:
            pl.DataFrame(self._log_rows, schema=self._SCHEMA).write_parquet(str(self.LOG_PATH))
        except Exception:
            pass


# ── Unit tests ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Running Phase 9 unit tests…\n")

    # Test 1: clean state allows trade
    cb = CircuitBreakerAgent()
    cb.set_balance(1000.0)
    r = cb.check()
    print(f"Test 1 (clean state)     → allowed={r.allows_trade}  state={r.state.value}")
    assert r.allows_trade and r.state == BreakerState.OK

    # Test 2: 2 losses → daily pause
    cb2 = CircuitBreakerAgent()
    cb2.set_balance(1000.0)
    cb2.on_trade_result(-5.0, 995.0)
    cb2.on_trade_result(-5.0, 990.0)
    r2 = cb2.check()
    print(f"Test 2 (2 losses)        → allowed={r2.allows_trade}  state={r2.state.value}")
    assert not r2.allows_trade and r2.state == BreakerState.DAILY_PAUSED

    # Test 3: max daily trades
    cb3 = CircuitBreakerAgent()
    cb3.set_balance(1000.0)
    for _ in range(MAX_DAILY_TRADES):
        cb3.on_trade_result(5.0, 1020.0)
    r3 = cb3.check()
    print(f"Test 3 (max trades)      → allowed={r3.allows_trade}  state={r3.state.value}")
    assert not r3.allows_trade

    # Test 4: drawdown shutdown
    cb4 = CircuitBreakerAgent()
    cb4.set_balance(1000.0)
    r4 = cb4.on_trade_result(-30.0, 970.0)   # 3% drawdown > 2% limit
    print(f"Test 4 (2% drawdown)     → allowed={r4.allows_trade}  state={r4.state.value}")
    assert not r4.allows_trade and r4.state == BreakerState.SHUTDOWN

    # Test 5: rolling WR alert → size reduced
    # Simulate across multiple trading days to avoid daily loss limit interfering
    from datetime import date as _date
    cb5 = CircuitBreakerAgent()
    cb5.set_balance(1000.0)
    for i in range(WR_REVIEW_WINDOW):
        # Advance the day counter every 2 trades so daily loss limit never fires
        cb5._trading_date = _date(2025, 1, i + 1)
        cb5._daily_losses = 0
        cb5._daily_trades = 0
        pnl = -3.0 if i % 3 != 0 else 3.0   # ~33% WR → below 50%
        cb5.on_trade_result(pnl, 1000.0)
    print(f"Test 5 (low WR)          → size_factor={cb5._size_factor}  state={cb5._state.value}")
    assert cb5._size_factor <= SIZE_REDUCTION

    # Test 6: Polars log
    df = cb2.get_log_df()
    print(f"Test 6 (Polars log)      → {len(df)} rows")
    assert len(df) >= 1

    print("\n✅ All Phase 9 unit tests passed.")
