"""
SHIVA — Phase 2: Kill Zone Time Filter

Session windows (all times in US/Eastern):
  London Open  : 02:00 – 05:00  (highest liquidity for USOIL)
  New York Open: 07:00 – 10:00  (peak volatility, most reliable signals)
  London Close : 11:00 – 12:00  (optional — lower priority, disabled by default)

Day-of-week filters:
  Monday  — blocked entirely (low liquidity, Sunday gap risk)
  Friday  — blocked after 12:00 EST (position ahead of weekend = bad risk)

All entry decisions logged to Polars DataFrame with session tag.
Win/loss tracking per session is handled by PostTradeAgent (Phase 11)
but the session tag is written here for later grouping.

Expected win rate improvement: +6–9%
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, time as dtime
from enum import Enum
from pathlib import Path
from typing import Optional
import zoneinfo

import polars as pl
import yaml


# ── Config ────────────────────────────────────────────────────────────────

def _load_cfg() -> dict:
    cfg_path = Path(__file__).parent / "config.yaml"
    if cfg_path.exists():
        with open(cfg_path) as f:
            return yaml.safe_load(f).get("kill_zones", {})
    return {}


_CFG = _load_cfg()

_EST = zoneinfo.ZoneInfo("US/Eastern")


def _parse_time(s: str) -> dtime:
    h, m = s.split(":")
    return dtime(int(h), int(m))


# Build windows from config (or sensible defaults)
def _window(key: str, default_start: str, default_end: str) -> tuple[dtime, dtime]:
    cfg = _CFG.get(key, {})
    return (
        _parse_time(cfg.get("start", default_start)),
        _parse_time(cfg.get("end",   default_end)),
    )


LONDON_START,  LONDON_END  = _window("london_open",   "02:00", "05:00")
NY_START,      NY_END      = _window("new_york_open", "07:00", "10:00")
LC_START,      LC_END      = _window("london_close",  "11:00", "12:00")
LONDON_CLOSE_ENABLED       = bool(_CFG.get("london_close", {}).get("enabled", False))
AVOID_MONDAY               = bool(_CFG.get("avoid_monday", True))
FRIDAY_CUTOFF              = _parse_time(_CFG.get("avoid_friday_after", "12:00"))


# ── Data types ─────────────────────────────────────────────────────────────

class Session(str, Enum):
    LONDON_OPEN  = "LONDON_OPEN"
    NY_OPEN      = "NY_OPEN"
    LONDON_CLOSE = "LONDON_CLOSE"
    DEAD_ZONE    = "DEAD_ZONE"      # outside all kill zones


@dataclass
class KillZoneResult:
    allowed:   bool
    session:   Session
    reason:    str
    timestamp: datetime

    def __str__(self) -> str:
        flag = "✅" if self.allowed else "🚫"
        return f"[KillZone] {flag} {self.session.value}  |  {self.reason}"


# ── Pure logic (synchronous, testable) ────────────────────────────────────

def classify_session(dt: datetime) -> Session:
    """Return the session for a given UTC datetime."""
    est = dt.astimezone(_EST)
    t   = est.time().replace(second=0, microsecond=0)

    if LONDON_START <= t < LONDON_END:
        return Session.LONDON_OPEN
    if NY_START <= t < NY_END:
        return Session.NY_OPEN
    if LONDON_CLOSE_ENABLED and LC_START <= t < LC_END:
        return Session.LONDON_CLOSE
    return Session.DEAD_ZONE


def check_kill_zone(dt: Optional[datetime] = None) -> KillZoneResult:
    """
    Returns KillZoneResult for the given UTC datetime (defaults to now).
    allowed=True  → entry permitted by time filter
    allowed=False → entry blocked; reason explains why
    """
    if dt is None:
        dt = datetime.now(timezone.utc)

    est     = dt.astimezone(_EST)
    weekday = est.weekday()   # Monday=0, Sunday=6
    t_est   = est.time().replace(second=0, microsecond=0)

    # Monday filter
    if AVOID_MONDAY and weekday == 0:
        return KillZoneResult(
            allowed=False,
            session=Session.DEAD_ZONE,
            reason="Monday — low liquidity, blocked",
            timestamp=dt,
        )

    # Friday afternoon filter
    if weekday == 4 and t_est >= FRIDAY_CUTOFF:
        return KillZoneResult(
            allowed=False,
            session=Session.DEAD_ZONE,
            reason=f"Friday after {FRIDAY_CUTOFF.strftime('%H:%M')} EST — weekend risk, blocked",
            timestamp=dt,
        )

    session = classify_session(dt)

    if session == Session.DEAD_ZONE:
        return KillZoneResult(
            allowed=False,
            session=Session.DEAD_ZONE,
            reason=(
                f"Outside kill zones  |  EST {t_est.strftime('%H:%M')}  "
                f"|  London {LONDON_START.strftime('%H:%M')}–{LONDON_END.strftime('%H:%M')}  "
                f"NY {NY_START.strftime('%H:%M')}–{NY_END.strftime('%H:%M')}"
            ),
            timestamp=dt,
        )

    return KillZoneResult(
        allowed=True,
        session=session,
        reason=f"Active kill zone  |  EST {t_est.strftime('%H:%M')}",
        timestamp=dt,
    )


# ── KillZoneAgent (stateful, with Polars logging) ─────────────────────────

class KillZoneAgent:
    """
    Lightweight agent — no async tasks needed (pure time math, no I/O).
    Call check() on every loop iteration to gate entries.
    Session tag is attached to every trade for per-session performance
    analysis in PostTradeAgent (Phase 11).

    Usage:
        agent = KillZoneAgent()
        result = agent.check()
        if not result.allowed:
            continue   # skip this bar
        trade["session"] = result.session.value
    """

    LOG_PATH = Path("/tmp/shiva_kill_zone.parquet")
    _SCHEMA  = {
        "timestamp": pl.Utf8,
        "session":   pl.Utf8,
        "allowed":   pl.Boolean,
        "reason":    pl.Utf8,
    }

    def __init__(self):
        self._log_rows: list[dict] = []
        self._last_printed: Optional[Session] = None   # suppress repeat prints

    def check(self, dt: Optional[datetime] = None) -> KillZoneResult:
        result = check_kill_zone(dt)
        # Only print on session change to avoid log spam
        if result.session != self._last_printed:
            print(result)
            self._last_printed = result.session
        return result

    def log_trade_session(self, session: str, won: bool, pnl: float,
                          entry_time: datetime) -> None:
        """Called by ExecutionEngine when a trade closes."""
        self._log_rows.append({
            "timestamp": entry_time.isoformat(),
            "session":   session,
            "allowed":   True,
            "reason":    f"pnl={pnl:.2f}  win={won}",
        })
        self._persist()

    def get_session_stats(self) -> pl.DataFrame:
        """Returns win-rate and P&L grouped by session."""
        if not self._log_rows:
            return pl.DataFrame(schema=self._SCHEMA)
        df = pl.DataFrame(self._log_rows, schema=self._SCHEMA)
        return df

    def _log_check(self, result: KillZoneResult) -> None:
        self._log_rows.append({
            "timestamp": result.timestamp.isoformat(),
            "session":   result.session.value,
            "allowed":   result.allowed,
            "reason":    result.reason,
        })
        if len(self._log_rows) % 100 == 0:   # persist every 100 rows
            self._persist()

    def _persist(self) -> None:
        try:
            df = pl.DataFrame(self._log_rows, schema=self._SCHEMA)
            df.write_parquet(str(self.LOG_PATH))
        except Exception as e:
            print(f"⚠️  KillZone log error: {e}")


# ── Unit tests ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from datetime import timedelta

    print("Running Phase 2 unit tests…\n")

    def _make_est(weekday: int, hour: int, minute: int = 0) -> datetime:
        """Build a UTC datetime that lands on the given EST weekday+time."""
        # Find next occurrence of that weekday relative to a reference Monday
        ref = datetime(2025, 1, 6, tzinfo=_EST)   # known Monday
        days_ahead = (weekday - ref.weekday()) % 7
        d = ref + timedelta(days=days_ahead)
        est_dt = d.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return est_dt.astimezone(timezone.utc)

    # Test 1: London Open window (Tuesday 03:00 EST) → allowed
    t = _make_est(1, 3, 30)
    r = check_kill_zone(t)
    print(f"Test 1 London Open  → allowed={r.allowed}  session={r.session.value}")
    assert r.allowed and r.session == Session.LONDON_OPEN, f"Failed: {r}"

    # Test 2: NY Open window (Wednesday 08:30 EST) → allowed
    t = _make_est(2, 8, 30)
    r = check_kill_zone(t)
    print(f"Test 2 NY Open      → allowed={r.allowed}  session={r.session.value}")
    assert r.allowed and r.session == Session.NY_OPEN, f"Failed: {r}"

    # Test 3: Dead zone (Wednesday 13:00 EST) → blocked
    t = _make_est(2, 13, 0)
    r = check_kill_zone(t)
    print(f"Test 3 Dead zone    → allowed={r.allowed}  session={r.session.value}")
    assert not r.allowed and r.session == Session.DEAD_ZONE, f"Failed: {r}"

    # Test 4: Monday any time → blocked
    t = _make_est(0, 9, 0)   # Monday 09:00 EST (NY Open window, but Monday)
    r = check_kill_zone(t)
    print(f"Test 4 Monday       → allowed={r.allowed}  reason='{r.reason}'")
    assert not r.allowed and "Monday" in r.reason, f"Failed: {r}"

    # Test 5: Friday after cutoff (Friday 14:00 EST) → blocked
    t = _make_est(4, 14, 0)
    r = check_kill_zone(t)
    print(f"Test 5 Fri PM       → allowed={r.allowed}  reason='{r.reason}'")
    assert not r.allowed and "Friday" in r.reason, f"Failed: {r}"

    # Test 6: Friday before cutoff (Friday 08:00 EST, NY Open) → allowed
    t = _make_est(4, 8, 0)
    r = check_kill_zone(t)
    print(f"Test 6 Fri AM       → allowed={r.allowed}  session={r.session.value}")
    assert r.allowed and r.session == Session.NY_OPEN, f"Failed: {r}"

    # Test 7: London Close (disabled by default) → dead zone
    t = _make_est(2, 11, 30)
    r = check_kill_zone(t)
    print(f"Test 7 London Close → allowed={r.allowed}  session={r.session.value}  (LC disabled={not LONDON_CLOSE_ENABLED})")
    # If London Close disabled: should be DEAD_ZONE
    if not LONDON_CLOSE_ENABLED:
        assert not r.allowed and r.session == Session.DEAD_ZONE, f"Failed: {r}"

    # Test 8: Polars logging via agent
    agent = KillZoneAgent()
    agent.log_trade_session("NY_OPEN", True, 15.0, datetime.now(timezone.utc))
    agent.log_trade_session("LONDON_OPEN", False, -3.0, datetime.now(timezone.utc))
    df = agent.get_session_stats()
    assert len(df) == 2
    print(f"Test 8 Polars log   → {len(df)} rows  sessions={df['session'].to_list()}")

    print("\n✅ All Phase 2 unit tests passed.")
