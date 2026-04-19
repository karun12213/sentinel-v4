"""
SHIVA — Phase 11: Performance Logger & Dashboard (PostTradeAgent)

Logs every trade with full context:
  timestamp, direction, entry_price, sl, tp1, tp2, exit_price, pnl,
  confluence_score, regime, session, news_nearby, cnn_confidence,
  win/loss, reason_for_entry

Features:
  - Polars DataFrame — fast columnar storage, exported to CSV daily
  - Rolling 20-trade win rate with alert at < 55%
  - Daily summary printed at configurable UTC hour
  - /status endpoint returns live analytics JSON
"""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Optional

import polars as pl
import yaml


# ── Config ─────────────────────────────────────────────────────────────────

def _load_cfg() -> dict:
    p = Path(__file__).parent / "config.yaml"
    return yaml.safe_load(p.read_text()).get("performance", {}) if p.exists() else {}

_CFG = _load_cfg()

CSV_EXPORT_PATH    = Path(_CFG.get("csv_export_path",    "/tmp/shiva_trades"))
ROLLING_WR_WINDOW  = int(_CFG.get("rolling_wr_window",  20))
WR_ALERT_THRESHOLD = float(_CFG.get("wr_alert_threshold", 0.55))
DAILY_SUMMARY_HOUR = int(_CFG.get("daily_summary_hour_utc", 22))


# ── Trade record ──────────────────────────────────────────────────────────

@dataclass
class TradeRecord:
    """One row in the trade log."""
    position_id:      str
    timestamp:        str           # UTC ISO open time
    direction:        str           # "BUY" / "SELL"
    entry_price:      float
    sl:               float
    tp1:              float
    tp2:              float = 0.0
    exit_price:       float = 0.0
    pnl:              float = 0.0
    confluence_score: int   = 0
    regime:           str   = ""
    session:          str   = ""
    news_nearby:      bool  = False
    cnn_confidence:   float = 0.0
    win:              Optional[bool] = None
    reason:           str   = ""
    strategy:         str   = ""
    lot:              float = 0.0
    status:           str   = "OPEN"  # "OPEN" / "CLOSED"
    close_time:       str   = ""


# ── PostTradeAgent ─────────────────────────────────────────────────────────

class PostTradeAgent:
    """
    Centralised trade logger that replaces the basic AnalyticsEngine.
    Drop-in compatible: exposes log_open() / log_close() / summary().

    Usage:
        agent = PostTradeAgent()
        # On open:
        agent.log_open(pos_id, side, entry, sl, tp1, tp2, lot, strategy,
                       confluence_score=85, regime="TRENDING_BULL",
                       session="NY_OPEN", cnn_confidence=0.72, reason="FVG+HTF")
        # On close:
        agent.log_close(pos_id, exit_price, pnl)
        # Periodic:
        agent.maybe_print_daily_summary()
    """

    _SCHEMA = {
        "position_id":      pl.Utf8,
        "timestamp":        pl.Utf8,
        "direction":        pl.Utf8,
        "entry_price":      pl.Float64,
        "sl":               pl.Float64,
        "tp1":              pl.Float64,
        "tp2":              pl.Float64,
        "exit_price":       pl.Float64,
        "pnl":              pl.Float64,
        "confluence_score": pl.Int32,
        "regime":           pl.Utf8,
        "session":          pl.Utf8,
        "news_nearby":      pl.Boolean,
        "cnn_confidence":   pl.Float64,
        "win":              pl.Boolean,
        "reason":           pl.Utf8,
        "strategy":         pl.Utf8,
        "lot":              pl.Float64,
        "status":           pl.Utf8,
        "close_time":       pl.Utf8,
    }

    def __init__(self):
        self._records: list[TradeRecord] = []
        self._last_summary_date: Optional[date] = None
        self._load()

    # ── Public interface ──

    def log_open(
        self,
        position_id:      str,
        side:             str,
        entry:            float,
        sl:               float,
        tp1:              float,
        tp2:              float         = 0.0,
        lot:              float         = 0.01,
        strategy:         str           = "",
        confluence_score: int           = 0,
        regime:           str           = "",
        session:          str           = "",
        news_nearby:      bool          = False,
        cnn_confidence:   float         = 0.0,
        reason:           str           = "",
    ) -> None:
        rec = TradeRecord(
            position_id=position_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            direction=side,
            entry_price=entry,
            sl=sl,
            tp1=tp1,
            tp2=tp2,
            lot=lot,
            strategy=strategy,
            confluence_score=confluence_score,
            regime=regime,
            session=session,
            news_nearby=news_nearby,
            cnn_confidence=cnn_confidence,
            reason=reason,
            status="OPEN",
        )
        self._records.append(rec)
        self._save()

    def log_close(self, position_id: str, exit_price: float, pnl: float) -> None:
        for rec in self._records:
            if rec.position_id == position_id and rec.status == "OPEN":
                rec.exit_price = exit_price
                rec.pnl        = pnl
                rec.win        = pnl > 0
                rec.status     = "CLOSED"
                rec.close_time = datetime.now(timezone.utc).isoformat()
                break
        self._save()
        self._check_rolling_wr()

    def summary(self) -> dict:
        """Compatible with old AnalyticsEngine.summary() — used by /status endpoint."""
        closed = [r for r in self._records if r.status == "CLOSED"]
        open_  = [r for r in self._records if r.status == "OPEN"]
        if not closed:
            return {"total_closed": 0, "open_positions": len(open_)}
        wins      = sum(1 for r in closed if (r.pnl or 0) > 0)
        total_pnl = sum(r.pnl for r in closed)
        g_win     = sum(r.pnl for r in closed if r.pnl > 0)
        g_loss    = abs(sum(r.pnl for r in closed if r.pnl < 0))
        pf        = round(g_win / g_loss, 3) if g_loss > 0 else None
        rolling   = self._rolling_wr()
        return {
            "total_closed":   len(closed),
            "open_positions": len(open_),
            "win_rate":       f"{wins/len(closed):.1%}",
            "rolling_wr":     f"{rolling:.1%}" if rolling else "n/a",
            "profit_factor":  pf,
            "net_pnl":        round(total_pnl, 2),
        }

    def maybe_print_daily_summary(self) -> None:
        now  = datetime.now(timezone.utc)
        today = now.date()
        if now.hour == DAILY_SUMMARY_HOUR and today != self._last_summary_date:
            self._last_summary_date = today
            self._print_daily_summary(today)
            self._export_csv()

    def get_df(self) -> pl.DataFrame:
        """Return full Polars DataFrame of all trades."""
        if not self._records:
            return pl.DataFrame(schema=self._SCHEMA)
        rows = [self._record_to_dict(r) for r in self._records]
        return pl.DataFrame(rows, schema=self._SCHEMA)

    def closed_df(self) -> pl.DataFrame:
        rows = [self._record_to_dict(r) for r in self._records if r.status == "CLOSED"]
        if not rows:
            return pl.DataFrame(schema=self._SCHEMA)
        return pl.DataFrame(rows, schema=self._SCHEMA)

    # ── Internal ──

    def _rolling_wr(self) -> Optional[float]:
        closed = [r for r in self._records if r.status == "CLOSED"]
        if len(closed) < ROLLING_WR_WINDOW:
            return None
        window = closed[-ROLLING_WR_WINDOW:]
        wins   = sum(1 for r in window if r.pnl > 0)
        return wins / ROLLING_WR_WINDOW

    def _check_rolling_wr(self) -> None:
        wr = self._rolling_wr()
        if wr is not None and wr < WR_ALERT_THRESHOLD:
            print(
                f"⚠️  ROLLING WIN RATE ALERT: {wr:.1%} < {WR_ALERT_THRESHOLD:.0%}  "
                f"(last {ROLLING_WR_WINDOW} trades)"
            )

    def _print_daily_summary(self, for_date: date) -> None:
        today_closed = [
            r for r in self._records
            if r.status == "CLOSED" and r.close_time[:10] == str(for_date)
        ]
        if not today_closed:
            print(f"\n📊 Daily summary {for_date}: no closed trades today")
            return

        wins    = sum(1 for r in today_closed if r.pnl > 0)
        net     = sum(r.pnl for r in today_closed)
        best    = max(today_closed, key=lambda r: r.pnl)
        worst   = min(today_closed, key=lambda r: r.pnl)
        rolling = self._rolling_wr()

        print(f"\n{'═'*60}")
        print(f"  📊 DAILY SUMMARY  {for_date}")
        print(f"{'═'*60}")
        print(f"  Trades today : {len(today_closed)}")
        print(f"  Win rate     : {wins}/{len(today_closed)} ({wins/len(today_closed):.1%})")
        print(f"  Net P&L      : ${net:+.2f}")
        print(f"  Best trade   : ${best.pnl:+.2f}  [{best.strategy}  {best.direction}]")
        print(f"  Worst trade  : ${worst.pnl:+.2f}  [{worst.strategy}  {worst.direction}]")
        if rolling:
            flag = "✅" if rolling >= WR_ALERT_THRESHOLD else "⚠️ "
            print(f"  Rolling WR   : {flag} {rolling:.1%} (last {ROLLING_WR_WINDOW})")

        # Breakdown by session
        sessions: dict[str, list] = {}
        for r in today_closed:
            sessions.setdefault(r.session or "UNKNOWN", []).append(r)
        for sess, trades in sessions.items():
            w = sum(1 for t in trades if t.pnl > 0)
            print(f"  Session [{sess}]: {len(trades)} trades  {w}/{len(trades)} wins")

        print(f"{'═'*60}\n")

    def _export_csv(self) -> None:
        CSV_EXPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        path = Path(f"{CSV_EXPORT_PATH}_{datetime.now(timezone.utc).date()}.csv")
        try:
            df = self.closed_df()
            import pandas as pd
            df.to_pandas().to_csv(path, index=False)
            print(f"📁 Trade log exported → {path}  ({len(df)} closed trades)")
        except Exception as e:
            print(f"⚠️  CSV export error: {e}")

    _LOG_PATH = Path("/tmp/shiva_post_trade.json")

    def _load(self) -> None:
        try:
            if self._LOG_PATH.exists():
                data = json.loads(self._LOG_PATH.read_text())
                self._records = [TradeRecord(**d) for d in data]
        except Exception:
            self._records = []

    def _save(self) -> None:
        try:
            self._LOG_PATH.write_text(
                json.dumps([asdict(r) for r in self._records], indent=2)
            )
        except Exception as e:
            print(f"⚠️  PostTradeAgent save error: {e}")

    @staticmethod
    def _record_to_dict(r: TradeRecord) -> dict:
        d = asdict(r)
        # win may be None for open trades — coerce to False for Polars Boolean
        if d["win"] is None:
            d["win"] = False
        return d


# ── Unit tests ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import tempfile, os
    print("Running Phase 11 unit tests…\n")

    # Redirect log path to temp dir
    PostTradeAgent._LOG_PATH = Path(tempfile.mktemp(suffix=".json"))

    agent = PostTradeAgent()

    # Test 1: log_open
    agent.log_open(
        "pos_001", "BUY", 75.0, 74.7, 75.3, 75.5,
        lot=0.01, strategy="FVG_SCALP",
        confluence_score=88, regime="TRENDING_BULL",
        session="NY_OPEN", cnn_confidence=0.72,
        reason="FVG retest + HTF bull + kill zone",
    )
    assert len(agent._records) == 1 and agent._records[0].status == "OPEN"
    print("Test 1 (log_open)          → ✅  1 record OPEN")

    # Test 2: log_close WIN
    agent.log_close("pos_001", 75.3, 3.0)
    assert agent._records[0].status == "CLOSED" and agent._records[0].win
    print("Test 2 (log_close WIN)     → ✅  closed  win=True")

    # Test 3: log_close LOSS
    agent.log_open("pos_002", "SELL", 76.0, 76.3, 75.7, 75.4, lot=0.01)
    agent.log_close("pos_002", 76.3, -3.0)
    assert not agent._records[1].win
    print("Test 3 (log_close LOSS)    → ✅  win=False")

    # Test 4: summary()
    s = agent.summary()
    print(f"Test 4 (summary)           → {s}")
    assert s["total_closed"] == 2 and s["net_pnl"] == 0.0

    # Test 5: rolling WR (insufficient data)
    wr = agent._rolling_wr()
    print(f"Test 5 (rolling WR)        → {wr}  (need {ROLLING_WR_WINDOW} trades)")
    assert wr is None  # only 2 trades

    # Test 6: rolling WR with enough data
    for i in range(ROLLING_WR_WINDOW):
        pid = f"pos_fill_{i}"
        agent.log_open(pid, "BUY", 75.0, 74.7, 75.3, lot=0.01)
        pnl = 3.0 if i % 2 == 0 else -3.0   # 50% WR
        agent.log_close(pid, 75.3, pnl)
    wr2 = agent._rolling_wr()
    print(f"Test 6 (rolling WR 50%)    → {wr2:.1%}")
    assert wr2 is not None and abs(wr2 - 0.50) < 0.01

    # Test 7: Polars DataFrame shape
    df = agent.get_df()
    print(f"Test 7 (Polars df)         → {df.shape}  cols={list(df.columns[:5])}")
    assert df.shape[0] == len(agent._records)
    assert "confluence_score" in df.columns

    # Test 8: daily summary (force)
    agent._print_daily_summary(datetime.now(timezone.utc).date())
    print("Test 8 (daily summary)     → ✅  printed above")

    # Cleanup
    try:
        os.unlink(str(PostTradeAgent._LOG_PATH))
    except Exception:
        pass

    print("\n✅ All Phase 11 unit tests passed.")
