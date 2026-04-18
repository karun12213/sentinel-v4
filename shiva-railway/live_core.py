"""
SHIVA V5 — SMC IFVG Trading System
Entry: Inverse Fair Value Gap retests in Discount / Premium zones
SL: IFVG zone boundary (wick level)
TP: 6R
Cooldown: 6 minutes after any SL/TP hit before next entry
"""
import asyncio
import json
import os
import signal
import sys
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import urllib.request
import urllib.error

import numpy as np
import pandas as pd
import pandas_ta as ta
from dotenv import load_dotenv
from metaapi_cloud_sdk import MetaApi


# ─────────────────────────────────────────────
# DISCORD
# ─────────────────────────────────────────────
def _discord_post(webhook_url: str, payload: dict):
    if not webhook_url:
        return
    try:
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(
            webhook_url, data=data,
            headers={'Content-Type': 'application/json'}, method='POST'
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"⚠️  Discord post failed: {e}")


def discord_trade_open(side: str, symbol: str, entry: float,
                       sl: float, tp: float, lot: float, zone: str, wick: float):
    webhook = os.getenv('DISCORD_TRADES') or os.getenv('DISCORD_ALERTS', '')
    risk    = abs(entry - sl)
    reward  = abs(tp - entry)
    emoji   = '🚀' if side == 'BUY' else '📉'
    color   = 0x00FF88 if side == 'BUY' else 0xFF4444
    _discord_post(webhook, {"embeds": [{"title": f"{emoji} {side}  {symbol}",
        "color": color,
        "fields": [
            {"name": "Entry",   "value": f"`{entry:.2f}`",  "inline": True},
            {"name": "SL",      "value": f"`{sl:.2f}`",     "inline": True},
            {"name": "TP",      "value": f"`{tp:.2f}`",     "inline": True},
            {"name": "Risk",    "value": f"`{risk:.2f}`",   "inline": True},
            {"name": "Reward",  "value": f"`{reward:.2f}`", "inline": True},
            {"name": "R:R",     "value": f"`1 : {reward/risk:.1f}`" if risk else "`—`", "inline": True},
            {"name": "Lot",     "value": f"`{lot}`",        "inline": True},
            {"name": "Zone",    "value": f"`{zone}`",       "inline": True},
            {"name": "Wick SL", "value": f"`{wick:.2f}`",  "inline": True},
        ],
        "footer": {"text": "SHIVA V5 — IFVG SMC"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }]})


def discord_trade_close(side: str, symbol: str, entry: float,
                        exit_price: float, pnl: float):
    webhook = os.getenv('DISCORD_TRADES') or os.getenv('DISCORD_ALERTS', '')
    result  = 'WIN ✅' if pnl > 0 else 'LOSS ❌'
    color   = 0x00FF88 if pnl > 0 else 0xFF4444
    _discord_post(webhook, {"embeds": [{"title": f"{result}  {symbol}  {side}",
        "color": color,
        "fields": [
            {"name": "Entry",  "value": f"`{entry:.2f}`",      "inline": True},
            {"name": "Exit",   "value": f"`{exit_price:.2f}`", "inline": True},
            {"name": "PnL",    "value": f"`${pnl:.2f}`",       "inline": True},
        ],
        "footer": {"text": "SHIVA V5 — IFVG SMC"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }]})

# ─────────────────────────────────────────────
# HEALTH SERVER
# ─────────────────────────────────────────────
class _HealthHandler(BaseHTTPRequestHandler):
    _analytics_ref = None

    def do_GET(self):
        if self.path == '/status' and self._analytics_ref:
            body = json.dumps(self._analytics_ref.summary(), indent=2).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            self.wfile.write(b'OK - SHIVA V5 LIVE')

    def log_message(self, *_):
        return


def start_health_server(analytics=None):
    port = os.getenv('PORT')
    if not port:
        return
    _HealthHandler._analytics_ref = analytics
    server = HTTPServer(('0.0.0.0', int(port)), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"🌐 Health server on :{port}  |  /status for live analytics")


def require_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


# ─────────────────────────────────────────────
# FEATURE ENGINE
# ─────────────────────────────────────────────
class FeatureEngine:
    @staticmethod
    def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Normalise column names (MetaApi may return camelCase)
        df.columns = [c.lower() for c in df.columns]
        for alias, canon in [('tickvolume', 'volume'), ('brokertime', 'time')]:
            if alias in df.columns and canon not in df.columns:
                df.rename(columns={alias: canon}, inplace=True)

        df['RSI'] = ta.rsi(df['close'], length=14)
        df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        df['EMA_20']  = ta.ema(df['close'], length=20)
        df['EMA_50']  = ta.ema(df['close'], length=50)
        df['EMA_200'] = ta.ema(df['close'], length=200)

        try:
            bb = ta.bbands(df['close'], length=20, std=2)
            if bb is not None and not bb.empty:
                upper = next((c for c in bb.columns if c.startswith('BBU')), None)
                lower = next((c for c in bb.columns if c.startswith('BBL')), None)
                mid   = next((c for c in bb.columns if c.startswith('BBM')), None)
                if upper: df['BB_upper'] = bb[upper].values
                if lower: df['BB_lower'] = bb[lower].values
                if mid:   df['BB_mid']   = bb[mid].values
        except Exception:
            pass

        core = ['RSI', 'ATR', 'EMA_20', 'EMA_50']
        return df.dropna(subset=core)


# ─────────────────────────────────────────────
# BASE STRATEGY
# ─────────────────────────────────────────────
class BaseStrategy(ABC):
    MIN_TRADES = 999999
    DISABLE_WR  = 0.0

    def __init__(self, name: str):
        self.name    = name
        self.enabled = True
        self.trades: list[dict] = []

    @property
    def n_trades(self): return len(self.trades)

    @property
    def wins(self): return sum(1 for t in self.trades if t['pnl'] > 0)

    @property
    def win_rate(self):
        return self.wins / self.n_trades if self.n_trades > 0 else 0.5

    @property
    def profit_factor(self):
        g_win  = sum(t['pnl'] for t in self.trades if t['pnl'] > 0)
        g_loss = abs(sum(t['pnl'] for t in self.trades if t['pnl'] < 0))
        return g_win / g_loss if g_loss > 0 else 1.0

    @property
    def net_pnl(self): return sum(t['pnl'] for t in self.trades)

    @property
    def weight(self): return 1.0

    def record_trade(self, pnl: float):
        self.trades.append({'pnl': pnl, 'time': datetime.now(timezone.utc).isoformat()})

    def status(self) -> dict:
        return {
            'strategy':      self.name,
            'enabled':       self.enabled,
            'trades':        self.n_trades,
            'win_rate':      f"{self.win_rate:.1%}",
            'profit_factor': f"{self.profit_factor:.2f}",
            'net_pnl':       f"${self.net_pnl:.2f}",
        }

    @abstractmethod
    def generate_signal(self, df: pd.DataFrame) -> int:
        """Return +1=BUY, -1=SELL, 0=HOLD"""


# ─────────────────────────────────────────────
# IFVG + DISCOUNT/PREMIUM STRATEGY
# ─────────────────────────────────────────────
class IFVGStrategy(BaseStrategy):
    """
    Smart Money Concepts entry logic:
    1. Identify swing range → midpoint → Discount (lower half) / Premium (upper half)
    2. Scan for FVGs (Fair Value Gaps) in recent candles
    3. Detect IFVGs: FVGs that price fully traded through (inverted role)
       - Filled bearish FVG → bullish IFVG (support) → BUY in discount
       - Filled bullish FVG → bearish IFVG (resistance) → SELL in premium
    4. SL at IFVG zone boundary (wick), TP at 6R
    """
    LOOKBACK  = 80   # bars to search for FVGs
    ZONE_BARS = 120  # bars for swing high/low range
    ATR_TOL   = 2.0  # proximity tolerance in ATR units

    def __init__(self):
        super().__init__("IFVG_SMC")

    # ── internal helpers ──

    def _find_fvgs(self, df: pd.DataFrame) -> list[dict]:
        bars = df.reset_index(drop=True)
        n    = len(bars)
        fvgs = []
        for i in range(1, n - 1):
            ph = float(bars.iloc[i - 1]['high'])
            pl = float(bars.iloc[i - 1]['low'])
            nh = float(bars.iloc[i + 1]['high'])
            nl = float(bars.iloc[i + 1]['low'])
            # Bullish FVG: gap up between prev high and next low
            if ph < nl:
                fvgs.append({'type': 'bull', 'low': ph, 'high': nl, 'idx': i})
            # Bearish FVG: gap down between prev low and next high
            if pl > nh:
                fvgs.append({'type': 'bear', 'low': nh, 'high': pl, 'idx': i})
        return fvgs

    def _get_ifvgs(self, df: pd.DataFrame) -> list[dict]:
        """
        IFVGs = FVGs that have been fully filled by subsequent price action.
        Filled bullish FVG  → bearish IFVG (price traded down through it)
        Filled bearish FVG  → bullish IFVG (price traded up through it)
        """
        bars = df.reset_index(drop=True)
        fvgs = self._find_fvgs(bars)
        ifvgs = []
        for fvg in fvgs:
            after_idx = fvg['idx'] + 2
            if after_idx >= len(bars):
                continue
            after = bars.iloc[after_idx:]
            if fvg['type'] == 'bull':
                if float(after['low'].min()) <= fvg['low']:
                    ifvgs.append({'type': 'bear', 'low': fvg['low'], 'high': fvg['high']})
            else:
                if float(after['high'].max()) >= fvg['high']:
                    ifvgs.append({'type': 'bull', 'low': fvg['low'], 'high': fvg['high']})
        return ifvgs

    # ── public API ──

    def get_signal_and_wick(self, df: pd.DataFrame) -> tuple[int, float]:
        """
        Returns (signal, wick_price).
        wick_price is the IFVG boundary used as SL reference:
          BUY  → IFVG low  (SL goes just below this)
          SELL → IFVG high (SL goes just above this)
        Returns (0, 0.0) when no setup found.
        """
        if len(df) < self.ZONE_BARS + 5:
            return 0, 0.0

        r   = df.iloc[-1]
        atr = float(r.get('ATR', 0) or 0)
        if atr == 0:
            return 0, 0.0

        current = float(r['close'])

        # Discount / Premium
        zone_df    = df.tail(self.ZONE_BARS)
        swing_high = float(zone_df['high'].max())
        swing_low  = float(zone_df['low'].min())
        midpoint   = (swing_high + swing_low) / 2.0
        in_discount = current < midpoint
        in_premium  = current > midpoint

        # IFVGs from recent lookback
        ifvgs = self._get_ifvgs(df.tail(self.LOOKBACK + 3))
        tol   = atr * self.ATR_TOL

        if in_discount:
            for z in sorted(ifvgs, key=lambda x: abs(current - (x['low'] + x['high']) / 2)):
                if z['type'] == 'bull' and z['low'] - tol <= current <= z['high'] + tol:
                    print(
                        f"  🔵 IFVG Bullish zone [{z['low']:.2f} – {z['high']:.2f}] "
                        f"| Discount | price={current:.2f}"
                    )
                    return 1, z['low']

        if in_premium:
            for z in sorted(ifvgs, key=lambda x: abs(current - (x['low'] + x['high']) / 2)):
                if z['type'] == 'bear' and z['low'] - tol <= current <= z['high'] + tol:
                    print(
                        f"  🔴 IFVG Bearish zone [{z['low']:.2f} – {z['high']:.2f}] "
                        f"| Premium | price={current:.2f}"
                    )
                    return -1, z['high']

        # Log zone status for debugging
        zone = "DISCOUNT" if in_discount else "PREMIUM"
        print(f"  ⏸  No IFVG match | {zone} | price={current:.2f} mid={midpoint:.2f} | IFVGs found: {len(ifvgs)}")
        return 0, 0.0

    def generate_signal(self, df: pd.DataFrame) -> int:
        signal, _ = self.get_signal_and_wick(df)
        return signal


# ─────────────────────────────────────────────
# META CONTROLLER
# ─────────────────────────────────────────────
class MetaController:
    def __init__(self, strategies: list[BaseStrategy]):
        self.strategies   = strategies
        self._last_report = 0.0

    def get_signal(self, df: pd.DataFrame) -> tuple[int, str]:
        for s in self.strategies:
            if not s.enabled:
                continue
            sig = s.generate_signal(df)
            if sig != 0:
                return sig, s.name
        return 0, ''

    def report(self):
        now = time.time()
        if now - self._last_report < 1800:
            return
        self._last_report = now
        print("\n╔══════════════════ STRATEGY STATUS ══════════════════╗")
        for s in self.strategies:
            st    = s.status()
            state = "✅ ACTIVE  " if st['enabled'] else "⛔ DISABLED"
            print(
                f"║ {state}  {st['strategy']:<16}"
                f"  T={st['trades']:<4}  WR={st['win_rate']:<8}"
                f"  PF={st['profit_factor']:<6}  PnL={st['net_pnl']}"
            )
        print("╚══════════════════════════════════════════════════════╝\n")


# ─────────────────────────────────────────────
# ANALYTICS ENGINE
# ─────────────────────────────────────────────
LOG_PATH = Path('/tmp/shiva_trades.json')


class AnalyticsEngine:
    def __init__(self):
        self.records: list[dict] = []
        self._load()

    def _load(self):
        try:
            if LOG_PATH.exists():
                self.records = json.loads(LOG_PATH.read_text())
        except Exception:
            self.records = []

    def _save(self):
        try:
            LOG_PATH.write_text(json.dumps(self.records, indent=2))
        except Exception as e:
            print(f"⚠️  Analytics save error: {e}")

    def log_open(self, pos_id: str, side: str, entry: float,
                 sl: float, tp: float, lot: float, strategy: str):
        self.records.append({
            'position_id': pos_id,
            'entry_time':  datetime.now(timezone.utc).isoformat(),
            'exit_time':   None,
            'side':        side,
            'entry':       entry,
            'exit':        None,
            'sl':          sl,
            'tp':          tp,
            'lot':         lot,
            'strategy':    strategy,
            'pnl':         None,
            'status':      'OPEN',
        })
        self._save()

    def log_close(self, pos_id: str, exit_price: float, pnl: float):
        for r in self.records:
            if r['position_id'] == pos_id and r['status'] == 'OPEN':
                r['exit_time'] = datetime.now(timezone.utc).isoformat()
                r['exit']      = exit_price
                r['pnl']       = pnl
                r['status']    = 'CLOSED'
                break
        self._save()

    def summary(self) -> dict:
        closed = [r for r in self.records if r['status'] == 'CLOSED']
        open_  = [r for r in self.records if r['status'] == 'OPEN']
        if not closed:
            return {'total_closed': 0, 'open_positions': len(open_)}
        wins      = sum(1 for r in closed if (r['pnl'] or 0) > 0)
        total_pnl = sum(r['pnl'] or 0 for r in closed)
        g_win     = sum(r['pnl'] for r in closed if (r['pnl'] or 0) > 0)
        g_loss    = abs(sum(r['pnl'] for r in closed if (r['pnl'] or 0) < 0))
        pf        = round(g_win / g_loss, 3) if g_loss > 0 else None
        return {
            'total_closed':   len(closed),
            'open_positions': len(open_),
            'win_rate':       f"{wins/len(closed):.1%}",
            'profit_factor':  pf,
            'net_pnl':        round(total_pnl, 2),
        }


# ─────────────────────────────────────────────
# EXECUTION ENGINE
# ─────────────────────────────────────────────
class ExecutionEngine:
    COOLDOWN_SECS = 360   # 6 minutes after SL/TP before next entry

    def __init__(self, token: str, account_id: str, symbol: str = "USOIL"):
        self.token      = token
        self.account_id = account_id
        self.symbol     = symbol
        self.is_running = True

        self.ifvg_strategy = IFVGStrategy()
        self.meta          = MetaController([self.ifvg_strategy])
        self.analytics     = AnalyticsEngine()

        self.connection  = None
        self.account     = None
        self.symbol_spec = None
        self.api         = None

        self.tracked: dict[str, str] = {}
        self.last_close_time: float  = 0.0

    def stop(self, *_):
        self.is_running = False
        print("🛑 Shutdown signal received")

    # ── broker helpers ──

    async def _refresh_spec(self):
        try:
            self.symbol_spec = await self.connection.get_symbol_specification(self.symbol)
        except Exception as e:
            print(f"⚠️  Symbol spec refresh: {e}")
            self.symbol_spec = None

    async def _get_price(self) -> dict:
        try:
            return await self.connection.get_symbol_price(self.symbol)
        except Exception as e:
            print(f"⚠️  Price fetch: {e}")
            return {}

    def _point(self) -> float:
        if self.symbol_spec:
            ts = self.symbol_spec.get('tickSize')
            if ts:
                return float(ts)
            d = self.symbol_spec.get('digits')
            if d is not None:
                return 10 ** (-int(d))
        return 0.01

    def _min_stop(self) -> float:
        stop_level = 0
        if self.symbol_spec:
            stop_level = (
                self.symbol_spec.get('tradeStopsLevel')
                or self.symbol_spec.get('stopsLevel')
                or self.symbol_spec.get('stopLevel')
                or 0
            )
        return max(float(stop_level) * self._point(), 0.10)

    def _snap(self, value: float, direction: str = 'nearest') -> float:
        pt     = Decimal(str(self._point()))
        scaled = Decimal(str(value)) / pt
        rmap   = {'down': ROUND_FLOOR, 'up': ROUND_CEILING, 'nearest': ROUND_HALF_UP}
        return float(scaled.to_integral_value(rounding=rmap[direction]) * pt)

    def _build_levels(self, side: str, entry: float, wick: float):
        """
        wick = IFVG zone boundary used as SL anchor.
        BUY:  SL just below wick (IFVG low), TP = entry + 6 * risk
        SELL: SL just above wick (IFVG high), TP = entry - 6 * risk
        """
        min_d = self._min_stop()
        buf   = self._point()
        if side == 'BUY':
            desired_sl = wick - buf
            sl         = min(desired_sl, entry - min_d)
            sl         = self._snap(sl, 'down')
            risk       = max(entry - sl, min_d)
            tp         = self._snap(entry + risk * 6, 'up')
        else:
            desired_sl = wick + buf
            sl         = max(desired_sl, entry + min_d)
            sl         = self._snap(sl, 'up')
            risk       = max(sl - entry, min_d)
            tp         = self._snap(entry - risk * 6, 'down')
        return sl, tp

    # ── closed-position detection ──

    async def _process_closed_positions(self, live_ids: set[str]):
        closed_ids = set(self.tracked.keys()) - live_ids
        if not closed_ids:
            return

        deals_by_pos: dict[str, list] = {}
        try:
            now      = datetime.now(timezone.utc)
            from_dt  = now - timedelta(hours=24)
            all_deals = await self.connection.get_deals_by_time_range(from_dt, now)
            for d in all_deals:
                pid = d.get('positionId')
                if pid:
                    deals_by_pos.setdefault(pid, []).append(d)
        except Exception as e:
            print(f"⚠️  Deal history fetch: {e}")

        for pos_id in closed_ids:
            strategy_name = self.tracked.pop(pos_id)
            pnl, exit_price = 0.0, 0.0
            pos_deals = deals_by_pos.get(pos_id, [])
            closing   = next(
                (d for d in pos_deals if d.get('entryType') == 'DEAL_ENTRY_OUT'), None
            )
            if closing:
                pnl        = float(closing.get('profit', 0))
                exit_price = float(closing.get('price', 0))

            strategy = next((s for s in self.meta.strategies if s.name == strategy_name), None)
            if strategy:
                strategy.record_trade(pnl)

            self.analytics.log_close(pos_id, exit_price, pnl)
            self.last_close_time = time.time()
            result = "WIN ✅" if pnl > 0 else "LOSS ❌"
            print(
                f"📊 Position closed | {result} | PnL=${pnl:.2f} | "
                f"Cooldown: next entry in {self.COOLDOWN_SECS // 60} min"
            )
            # Find entry price from analytics for Discord close message
            rec = next((r for r in self.analytics.records
                        if r['position_id'] == pos_id), None)
            entry_price = rec['entry'] if rec else 0.0
            side        = rec['side']  if rec else '?'
            discord_trade_close(side, self.symbol, entry_price, exit_price, pnl)

        self.meta.report()

    # ── candle fetch (account-level REST, works with RPC connection) ──

    async def _fetch_candles(self) -> pd.DataFrame:
        start_time = datetime.now(timezone.utc) - timedelta(days=10)
        candles = await self.account.get_historical_candles(
            self.symbol, '15m', start_time, 500
        )
        if not candles:
            raise RuntimeError('No candles returned')
        df = pd.DataFrame(candles)
        return FeatureEngine.add_indicators(df)

    # ── main loop ──

    async def run(self):
        print(f"🔱 SHIVA V5 LIVE | {self.symbol} | IFVG + Discount/Premium")
        self.api = MetaApi(self.token)
        try:
            self.account = await self.api.metatrader_account_api.get_account(self.account_id)
            await self.account.wait_connected()
            self.connection = self.account.get_rpc_connection()
            await self.connection.connect()
            await self.connection.wait_synchronized()
            await self._refresh_spec()
            print(
                f"✅ MetaApi synchronized | {self.symbol} | "
                f"Server: {getattr(self.account, 'server', '?')} | "
                f"Login: {getattr(self.account, 'login', '?')}"
            )
            print("\n── Strategy ─────────────────────────────────────────")
            print("  [ACTIVE] IFVG_SMC  (Inverse FVG + Discount/Premium)")
            print("  SL: IFVG zone boundary (wick)  |  TP: 6R")
            print(f"  Cooldown after SL/TP: {self.COOLDOWN_SECS // 60} min")
            print("─────────────────────────────────────────────────────\n")

            while self.is_running:
                try:
                    # 1. Candles + indicators
                    df = await self._fetch_candles()
                    if df.empty:
                        raise RuntimeError('Empty indicator frame')

                    # 2. Live positions
                    positions = await self.connection.get_positions()
                    live_ids  = {p['id'] for p in positions}
                    current   = next((p for p in positions if p['symbol'] == self.symbol), None)

                    # 3. Detect closed positions
                    await self._process_closed_positions(live_ids)

                    # 4. Cooldown check
                    secs_since_close = time.time() - self.last_close_time
                    in_cooldown = self.last_close_time > 0 and secs_since_close < self.COOLDOWN_SECS
                    if in_cooldown and not current:
                        remaining = int(self.COOLDOWN_SECS - secs_since_close)
                        print(f"⏳ Cooldown — next entry in {remaining}s")
                        await asyncio.sleep(30)
                        continue

                    # 5. Signal from IFVG strategy
                    if not current:
                        signal, wick = self.ifvg_strategy.get_signal_and_wick(df)
                    else:
                        signal, wick = 0, 0.0

                    # 6. Execute
                    if not current and signal != 0 and wick != 0.0:
                        lot   = float(os.getenv('LOT_SIZE', '0.01'))
                        price = await self._get_price()

                        if signal == 1:
                            entry = float(price.get('ask') or price.get('bid') or df.iloc[-1]['close'])
                            sl, tp = self._build_levels('BUY', entry, wick)
                            print(f"🚀 BUY  {self.symbol} @ {entry:.2f} | SL {sl:.2f} | TP {tp:.2f} | wick={wick:.2f}")
                            result = await self.connection.create_market_buy_order(
                                self.symbol, lot, sl, tp,
                                {'comment': 'SHIVA:IFVG_BUY'},
                            )
                        else:
                            entry = float(price.get('bid') or price.get('ask') or df.iloc[-1]['close'])
                            sl, tp = self._build_levels('SELL', entry, wick)
                            print(f"📉 SELL {self.symbol} @ {entry:.2f} | SL {sl:.2f} | TP {tp:.2f} | wick={wick:.2f}")
                            result = await self.connection.create_market_sell_order(
                                self.symbol, lot, sl, tp,
                                {'comment': 'SHIVA:IFVG_SELL'},
                            )

                        # Track new position
                        new_positions = await self.connection.get_positions()
                        new_pos = next(
                            (p for p in new_positions
                             if p['symbol'] == self.symbol and p['id'] not in live_ids),
                            None,
                        )
                        if new_pos:
                            side = 'BUY' if signal == 1 else 'SELL'
                            zone = "DISCOUNT" if signal == 1 else "PREMIUM"
                            self.tracked[new_pos['id']] = self.ifvg_strategy.name
                            self.analytics.log_open(
                                new_pos['id'], side, entry, sl, tp, lot, self.ifvg_strategy.name
                            )
                            discord_trade_open(side, self.symbol, entry, sl, tp, lot, zone, wick)

                    await asyncio.sleep(60)

                except Exception as e:
                    msg = str(e)
                    print(f"⚠️  Loop warning: {msg[:200]}")
                    if 'cpu credits' in msg or '429' in msg or 'rate' in msg.lower():
                        print("🚦 Rate limited — backing off 10 min")
                        await asyncio.sleep(600)
                    elif 'Market is closed' in msg:
                        await asyncio.sleep(300)
                    elif 'Invalid stops' in msg:
                        await self._refresh_spec()
                        await asyncio.sleep(120)
                    else:
                        await asyncio.sleep(60)

        except Exception as e:
            print(f"❌ Fatal: {e}")
            raise
        finally:
            try:
                if self.connection:
                    await self.connection.close()
            except Exception:
                pass
            print("🔁 Engine exited")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if os.path.exists('.shiva_env'):
        load_dotenv('.shiva_env')
    else:
        load_dotenv()

    TOKEN      = require_env('METAAPI_TOKEN')
    ACCOUNT_ID = require_env('METAAPI_ACCOUNT_ID')
    SYMBOL     = os.getenv('SYMBOL', 'USOIL')

    print(f"🚀 Starting SHIVA V5  |  {SYMBOL}  |  IFVG + DISCOUNT/PREMIUM")

    _bootstrap_analytics = AnalyticsEngine()
    start_health_server(_bootstrap_analytics)

    while True:
        engine = None
        try:
            engine = ExecutionEngine(TOKEN, ACCOUNT_ID, SYMBOL)
            engine.analytics = _bootstrap_analytics
            signal.signal(signal.SIGTERM, engine.stop)
            signal.signal(signal.SIGINT,  engine.stop)
            asyncio.run(engine.run())
            if not engine.is_running:
                print("🛑 Clean shutdown")
                break
        except Exception as e:
            print(f"❌ Main loop error: {e}")
            print("⏳ Restarting in 30 s…")
            time.sleep(30)
