"""
SHIVA V5 — Multi-Strategy Autonomous USOIL Trading System
Architecture: Multi-Agent Strategies → Meta Controller → Execution Engine
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

import numpy as np
import pandas as pd
import pandas_ta as ta
from dotenv import load_dotenv
from metaapi_cloud_sdk import MetaApi

# ─────────────────────────────────────────────
# KRONOS AI (optional — set KRONOS_ENABLED=true)
# ─────────────────────────────────────────────
KRONOS_AVAILABLE = False
KronosPredictor = None
KronosTokenizer = None
Kronos = None

if os.getenv('KRONOS_ENABLED', '').lower() == 'true':
    try:
        _kronos_path = str(Path(__file__).parent.parent / 'lib' / 'kronos' / 'repo')
        if _kronos_path not in sys.path:
            sys.path.insert(0, _kronos_path)
        from model import Kronos, KronosTokenizer, KronosPredictor  # noqa: F811
        KRONOS_AVAILABLE = True
        print("✅ Kronos model library imported")
    except Exception as _e:
        print(f"⚠️  Kronos import failed ({_e}). Running without AI layer.")

# ─────────────────────────────────────────────
# HEALTH SERVER
# ─────────────────────────────────────────────
class _HealthHandler(BaseHTTPRequestHandler):
    _analytics_ref = None  # set by start_health_server

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
        df['RSI']     = ta.rsi(df['close'], length=14)
        df['ATR']     = ta.atr(df['high'], df['low'], df['close'], length=14)
        df['EMA_20']  = ta.ema(df['close'], length=20)
        df['EMA_50']  = ta.ema(df['close'], length=50)
        df['EMA_200'] = ta.ema(df['close'], length=200)

        # Bollinger Bands — detect actual column names at runtime
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

        # MACD — detect actual column names at runtime
        try:
            macd = ta.macd(df['close'])
            if macd is not None and not macd.empty:
                mc = next((c for c in macd.columns if 'MACDh' in c), None)
                ms = next((c for c in macd.columns if 'MACDs' in c), None)
                mv = next((c for c in macd.columns if c.startswith('MACD') and 'h' not in c and 's' not in c), None)
                if mv: df['MACD']      = macd[mv].values
                if ms: df['MACD_sig']  = macd[ms].values
                if mc: df['MACD_hist'] = macd[mc].values
        except Exception:
            pass

        # Only require the core columns to be non-NaN (EMA_200 needs 200 bars)
        core = ['RSI', 'ATR', 'EMA_20', 'EMA_50', 'EMA_200']
        return df.dropna(subset=core)


# ─────────────────────────────────────────────
# BASE STRATEGY
# ─────────────────────────────────────────────
class BaseStrategy(ABC):
    MIN_TRADES = 20           # minimum trades before auto-disable check
    DISABLE_WR  = 0.35        # disable if win rate falls below this

    def __init__(self, name: str):
        self.name    = name
        self.enabled = True
        self.trades: list[dict] = []

    # ── performance properties ──
    @property
    def n_trades(self): return len(self.trades)

    @property
    def wins(self): return sum(1 for t in self.trades if t['pnl'] > 0)

    @property
    def losses(self): return sum(1 for t in self.trades if t['pnl'] <= 0)

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
    def weight(self):
        """Signal weight: scales with recent win rate (0.1 – 2.0)."""
        if self.n_trades < 5:
            return 1.0
        recent_wr = sum(1 for t in self.trades[-10:] if t['pnl'] > 0) / min(10, self.n_trades)
        return round(max(0.1, recent_wr * 2), 3)

    # ── attribution ──
    def record_trade(self, pnl: float):
        self.trades.append({'pnl': pnl, 'time': datetime.now(timezone.utc).isoformat()})
        if self.n_trades >= self.MIN_TRADES and self.win_rate < self.DISABLE_WR:
            self.enabled = False
            print(f"⛔  {self.name} auto-disabled  WR={self.win_rate:.1%}  PF={self.profit_factor:.2f}")

    def status(self) -> dict:
        return {
            'strategy':       self.name,
            'enabled':        self.enabled,
            'trades':         self.n_trades,
            'win_rate':       f"{self.win_rate:.1%}",
            'profit_factor':  f"{self.profit_factor:.2f}",
            'net_pnl':        f"${self.net_pnl:.2f}",
            'weight':         self.weight,
        }

    @abstractmethod
    def generate_signal(self, df: pd.DataFrame) -> int:
        """Return +1=BUY, -1=SELL, 0=HOLD"""


# ─────────────────────────────────────────────
# STRATEGY 1 — TREND (EMA Crossover + MACD)
# ─────────────────────────────────────────────
class TrendStrategy(BaseStrategy):
    def __init__(self):
        super().__init__("TrendEMA")

    def generate_signal(self, df: pd.DataFrame) -> int:
        r = df.iloc[-1]
        if r['EMA_50'] > r['EMA_200'] and r['close'] > r['EMA_20']:
            return 1
        if r['EMA_50'] < r['EMA_200'] and r['close'] < r['EMA_20']:
            return -1
        return 0


# ─────────────────────────────────────────────
# STRATEGY 2 — MEAN REVERSION (RSI)
# ─────────────────────────────────────────────
class ReversionStrategy(BaseStrategy):
    def __init__(self):
        super().__init__("RSIReversion")

    def generate_signal(self, df: pd.DataFrame) -> int:
        rsi = df.iloc[-1].get('RSI', 50) or 50
        if rsi < 40:
            return 1   # Oversold → BUY
        if rsi > 60:
            return -1  # Overbought → SELL
        return 0


# ─────────────────────────────────────────────
# STRATEGY 3 — BREAKOUT (Bollinger Bands + ATR)
# ─────────────────────────────────────────────
class BreakoutStrategy(BaseStrategy):
    def __init__(self):
        super().__init__("BBBreakout")

    def generate_signal(self, df: pd.DataFrame) -> int:
        if len(df) < 3:
            return 0
        r    = df.iloc[-1]
        prev = df.iloc[-2]

        bb_upper = r.get('BB_upper', None)
        bb_lower = r.get('BB_lower', None)
        if bb_upper is None or bb_lower is None:
            return 0

        bb_width = float(bb_upper) - float(bb_lower)
        atr      = float(r.get('ATR', 0) or 0)
        if bb_width <= 0 or atr <= 0:
            return 0

        close_now  = float(r['close'])
        bb_upper_f = float(bb_upper)
        bb_lower_f = float(bb_lower)
        bb_mid     = float(r.get('BB_mid', (bb_upper_f + bb_lower_f) / 2))

        if close_now > bb_mid:
            return 1   # Above midline → bullish
        if close_now < bb_mid:
            return -1  # Below midline → bearish
        return 0


# ─────────────────────────────────────────────
# STRATEGY 4 — KRONOS AI (optional)
# ─────────────────────────────────────────────
class KronosStrategy(BaseStrategy):
    PRED_LEN      = 8     # candles to predict ahead (8 × 15 min = 2 h)
    MIN_CHANGE    = 0.002 # ≥0.2% predicted move to signal

    def __init__(self):
        super().__init__("KronosAI")
        self.predictor = None
        if KRONOS_AVAILABLE:
            self._load()
        else:
            self.enabled = False

    def _load(self):
        try:
            print("⏳ KronosAI: Loading Kronos-mini from HuggingFace…")
            tokenizer      = KronosTokenizer.from_pretrained("NeoQuasar/Kronos-Tokenizer-2k")
            model          = Kronos.from_pretrained("NeoQuasar/Kronos-mini")
            self.predictor = KronosPredictor(model, tokenizer, max_context=256)
            print("✅ KronosAI: Ready")
        except Exception as e:
            print(f"⚠️  KronosAI load failed: {e} — disabled")
            self.enabled = False

    def generate_signal(self, df: pd.DataFrame) -> int:
        if not self.enabled or self.predictor is None:
            return 0
        try:
            context = df.tail(200)[['open', 'high', 'low', 'close', 'volume']].reset_index(drop=True)
            n = len(context)
            x_ts = pd.to_datetime(pd.Series(range(n)), unit='s')
            y_ts = pd.to_datetime(pd.Series(range(n, n + self.PRED_LEN)), unit='s')

            pred = self.predictor.predict(
                df=context,
                x_timestamp=x_ts,
                y_timestamp=y_ts,
                pred_len=self.PRED_LEN,
                T=1.0, top_p=0.9, sample_count=1, verbose=False,
            )
            current  = float(context.iloc[-1]['close'])
            forecast = float(pred.iloc[-1]['close'])
            chg      = (forecast - current) / current

            if chg > self.MIN_CHANGE:
                return 1
            if chg < -self.MIN_CHANGE:
                return -1
        except Exception as e:
            print(f"⚠️  KronosAI signal error: {e}")
        return 0


# ─────────────────────────────────────────────
# META CONTROLLER
# ─────────────────────────────────────────────
class MetaController:
    """
    Combines signals from all active strategies via weighted voting.
    Periodically prints a strategy status table.
    """
    SIGNAL_THRESHOLD = 0.10   # weighted-average must exceed this

    def __init__(self, strategies: list[BaseStrategy]):
        self.strategies  = strategies
        self._last_report = 0.0

    def get_signal(self, df: pd.DataFrame) -> tuple[int, str]:
        """Returns (signal, comma-separated strategy names that agreed)."""
        votes = []
        for s in self.strategies:
            if not s.enabled:
                continue
            sig = s.generate_signal(df)
            if sig != 0:
                votes.append((s.name, sig, s.weight))

        if not votes:
            return 0, ''

        weighted_sum   = sum(sig * w for _, sig, w in votes)
        total_weight   = sum(w for _, _, w in votes)
        normalized     = weighted_sum / total_weight if total_weight else 0

        if normalized > self.SIGNAL_THRESHOLD:
            agree = '+'.join(n for n, sig, _ in votes if sig == 1)
            return 1, agree
        if normalized < -self.SIGNAL_THRESHOLD:
            agree = '+'.join(n for n, sig, _ in votes if sig == -1)
            return -1, agree
        return 0, ''

    def report(self):
        now = time.time()
        if now - self._last_report < 1800:  # every 30 min
            return
        self._last_report = now
        print("\n╔══════════════════ STRATEGY STATUS ══════════════════╗")
        for s in self.strategies:
            st    = s.status()
            state = "✅ ACTIVE  " if st['enabled'] else "⛔ DISABLED"
            print(
                f"║ {state}  {st['strategy']:<14}"
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
        rec = {
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
        }
        self.records.append(rec)
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

        # Max drawdown (equity curve)
        equity = 0.0
        peak   = 0.0
        mdd    = 0.0
        for r in sorted(closed, key=lambda x: x['entry_time']):
            equity += r['pnl'] or 0
            peak    = max(peak, equity)
            dd      = peak - equity
            mdd     = max(mdd, dd)

        # Per-strategy breakdown
        by_strategy: dict[str, dict] = {}
        for r in closed:
            s = r.get('strategy', 'unknown')
            if s not in by_strategy:
                by_strategy[s] = {'trades': 0, 'wins': 0, 'pnl': 0.0}
            by_strategy[s]['trades'] += 1
            by_strategy[s]['pnl']    += r['pnl'] or 0
            if (r['pnl'] or 0) > 0:
                by_strategy[s]['wins'] += 1
        for s, v in by_strategy.items():
            v['win_rate'] = f"{v['wins']/v['trades']:.1%}" if v['trades'] else '0%'
            v['pnl']      = round(v['pnl'], 2)

        return {
            'total_closed':    len(closed),
            'open_positions':  len(open_),
            'win_rate':        f"{wins/len(closed):.1%}",
            'profit_factor':   pf,
            'net_pnl':         round(total_pnl, 2),
            'max_drawdown':    round(mdd, 2),
            'by_strategy':     by_strategy,
        }


# ─────────────────────────────────────────────
# EXECUTION ENGINE
# ─────────────────────────────────────────────
class ExecutionEngine:
    def __init__(self, token: str, account_id: str, symbol: str = "USOIL"):
        self.token      = token
        self.account_id = account_id
        self.symbol     = symbol
        self.is_running = True

        # Layers
        strategies = [
            TrendStrategy(),
            ReversionStrategy(),
            BreakoutStrategy(),
            KronosStrategy(),
        ]
        self.meta      = MetaController(strategies)
        self.analytics = AnalyticsEngine()

        # State
        self.connection   = None
        self.account      = None
        self.symbol_spec  = None
        self.api          = None

        # Position tracking for PnL attribution: {positionId: strategy_name}
        self.tracked: dict[str, str] = {}
        # Cooldown: block new entries for 6 min after a position closes
        self.last_close_time: float = 0.0
        self.COOLDOWN_SECS: int = 0  # no cooldown — re-enter immediately

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
        pt = Decimal(str(self._point()))
        scaled = Decimal(str(value)) / pt
        rmap = {'down': ROUND_FLOOR, 'up': ROUND_CEILING, 'nearest': ROUND_HALF_UP}
        return float(scaled.to_integral_value(rounding=rmap[direction]) * pt)

    def _build_levels(self, side: str, entry: float, wick: float):
        min_d = self._min_stop()
        buf   = self._point()
        if side == 'BUY':
            desired_sl = wick - buf
            sl   = min(desired_sl, entry - min_d)
            sl   = self._snap(sl, 'down')
            risk = max(entry - sl, min_d)
            tp   = self._snap(entry + risk * 6, 'up')
            return sl, tp, self._snap(desired_sl, 'down')
        desired_sl = wick + buf
        sl   = max(desired_sl, entry + min_d)
        sl   = self._snap(sl, 'up')
        risk = max(sl - entry, min_d)
        tp   = self._snap(entry - risk * 6, 'down')
        return sl, tp, self._snap(desired_sl, 'up')

    # ── position-close detection ──
    async def _process_closed_positions(self, live_ids: set[str]):
        """Detect positions that vanished and attribute PnL to their strategy."""
        closed_ids = set(self.tracked.keys()) - live_ids
        if not closed_ids:
            return

        # Fetch today's deals once (if any positions closed)
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
                (d for d in pos_deals if d.get('entryType') == 'DEAL_ENTRY_OUT'),
                None
            )
            if closing:
                pnl        = float(closing.get('profit', 0))
                exit_price = float(closing.get('price', 0))

            # Attribute PnL to strategy
            strategy = next((s for s in self.meta.strategies if s.name == strategy_name), None)
            if strategy:
                strategy.record_trade(pnl)

            self.analytics.log_close(pos_id, exit_price, pnl)
            self.last_close_time = time.time()
            result = "WIN" if pnl > 0 else "LOSS"
            print(
                f"📊 Position closed | {result} | strategy={strategy_name} | PnL=${pnl:.2f} | "
                f"Next entry allowed in {self.COOLDOWN_SECS // 60} min"
            )

        self.meta.report()

    # ── main run loop ──
    async def run(self):
        print(f"🔱 SHIVA V5 LIVE | {self.symbol} | Multi-Strategy Meta Controller")
        self.api = MetaApi(self.token, {
            'provisioningUrl': 'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai',
            'mtUrl':           'https://mt-client-api-v1.london.agiliumtrade.agiliumtrade.ai',
        })
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

            # Print active strategy list
            print("\n── Active Strategies ─────────────────────────")
            for s in self.meta.strategies:
                tag = "ACTIVE" if s.enabled else "DISABLED"
                print(f"  [{tag}] {s.name}")
            print("──────────────────────────────────────────────\n")

            while self.is_running:
                try:
                    # ── 1. Fetch candles and indicators ──
                    candles = await self.account.get_historical_candles(self.symbol, '15m', limit=500)
                    df = FeatureEngine.add_indicators(pd.DataFrame(candles))
                    if df.empty:
                        raise RuntimeError('Indicator frame empty after preprocessing')

                    # ── 2. Get live positions ──
                    positions  = await self.connection.get_positions()
                    live_ids   = {p['id'] for p in positions}
                    current    = next((p for p in positions if p['symbol'] == self.symbol), None)

                    # ── 3. Detect and process closed positions ──
                    await self._process_closed_positions(live_ids)

                    # ── 4. Meta Controller signal ──
                    signal, triggering_strategies = self.meta.get_signal(df)

                    # ── 5. Execute if no open position, cooldown passed, and signal exists ──
                    secs_since_close = time.time() - self.last_close_time
                    in_cooldown = self.last_close_time > 0 and secs_since_close < self.COOLDOWN_SECS
                    if in_cooldown and not current:
                        remaining = int(self.COOLDOWN_SECS - secs_since_close)
                        print(f"⏳ Cooldown active — next entry in {remaining}s")

                    if not current and signal != 0 and not in_cooldown:
                        lot   = 0.01
                        price = await self._get_price()
                        r     = df.iloc[-1]

                        if signal == 1:
                            entry = float(price.get('ask') or price.get('bid') or r['close'])
                            sl, tp, target_sl = self._build_levels('BUY', entry, float(r['low']))
                            print(
                                f"🚀 BUY  {self.symbol} @ {entry:.2f} | "
                                f"SL {sl:.2f} | TP {tp:.2f} | [{triggering_strategies}]"
                            )
                            result = await self.connection.create_market_buy_order(
                                self.symbol, lot, sl, tp,
                                {'comment': f'SHIVA:{triggering_strategies[:20]}'},
                            )
                        else:
                            entry = float(price.get('bid') or price.get('ask') or r['close'])
                            sl, tp, target_sl = self._build_levels('SELL', entry, float(r['high']))
                            print(
                                f"📉 SELL {self.symbol} @ {entry:.2f} | "
                                f"SL {sl:.2f} | TP {tp:.2f} | [{triggering_strategies}]"
                            )
                            result = await self.connection.create_market_sell_order(
                                self.symbol, lot, sl, tp,
                                {'comment': f'SHIVA:{triggering_strategies[:20]}'},
                            )

                        # Track the new position for attribution
                        new_positions = await self.connection.get_positions()
                        new_pos = next(
                            (p for p in new_positions
                             if p['symbol'] == self.symbol and p['id'] not in live_ids),
                            None
                        )
                        if new_pos:
                            pos_id = new_pos['id']
                            side   = 'BUY' if signal == 1 else 'SELL'
                            self.tracked[pos_id] = triggering_strategies
                            self.analytics.log_open(
                                pos_id, side, entry, sl, tp, lot, triggering_strategies
                            )

                    # ── 6. Periodic summary ──
                    summary = self.analytics.summary()
                    if summary.get('total_closed', 0) > 0 and summary['total_closed'] % 5 == 0:
                        print(f"📈 Analytics: {summary}")

                    await asyncio.sleep(60)

                except Exception as e:
                    msg = str(e)
                    print(f"⚠️  Loop warning: {msg}")
                    if 'Market is closed' in msg:
                        await asyncio.sleep(300)
                    elif 'Invalid stops' in msg:
                        await self._refresh_spec()
                        await asyncio.sleep(120)
                    else:
                        await asyncio.sleep(10)

        except Exception as e:
            print(f"❌ Fatal: {e}")
            raise
        finally:
            try:
                if self.connection:
                    await self.connection.close()
            except Exception as e:
                print(f"⚠️  Connection close: {e}")
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

    print(f"🚀 Starting SHIVA V5  |  {SYMBOL}  |  MULTI-STRATEGY")

    # Bootstrap analytics so health server can serve /status
    _bootstrap_analytics = AnalyticsEngine()
    start_health_server(_bootstrap_analytics)

    while True:
        engine = None
        try:
            engine = ExecutionEngine(TOKEN, ACCOUNT_ID, SYMBOL)
            engine.analytics = _bootstrap_analytics  # share the same analytics instance
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
