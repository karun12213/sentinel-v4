"""
SHIVA V6 — IFVG + FVG Scalp | Fixed SL/TP | Dynamic Lot | Max 9 trades/day
Entry: Fresh FVG zone retests + EMA20 bounce
SL: Fixed 0.30pt ($3 at 0.01 lot)  TP: Fixed 1.80pt ($18 at 0.01 lot)  RR: 1:6
Lot scaling every $100 of balance (aggressive compounding toward $500/week):
  $100-$199 → 0.01 lot  ($3 SL  / $18 TP)
  $200-$299 → 0.02 lot  ($6 SL  / $36 TP)
  $300-$399 → 0.03 lot  ($9 SL  / $54 TP)
  $1000+    → 0.10 lot  ($30 SL / $180 TP)
  $3000+    → 0.30 lot  ($90 SL / $540 TP) ← ~$500/week target
  Max: 1.00 lot
Circuit breaker: 3 consecutive losses in a day → pause until next day
Daily limit: MAX_DAILY_TRADES (default 9)
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
                       sl: float, tp: float, lot: float, zone: str, wick: float,
                       strategy_name: str = '', day_count: int = 0, max_day: int = 9):
    webhook = os.getenv('DISCORD_TRADES') or os.getenv('DISCORD_ALERTS', '')
    risk    = abs(entry - sl)
    reward  = abs(tp - entry)
    emoji   = '🚀' if side == 'BUY' else '📉'
    color   = 0x00FF88 if side == 'BUY' else 0xFF4444
    _discord_post(webhook, {"embeds": [{"title": f"{emoji} {side}  {symbol}  [{day_count}/{max_day} today]",
        "color": color,
        "fields": [
            {"name": "Entry",    "value": f"`{entry:.2f}`",       "inline": True},
            {"name": "SL",       "value": f"`{sl:.2f}`",          "inline": True},
            {"name": "TP",       "value": f"`{tp:.2f}`",          "inline": True},
            {"name": "Risk pts", "value": f"`{risk:.2f}`",        "inline": True},
            {"name": "Reward",   "value": f"`{reward:.2f}`",      "inline": True},
            {"name": "R:R",      "value": f"`1 : {reward/risk:.1f}`" if risk else "`—`", "inline": True},
            {"name": "Lot",      "value": f"`{lot}`",             "inline": True},
            {"name": "Zone",     "value": f"`{zone}`",            "inline": True},
            {"name": "Strategy", "value": f"`{strategy_name}`",   "inline": True},
        ],
        "footer": {"text": "SHIVA V6 — IFVG+FVG Scalp"},
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
        "footer": {"text": "SHIVA V6 — IFVG+FVG Scalp"},
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
            self.wfile.write(b'OK - SHIVA V6 LIVE')

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
# DYNAMIC LOT SIZING
# ─────────────────────────────────────────────
def compute_lot_size(capital: float,
                     step: float = 100.0,
                     base_lot: float = 0.01,
                     max_lot: float = 1.0) -> float:
    """
    Scales lot by $100 tiers — aggressive compounding toward $500/week.
      $100 → 0.01  $200 → 0.02  $300 → 0.03  $1000 → 0.10  $3000 → 0.30
    """
    tier = max(1, int(capital // step))
    lot  = round(tier * base_lot, 2)
    return min(lot, max_lot)


# ─────────────────────────────────────────────
# FEATURE ENGINE
# ─────────────────────────────────────────────
class FeatureEngine:
    @staticmethod
    def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [c.lower() for c in df.columns]
        for alias, canon in [('tickvolume', 'volume'), ('brokertime', 'time')]:
            if alias in df.columns and canon not in df.columns:
                df.rename(columns={alias: canon}, inplace=True)

        df['RSI']    = ta.rsi(df['close'], length=14)
        df['ATR']    = ta.atr(df['high'], df['low'], df['close'], length=14)
        df['EMA_20'] = ta.ema(df['close'], length=20)
        df['EMA_50'] = ta.ema(df['close'], length=50)
        df['EMA_200']= ta.ema(df['close'], length=200)

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
    def get_signal_and_wick(self, df: pd.DataFrame) -> tuple[int, float]:
        """Return (+1=BUY / -1=SELL / 0=HOLD, wick_price)"""

    def generate_signal(self, df: pd.DataFrame) -> int:
        sig, _ = self.get_signal_and_wick(df)
        return sig


# ─────────────────────────────────────────────
# IFVG + DISCOUNT/PREMIUM STRATEGY  (quality — high conviction)
# ─────────────────────────────────────────────
class IFVGStrategy(BaseStrategy):
    """
    10-point checklist (all must pass):
    ① Zone width ≥ 0.2% of price
    ② IFVG age ≤ 20 bars
    ③ Zone not invalidated post-formation
    ④ EMA-200 trend alignment
    ⑤ Discount (BUY) / Premium (SELL) zone
    ⑥ RSI range (BUY 20-55, SELL 45-80)
    ⑦ Candle enters IFVG zone from correct side
    ⑧ Candle body confirms zone held
    ⑨ Bullish/bearish rejection candle
    ⑩ Risk ≤ 1.5 × ATR
    """
    LOOKBACK      = 50
    ZONE_BARS     = 100
    MAX_AGE       = 20
    MIN_WIDTH_PCT = 0.002
    MAX_RISK_ATR  = 1.5

    def __init__(self):
        super().__init__("IFVG_SMC")

    def _find_fvgs(self, bars: pd.DataFrame) -> list[dict]:
        bars = bars.reset_index(drop=True)
        n    = len(bars)
        fvgs = []
        for i in range(1, n - 1):
            ph = float(bars.iloc[i - 1]['high'])
            pl = float(bars.iloc[i - 1]['low'])
            nh = float(bars.iloc[i + 1]['high'])
            nl = float(bars.iloc[i + 1]['low'])
            if ph < nl:
                fvgs.append({'type': 'bull', 'low': ph, 'high': nl, 'idx': i})
            if pl > nh:
                fvgs.append({'type': 'bear', 'low': nh, 'high': pl, 'idx': i})
        return fvgs

    def _get_ifvgs(self, df: pd.DataFrame) -> list[dict]:
        bars  = df.reset_index(drop=True)
        n     = len(bars)
        fvgs  = self._find_fvgs(bars)
        ifvgs = []

        for fvg in fvgs:
            start = fvg['idx'] + 2
            if start >= n:
                continue
            after = bars.iloc[start:]
            age   = n - 1 - fvg['idx']

            if fvg['type'] == 'bull':
                fill_mask = after['low'] <= fvg['low']
                if not fill_mask.any():
                    continue
                fill_pos  = fill_mask.idxmax()
                post_fill = bars.loc[fill_pos + 1:]
                if not post_fill.empty and float(post_fill['close'].max()) > fvg['high']:
                    continue
                ifvgs.append({
                    'type': 'bear', 'low': fvg['low'], 'high': fvg['high'],
                    'age': age, 'width': fvg['high'] - fvg['low'],
                })
            else:
                fill_mask = after['high'] >= fvg['high']
                if not fill_mask.any():
                    continue
                fill_pos  = fill_mask.idxmax()
                post_fill = bars.loc[fill_pos + 1:]
                if not post_fill.empty and float(post_fill['close'].min()) < fvg['low']:
                    continue
                ifvgs.append({
                    'type': 'bull', 'low': fvg['low'], 'high': fvg['high'],
                    'age': age, 'width': fvg['high'] - fvg['low'],
                })

        return ifvgs

    def get_signal_and_wick(self, df: pd.DataFrame) -> tuple[int, float]:
        if len(df) < self.ZONE_BARS + 5:
            return 0, 0.0

        r      = df.iloc[-1]
        atr    = float(r.get('ATR', 0) or 0)
        if atr == 0:
            return 0, 0.0

        close  = float(r['close'])
        open_  = float(r['open'])
        high   = float(r['high'])
        low    = float(r['low'])
        ema200 = float(r.get('EMA_200', close) or close)
        rsi    = float(r.get('RSI', 50) or 50)

        uptrend   = close > ema200
        downtrend = close < ema200

        zone_df    = df.tail(self.ZONE_BARS)
        swing_high = float(zone_df['high'].max())
        swing_low  = float(zone_df['low'].min())
        midpoint   = (swing_high + swing_low) / 2.0
        in_discount = close < midpoint
        in_premium  = close > midpoint

        ifvgs = self._get_ifvgs(df.tail(self.LOOKBACK + 3))
        min_w = close * self.MIN_WIDTH_PCT

        if in_discount and uptrend and 20 <= rsi <= 55:
            candidates = []
            for z in ifvgs:
                if z['type'] != 'bull':                          continue
                if z['age'] > self.MAX_AGE:                      continue
                if z['width'] < min_w:                           continue
                if low  > z['high']:                             continue
                if high < z['low']:                              continue
                if close < z['low']:                             continue
                if close < open_:                                continue
                risk = close - z['low']
                if risk <= 0 or risk > atr * self.MAX_RISK_ATR: continue
                candidates.append(z)
            if candidates:
                z = sorted(candidates, key=lambda x: (x['age'], x['width']))[0]
                print(f"  🔵 IFVG BUY  [{z['low']:.3f}–{z['high']:.3f}] age={z['age']}  Discount  RSI={rsi:.0f}")
                return 1, z['low']

        if in_premium and downtrend and 45 <= rsi <= 80:
            candidates = []
            for z in ifvgs:
                if z['type'] != 'bear':                          continue
                if z['age'] > self.MAX_AGE:                      continue
                if z['width'] < min_w:                           continue
                if high < z['low']:                              continue
                if low  > z['high']:                             continue
                if close > z['high']:                            continue
                if close > open_:                                continue
                risk = z['high'] - close
                if risk <= 0 or risk > atr * self.MAX_RISK_ATR: continue
                candidates.append(z)
            if candidates:
                z = sorted(candidates, key=lambda x: (x['age'], x['width']))[0]
                print(f"  🔴 IFVG SELL [{z['low']:.3f}–{z['high']:.3f}] age={z['age']}  Premium   RSI={rsi:.0f}")
                return -1, z['high']

        return 0, 0.0


# ─────────────────────────────────────────────
# FVG SCALP STRATEGY  (frequency — generates more signals)
# ─────────────────────────────────────────────
class FVGScalpStrategy(BaseStrategy):
    """
    Fresh unmitigated FVG first-touch scalp.
    Requires EMA-200 trend alignment + reversal candle.
    Age ≤ 5 bars (very fresh zones only, no re-entry).
    Designed for 5m candles to generate ~9 signals/day.
    """
    LOOKBACK = 40
    MAX_AGE  = 5    # only take the very first touch

    def __init__(self):
        super().__init__("FVG_SCALP")

    def _find_fvgs(self, bars: pd.DataFrame) -> list[dict]:
        bars = bars.reset_index(drop=True)
        n    = len(bars)
        fvgs = []
        for i in range(1, n - 1):
            ph = float(bars.iloc[i - 1]['high'])
            pl = float(bars.iloc[i - 1]['low'])
            nh = float(bars.iloc[i + 1]['high'])
            nl = float(bars.iloc[i + 1]['low'])
            if ph < nl:
                fvgs.append({'type': 'bull', 'low': ph, 'high': nl, 'idx': i})
            if pl > nh:
                fvgs.append({'type': 'bear', 'low': nh, 'high': pl, 'idx': i})
        return fvgs

    def get_signal_and_wick(self, df: pd.DataFrame) -> tuple[int, float]:
        if len(df) < 60:
            return 0, 0.0

        r      = df.iloc[-1]
        close  = float(r['close'])
        open_  = float(r['open'])
        high   = float(r['high'])
        low    = float(r['low'])
        ema200 = float(r.get('EMA_200', close) or close)
        rsi    = float(r.get('RSI', 50) or 50)

        ema20  = float(r.get('EMA_20', close) or close)
        atr    = float(r.get('ATR', 0) or 0)
        uptrend   = close > ema200
        downtrend = close < ema200

        # Skip when volatility is too high for fixed SL (ATR > 2× SL points)
        sl_pts = float(os.getenv('SL_POINTS', '0.30'))
        if atr > sl_pts * 2.0:
            return 0, 0.0

        bars = df.tail(self.LOOKBACK + 3)
        n    = len(bars.reset_index(drop=True))
        fvgs = self._find_fvgs(bars)

        r_prev     = df.iloc[-2]
        prev_close = float(r_prev['close'])
        prev_ema20 = float(r_prev.get('EMA_20', close) or close)
        ema20_slope = ema20 - prev_ema20   # positive = EMA20 rising, negative = falling
        bar_mid    = (high + low) / 2.0

        # BUY: bullish FVG retest in uptrend, EMA20 rising, strong close above bar mid
        if uptrend and rsi < 65 and ema20_slope > 0 and close >= bar_mid:
            for fvg in reversed(fvgs):
                if fvg['type'] != 'bull':        continue
                age = n - 1 - fvg['idx']
                if age > self.MAX_AGE:           continue
                if age < 1:                      continue
                if low  > fvg['high']:           continue  # didn't enter zone
                if close < fvg['low']:           continue  # broke through zone — skip
                if close < open_:               continue  # need bullish close
                print(f"  🟢 FVG SCALP BUY  [{fvg['low']:.3f}–{fvg['high']:.3f}] age={age}  RSI={rsi:.0f}  ATR={atr:.3f}")
                return 1, fvg['low']

        # SELL: bearish FVG retest in downtrend, EMA20 falling, RSI > 50 (not yet oversold)
        if downtrend and 50 < rsi < 80 and ema20_slope < 0 and close <= bar_mid:
            for fvg in reversed(fvgs):
                if fvg['type'] != 'bear':        continue
                age = n - 1 - fvg['idx']
                if age > self.MAX_AGE:           continue
                if age < 1:                      continue
                if high < fvg['low']:            continue  # didn't reach zone
                if close > fvg['high']:          continue  # broke through zone — skip
                if close > open_:               continue  # need bearish close
                print(f"  🔴 FVG SCALP SELL [{fvg['low']:.3f}–{fvg['high']:.3f}] age={age}  RSI={rsi:.0f}  ATR={atr:.3f}")
                return -1, fvg['high']

        return 0, 0.0


# ─────────────────────────────────────────────
# EMA BOUNCE STRATEGY  (frequency — EMA20 dynamic S/R crosses)
# ─────────────────────────────────────────────
class EMABounceStrategy(BaseStrategy):
    """
    EMA20 dynamic support/resistance bounce.
    BUY:  prev close < EMA20, current close > EMA20 + bullish candle (uptrend)
    SELL: prev close > EMA20, current close < EMA20 + bearish candle (downtrend)
    ATR filter same as FVGScalpStrategy.
    Provides 3-5 extra signals per day on 5m.
    """
    def __init__(self):
        super().__init__("EMA_BOUNCE")

    def get_signal_and_wick(self, df: pd.DataFrame) -> tuple[int, float]:
        if len(df) < 60:
            return 0, 0.0

        r    = df.iloc[-1]
        prev = df.iloc[-2]

        close  = float(r['close'])
        open_  = float(r['open'])
        high   = float(r['high'])
        low    = float(r['low'])
        ema20  = float(r.get('EMA_20', close) or close)
        ema200 = float(r.get('EMA_200', close) or close)
        atr    = float(r.get('ATR', 0) or 0)
        rsi    = float(r.get('RSI', 50) or 50)

        prev_close = float(prev['close'])
        prev_ema20 = float(prev.get('EMA_20', prev_close) or prev_close)

        sl_pts  = float(os.getenv('SL_POINTS', '0.30'))
        tp_pts  = sl_pts * float(os.getenv('TP_MULT', '6.0'))
        # Skip if ATR is too high (whipsaw) or too low (market not moving enough for TP)
        min_atr = float(os.getenv('MIN_ATR', '0.0'))
        if atr > sl_pts * 2.0:          return 0, 0.0   # too volatile for tight SL
        if min_atr > 0 and atr < min_atr: return 0, 0.0  # too quiet for TP to be reachable

        bar_mid = (high + low) / 2.0
        uptrend   = close > ema200
        downtrend = close < ema200

        ema20_slope = ema20 - prev_ema20   # EMA20 direction filter

        # BUY: bounced off EMA20 in uptrend, EMA20 slope rising
        if (uptrend and
                ema20_slope > 0 and           # EMA20 rising (confirmed uptrend)
                prev_close < prev_ema20 and   # previous bar below EMA20
                close > ema20 and             # current bar crossed back above
                close > open_ and             # bullish body
                close >= bar_mid and          # strong close
                rsi < 65):
            wick = low
            print(f"  🟦 EMA BOUNCE BUY  close={close:.3f} > EMA20={ema20:.3f}  RSI={rsi:.0f}  ATR={atr:.3f}")
            return 1, wick

        # SELL: rejected at EMA20 in downtrend, RSI > 50 (not already oversold = crash mode)
        if (downtrend and
                ema20_slope < 0 and           # EMA20 falling (confirmed downtrend)
                prev_close > prev_ema20 and   # previous bar above EMA20
                close < ema20 and             # current bar crossed back below
                close < open_ and             # bearish body
                close <= bar_mid and          # strong close
                50 < rsi < 80):              # RSI above 50 = momentum still elevated
            wick = high
            print(f"  🟧 EMA BOUNCE SELL close={close:.3f} < EMA20={ema20:.3f}  RSI={rsi:.0f}  ATR={atr:.3f}")
            return -1, wick

        return 0, 0.0


# ─────────────────────────────────────────────
# META CONTROLLER
# ─────────────────────────────────────────────
class MetaController:
    def __init__(self, strategies: list[BaseStrategy]):
        self.strategies   = strategies
        self._last_report = 0.0

    def get_signal_and_wick(self, df: pd.DataFrame) -> tuple[int, float, str]:
        """Returns (signal, wick, strategy_name)"""
        for s in self.strategies:
            if not s.enabled:
                continue
            sig, wick = s.get_signal_and_wick(df)
            if sig != 0:
                return sig, wick, s.name
        return 0, 0.0, ''

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
    COOLDOWN_SECS = 300  # 5 min after each trade (allows ~9-12/day)

    def __init__(self, token: str, account_id: str, symbol: str = "USOIL"):
        self.token      = token
        self.account_id = account_id
        self.symbol     = symbol
        self.is_running = True

        # Strategies: FVG_SCALP (quality zones) + EMA_BOUNCE (frequency)
        # IFVG_SMC excluded in fixed-SL mode (designed for dynamic ATR-based SL)
        self.fvg_scalp     = FVGScalpStrategy()
        self.ema_bounce    = EMABounceStrategy()
        self.meta          = MetaController([self.fvg_scalp, self.ema_bounce])
        self.analytics     = AnalyticsEngine()

        self.connection  = None
        self.account     = None
        self.symbol_spec = None
        self.api         = None

        self.tracked: dict[str, str] = {}
        self.last_close_time: float  = 0.0

        # Daily trade counter + circuit breaker
        self.max_daily_trades   = int(os.getenv('MAX_DAILY_TRADES', '9'))
        self.daily_trades       = 0
        self.daily_reset_date   = datetime.now(timezone.utc).date()
        self.consec_losses      = 0
        self.circuit_broken     = False   # 3 consec losses → pause rest of day
        self.max_consec_losses  = int(os.getenv('MAX_CONSEC_LOSSES', '3'))

    def stop(self, *_):
        self.is_running = False
        print("🛑 Shutdown signal received")

    def _check_daily_reset(self):
        today = datetime.now(timezone.utc).date()
        if today != self.daily_reset_date:
            print(f"📅 New trading day ({today}) — daily counter reset (was {self.daily_trades} trades)")
            self.daily_trades   = 0
            self.daily_reset_date = today
            self.consec_losses  = 0
            self.circuit_broken = False

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

    async def _get_balance(self) -> float:
        """Fetch current account balance for dynamic lot sizing."""
        try:
            info = await self.connection.get_account_information()
            return float(info.get('balance', 100.0))
        except Exception as e:
            print(f"⚠️  Balance fetch: {e}")
            return 100.0

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
        Fixed SL/TP mode: uses SL_POINTS env var (default 0.30 pts for $3 at 0.01 lot).
        Falls back to IFVG wick-based dynamic SL if SL_POINTS not set.
        """
        sl_pts  = float(os.getenv('SL_POINTS',  '0.30'))
        tp_mult = float(os.getenv('TP_MULT',    '6.0'))
        min_d   = self._min_stop()

        if sl_pts > 0:
            if side == 'BUY':
                sl = self._snap(entry - max(sl_pts, min_d), 'down')
                tp = self._snap(entry + sl_pts * tp_mult,   'up')
            else:
                sl = self._snap(entry + max(sl_pts, min_d), 'up')
                tp = self._snap(entry - sl_pts * tp_mult,   'down')
            return sl, tp

        # Dynamic fallback (IFVG boundary)
        buf = self._point()
        if side == 'BUY':
            desired_sl = wick - buf
            sl   = self._snap(min(desired_sl, entry - min_d), 'down')
            risk = max(entry - sl, min_d)
            tp   = self._snap(entry + risk * tp_mult, 'up')
        else:
            desired_sl = wick + buf
            sl   = self._snap(max(desired_sl, entry + min_d), 'up')
            risk = max(sl - entry, min_d)
            tp   = self._snap(entry - risk * tp_mult, 'down')
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

            # Circuit breaker tracking
            if pnl > 0:
                self.consec_losses = 0
            else:
                self.consec_losses += 1
                if self.consec_losses >= self.max_consec_losses:
                    self.circuit_broken = True
                    print(f"⚡ CIRCUIT BREAKER: {self.consec_losses} consecutive losses — pausing for today")

            print(
                f"📊 Position closed | {result} | PnL=${pnl:.2f} | "
                f"ConsecLoss={self.consec_losses} | "
                f"Daily: {self.daily_trades}/{self.max_daily_trades} | "
                f"Cooldown: {self.COOLDOWN_SECS // 60} min"
            )
            rec = next((r for r in self.analytics.records if r['position_id'] == pos_id), None)
            entry_price = rec['entry'] if rec else 0.0
            side        = rec['side']  if rec else '?'
            discord_trade_close(side, self.symbol, entry_price, exit_price, pnl)

        self.meta.report()

    # ── candle fetch ──

    async def _fetch_candles(self) -> pd.DataFrame:
        start_time = datetime.now(timezone.utc) - timedelta(days=5)
        candles = await self.account.get_historical_candles(
            self.symbol, '5m', start_time, 500
        )
        if not candles:
            raise RuntimeError('No candles returned')
        df = pd.DataFrame(candles)
        return FeatureEngine.add_indicators(df)

    # ── main loop ──

    async def run(self):
        print(f"🔱 SHIVA V6 LIVE | {self.symbol} | IFVG + FVG Scalp | Max {self.max_daily_trades}/day")
        self.api = MetaApi(self.token)
        try:
            self.account = await self.api.metatrader_account_api.get_account(self.account_id)
            await self.account.wait_connected()
            self.connection = self.account.get_rpc_connection()
            await self.connection.connect()
            await self.connection.wait_synchronized()
            await self._refresh_spec()

            sl_pts  = float(os.getenv('SL_POINTS', '0.30'))
            tp_mult = float(os.getenv('TP_MULT', '6.0'))
            print(
                f"✅ MetaApi synchronized | {self.symbol}\n"
                f"── Strategy ──────────────────────────────────────────\n"
                f"  [1] FVG_SCALP  — fresh FVG first-touch scalp\n"
                f"  [2] EMA_BOUNCE — EMA20 dynamic S/R bounce\n"
                f"  Fixed SL: {sl_pts} pts | TP: {sl_pts * tp_mult:.2f} pts ({tp_mult:.0f}R)\n"
                f"  Dynamic lot: $100=$0.01  $300=$0.03  $600=$0.06  $900=$0.09\n"
                f"  Daily limit: {self.max_daily_trades} trades | Cooldown: {self.COOLDOWN_SECS // 60} min\n"
                f"─────────────────────────────────────────────────────\n"
            )

            while self.is_running:
                try:
                    self._check_daily_reset()

                    df = await self._fetch_candles()
                    if df.empty:
                        raise RuntimeError('Empty indicator frame')

                    positions = await self.connection.get_positions()
                    live_ids  = {p['id'] for p in positions}
                    current   = next((p for p in positions if p['symbol'] == self.symbol), None)

                    await self._process_closed_positions(live_ids)

                    # Cooldown after last trade
                    secs_since_close = time.time() - self.last_close_time
                    in_cooldown = self.last_close_time > 0 and secs_since_close < self.COOLDOWN_SECS
                    if in_cooldown and not current:
                        remaining = int(self.COOLDOWN_SECS - secs_since_close)
                        print(f"⏳ Cooldown — next entry in {remaining}s  |  Daily: {self.daily_trades}/{self.max_daily_trades}")
                        await asyncio.sleep(30)
                        continue

                    # Daily limit check
                    if self.daily_trades >= self.max_daily_trades:
                        print(f"🚫 Daily limit reached ({self.daily_trades}/{self.max_daily_trades}) — waiting for next day")
                        await asyncio.sleep(300)
                        continue

                    # Circuit breaker: 3 consecutive losses → pause rest of day
                    if self.circuit_broken:
                        print(f"⚡ Circuit breaker active ({self.consec_losses} consec losses) — resuming tomorrow")
                        await asyncio.sleep(300)
                        continue

                    # Get signal
                    if not current:
                        sig, wick, strat_name = self.meta.get_signal_and_wick(df)
                    else:
                        sig, wick, strat_name = 0, 0.0, ''

                    # Execute
                    if not current and sig != 0 and wick != 0.0:
                        # Dynamic lot based on live balance
                        balance = await self._get_balance()
                        lot     = compute_lot_size(balance)
                        price   = await self._get_price()

                        if sig == 1:
                            entry  = float(price.get('ask') or price.get('bid') or df.iloc[-1]['close'])
                            sl, tp = self._build_levels('BUY', entry, wick)
                            sl_usd = abs(entry - sl) * lot * 1000
                            tp_usd = abs(tp - entry) * lot * 1000
                            print(f"🚀 BUY  {self.symbol} @ {entry:.2f} | SL {sl:.2f} (${sl_usd:.0f}) | TP {tp:.2f} (${tp_usd:.0f}) | lot={lot} | bal=${balance:.0f} | [{strat_name}]")
                            result = await self.connection.create_market_buy_order(
                                self.symbol, lot, sl, tp,
                                {'comment': f'SHIVA:{strat_name}'},
                            )
                        else:
                            entry  = float(price.get('bid') or price.get('ask') or df.iloc[-1]['close'])
                            sl, tp = self._build_levels('SELL', entry, wick)
                            sl_usd = abs(sl - entry) * lot * 1000
                            tp_usd = abs(entry - tp) * lot * 1000
                            print(f"📉 SELL {self.symbol} @ {entry:.2f} | SL {sl:.2f} (${sl_usd:.0f}) | TP {tp:.2f} (${tp_usd:.0f}) | lot={lot} | bal=${balance:.0f} | [{strat_name}]")
                            result = await self.connection.create_market_sell_order(
                                self.symbol, lot, sl, tp,
                                {'comment': f'SHIVA:{strat_name}'},
                            )

                        new_positions = await self.connection.get_positions()
                        new_pos = next(
                            (p for p in new_positions
                             if p['symbol'] == self.symbol and p['id'] not in live_ids),
                            None,
                        )
                        if new_pos:
                            side = 'BUY' if sig == 1 else 'SELL'
                            zone = "DISCOUNT" if sig == 1 else "PREMIUM"
                            self.tracked[new_pos['id']] = strat_name
                            self.analytics.log_open(new_pos['id'], side, entry, sl, tp, lot, strat_name)
                            self.daily_trades += 1
                            discord_trade_open(
                                side, self.symbol, entry, sl, tp, lot, zone, wick,
                                strat_name, self.daily_trades, self.max_daily_trades
                            )
                            print(f"  Daily trades: {self.daily_trades}/{self.max_daily_trades}  |  Lot tier: {lot} (bal=${balance:.0f})")

                    await asyncio.sleep(30)  # check every 30s (5m candles)

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

    print(f"🚀 Starting SHIVA V6  |  {SYMBOL}  |  IFVG+FVG Scalp  |  Fixed SL/TP")

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
