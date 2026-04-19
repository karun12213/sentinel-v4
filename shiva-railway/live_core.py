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

# Phase 1 — HTF Bias Engine
try:
    from htf_bias import HTFBiasAgent
    _HTF_BIAS_AVAILABLE = True
except ImportError:
    _HTF_BIAS_AVAILABLE = False

# Phase 2 — Kill Zone Time Filter
try:
    from kill_zone import KillZoneAgent
    _KILL_ZONE_AVAILABLE = True
except ImportError:
    _KILL_ZONE_AVAILABLE = False

# Phase 3 — News Filter
try:
    from news_filter import NewsFilterAgent
    _NEWS_FILTER_AVAILABLE = True
except ImportError:
    _NEWS_FILTER_AVAILABLE = False

# Phase 4 — Confluence Scorer
try:
    from confluence import ConfluenceScorer, MarketState, EntrySize
    _CONFLUENCE_AVAILABLE = True
except ImportError:
    _CONFLUENCE_AVAILABLE = False

# Phase 5 — Regime Classifier
try:
    from regime import RegimeClassifier
    _REGIME_AVAILABLE = True
except ImportError:
    _REGIME_AVAILABLE = False

# Phase 6 — CNN Ensemble
try:
    from cnn_ensemble import CNNEnsembleAgent
    _CNN_AVAILABLE = True
except ImportError:
    _CNN_AVAILABLE = False

# Phase 7 — Judas Swing
try:
    from judas_swing import JudasSwingAgent
    _JUDAS_AVAILABLE = True
except ImportError:
    _JUDAS_AVAILABLE = False

# Phase 8 — Dynamic SL
try:
    from dynamic_sl import DynamicSLEngine, check_breakeven, trail_by_structure
    _DYNAMIC_SL_AVAILABLE = True
except ImportError:
    _DYNAMIC_SL_AVAILABLE = False

# Phase 9 — Circuit Breaker
try:
    from circuit_breaker import CircuitBreakerAgent
    _CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    _CIRCUIT_BREAKER_AVAILABLE = False

# Phase 11 — Post-Trade Logger
try:
    from post_trade import PostTradeAgent
    _POST_TRADE_AVAILABLE = True
except ImportError:
    _POST_TRADE_AVAILABLE = False
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
        "footer": {"text": "SHIVA V11 — FVG+OB+LS Scalp"},
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
        "footer": {"text": "SHIVA V11 — FVG+OB+LS Scalp"},
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
            self.wfile.write(b'OK - SHIVA V11 LIVE')

    def log_message(self, *_):
        return


def discord_circuit_alert(reason: str):
    webhook = os.getenv('DISCORD_TRADES') or os.getenv('DISCORD_ALERTS', '')
    _discord_post(webhook, {"embeds": [{"title": "⚡ Circuit Breaker Alert",
        "color": 0xFF8800,
        "fields": [{"name": "Reason", "value": f"`{reason}`", "inline": False}],
        "footer": {"text": "SHIVA — Phase 9 Circuit Breaker"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }]})


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
            adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
            if adx_df is not None and not adx_df.empty:
                adx_col = next((c for c in adx_df.columns if c.startswith('ADX_')), None)
                if adx_col:
                    df['ADX_14'] = adx_df[adx_col].values
        except Exception:
            pass

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

        # VWAP — resets daily; used as intraday institutional reference
        try:
            if 'volume' in df.columns and df['volume'].sum() > 0:
                idx = df.index
                if not isinstance(idx, pd.DatetimeIndex):
                    if 'time' in df.columns:
                        idx = pd.to_datetime(df['time'], utc=True)
                    else:
                        raise ValueError("no datetime")
                if getattr(idx, 'tz', None) is None:
                    idx = idx.tz_localize('UTC')
                date_grp = idx.normalize()
                tp    = (df['high'] + df['low'] + df['close']) / 3.0
                tp_v  = tp * df['volume'].replace(0, np.nan).fillna(1)
                vol_c = df['volume'].replace(0, np.nan).fillna(1)
                df['VWAP'] = tp_v.groupby(date_grp).cumsum() / vol_c.groupby(date_grp).cumsum()
        except Exception:
            pass

        # Volume SMA(20) — used for volume-confirmation filter
        if 'volume' in df.columns:
            df['VOL_SMA20'] = df['volume'].rolling(20).mean()

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
        ema50  = float(r.get('EMA_50', close) or close)
        atr    = float(r.get('ATR', 0) or 0)
        adx    = float(r.get('ADX_14', 0) or 0)
        uptrend   = close > ema200
        downtrend = close < ema200

        sl_pts  = float(os.getenv('SL_POINTS', '0.30'))
        adx_min = float(os.getenv('ADX_MIN', '25'))   # V12: raised to 25 for quality filter
        if atr > sl_pts * 1.5:           return 0, 0.0  # too volatile
        if adx > 0 and adx < adx_min:   return 0, 0.0  # no trend

        # Session filter: skip weekend open gap / Sunday gaps only
        try:
            bar_dt  = df.index[-1]
            bar_hour = bar_dt.hour if hasattr(bar_dt, 'hour') else None
            if bar_hour is not None and not (1 <= bar_hour < 23):
                return 0, 0.0
        except Exception:
            pass

        vwap = float(r.get('VWAP', 0) or 0)   # available for display in logs

        bars = df.tail(self.LOOKBACK + 3)
        n    = len(bars.reset_index(drop=True))
        fvgs = self._find_fvgs(bars)

        r_prev      = df.iloc[-2]
        prev_ema20  = float(r_prev.get('EMA_20', close) or close)
        ema20_slope = ema20 - prev_ema20
        ema50_slope = ema50 - float(df.iloc[-6].get('EMA_50', ema50) or ema50)
        bar_mid     = (high + low) / 2.0

        # BUY: FVG retest in uptrend, EMA20+EMA50 rising, RSI 55–68 (quality zone)
        if uptrend and 55 <= rsi <= 68 and ema20_slope > 0 and ema50_slope >= 0 and close >= bar_mid:
            for fvg in reversed(fvgs):
                if fvg['type'] != 'bull':        continue
                age = n - 1 - fvg['idx']
                if age > self.MAX_AGE:           continue
                if age < 1:                      continue
                if low  > fvg['high']:           continue
                if close < fvg['low']:           continue
                if close < open_:               continue
                print(f"  🟢 FVG SCALP BUY  [{fvg['low']:.3f}–{fvg['high']:.3f}] age={age}  RSI={rsi:.0f}  ATR={atr:.3f}  ADX={adx:.0f}")
                return 1, fvg['low']

        if os.getenv('SELL_ENABLED', '1') == '0':
            return 0, 0.0

        # SELL: bearish FVG retest, downtrend, EMA20+EMA50 both falling, RSI 32–45 (quality zone)
        if downtrend and 32 <= rsi <= 45 and ema20_slope < 0 and ema50_slope <= 0 and close <= bar_mid:
            for fvg in reversed(fvgs):
                if fvg['type'] != 'bear':        continue
                age = n - 1 - fvg['idx']
                if age > self.MAX_AGE:           continue
                if age < 1:                      continue
                if high < fvg['low']:            continue  # didn't reach zone
                if close > fvg['high']:          continue  # broke through zone — skip
                if close > open_:               continue  # need bearish close
                print(f"  🔴 FVG SCALP SELL [{fvg['low']:.3f}–{fvg['high']:.3f}] age={age}  RSI={rsi:.0f}  ATR={atr:.3f}  ADX={adx:.0f}")
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
        ema50  = float(r.get('EMA_50', close) or close)
        ema200 = float(r.get('EMA_200', close) or close)
        atr    = float(r.get('ATR', 0) or 0)
        adx    = float(r.get('ADX_14', 0) or 0)
        rsi    = float(r.get('RSI', 50) or 50)

        prev_close = float(prev['close'])
        prev_ema20 = float(prev.get('EMA_20', prev_close) or prev_close)

        sl_pts  = float(os.getenv('SL_POINTS', '0.30'))
        adx_min = float(os.getenv('ADX_MIN', '20'))
        if atr > sl_pts * 1.5:           return 0, 0.0
        if adx > 0 and adx < adx_min:   return 0, 0.0

        bar_mid   = (high + low) / 2.0
        uptrend   = close > ema200
        downtrend = close < ema200
        ema20_slope = ema20 - prev_ema20
        ema50_slope = ema50 - float(df.iloc[-6].get('EMA_50', ema50) or ema50)

        # BUY: EMA20 bounce in uptrend, EMA20+EMA50 rising, RSI 35–68
        if (uptrend and
                35 < rsi < 65 and
                ema20_slope > 0 and
                ema50_slope >= 0 and
                prev_close < prev_ema20 and
                close > ema20 and
                close > open_ and
                close >= bar_mid):
            wick = low
            print(f"  🟦 EMA BOUNCE BUY  close={close:.3f} > EMA20={ema20:.3f}  RSI={rsi:.0f}  ATR={atr:.3f}  ADX={adx:.0f}")
            return 1, wick

        if os.getenv('SELL_ENABLED', '1') == '0':
            return 0, 0.0

        # SELL: EMA20 rejection in downtrend, EMA20+EMA50 both falling, RSI 50–75
        if (downtrend and
                50 < rsi < 75 and
                ema20_slope < 0 and
                ema50_slope <= 0 and
                prev_close > prev_ema20 and
                close < ema20 and
                close < open_ and
                close <= bar_mid):
            wick = high
            print(f"  🟧 EMA BOUNCE SELL close={close:.3f} < EMA20={ema20:.3f}  RSI={rsi:.0f}  ATR={atr:.3f}  ADX={adx:.0f}")
            return -1, wick

        return 0, 0.0


# ─────────────────────────────────────────────
# ORDER BLOCK STRATEGY  (SMC — last opposing candle before impulse)
# ─────────────────────────────────────────────
class OrderBlockStrategy(BaseStrategy):
    """
    Bullish OB: last bearish candle before a strong bullish impulse ≥ 0.8×ATR.
    Bearish OB: last bullish candle before a strong bearish impulse ≥ 0.8×ATR.
    Enter on retest into OB zone with confirming close + trend alignment.
    Complements FVG_SCALP — activates on higher-quality institutional zones.
    """
    LOOKBACK         = 30
    MAX_AGE          = 12   # fresher OBs are more reliable
    MIN_IMPULSE_ATR  = 1.3  # require a strong impulse to validate the OB

    def __init__(self):
        super().__init__("OB_SMC")

    def _find_obs(self, bars: pd.DataFrame, atr: float) -> list[dict]:
        bars = bars.reset_index(drop=True)
        n    = len(bars)
        obs  = []
        for i in range(2, n - 4):
            bar = bars.iloc[i]
            # Bullish OB: bearish candle → subsequent bullish impulse
            if float(bar['close']) < float(bar['open']):
                future = bars.iloc[i + 1: min(i + 5, n)]
                if not future.empty:
                    impulse = float(future['high'].max()) - float(bar['high'])
                    if impulse >= atr * self.MIN_IMPULSE_ATR:
                        obs.append({'type': 'bull', 'low': float(bar['low']),
                                    'high': float(bar['high']), 'idx': i})
            # Bearish OB: bullish candle → subsequent bearish impulse
            elif float(bar['close']) > float(bar['open']):
                future = bars.iloc[i + 1: min(i + 5, n)]
                if not future.empty:
                    impulse = float(bar['low']) - float(future['low'].min())
                    if impulse >= atr * self.MIN_IMPULSE_ATR:
                        obs.append({'type': 'bear', 'low': float(bar['low']),
                                    'high': float(bar['high']), 'idx': i})
        return obs

    def get_signal_and_wick(self, df: pd.DataFrame) -> tuple[int, float]:
        if len(df) < 65:
            return 0, 0.0

        r      = df.iloc[-1]
        close  = float(r['close'])
        open_  = float(r['open'])
        high   = float(r['high'])
        low    = float(r['low'])
        ema200 = float(r.get('EMA_200', close) or close)
        ema20  = float(r.get('EMA_20',  close) or close)
        ema50  = float(r.get('EMA_50',  close) or close)
        rsi    = float(r.get('RSI', 50)   or 50)
        atr    = float(r.get('ATR', 0)    or 0)
        adx    = float(r.get('ADX_14', 0) or 0)

        if atr == 0: return 0, 0.0

        sl_pts  = float(os.getenv('SL_POINTS', '0.30'))
        adx_min = float(os.getenv('ADX_MIN',   '20'))
        if atr > sl_pts * 1.5:         return 0, 0.0
        if adx > 0 and adx < adx_min: return 0, 0.0

        # Session filter: OBs are most reliable during London+NY active hours
        try:
            bh = df.index[-1].hour if hasattr(df.index[-1], 'hour') else None
            if bh is not None and not (7 <= bh < 20):
                return 0, 0.0
        except Exception:
            pass

        # Volume confirmation — OBs require meaningful participation
        try:
            vol_sma = float(r.get('VOL_SMA20', 0) or 0)
            cur_vol = float(r.get('volume',    0) or 0)
            if vol_sma > 0 and cur_vol < vol_sma * 0.75:
                return 0, 0.0
        except Exception:
            pass

        uptrend   = close > ema200
        downtrend = close < ema200
        r_prev    = df.iloc[-2]
        ema20_slope = ema20 - float(r_prev.get('EMA_20', ema20) or ema20)
        ema50_slope = ema50 - float(df.iloc[-6].get('EMA_50', ema50) or ema50)
        bar_mid     = (high + low) / 2.0

        bars = df.tail(self.LOOKBACK + 5)
        n    = len(bars)
        obs  = self._find_obs(bars, atr)

        # BUY: bullish OB retest — price touches OB, closes bullishly above midpoint
        if uptrend and 35 < rsi < 65 and ema20_slope > 0 and ema50_slope >= 0:
            for ob in reversed(obs):
                if ob['type'] != 'bull': continue
                age = n - 1 - ob['idx']
                if age > self.MAX_AGE or age < 3: continue
                if low  > ob['high']:  continue   # didn't reach OB
                if close < ob['low']:  continue   # pierced through — zone failed
                if close < open_:      continue   # bearish close inside OB = bad
                if close < bar_mid:    continue   # close in lower half = weak
                print(f"  🟦 OB BUY   [{ob['low']:.3f}–{ob['high']:.3f}] age={age}  RSI={rsi:.0f}  ATR={atr:.3f}  ADX={adx:.0f}")
                return 1, ob['low']

        if os.getenv('SELL_ENABLED', '1') == '0':
            return 0, 0.0

        # SELL: bearish OB retest
        if downtrend and 50 < rsi < 75 and ema20_slope < 0 and ema50_slope <= 0:
            for ob in reversed(obs):
                if ob['type'] != 'bear': continue
                age = n - 1 - ob['idx']
                if age > self.MAX_AGE or age < 3: continue
                if high < ob['low']:   continue
                if close > ob['high']: continue
                if close > open_:      continue
                if close > bar_mid:    continue
                print(f"  🟧 OB SELL  [{ob['low']:.3f}–{ob['high']:.3f}] age={age}  RSI={rsi:.0f}  ATR={atr:.3f}  ADX={adx:.0f}")
                return -1, ob['high']

        return 0, 0.0


# ─────────────────────────────────────────────
# LIQUIDITY SWEEP + FVG STRATEGY  (highest-conviction SMC setup)
# ─────────────────────────────────────────────
class LiquiditySweepFVGStrategy(BaseStrategy):
    """
    Detects: liquidity sweep of a prior swing high/low (stop-hunt) immediately
    followed by a fresh FVG in the opposite direction.
    This is the gold-standard SMC entry: smart money takes liquidity THEN fills FVG.
    Age of sweep: ≤ 3 bars before FVG formation.
    """
    LOOKBACK    = 40
    SWING_BARS  = 12   # how far back to look for the swept swing level
    MAX_AGE_FVG = 5

    def __init__(self):
        super().__init__("LS_FVG")

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

    def _swept_liquidity(self, bars: pd.DataFrame, fvg_idx: int) -> bool:
        """True if within 3 bars before fvg_idx there was a wick above swing_high (bull sweep → bearish FVG)."""
        bars = bars.reset_index(drop=True)
        n    = len(bars)
        sweep_window = bars.iloc[max(0, fvg_idx - 3): fvg_idx]
        if sweep_window.empty:
            return False
        lookback_start = max(0, fvg_idx - self.SWING_BARS - 3)
        prior = bars.iloc[lookback_start: fvg_idx - 3]
        if prior.empty:
            return False
        swing_high = float(prior['high'].max())
        swing_low  = float(prior['low'].min())
        # Liquidity sweep above → bearish reversal (for SELL setup)
        swept_high = any(float(b['high']) > swing_high for _, b in sweep_window.iterrows())
        # Liquidity sweep below → bullish reversal (for BUY setup)
        swept_low  = any(float(b['low'])  < swing_low  for _, b in sweep_window.iterrows())
        return swept_high or swept_low

    def get_signal_and_wick(self, df: pd.DataFrame) -> tuple[int, float]:
        if len(df) < 70:
            return 0, 0.0

        r      = df.iloc[-1]
        close  = float(r['close'])
        open_  = float(r['open'])
        high   = float(r['high'])
        low    = float(r['low'])
        ema200 = float(r.get('EMA_200', close) or close)
        ema20  = float(r.get('EMA_20',  close) or close)
        ema50  = float(r.get('EMA_50',  close) or close)
        rsi    = float(r.get('RSI', 50)   or 50)
        atr    = float(r.get('ATR', 0)    or 0)
        adx    = float(r.get('ADX_14', 0) or 0)

        if atr == 0: return 0, 0.0
        sl_pts  = float(os.getenv('SL_POINTS', '0.30'))
        adx_min = float(os.getenv('ADX_MIN', '20'))
        if atr > sl_pts * 1.8:         return 0, 0.0  # slightly wider — LS setups happen in volatile bars
        if adx > 0 and adx < adx_min: return 0, 0.0

        # Session filter
        try:
            bh = df.index[-1].hour if hasattr(df.index[-1], 'hour') else None
            if bh is not None and not (7 <= bh < 17):
                return 0, 0.0
        except Exception:
            pass

        uptrend   = close > ema200
        downtrend = close < ema200
        r_prev    = df.iloc[-2]
        ema20_slope = ema20 - float(r_prev.get('EMA_20', ema20) or ema20)
        ema50_slope = ema50 - float(df.iloc[-6].get('EMA_50', ema50) or ema50)
        bar_mid     = (high + low) / 2.0

        bars = df.tail(self.LOOKBACK + 5)
        n    = len(bars)
        fvgs = self._find_fvgs(bars)

        # BUY: sweep below swing low → bullish FVG retested
        if uptrend and 30 < rsi < 65 and ema20_slope > 0 and ema50_slope >= 0 and close >= bar_mid:
            for fvg in reversed(fvgs):
                if fvg['type'] != 'bull': continue
                age = n - 1 - fvg['idx']
                if age > self.MAX_AGE_FVG or age < 1: continue
                if low  > fvg['high']:  continue
                if close < fvg['low']:  continue
                if close < open_:       continue
                if self._swept_liquidity(bars, fvg['idx']):
                    print(f"  🟢 LS+FVG BUY  [{fvg['low']:.3f}–{fvg['high']:.3f}] age={age}  RSI={rsi:.0f}  ATR={atr:.3f}")
                    return 1, fvg['low']

        if os.getenv('SELL_ENABLED', '1') == '0':
            return 0, 0.0

        # SELL: sweep above swing high → bearish FVG retested
        if downtrend and 45 < rsi < 75 and ema20_slope < 0 and ema50_slope <= 0 and close <= bar_mid:
            for fvg in reversed(fvgs):
                if fvg['type'] != 'bear': continue
                age = n - 1 - fvg['idx']
                if age > self.MAX_AGE_FVG or age < 1: continue
                if high < fvg['low']:   continue
                if close > fvg['high']: continue
                if close > open_:       continue
                if self._swept_liquidity(bars, fvg['idx']):
                    print(f"  🔴 LS+FVG SELL [{fvg['low']:.3f}–{fvg['high']:.3f}] age={age}  RSI={rsi:.0f}  ATR={atr:.3f}")
                    return -1, fvg['high']

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

        # V11: FVG_SCALP + OB_SMC + LS_FVG — three complementary SMC strategies
        self.fvg_scalp   = FVGScalpStrategy()
        self.ob_smc      = OrderBlockStrategy()
        self.ls_fvg      = LiquiditySweepFVGStrategy()
        self.meta        = MetaController([self.fvg_scalp, self.ob_smc, self.ls_fvg])
        self.analytics     = AnalyticsEngine()

        self.connection  = None
        self.account     = None
        self.symbol_spec = None
        self.api         = None

        self.tracked: dict[str, str] = {}
        self.last_close_time: float  = 0.0

        # Phase 1: HTF Bias Agent
        self.htf_bias_agent: "HTFBiasAgent | None" = (
            HTFBiasAgent(symbol) if _HTF_BIAS_AVAILABLE else None
        )

        # Phase 2: Kill Zone Agent
        self.kill_zone_agent: "KillZoneAgent | None" = (
            KillZoneAgent() if _KILL_ZONE_AVAILABLE else None
        )

        # Phase 3: News Filter Agent
        self.news_agent: "NewsFilterAgent | None" = (
            NewsFilterAgent() if _NEWS_FILTER_AVAILABLE else None
        )

        # Phase 4: Confluence Scorer
        self.confluence_scorer: "ConfluenceScorer | None" = (
            ConfluenceScorer() if _CONFLUENCE_AVAILABLE else None
        )

        # Phase 5: Regime Classifier
        self.regime_classifier: "RegimeClassifier | None" = (
            RegimeClassifier() if _REGIME_AVAILABLE else None
        )

        # Phase 6: CNN Ensemble
        self.cnn_agent: "CNNEnsembleAgent | None" = (
            CNNEnsembleAgent() if _CNN_AVAILABLE else None
        )

        # Phase 7: Judas Swing Agent
        self.judas_agent: "JudasSwingAgent | None" = (
            JudasSwingAgent() if _JUDAS_AVAILABLE else None
        )

        # Phase 8: Dynamic SL Engine
        self.dynamic_sl: "DynamicSLEngine | None" = (
            DynamicSLEngine() if _DYNAMIC_SL_AVAILABLE else None
        )

        # Phase 9: Circuit Breaker (replaces ad-hoc circuit breaker)
        self.circuit_breaker: "CircuitBreakerAgent | None" = (
            CircuitBreakerAgent(
                on_shutdown=lambda r: print(f"🚨 CIRCUIT BREAKER SHUTDOWN: {r}"),
                on_alert=lambda r: discord_circuit_alert(r),
            ) if _CIRCUIT_BREAKER_AVAILABLE else None
        )

        # Phase 11: Post-Trade Logger (replaces AnalyticsEngine)
        self.post_trade: "PostTradeAgent | None" = (
            PostTradeAgent() if _POST_TRADE_AVAILABLE else None
        )

        # Daily EMA200 macro filter (legacy — still active as secondary check)
        self._daily_ema200      = None
        self._daily_ema200_date = None

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
        tp_mult = float(os.getenv('TP_MULT',    '3.0'))   # V12: 1:3 RR → 60% WR target
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

            # Phase 11: rich close log
            if self.post_trade:
                self.post_trade.log_close(pos_id, exit_price, pnl)
                self.post_trade.maybe_print_daily_summary()
            else:
                self.analytics.log_close(pos_id, exit_price, pnl)
            self.last_close_time = time.time()
            result = "WIN ✅" if pnl > 0 else "LOSS ❌"

            # Phase 9: circuit breaker on close
            if self.circuit_breaker:
                bal = 0.0
                try:
                    bal = float((await self.connection.get_account_information()).get('balance', 0))
                except Exception:
                    pass
                self.circuit_breaker.on_trade_result(pnl, bal)
            else:
                # Legacy circuit breaker tracking
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

    async def _update_daily_ema200(self):
        """Fetch daily candles, compute EMA200, cache for macro trend filter."""
        today = datetime.now(timezone.utc).date()
        if self._daily_ema200_date == today and self._daily_ema200 is not None:
            return  # already fresh for today
        try:
            start = datetime.now(timezone.utc) - timedelta(days=300)
            candles = await self.account.get_historical_candles(self.symbol, '1d', start, 220)
            if candles and len(candles) >= 200:
                closes = pd.Series([c['close'] for c in candles])
                ema = ta.ema(closes, length=200)
                self._daily_ema200 = float(ema.iloc[-1])
                self._daily_ema200_date = today
                print(f"📅 Daily EMA200 updated: {self._daily_ema200:.3f}")
        except Exception as e:
            print(f"⚠️  Daily EMA200 update failed: {e}")

    # ── main loop ──

    async def run(self):
        print(f"🔱 SHIVA V12 LIVE | {self.symbol} | FVG+OB+LS | ADX25+RSI55-68 | 60%WR | Max {self.max_daily_trades}/day")
        self.api = MetaApi(self.token)
        try:
            self.account = await self.api.metatrader_account_api.get_account(self.account_id)
            await self.account.wait_connected()
            self.connection = self.account.get_rpc_connection()
            await self.connection.connect()
            await self.connection.wait_synchronized()
            await self._refresh_spec()

            sl_pts  = float(os.getenv('SL_POINTS', '0.30'))
            tp_mult = float(os.getenv('TP_MULT', '3.0'))
            print(
                f"✅ MetaApi synchronized | {self.symbol}\n"
                f"── Strategy ──────────────────────────────────────────\n"
                f"  [1] FVG_SCALP — fresh FVG first-touch (ADX≥25 | RSI 55–68 | 60% WR target)\n"
                f"  [2] OB_SMC    — order block retest (SMC institutional zones)\n"
                f"  [3] LS_FVG    — liquidity sweep + FVG combo (gold-standard SMC)\n"
                f"  Fixed SL: {sl_pts} pts | TP: {sl_pts * tp_mult:.2f} pts (1:{tp_mult:.0f}R)\n"
                f"  Dynamic lot: $100=$0.01  $300=$0.03  $600=$0.06  $900=$0.09\n"
                f"  Daily limit: {self.max_daily_trades} trades | Session: 07:00–17:00 UTC\n"
                f"─────────────────────────────────────────────────────\n"
            )

            # Phase 1: start HTF Bias Agent
            if self.htf_bias_agent:
                await self.htf_bias_agent.start(self.account)

            # Phase 3: start News Filter Agent
            if self.news_agent:
                await self.news_agent.start()

            # Initial daily EMA200 fetch (legacy macro filter)
            await self._update_daily_ema200()

            while self.is_running:
                try:
                    self._check_daily_reset()
                    await self._update_daily_ema200()   # refreshes once/day

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

                    # Phase 2: Kill Zone gate
                    kz_result = None
                    if self.kill_zone_agent:
                        kz_result = self.kill_zone_agent.check()
                        if not kz_result.allowed:
                            await asyncio.sleep(30)
                            continue

                    # Phase 3: News gate
                    if self.news_agent:
                        news_result = self.news_agent.check()
                        if not news_result.allowed:
                            await asyncio.sleep(30)
                            continue

                    # Phase 9: Circuit Breaker gate (replaces ad-hoc logic below)
                    if self.circuit_breaker:
                        balance_cb = await self._get_balance()
                        self.circuit_breaker.set_balance(balance_cb)
                        cb_result = self.circuit_breaker.check()
                        if not cb_result.allows_trade:
                            print(f"⚡ Circuit breaker: {cb_result.reason}")
                            await asyncio.sleep(300)
                            continue

                    # Get signal
                    if not current:
                        sig, wick, strat_name = self.meta.get_signal_and_wick(df)
                        # Phase 1: HTF Bias gate (primary) — supersedes legacy EMA200 filter
                        if sig != 0 and self.htf_bias_agent:
                            bias = self.htf_bias_agent.current_bias
                            if bias:
                                if sig == 1 and bias.blocks_buy():
                                    print(f"🚫 BUY blocked by HTF Bias: {bias.bias.value}  score={bias.score:+d}")
                                    sig = 0
                                elif sig == -1 and bias.blocks_sell():
                                    print(f"🚫 SELL blocked by HTF Bias: {bias.bias.value}  score={bias.score:+d}")
                                    sig = 0

                        # Legacy daily EMA200 macro filter (fallback when HTFBiasAgent has no data)
                        if sig != 0 and (not self.htf_bias_agent or not self.htf_bias_agent.current_bias):
                            if self._daily_ema200 is not None:
                                cur_close = float(df.iloc[-1]['close'])
                                if sig == 1 and cur_close < self._daily_ema200:
                                    print(f"🚫 BUY blocked (legacy EMA200): close {cur_close:.2f} < {self._daily_ema200:.2f}")
                                    sig = 0
                                elif sig == -1 and cur_close > self._daily_ema200:
                                    print(f"🚫 SELL blocked (legacy EMA200): close {cur_close:.2f} > {self._daily_ema200:.2f}")
                                    sig = 0
                    else:
                        sig, wick, strat_name = 0, 0.0, ''

                    # Phase 5: Regime classification — adjust size or skip HV
                    regime_label = ""
                    regime_size_factor = 1.0
                    if sig != 0 and self.regime_classifier:
                        regime_result = self.regime_classifier.classify(df)
                        regime_label  = regime_result.regime.value
                        regime_size_factor = regime_result.size_factor

                    # ── Backtest-proven signal filters (WR 28%, PF 1.975) ──────────────────
                    if sig != 0:
                        import zoneinfo as _zi
                        _now_est = datetime.now(timezone.utc).astimezone(_zi.ZoneInfo("US/Eastern"))

                        # 1. TRENDING_BULL only (RANGING=11.9% WR, TRENDING_BEAR=12.3% — both below BE)
                        if regime_label and regime_label != "TRENDING_BULL":
                            print(f"🚫 Signal blocked — regime {regime_label} (only TRENDING_BULL allowed)")
                            sig = 0

                        # 2. BUY direction only in bull regime (SELL WR=9.1% vs BUY WR=18.9%)
                        if sig == -1:
                            print("🚫 SELL blocked — BUY-only policy in TRENDING_BULL regime")
                            sig = 0

                        # 3. Skip Monday EST (illiquid weekly open + low WR)
                        if sig != 0 and _now_est.weekday() == 0:
                            print("🚫 Signal blocked — Monday EST (low WR day)")
                            sig = 0

                        # 4. Skip Wednesday EST (WR=8.9% in backtest)
                        if sig != 0 and _now_est.weekday() == 2:
                            print("🚫 Signal blocked — Wednesday EST (worst WR day)")
                            sig = 0

                        # 5. Skip Friday after 10am EST (weekend gap prevention)
                        if sig != 0 and _now_est.weekday() == 4 and _now_est.hour >= 10:
                            print("🚫 Signal blocked — Friday after 10am EST (weekend gap risk)")
                            sig = 0

                        # 6. ATR spike filter — extreme volatility bars cause gap losses
                        if sig != 0 and "ATR" in df.columns and len(df) >= 20:
                            _atr_now = float(df["ATR"].iloc[-1] or 0)
                            _atr_ma  = float(df["ATR"].rolling(20).mean().iloc[-1] or 0)
                            if _atr_ma > 0 and _atr_now > 2.5 * _atr_ma:
                                print(f"🚫 Signal blocked — ATR spike {_atr_now:.3f} > 2.5×avg {_atr_ma:.3f}")
                                sig = 0
                    # ──────────────────────────────────────────────────────────────────────

                    # Phase 6: CNN Ensemble signal
                    cnn_dir, cnn_conf = 0, 0.0
                    if sig != 0 and self.cnn_agent:
                        cnn_result = self.cnn_agent.predict(df)
                        if cnn_result.passes_threshold:
                            cnn_dir  = cnn_result.direction
                            cnn_conf = cnn_result.confidence

                    # Phase 7: Judas Swing overlay — can override signal
                    if not current and self.judas_agent:
                        judas_result = self.judas_agent.check(df)
                        if judas_result.is_actionable and sig == 0:
                            sig   = judas_result.direction
                            wick  = judas_result.fvg_low if sig == 1 else judas_result.fvg_high
                            strat_name = "JUDAS_SWING"

                    # Phase 4: Confluence gate — score signal before execution
                    confluence_score = 0
                    entry_size_label = "FULL"
                    if sig != 0 and self.confluence_scorer:
                        mstate = MarketState(
                            signal=sig,
                            df=df,
                            htf_bias_score=self.htf_bias_agent.bias_score() if self.htf_bias_agent else 0,
                            htf_bias_label=self.htf_bias_agent.current_bias.bias.value if (
                                self.htf_bias_agent and self.htf_bias_agent.current_bias) else "NEUTRAL",
                            kill_zone_active=kz_result.allowed if kz_result else True,
                            session_name=kz_result.session.value if kz_result else "",
                            cnn_confidence=cnn_conf,
                            cnn_direction=cnn_dir,
                            regime=regime_label,
                        )
                        conf_result = self.confluence_scorer.score(mstate)
                        confluence_score = conf_result.score
                        entry_size_label = conf_result.entry_size.value
                        if not conf_result.allows_entry:
                            sig = 0  # below threshold — block trade

                    # Execute
                    if not current and sig != 0 and wick != 0.0:
                        # Dynamic lot based on live balance
                        balance = await self._get_balance()
                        lot     = compute_lot_size(balance)
                        price   = await self._get_price()

                        # Phase 8: dynamic SL/TP (falls back to fixed if unavailable)
                        if sig == 1:
                            entry = float(price.get('ask') or price.get('bid') or df.iloc[-1]['close'])
                            if self.dynamic_sl:
                                dyn = self.dynamic_sl.compute('BUY', entry, df, self._point(), self._min_stop())
                                if not dyn.valid:
                                    print(f"🚫 BUY skipped: {dyn.reason}")
                                    await asyncio.sleep(30)
                                    continue
                                sl, tp = dyn.sl, dyn.tp1  # tp1=1:1, tp2=runner (Phase 8)
                            else:
                                sl, tp = self._build_levels('BUY', entry, wick)
                            # Phase 9: apply size factor from regime + circuit breaker
                            lot_adj = round(lot * regime_size_factor * (
                                self.circuit_breaker._size_factor if self.circuit_breaker else 1.0
                            ), 2)
                            lot_adj = max(lot_adj, 0.01)
                            sl_usd = abs(entry - sl) * lot_adj * 1000
                            tp_usd = abs(tp - entry) * lot_adj * 1000
                            print(f"🚀 BUY  {self.symbol} @ {entry:.2f} | SL {sl:.2f} (${sl_usd:.0f}) | TP {tp:.2f} (${tp_usd:.0f}) | lot={lot_adj} | bal=${balance:.0f} | [{strat_name}] conf={confluence_score}")
                            result = await self.connection.create_market_buy_order(
                                self.symbol, lot_adj, sl, tp,
                                {'comment': f'SHIVA:{strat_name}'},
                            )
                        else:
                            entry = float(price.get('bid') or price.get('ask') or df.iloc[-1]['close'])
                            if self.dynamic_sl:
                                dyn = self.dynamic_sl.compute('SELL', entry, df, self._point(), self._min_stop())
                                if not dyn.valid:
                                    print(f"🚫 SELL skipped: {dyn.reason}")
                                    await asyncio.sleep(30)
                                    continue
                                sl, tp = dyn.sl, dyn.tp1
                            else:
                                sl, tp = self._build_levels('SELL', entry, wick)
                            lot_adj = round(lot * regime_size_factor * (
                                self.circuit_breaker._size_factor if self.circuit_breaker else 1.0
                            ), 2)
                            lot_adj = max(lot_adj, 0.01)
                            sl_usd = abs(sl - entry) * lot_adj * 1000
                            tp_usd = abs(entry - tp) * lot_adj * 1000
                            print(f"📉 SELL {self.symbol} @ {entry:.2f} | SL {sl:.2f} (${sl_usd:.0f}) | TP {tp:.2f} (${tp_usd:.0f}) | lot={lot_adj} | bal=${balance:.0f} | [{strat_name}] conf={confluence_score}")
                            result = await self.connection.create_market_sell_order(
                                self.symbol, lot_adj, sl, tp,
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
                            # Phase 2: tag session for performance tracking
                            session_tag = (
                                self.kill_zone_agent.check().session.value
                                if self.kill_zone_agent else "UNKNOWN"
                            )
                            self.tracked[new_pos['id']] = strat_name
                            # Phase 11: rich trade log
                            if self.post_trade:
                                tp2_val = dyn.tp2 if (self.dynamic_sl and 'dyn' in dir()) else tp
                                news_nearby = bool(
                                    self.news_agent and self.news_agent._last_check
                                    and self.news_agent._last_check.mins_to_event is not None
                                    and abs(self.news_agent._last_check.mins_to_event) < 60
                                )
                                self.post_trade.log_open(
                                    new_pos['id'], side, entry, sl, tp, tp2_val,
                                    lot=lot_adj, strategy=strat_name,
                                    confluence_score=confluence_score,
                                    regime=regime_label, session=session_tag,
                                    news_nearby=news_nearby, cnn_confidence=cnn_conf,
                                    reason=" | ".join([strat_name, entry_size_label, regime_label]),
                                )
                            else:
                                self.analytics.log_open(new_pos['id'], side, entry, sl, tp, lot_adj, strat_name)
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

    print(f"🚀 Starting SHIVA V12  |  {SYMBOL}  |  FVG+OB+LS | ADX≥25 | RSI 55-68 | 60% WR | TP=3R")

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
