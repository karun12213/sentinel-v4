"""
SHIVA V6 — IFVG + FVG Scalp | Dynamic Lot | 9-Month Backtest
Data: CL=F (WTI Crude / USOIL proxy)
  - 1h bars: 9 months (yfinance supports 2yr for 1h)
  - 15m bars: last 60 days (for comparison)

Fixed SL/TP (price pts):
  SL = 0.30 pt   TP = 1.80 pt  (1:6 RR)

Dynamic lot every $300 of capital:
  $100 → 0.01 lot  ($3 SL  / $18 TP)
  $300 → 0.03 lot  ($9 SL  / $54 TP)
  $600 → 0.06 lot  ($18 SL / $108 TP)
  $900 → 0.09 lot  ($27 SL / $162 TP)

Capital: $100  |  0.01 lot base  |  Max 9 trades/day
"""
import sys, os, warnings, math
warnings.filterwarnings('ignore')

from datetime import datetime, timedelta, timezone
from collections import defaultdict

import pandas as pd
import numpy as np

try:
    import yfinance as yf
except ImportError:
    print("pip install yfinance"); sys.exit(1)

sys.path.insert(0, os.path.dirname(__file__))
from live_core import FeatureEngine, FVGScalpStrategy, EMABounceStrategy, compute_lot_size


# ─────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────
def fetch(interval: str, start: str = None, period: str = None) -> pd.DataFrame:
    label = f"start={start}" if start else f"period={period}"
    print(f"⬇️  Fetching CL=F  interval={interval}  {label} …")
    if start:
        df = yf.download('CL=F', start=start, interval=interval,
                         auto_adjust=True, progress=False, multi_level_index=False)
    else:
        df = yf.download('CL=F', period=period, interval=interval,
                         auto_adjust=True, progress=False, multi_level_index=False)
    if df is None or df.empty:
        print("   ⚠️  No data returned"); return pd.DataFrame()
    df.columns = [c.lower() for c in df.columns]
    df.index   = pd.to_datetime(df.index, utc=True)
    df         = df[['open','high','low','close','volume']].dropna()
    print(f"   {len(df):,} bars  ({df.index[0].date()} → {df.index[-1].date()})")
    return df


# ─────────────────────────────────────────────
# LOT / PNL HELPERS
# ─────────────────────────────────────────────
BASE_LOT       = 0.01
PT_USD_PER_LOT = 1000.0   # $1000 per lot per 1-pt move → $10 per 0.01 lot

def pt_value(lot: float) -> float:
    """Dollar value of 1 price point at given lot size."""
    return lot * PT_USD_PER_LOT

def pnl_fixed(hit: str, lot: float, sl_pts: float, tp_pts: float,
              commission_usd: float = 0.0) -> float:
    pv = pt_value(lot)
    if hit == 'TP':
        return round(pv * tp_pts - commission_usd, 2)
    elif hit == 'SL':
        return round(-pv * sl_pts - commission_usd, 2)
    return 0.0

def pnl_mtm(side: str, entry: float, exit_p: float, lot: float,
            commission_usd: float = 0.0) -> float:
    move = (exit_p - entry) if side == 'BUY' else (entry - exit_p)
    return round(move * pt_value(lot) - commission_usd, 2)


# ─────────────────────────────────────────────
# BAR-BY-BAR BACKTEST
# ─────────────────────────────────────────────
def run(df: pd.DataFrame, label: str,
        initial_capital: float = 100.0,
        sl_pts:  float = 0.30,
        tp_pts:  float = 1.80,
        commission_pct: float = 0.0003,
        cooldown_bars: int = 1,
        max_daily_trades: int = 9,
        strategy_list=None,
        daily_ema200: pd.Series = None) -> pd.DataFrame:
    """
    daily_ema200: optional Series (daily index) of EMA200 values for macro trend filter.
    When provided, BUY only if close > daily_ema200, SELL only if close < daily_ema200.
    Overrides the intraday EMA200 used by strategies for trend alignment.
    """
    if df.empty:
        print(f"  [SKIP] {label} — no data")
        return pd.DataFrame()

    strategies = strategy_list if strategy_list is not None else [FVGScalpStrategy(), EMABounceStrategy()]
    warmup       = 215   # EMA-200 + indicators need ~200 bars

    trades        = []
    capital       = initial_capital
    equity_curve  = [initial_capital]
    position      = None
    last_close_bar = -9999
    daily_counts  = defaultdict(int)

    for i in range(warmup, len(df)):
        bar  = df.iloc[i]
        t    = df.index[i]
        date = t.date()

        # ── Check if open position hit SL or TP ──
        if position is not None:
            s   = position['side']
            sl  = position['sl']
            tp  = position['tp']
            lot = position['lot']
            hit = None
            exit_p = None

            if s == 'BUY':
                op = float(bar['open'])
                if   op <= sl:                 hit, exit_p = 'SL', op
                elif op >= tp:                 hit, exit_p = 'TP', op
                elif float(bar['low'])  <= sl: hit, exit_p = 'SL', sl
                elif float(bar['high']) >= tp: hit, exit_p = 'TP', tp
            else:
                op = float(bar['open'])
                if   op >= sl:                 hit, exit_p = 'SL', op
                elif op <= tp:                 hit, exit_p = 'TP', op
                elif float(bar['high']) >= sl: hit, exit_p = 'SL', sl
                elif float(bar['low'])  <= tp: hit, exit_p = 'TP', tp

            if hit:
                comm    = lot * PT_USD_PER_LOT * sl_pts * commission_pct
                pnl_val = pnl_fixed(hit, lot, sl_pts, tp_pts, comm)
                capital += pnl_val
                result   = 'WIN' if pnl_val > 0 else 'LOSS'
                trades.append({
                    'trade_no':   len(trades) + 1,
                    'date':       str(date),
                    'entry_time': position['entry_time'],
                    'exit_time':  str(t),
                    'strategy':   position['strategy'],
                    'side':       s,
                    'lot':        lot,
                    'entry':      position['entry'],
                    'sl':         sl,
                    'tp':         tp,
                    'exit':       round(exit_p, 3),
                    'hit':        hit,
                    'sl_usd':     round(pt_value(lot) * sl_pts, 2),
                    'tp_usd':     round(pt_value(lot) * tp_pts, 2),
                    'pnl':        pnl_val,
                    'capital':    round(capital, 2),
                    'result':     result,
                })
                last_close_bar = i
                position = None

        equity_curve.append(capital)

        # ── New entry ──
        if position is None and (i - last_close_bar) >= cooldown_bars:
            if daily_counts[date] >= max_daily_trades:
                continue

            window = df.iloc[:i + 1].copy()
            try:
                feat = FeatureEngine.add_indicators(window)
            except Exception:
                continue
            if feat.empty:
                continue

            sig, wick, strat_name = 0, 0.0, ''
            for strat in strategies:
                s_sig, s_wick = strat.get_signal_and_wick(feat)
                if s_sig != 0:
                    # Apply daily macro trend filter when provided
                    if daily_ema200 is not None:
                        cur_close = float(bar['close'])
                        # Find the daily EMA200 for this bar's date (most recent available)
                        try:
                            d_ema_avail = daily_ema200[daily_ema200.index <= pd.Timestamp(date)]
                            if not d_ema_avail.empty:
                                d_ema_val = float(d_ema_avail.iloc[-1])
                                if s_sig == 1 and cur_close < d_ema_val:
                                    continue   # macro downtrend — skip BUY
                                if s_sig == -1 and cur_close > d_ema_val:
                                    continue   # macro uptrend — skip SELL
                        except Exception:
                            pass
                    sig, wick, strat_name = s_sig, s_wick, strat.name
                    break

            if sig == 0:
                continue

            entry = float(bar['close'])
            side  = 'BUY' if sig == 1 else 'SELL'
            lot   = compute_lot_size(capital)

            if side == 'BUY':
                sl = round(entry - sl_pts, 3)
                tp = round(entry + tp_pts, 3)
            else:
                sl = round(entry + sl_pts, 3)
                tp = round(entry - tp_pts, 3)

            daily_counts[date] += 1
            position = {
                'side':       side,
                'entry':      entry,
                'sl':         sl,
                'tp':         tp,
                'wick':       wick,
                'lot':        lot,
                'entry_bar':  i,
                'entry_time': str(t),
                'strategy':   strat_name,
            }

    # Close any open position at last bar
    if position is not None:
        last_bar = df.iloc[-1]
        exit_p   = float(last_bar['close'])
        lot      = position['lot']
        comm     = lot * PT_USD_PER_LOT * sl_pts * commission_pct
        pnl_val  = pnl_mtm(position['side'], position['entry'], exit_p, lot, comm)
        capital += pnl_val
        trades.append({
            'trade_no':   len(trades) + 1,
            'date':       str(df.index[-1].date()),
            'entry_time': position['entry_time'],
            'exit_time':  str(df.index[-1]),
            'strategy':   position['strategy'],
            'side':       position['side'],
            'lot':        lot,
            'entry':      position['entry'],
            'sl':         position['sl'],
            'tp':         position['tp'],
            'exit':       round(exit_p, 3),
            'hit':        'OPEN@END',
            'sl_usd':     round(pt_value(lot) * sl_pts, 2),
            'tp_usd':     round(pt_value(lot) * tp_pts, 2),
            'pnl':        pnl_val,
            'capital':    round(capital, 2),
            'result':     'WIN' if pnl_val > 0 else 'LOSS',
        })

    trades_df = pd.DataFrame(trades)

    # ── Summary ──
    print(f"\n{'='*70}")
    print(f"  BACKTEST  |  {label}")
    print(f"  SL={sl_pts}pt  TP={tp_pts}pt  RR=1:{tp_pts/sl_pts:.0f}  |  Dynamic lot $300 steps")
    print(f"  Capital ${initial_capital}  |  Max {max_daily_trades} trades/day")
    print(f"{'='*70}")
    if trades_df.empty:
        print("  No trades generated.")
        return trades_df

    n      = len(trades_df)
    wins   = (trades_df['result'] == 'WIN').sum()
    losses = (trades_df['result'] == 'LOSS').sum()
    wr     = wins / n
    net    = trades_df['pnl'].sum()
    g_win  = trades_df.loc[trades_df['pnl'] > 0, 'pnl'].sum()
    g_loss = abs(trades_df.loc[trades_df['pnl'] < 0, 'pnl'].sum())
    pf     = round(g_win / g_loss, 3) if g_loss > 0 else float('inf')

    # Max drawdown from equity curve
    ec   = equity_curve
    peak = ec[0]
    mdd  = 0.0
    for v in ec:
        peak = max(peak, v)
        mdd  = max(mdd, peak - v)

    active_days     = max(1, len(daily_counts))
    trades_per_day  = n / active_days
    best_capital    = max(ec)
    avg_win_usd     = trades_df.loc[trades_df['pnl'] > 0,  'pnl'].mean() if wins   else 0
    avg_loss_usd    = trades_df.loc[trades_df['pnl'] < 0, 'pnl'].mean() if losses else 0

    # Lot tier usage
    lot_dist = trades_df.groupby('lot')['pnl'].agg(['count','sum']).rename(
        columns={'count': 'trades', 'sum': 'net_pnl'})
    lot_dist['sl_usd'] = (lot_dist.index * PT_USD_PER_LOT * sl_pts).round(2)
    lot_dist['tp_usd'] = (lot_dist.index * PT_USD_PER_LOT * tp_pts).round(2)

    print(f"  Initial capital  : ${initial_capital:,.2f}")
    print(f"  Final capital    : ${capital:,.2f}")
    print(f"  Peak capital     : ${best_capital:,.2f}")
    print(f"  Net P&L          : ${net:+,.2f}  ({net/initial_capital*100:+.1f}%)")
    print(f"  Total trades     : {n}  |  Active days: {active_days}  |  Avg/day: {trades_per_day:.1f}")
    print(f"  Wins / Losses    : {wins} / {losses}")
    print(f"  Win rate         : {wr:.1%}  (breakeven @ 1:6 = 14.3%)")
    print(f"  Profit factor    : {pf}")
    print(f"  Avg win          : ${avg_win_usd:+,.2f}")
    print(f"  Avg loss         : ${avg_loss_usd:+,.2f}")
    print(f"  Max drawdown     : ${mdd:,.2f}  ({mdd/initial_capital*100:.1f}%)")

    print(f"\n  By lot tier (dynamic sizing):")
    print(lot_dist.to_string())

    if 'strategy' in trades_df.columns:
        by_strat = trades_df.groupby('strategy').agg(
            trades=('pnl', 'count'),
            wins=('result', lambda x: (x == 'WIN').sum()),
            net_pnl=('pnl', 'sum'),
        )
        by_strat['wr'] = (by_strat['wins'] / by_strat['trades']).map('{:.1%}'.format)
        print(f"\n  By strategy:")
        print(by_strat[['trades', 'wins', 'wr', 'net_pnl']].to_string())

    by_side = trades_df.groupby('side').agg(
        trades=('pnl', 'count'),
        wins=('result', lambda x: (x == 'WIN').sum()),
        net_pnl=('pnl', 'sum'),
    )
    by_side['wr'] = (by_side['wins'] / by_side['trades']).map('{:.1%}'.format)
    print(f"\n  By direction:")
    print(by_side[['trades', 'wins', 'wr', 'net_pnl']].to_string())

    # Monthly breakdown
    if len(trades_df) > 0:
        trades_df['month'] = pd.to_datetime(trades_df['date']).dt.to_period('M')
        monthly = trades_df.groupby('month').agg(
            trades=('pnl', 'count'),
            wins=('result', lambda x: (x == 'WIN').sum()),
            net_pnl=('pnl', 'sum'),
        )
        monthly['wr'] = (monthly['wins'] / monthly['trades']).map('{:.1%}'.format)
        print(f"\n  Monthly breakdown:")
        print(monthly[['trades', 'wins', 'wr', 'net_pnl']].to_string())

    day_counts_s = trades_df.groupby('date').size()
    print(f"\n  Trades/day: max={day_counts_s.max()}  min={day_counts_s.min()}  "
          f"median={day_counts_s.median():.1f}  days at limit={( day_counts_s >= max_daily_trades).sum()}")

    print(f"{'='*70}\n")
    return trades_df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 70)
    print("  SHIVA V6 — FVG Scalp + EMA Bounce | Dynamic Lot | 9-Month Backtest")
    print("  Asset: USOIL (CL=F)  |  $100 capital  |  SL=$3→scales  TP=$18→scales")
    print("=" * 70)
    print()

    # NOTE: yfinance 15m data is limited to last 60 days.
    # For 9-month history, we use 1h bars (yfinance supports up to 2yr for 1h).
    # Live bot uses 5m candles — 1h backtest is a reasonable proxy for signal logic.

    # ── 9 months of 1h data ──
    nine_months_ago = (datetime.now(timezone.utc) - timedelta(days=274)).strftime('%Y-%m-%d')
    print(f"⚠️  Note: yfinance 15m limited to 60 days. Using 1h bars for 9-month history.")
    print(f"   (Live bot uses 5m candles — same signals, 3× frequency expected)\n")
    df_1h = fetch('1h', start=nine_months_ago)

    # Fetch daily data for daily EMA200 macro trend filter
    daily_start = (datetime.now(timezone.utc) - timedelta(days=274 + 300)).strftime('%Y-%m-%d')
    df_daily_trend = fetch('1d', start=daily_start)
    daily_ema200_series = None
    if not df_daily_trend.empty:
        try:
            import pandas_ta as _pta
            _close = df_daily_trend['close']
            _ema   = _pta.ema(_close, length=200)
            _ema   = _ema.dropna()
            _ema.index = pd.to_datetime(_ema.index).tz_localize(None)
            daily_ema200_series = _ema
            print(f"   Daily EMA200 loaded: {len(_ema)} values  ({_ema.index[0].date()} → {_ema.index[-1].date()})")
        except Exception as e:
            print(f"   ⚠️  Daily EMA200 failed: {e}")

    os.environ['SL_POINTS'] = '0.30'
    os.environ['MIN_ATR']   = '0.0'
    trades_1h = run(
        df_1h,
        label=f'1H  |  {nine_months_ago} → today  (~9 months)  [Daily EMA200 macro filter]',
        initial_capital=100.0,
        sl_pts=0.30,
        tp_pts=1.80,
        cooldown_bars=1,
        max_daily_trades=9,
        strategy_list=[FVGScalpStrategy(), EMABounceStrategy()],
        daily_ema200=daily_ema200_series,
    )

    # ── 15m last 60 days ──
    print("📊 Running 15m backtest (last 60 days — max yfinance supports for 15m)…\n")
    df_15m    = fetch('15m', period='60d')
    trades_15 = run(
        df_15m,
        label='15M  |  last 60 days (max available)  [FVG_SCALP + EMA_BOUNCE]',
        initial_capital=100.0,
        sl_pts=0.30,
        tp_pts=1.80,
        cooldown_bars=1,
        max_daily_trades=9,
        strategy_list=[FVGScalpStrategy(), EMABounceStrategy()],
    )

    # ── Save & print all trades ──
    pd.set_option('display.width', 220)
    pd.set_option('display.max_rows', None)

    cols = ['trade_no', 'date', 'entry_time', 'exit_time', 'strategy',
            'side', 'lot', 'sl_usd', 'tp_usd', 'entry', 'sl', 'tp',
            'exit', 'hit', 'pnl', 'capital', 'result']

    if not trades_1h.empty:
        trades_1h.to_csv('backtest_1h_trades.csv', index=False)
        print(f"📝 1h trade log  → backtest_1h_trades.csv  ({len(trades_1h)} trades)")
        print("\n─── ALL 1H TRADES ──────────────────────────────────────────────────────────")
        print(trades_1h[cols].to_string(index=False))

    if not trades_15.empty:
        trades_15.to_csv('backtest_15m_trades.csv', index=False)
        print(f"\n📝 15m trade log → backtest_15m_trades.csv  ({len(trades_15)} trades)")
        print("\n─── ALL 15M TRADES ─────────────────────────────────────────────────────────")
        print(trades_15[cols].to_string(index=False))
