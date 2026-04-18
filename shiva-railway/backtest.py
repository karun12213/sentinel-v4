"""
SHIVA V6 — IFVG + FVG Scalp Backtest
Data: CL=F (WTI Crude / USOIL proxy)
  - Daily bars 2018-2026 (full history)
  - 15m bars last 60 days (max yfinance allows for intraday)
Fixed: SL = $3 / TP = $18 (6:1 RR) at 0.01 lot (10 $/pt)
       → SL = 0.30 pts, TP = 1.80 pts
Daily limit: 9 trades/day
Capital: $100
"""
import sys, os, warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
from collections import defaultdict

try:
    import yfinance as yf
except ImportError:
    print("pip install yfinance"); sys.exit(1)

sys.path.insert(0, os.path.dirname(__file__))
from live_core import FeatureEngine, IFVGStrategy, FVGScalpStrategy, EMABounceStrategy


# ─────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────
def fetch(interval: str, start: str = '2018-01-01') -> pd.DataFrame:
    print(f"⬇️  Fetching CL=F  interval={interval}  start={start} …")
    df = yf.download('CL=F', start=start, interval=interval,
                     auto_adjust=True, progress=False, multi_level_index=False)
    if df is None or df.empty:
        df = yf.download('CL=F', period='60d', interval=interval,
                         auto_adjust=True, progress=False, multi_level_index=False)
    df.columns = [c.lower() for c in df.columns]
    df.index   = pd.to_datetime(df.index, utc=True)
    df         = df[['open','high','low','close','volume']].dropna()
    print(f"   {len(df):,} bars  ({df.index[0].date()} → {df.index[-1].date()})")
    return df


# ─────────────────────────────────────────────
# BAR-BY-BAR EVENT-DRIVEN BACKTEST
# ─────────────────────────────────────────────
def run(df: pd.DataFrame, label: str,
        initial_capital: float = 100.0,
        sl_pts:  float = 0.30,   # $3 SL at 0.01 lot (10 $/pt)
        tp_pts:  float = 1.80,   # $18 TP (6:1)
        pt_usd:  float = 10.0,   # dollar value per 1 price point at 0.01 lot
        commission_usd: float = 0.03,   # per round-trip
        cooldown_bars: int = 1,
        max_daily_trades: int = 9) -> pd.DataFrame:

    # FVG_SCALP + EMA_BOUNCE — IFVG excluded (uses dynamic ATR-based SL)
    strategies = [FVGScalpStrategy(), EMABounceStrategy()]
    warmup     = 210   # enough for EMA-200 + indicators

    trades           = []
    capital          = initial_capital
    equity           = [initial_capital]
    position         = None
    last_close_bar   = -9999

    # Daily trade counter
    daily_counts     = defaultdict(int)   # date → count

    def _pnl_fixed(hit: str) -> float:
        if hit == 'TP':
            return round(pt_usd * tp_pts - commission_usd, 2)
        elif hit == 'SL':
            return round(-pt_usd * sl_pts - commission_usd, 2)
        else:  # OPEN@END — mark-to-market
            return 0.0

    def _pnl_mtm(side: str, entry: float, exit_p: float) -> float:
        move = (exit_p - entry) if side == 'BUY' else (entry - exit_p)
        return round(move * pt_usd - commission_usd, 2)

    for i in range(warmup, len(df)):
        bar  = df.iloc[i]
        t    = df.index[i]
        date = t.date()

        # ── Check if open position hit SL or TP this bar ──
        if position is not None:
            s  = position['side']
            sl = position['sl']
            tp = position['tp']
            hit = None

            if s == 'BUY':
                open_p = float(bar['open'])
                if   open_p <= sl:               hit, exit_p = 'SL', open_p
                elif open_p >= tp:               hit, exit_p = 'TP', open_p
                elif float(bar['low'])  <= sl:   hit, exit_p = 'SL', sl
                elif float(bar['high']) >= tp:   hit, exit_p = 'TP', tp
            else:
                open_p = float(bar['open'])
                if   open_p >= sl:               hit, exit_p = 'SL', open_p
                elif open_p <= tp:               hit, exit_p = 'TP', open_p
                elif float(bar['high']) >= sl:   hit, exit_p = 'SL', sl
                elif float(bar['low'])  <= tp:   hit, exit_p = 'TP', tp

            if hit:
                pnl_val = _pnl_fixed(hit)
                capital += pnl_val
                result   = 'WIN' if pnl_val > 0 else 'LOSS'
                trades.append({
                    'trade_no':    len(trades) + 1,
                    'date':        str(date),
                    'entry_time':  position['entry_time'],
                    'exit_time':   str(t),
                    'strategy':    position['strategy'],
                    'side':        s,
                    'entry':       position['entry'],
                    'sl':          sl,
                    'tp':          tp,
                    'exit':        exit_p,
                    'hit':         hit,
                    'pnl':         pnl_val,
                    'capital':     round(capital, 2),
                    'result':      result,
                })
                last_close_bar = i
                position = None

        equity.append(capital)

        # ── Look for new entry ──
        if position is None and (i - last_close_bar) >= cooldown_bars:
            # Daily limit check
            if daily_counts[date] >= max_daily_trades:
                continue

            window = df.iloc[:i + 1].copy()
            try:
                feat = FeatureEngine.add_indicators(window)
            except Exception:
                continue
            if feat.empty:
                continue

            # Try strategies in priority order
            sig, wick, strat_name = 0, 0.0, ''
            for strat in strategies:
                s_sig, s_wick = strat.get_signal_and_wick(feat)
                if s_sig != 0:
                    sig, wick, strat_name = s_sig, s_wick, strat.name
                    break

            if sig == 0:
                continue

            entry = float(bar['close'])
            side  = 'BUY' if sig == 1 else 'SELL'

            if side == 'BUY':
                sl = round(entry - sl_pts, 2)
                tp = round(entry + tp_pts, 2)
            else:
                sl = round(entry + sl_pts, 2)
                tp = round(entry - tp_pts, 2)

            daily_counts[date] += 1
            position = {
                'side':       side,
                'entry':      entry,
                'sl':         sl,
                'tp':         tp,
                'wick':       wick,
                'entry_bar':  i,
                'entry_time': str(t),
                'strategy':   strat_name,
            }

    # Close any open position at last bar
    if position is not None:
        last_bar = df.iloc[-1]
        exit_p   = float(last_bar['close'])
        pnl_val  = _pnl_mtm(position['side'], position['entry'], exit_p)
        capital += pnl_val
        trades.append({
            'trade_no':   len(trades) + 1,
            'date':       str(df.index[-1].date()),
            'entry_time': position['entry_time'],
            'exit_time':  str(df.index[-1]),
            'strategy':   position['strategy'],
            'side':       position['side'],
            'entry':      position['entry'],
            'sl':         position['sl'],
            'tp':         position['tp'],
            'exit':       exit_p,
            'hit':        'OPEN@END',
            'pnl':        pnl_val,
            'capital':    round(capital, 2),
            'result':     'WIN' if pnl_val > 0 else 'LOSS',
        })

    trades_df = pd.DataFrame(trades)

    # ── Print summary ──
    print(f"\n{'='*66}")
    print(f"  BACKTEST  |  {label}")
    print(f"  Fixed: SL={sl_pts}pt (${sl_pts*pt_usd:.0f})  TP={tp_pts}pt (${tp_pts*pt_usd:.0f})  RR=1:{tp_pts/sl_pts:.0f}")
    print(f"  Capital ${initial_capital}  |  0.01 lot  |  Max {max_daily_trades} trades/day")
    print(f"{'='*66}")
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

    # Max drawdown
    eq   = [initial_capital] + list(trades_df['capital'])
    peak = initial_capital
    mdd  = 0.0
    for v in eq:
        peak = max(peak, v)
        mdd  = max(mdd, peak - v)

    avg_win  = trades_df.loc[trades_df['pnl'] > 0, 'pnl'].mean() if wins else 0
    avg_loss = trades_df.loc[trades_df['pnl'] < 0, 'pnl'].mean() if losses else 0
    best     = trades_df['pnl'].max()
    worst    = trades_df['pnl'].min()

    # Trading days and per-day stats
    active_days = len(daily_counts) if daily_counts else max(1, n)
    trades_per_day = n / active_days

    print(f"  Initial capital  : ${initial_capital:,.2f}")
    print(f"  Final capital    : ${capital:,.2f}")
    print(f"  Net P&L          : ${net:+,.2f}  ({net/initial_capital*100:+.1f}%)")
    print(f"  Total trades     : {n}  |  Active days: {active_days}  |  Avg/day: {trades_per_day:.1f}")
    print(f"  Wins / Losses    : {wins} / {losses}")
    print(f"  Win rate         : {wr:.1%}  (breakeven = 14.3% at 6:1)")
    print(f"  Profit factor    : {pf}")
    print(f"  Avg win          : ${avg_win:+,.2f}")
    print(f"  Avg loss         : ${avg_loss:+,.2f}")
    print(f"  Best trade       : ${best:+,.2f}")
    print(f"  Worst trade      : ${worst:+,.2f}")
    print(f"  Max drawdown     : ${mdd:,.2f}  ({mdd/initial_capital*100:.1f}%)")

    # By strategy
    if 'strategy' in trades_df.columns:
        by_strat = trades_df.groupby('strategy').agg(
            trades=('pnl', 'count'),
            wins=('result', lambda x: (x == 'WIN').sum()),
            pnl=('pnl', 'sum'),
        )
        by_strat['wr'] = (by_strat['wins'] / by_strat['trades']).map('{:.1%}'.format)
        print(f"\n  By strategy:")
        print(by_strat[['trades', 'wins', 'wr', 'pnl']].to_string())

    # By direction
    by_side = trades_df.groupby('side').agg(
        trades=('pnl', 'count'),
        wins=('result', lambda x: (x == 'WIN').sum()),
        pnl=('pnl', 'sum'),
    )
    by_side['wr'] = (by_side['wins'] / by_side['trades']).map('{:.1%}'.format)
    print(f"\n  By direction:")
    print(by_side[['trades', 'wins', 'wr', 'pnl']].to_string())

    # Daily distribution
    if 'date' in trades_df.columns:
        day_counts_series = trades_df.groupby('date').size()
        print(f"\n  Trades/day distribution:")
        print(f"    Max: {day_counts_series.max()}  |  "
              f"Min: {day_counts_series.min()}  |  "
              f"Median: {day_counts_series.median():.1f}  |  "
              f"Days hitting limit: {(day_counts_series >= max_daily_trades).sum()}")

    print(f"{'='*66}\n")
    return trades_df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 66)
    print("  SHIVA V6 — IFVG + FVG Scalp | Fixed $3 SL / $18 TP | $100 capital")
    print("  Asset: USOIL (CL=F WTI Crude Oil Futures)  |  0.01 lot")
    print("=" * 66)
    print()

    # ── 1. Daily 2018-2026 ──
    df_daily  = fetch('1d', start='2018-01-01')
    trades_d  = run(
        df_daily,
        label='DAILY 2018→2026',
        initial_capital=100.0,
        sl_pts=0.30,
        tp_pts=1.80,
        pt_usd=10.0,
        cooldown_bars=1,
        max_daily_trades=9,
    )

    # ── 2. 15m (last 60 days — max yfinance) ──
    df_15m    = fetch('15m')
    trades_15 = run(
        df_15m,
        label='15m last 60 days',
        initial_capital=100.0,
        sl_pts=0.30,
        tp_pts=1.80,
        pt_usd=10.0,
        cooldown_bars=1,   # 15-min cooldown (matching live bot 5-min on 5m candles)
        max_daily_trades=9,
    )

    # ── Save & print daily trades ──
    pd.set_option('display.width', 200)
    pd.set_option('display.max_rows', None)

    cols = ['trade_no', 'date', 'entry_time', 'exit_time', 'strategy',
            'side', 'entry', 'sl', 'tp', 'exit', 'hit', 'pnl', 'capital', 'result']

    if not trades_d.empty:
        trades_d.to_csv('backtest_daily_trades.csv', index=False)
        print(f"📝 Daily trade log → backtest_daily_trades.csv  ({len(trades_d)} trades)")
        print("\n─── ALL DAILY TRADES ──────────────────────────────────────────────────────")
        print(trades_d[cols].to_string(index=False))

    if not trades_15.empty:
        trades_15.to_csv('backtest_15m_trades.csv', index=False)
        print(f"\n📝 15m trade log   → backtest_15m_trades.csv  ({len(trades_15)} trades)")
        print("\n─── ALL 15m TRADES ────────────────────────────────────────────────────────")
        print(trades_15[cols].to_string(index=False))
