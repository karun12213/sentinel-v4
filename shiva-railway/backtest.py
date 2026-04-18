"""
SHIVA V5 — IFVG + Discount/Premium Backtest
Data: CL=F (WTI Crude / USOIL proxy) — daily bars 2018-2026
      + 15m bars last 60 days (max yfinance allows for intraday)
Strategy: IFVGStrategy from live_core.py (identical logic)
SL: IFVG zone boundary | TP: 6R | Cooldown: 1 bar after close
"""
import sys, os, warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np

try:
    import yfinance as yf
except ImportError:
    print("pip install yfinance"); sys.exit(1)

sys.path.insert(0, os.path.dirname(__file__))
from live_core import FeatureEngine, IFVGStrategy


# ─────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────
def fetch(interval: str, start: str = '2018-01-01') -> pd.DataFrame:
    print(f"⬇️  Fetching CL=F  interval={interval}  start={start} …")
    df = yf.download('CL=F', start=start, interval=interval,
                     auto_adjust=True, progress=False, multi_level_index=False)
    if df is None or df.empty:
        # fallback: period-based for intraday
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
        initial_capital: float = 10_000.0,
        lot_usd: float = 1_000.0,       # dollar value per lot (1 barrel ≈ $1 move per contract)
        commission_pct: float = 0.0003,  # 0.03% round-trip
        cooldown_bars: int = 1) -> pd.DataFrame:

    strategy  = IFVGStrategy()
    warmup    = max(strategy.ZONE_BARS, strategy.LOOKBACK) + 10

    trades    = []
    capital   = initial_capital
    equity    = [initial_capital]
    position  = None           # {side, entry, sl, tp, wick, entry_bar, entry_time, lot}
    last_close_bar = -9999

    def _point():
        return 0.01

    def _snap(v, d='nearest'):
        return round(v, 2)

    def _build_levels(side, entry, wick):
        buf   = 0.02
        min_d = 0.10
        if side == 'BUY':
            sl   = min(wick - buf, entry - min_d)
            sl   = _snap(sl)
            risk = max(entry - sl, min_d)
            tp   = _snap(entry + risk * 6)
        else:
            sl   = max(wick + buf, entry + min_d)
            sl   = _snap(sl)
            risk = max(sl - entry, min_d)
            tp   = _snap(entry - risk * 6)
        return sl, tp, risk

    def _pnl(side, entry, exit_p, risk, lot_usd):
        price_risk = risk
        if price_risk == 0:
            return 0.0
        # normalise to dollar PnL: 1 unit risk = lot_usd
        move = (exit_p - entry) if side == 'BUY' else (entry - exit_p)
        raw  = (move / price_risk) * lot_usd
        comm = lot_usd * commission_pct
        return round(raw - comm, 2)

    for i in range(warmup, len(df)):
        bar = df.iloc[i]
        t   = df.index[i]

        # ── Check if open position hit SL or TP this bar ──
        if position is not None:
            s    = position['side']
            sl   = position['sl']
            tp   = position['tp']
            risk = position['risk']
            hit  = None

            if s == 'BUY':
                # Assume: if open gaps past SL/TP, fill at that level
                open_p = float(bar['open'])
                if open_p <= sl:                           # gap below SL
                    hit, exit_p = 'SL', open_p
                elif open_p >= tp:                         # gap above TP
                    hit, exit_p = 'TP', open_p
                elif float(bar['low']) <= sl:              # SL touched intrabar
                    hit, exit_p = 'SL', sl
                elif float(bar['high']) >= tp:             # TP touched intrabar
                    hit, exit_p = 'TP', tp
            else:  # SELL
                open_p = float(bar['open'])
                if open_p >= sl:
                    hit, exit_p = 'SL', open_p
                elif open_p <= tp:
                    hit, exit_p = 'TP', open_p
                elif float(bar['high']) >= sl:
                    hit, exit_p = 'SL', sl
                elif float(bar['low']) <= tp:
                    hit, exit_p = 'TP', tp

            if hit:
                pnl_val = _pnl(s, position['entry'], exit_p, risk, position['lot_usd'])
                capital += pnl_val
                result   = 'WIN' if pnl_val > 0 else 'LOSS'

                trades.append({
                    'trade_no':    len(trades) + 1,
                    'entry_time':  position['entry_time'],
                    'exit_time':   str(t),
                    'side':        s,
                    'entry':       position['entry'],
                    'sl':          sl,
                    'tp':          position['tp'],
                    'wick':        position['wick'],
                    'exit':        exit_p,
                    'hit':         hit,
                    'risk_pts':    round(risk, 3),
                    'lot_usd':     position['lot_usd'],
                    'pnl':         pnl_val,
                    'capital':     round(capital, 2),
                    'result':      result,
                    'ifvg_zone':   position.get('zone','?'),
                })
                last_close_bar = i
                position = None

        equity.append(capital)

        # ── Look for new entry ──
        if position is None and (i - last_close_bar) >= cooldown_bars:
            window = df.iloc[:i + 1].copy()
            try:
                feat = FeatureEngine.add_indicators(window)
            except Exception:
                continue
            if feat.empty:
                continue

            sig, wick = strategy.get_signal_and_wick(feat)
            if sig == 0 or wick == 0.0:
                continue

            entry = float(bar['close'])   # enter at bar close (conservative)
            side  = 'BUY' if sig == 1 else 'SELL'
            sl, tp, risk = _build_levels(side, entry, wick)

            # Reject if SL is unrealistically far (> 10% of price)
            if risk / entry > 0.10:
                continue

            zone = 'DISCOUNT' if sig == 1 else 'PREMIUM'
            position = {
                'side': side, 'entry': entry, 'sl': sl, 'tp': tp,
                'wick': wick, 'risk': risk, 'lot_usd': lot_usd,
                'entry_bar': i, 'entry_time': str(t), 'zone': zone,
            }

    # Close any open position at last bar price
    if position is not None:
        last_bar = df.iloc[-1]
        exit_p   = float(last_bar['close'])
        pnl_val  = _pnl(position['side'], position['entry'], exit_p,
                         position['risk'], position['lot_usd'])
        capital += pnl_val
        trades.append({
            'trade_no':   len(trades) + 1,
            'entry_time': position['entry_time'],
            'exit_time':  str(df.index[-1]),
            'side':       position['side'],
            'entry':      position['entry'],
            'sl':         position['sl'],
            'tp':         position['tp'],
            'wick':       position['wick'],
            'exit':       exit_p,
            'hit':        'OPEN@END',
            'risk_pts':   round(position['risk'], 3),
            'lot_usd':    position['lot_usd'],
            'pnl':        pnl_val,
            'capital':    round(capital, 2),
            'result':     'WIN' if pnl_val > 0 else 'LOSS',
            'ifvg_zone':  position.get('zone','?'),
        })

    trades_df = pd.DataFrame(trades)

    # ── Print summary ──
    print(f"\n{'='*62}")
    print(f"  BACKTEST  |  {label}  |  CL=F (USOIL)")
    print(f"{'='*62}")
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

    print(f"  Initial capital : ${initial_capital:,.2f}")
    print(f"  Final capital   : ${capital:,.2f}")
    print(f"  Net P&L         : ${net:+,.2f}  ({net/initial_capital*100:+.1f}%)")
    print(f"  Total trades    : {n}")
    print(f"  Wins / Losses   : {wins} / {losses}")
    print(f"  Win rate        : {wr:.1%}")
    print(f"  Profit factor   : {pf}")
    print(f"  Avg win         : ${avg_win:+,.2f}")
    print(f"  Avg loss        : ${avg_loss:+,.2f}")
    print(f"  Best trade      : ${best:+,.2f}")
    print(f"  Worst trade     : ${worst:+,.2f}")
    print(f"  Max drawdown    : ${mdd:,.2f}")
    print(f"  Max DD %        : {mdd/initial_capital*100:.1f}%")

    by_zone = trades_df.groupby('ifvg_zone').agg(
        trades=('pnl','count'), wins=('result', lambda x: (x=='WIN').sum()),
        pnl=('pnl','sum')
    )
    by_zone['wr'] = (by_zone['wins']/by_zone['trades']).map('{:.1%}'.format)
    print(f"\n  By zone:")
    print(by_zone[['trades','wins','wr','pnl']].to_string())

    by_side = trades_df.groupby('side').agg(
        trades=('pnl','count'), wins=('result', lambda x: (x=='WIN').sum()),
        pnl=('pnl','sum')
    )
    by_side['wr'] = (by_side['wins']/by_side['trades']).map('{:.1%}'.format)
    print(f"\n  By direction:")
    print(by_side[['trades','wins','wr','pnl']].to_string())

    print(f"{'='*62}\n")

    return trades_df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 62)
    print("  SHIVA V5 — IFVG + Discount/Premium BACKTEST")
    print("  Asset: USOIL (CL=F WTI Crude Oil Futures)")
    print("=" * 62)
    print()

    # ── 1. Daily 2018-2026 (full history) ──
    df_daily = fetch('1d', start='2018-01-01')
    trades_d = run(df_daily, label='DAILY 2018→2026', cooldown_bars=1)

    # ── 2. 15-minute (last 60 days — max available) ──
    df_15m   = fetch('15m')
    trades_15 = run(df_15m, label='15m last 60 days', cooldown_bars=6, lot_usd=500.0)

    # ── Save full trade logs ──
    if not trades_d.empty:
        out = 'backtest_daily_trades.csv'
        trades_d.to_csv(out, index=False)
        print(f"📝 Daily trade log → {out}  ({len(trades_d)} trades)")

        # Print every single trade
        print("\n─── ALL DAILY TRADES ────────────────────────────────────────────────────")
        pd.set_option('display.width', 180)
        pd.set_option('display.max_rows', None)
        cols = ['trade_no','entry_time','exit_time','side','entry','sl','tp',
                'wick','exit','hit','risk_pts','pnl','capital','result','ifvg_zone']
        print(trades_d[cols].to_string(index=False))

    if not trades_15.empty:
        out2 = 'backtest_15m_trades.csv'
        trades_15.to_csv(out2, index=False)
        print(f"\n📝 15m trade log   → {out2}  ({len(trades_15)} trades)")

        print("\n─── ALL 15m TRADES ─────────────────────────────────────────────────────")
        cols = ['trade_no','entry_time','exit_time','side','entry','sl','tp',
                'wick','exit','hit','risk_pts','pnl','capital','result','ifvg_zone']
        print(trades_15[cols].to_string(index=False))
