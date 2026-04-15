import asyncio
import os
import pandas as pd
from datetime import datetime
from metaapi_cloud_sdk import MetaApi
import pandas_ta as ta

# --- LEAN FEATURE ENGINE ---
class FeatureEngine:
    @staticmethod
    def add_indicators(df):
        df = df.copy()
        df['RSI'] = ta.rsi(df['close'], length=14)
        df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        df['EMA_20'] = ta.ema(df['close'], length=20)
        df['EMA_50'] = ta.ema(df['close'], length=50)
        df['EMA_200'] = ta.ema(df['close'], length=200)
        return df.dropna()

# --- OPTIMIZED STRATEGIES ---
class TrendStrategy:
    def generate_signal(self, row):
        # EMA crossover trend following with volatility filter
        if row['EMA_50'] > row['EMA_200'] and row['close'] > row['EMA_20']:
            return 1 # BUY
        elif row['EMA_50'] < row['EMA_200'] and row['close'] < row['EMA_20']:
            return -1 # SELL
        return 0

class ReversionStrategy:
    def generate_signal(self, row):
        if row['RSI'] < 30: return 1 # BUY (Oversold)
        elif row['RSI'] > 70: return -1 # SELL (Overbought)
        return 0

# --- EXECUTION ENGINE ---
class ExecutionEngine:
    def __init__(self, token, account_id, symbol="USOIL"):
        self.token = token
        self.account_id = account_id
        self.symbol = symbol
        self.trend = TrendStrategy()
        self.reversion = ReversionStrategy()
        self.is_running = True

    async def run(self):
        print(f"🔱 SHIVA V4 LIVE | {self.symbol} | $100 Optimize")
        self.api = MetaApi(self.token)
        try:
            account = await self.api.metatrader_account_api.get_account(self.account_id)
            await account.wait_connected()
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()

            while self.is_running:
                try:
                    candles = await account.get_historical_candles(self.symbol, '15m', limit=300)
                    df = pd.DataFrame(candles)
                    df = FeatureEngine.add_indicators(df)
                    latest = df.iloc[-1]
                    
                    # ENSEMBLE SIGNAL
                    s1 = self.trend.generate_signal(latest)
                    s2 = self.reversion.generate_signal(latest)
                    signal = 1 if (s1 + s2) >= 1 else -1 if (s1 + s2) <= -1 else 0

                    positions = await connection.get_positions()
                    current = next((p for p in positions if p['symbol'] == self.symbol), None)
                    
                    if not current and signal != 0:
                        lot = 0.01
                        atr = latest['ATR']
                        sl_dist = atr * 1.5
                        tp_dist = atr * 4.5
                        
                        if signal == 1:
                            sl, tp = latest['close'] - sl_dist, latest['close'] + tp_dist
                            print(f"🚀 BUY {self.symbol} @ {latest['close']} | SL: {sl:.2f} | TP: {tp:.2f}")
                            await connection.create_market_buy_order(self.symbol, lot, sl, tp)
                        else:
                            sl, tp = latest['close'] + sl_dist, latest['close'] - tp_dist
                            print(f"📉 SELL {self.symbol} @ {latest['close']} | SL: {sl:.2f} | TP: {tp:.2f}")
                            await connection.create_market_sell_order(self.symbol, lot, sl, tp)

                    await asyncio.sleep(60)
                except Exception as e:
                    print(f"⚠️ Warning: {e}")
                    await asyncio.sleep(10)
        except Exception as e:
            print(f"❌ Fatal: {e}")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    TOKEN = os.getenv('METAAPI_TOKEN')
    ACCOUNT_ID = os.getenv('METAAPI_ACCOUNT_ID')
    engine = ExecutionEngine(TOKEN, ACCOUNT_ID)
    asyncio.run(engine.run())
