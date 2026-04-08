# SHIVA Godmode Overload - Discord Signal Integration

## Overview
Posts trading signals from SHIVA Godmode Overload (hosted on Vercel/Railway) to Discord with complete trade details.

## Key Features
- ✅ **One Direction Only**: Each signal includes SL/TP for ONLY the active direction (BUY or SELL), not both
- ✅ **Complete Trade Details**: Entry, SL, TP, lot size, confidence, AI consensus
- ✅ **Auto-Posts**: Signals, position opens, trade closures, wins/losses
- ✅ **Vercel + Railway**: Works with both deployment platforms

## Signal Format

### BUY Signal Example
```
📈 SHIVA Signal — Cycle #42
**BUY** (75% confidence)

📊 Market Data
Price: $72.450 | Symbol: SpotCrude
Equity: $1,250.00 | Balance: $1,200.00
PnL: +$50.00 (4.17%)

🎯 Trade Plan (BUY)
Direction: BUY
Entry: $72.450
Stop Loss: $72.150
Take Profit: TRAIL — winners run until trend breaks
Lot Size: 0.03

🤖 AI Consensus
🟢 BUY: 24 | 🔴 SELL: 8 | ⚪ HOLD: 8

📋 Positions: 2/6
📈 Record: Wins: 15 | Losses: 5

📝 Instructions
1. Bot auto-executes — no manual intervention needed
2. Do NOT manually close — bot trails winners & cuts losers
3. SL moves to breakeven once position is in profit
4. Let winners run — trail system handles exits
5. Losers cut at -$0.50 — small losses preserve capital
6. Monitor Discord for auto-updates on every trade event
```

### SELL Signal Example
```
📉 SHIVA Signal — Cycle #43
**SELL** (80% confidence)

🎯 Trade Plan (SELL)
Direction: SELL
Entry: $72.500
Stop Loss: $72.800
Take Profit: TRAIL — winners run until trend breaks
Lot Size: 0.03
```

## Deployment

### Railway

1. **Connect Repository** to Railway
2. **Set Environment Variables**:
   ```
   DISCORD_TRADES=https://discord.com/api/webhooks/YOUR/WEBHOOK
   SHIVA_LOG_FILE=logs/shiva_live.log
   VERCEL_URL=https://shiva-godmode-overlord-dday.vercel.app
   ```
3. **Deploy Command**:
   ```bash
   bash start_discord_poster.sh
   ```

### Manual/VPS

```bash
cd shiva-railway
pip install -r requirements.txt
export DISCORD_TRADES="https://discord.com/api/webhooks/YOUR/WEBHOOK"
python post_signal_unified.py
```

### Cron (Every 5 Minutes)

```bash
*/5 * * * * cd /path/to/shiva-railway && python post_signal_unified.py >> logs/discord_poster.log 2>&1
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DISCORD_TRADES` | Discord webhook URL for signals | ✅ |
| `SHIVA_LOG_FILE` | Path to SHIVA live log file | ✅ |
| `VERCEL_URL` | Vercel deployment URL (for sync) | ❌ |

## Usage

### Post Current Signal
```bash
python post_signal_unified.py
```

### Post Trade Closed (from your code)
```python
from post_signal_unified import post_trade_closed

post_trade_closed({
    "signal": "BUY",
    "pnl": 1.50,
    "reason": "🟢 Hit target / trailed to profit",
    "note": "`Risk is controlled. Trust the system.`"
})
```

### Post Position Opened (from your code)
```python
from post_signal_unified import post_position_opened

post_position_opened({
    "direction": "BUY",
    "entry": "72.450",
    "sl": "72.150",
    "tp": "TRAIL",
    "lot_size": "0.03"
})
```

## Integration with SHIVA Bot

The signal poster integrates with:
- **`shiva_live_bot.js`** - Main Railway bot
- **`continuous.js`** - Continuous trading cycle
- **Vercel API** - Dashboard sync

## Discord Webhook Setup

1. Go to Discord Server Settings → Integrations → Webhooks
2. Create a new webhook in your trading channel
3. Copy the webhook URL
4. Set as `DISCORD_TRADES` environment variable

## Files

- `post_signal_unified.py` - Main signal poster (NEW)
- `post_signal_discord.py` - Legacy signal poster
- `post_signal_v2.py` - V2 signal poster
- `shiva_sync_all.py` - Vercel + Discord sync
- `start_discord_poster.sh` - Startup script

## Notes

- Signals are posted with **one direction only** (BUY or SELL, not both)
- SL is calculated as ±$0.30 from entry
- TP uses trailing system (no fixed target)
- Bot auto-manages positions (trails winners, cuts losers at -$0.50)
- Max 6 positions open simultaneously
- 40 AI agents provide consensus every 30 seconds
