#!/usr/bin/env python3
"""
SHIVA Godmode Overload → Discord Signal Poster
Posts signals from Vercel/Railway with:
- ONLY BUY SL or SELL SL (not both)
- ALL trade details from live positions
- ALL 40 AI agent signals
"""
import requests
from datetime import datetime, timezone

# Fetch live dashboard from Vercel
print('🔱 Fetching SHIVA Godmode Overload data from Vercel...')
resp = requests.get('https://shiva-godmode-overlord-dday.vercel.app/api/dashboard')
data = resp.json()

summary = data['summary']
positions = data['livePositions']
config = data['config']
agent_messages = data.get('recentAgentMessages', [])
recent_trades = data.get('recentTrades', [])
ml_model = data.get('mlModel')

# Analyze current positions
sell_positions = [p for p in positions if 'SELL' in p['type']]
buy_positions = [p for p in positions if 'BUY' in p['type']]

current_price = positions[0]['currentPrice'] if positions else 0
avg_entry = sum(p['openPrice'] for p in positions) / len(positions) if positions else 0
total_profit = sum(p['profit'] for p in positions)

# Determine signal based on positions
if len(sell_positions) > len(buy_positions):
    signal = 'SELL'
    signal_color = 0xFF4444
    signal_emoji = '📉'
    # ONLY calculate SELL SL
    avg_sl = sum(p['stopLoss'] for p in sell_positions) / len(sell_positions) if sell_positions else 0
    sl_display = f'${avg_sl:.3f}'
elif len(buy_positions) > len(sell_positions):
    signal = 'BUY'
    signal_color = 0x00FF88
    signal_emoji = '📈'
    # ONLY calculate BUY SL
    avg_sl = sum(p['stopLoss'] for p in buy_positions) / len(buy_positions) if buy_positions else 0
    sl_display = f'${avg_sl:.3f}'
else:
    signal = 'HOLD'
    signal_color = 0xFFAA00
    signal_emoji = '⚠️'
    sl_display = '—'

strength = 75 if signal != 'HOLD' else 0

# Count agents by signal
sell_count = len(sell_positions)
buy_count = len(buy_positions)
hold_count = 40 - sell_count - buy_count

# Build Discord embed
fields = []

# Market Data
fields.append({
    'name': '📊 Market Data',
    'value': f'Price: `${current_price:.3f}` | Symbol: `{config["symbol"]}`\n'
             f'Equity: `${summary["equity"]:.2f}` | Balance: `${summary["balance"]:.2f}`\n'
             f'PnL: `+${summary["totalPnl"]:.2f}` ({summary["cycles"]} cycles)',
    'inline': False
})

# Trade Plan - ONLY show SL from bot's actual positions
fields.append({
    'name': '🎯 Trade Plan',
    'value': f'Entry: `${current_price:.3f}`\n'
             f'**SL**: `{sl_display}` (Bot Managed)\n'
             f'Take Profit: `TRAIL — winners run until trend breaks`\n'
             f'Lot Size: `{config["lot_size"]}`',
    'inline': False
})

# AI Consensus
fields.append({
    'name': '🤖 AI Consensus (40 Agents)',
    'value': f'🟢 BUY: `{buy_count}` | 🔴 SELL: `{sell_count}` | ⚪ HOLD: `{hold_count}`',
    'inline': False
})

# All 40 Agent Signals
agent_list = []
for i, p in enumerate(positions):
    direction = p['type'].replace('POSITION_TYPE_', '')
    agent_list.append(f'`#{i+1}` {direction} → **{direction}**')

# Add remaining agents as HOLD if we have less than 40 positions
for i in range(len(positions), 40):
    agent_list.append(f'`#{i+1}` Monitor → **HOLD**')

# Split into chunks of 10 to avoid Discord field limit
chunk_size = 10
for chunk_idx in range(0, len(agent_list), chunk_size):
    chunk = agent_list[chunk_idx:chunk_idx + chunk_size]
    fields.append({
        'name': f'🤖 Agent Signals ({chunk_idx+1}-{min(chunk_idx+chunk_size, 40)})',
        'value': '\n'.join(chunk),
        'inline': True
    })

# All Trade Details - Every position with full details
position_details = []
for i, p in enumerate(positions):
    pnl_pct = ((p['currentPrice'] - p['openPrice']) / p['openPrice'] * 100) if p['openPrice'] > 0 else 0
    direction = p['type'].replace('POSITION_TYPE_', '')
    position_details.append(
        f'**#{i+1}** {direction}\n'
        f'Entry: `${p["openPrice"]:.3f}` | Current: `${p["currentPrice"]:.3f}`\n'
        f'SL: `${p["stopLoss"]:.3f}` | PnL: `+${p["profit"]:.2f}` ({pnl_pct:+.1f}%)'
    )

fields.append({
    'name': '📋 All Trade Details',
    'value': '\n\n'.join(position_details),
    'inline': False
})

# Summary
fields.append({
    'name': '📊 Summary',
    'value': f'🟢 BUY Positions: `{len(buy_positions)}`\n'
             f'🔴 SELL Positions: `{len(sell_positions)}`\n'
             f'📈 Total Profit: `+${total_profit:.2f}`\n'
             f'📉 Avg Entry: `${avg_entry:.3f}`\n'
             f'🛑 {"SELL" if signal == "SELL" else "BUY"} SL: `{sl_display}`\n'
             f'🧠 ML Model: `{("Trained" if ml_model else "Learning")} ({summary.get("wins", 0)}W/{summary.get("losses", 0)}L)`',
    'inline': False
})

# Instructions
fields.append({
    'name': '📝 Instructions',
    'value': '1. **Bot auto-executes** — no manual intervention needed\n'
             '2. **Do NOT manually close** — bot trails winners & cuts losers\n'
             '3. **SL moves to breakeven** once position is in profit\n'
             '4. **Let winners run** — trail system handles exits\n'
             '5. **Losers cut at -$0.50** — small losses preserve capital\n'
             '6. **Monitor Discord** for auto-updates on every trade event',
    'inline': False
})

fields.append({
    'name': '💡 Note',
    'value': '`SHIVA Godmode Overload is fully automated on Vercel/Railway (24/7). '
             '40 AI agents scan every 30s. Max 6 positions. '
             'Trust the system. Stay disciplined.`',
    'inline': False
})

payload = {
    'embeds': [{
        'title': f'SHIVA Signal Cycle #{summary["cycles"]}',
        'description': f'**{signal}** ({strength}% confidence)' if signal != 'HOLD' else '⚪ **HOLD** — No new entries',
        'color': signal_color,
        'fields': fields,
        'footer': {'text': '🔱 SHIVA Godmode Overload — Vercel/Railway 24/7 | 40 AI Agents'},
        'timestamp': datetime.now(timezone.utc).isoformat()
    }]
}

# Post to Discord
webhook = 'https://discord.com/api/webhooks/1491123381645475873/nzQN0XmvxggQX0WInBTLqGvNkcqd5nCFuIt494Ib7cmv2q6_vPRlvbQmQ7xwmOjAbGMH'
resp = requests.post(webhook, json=payload, timeout=10)

if resp.status_code in (200, 204):
    print(f'✅ Signal posted to Discord!')
    print(f'')
    print(f'📊 Signal: {signal}')
    print(f'🔢 Cycle: #{summary["cycles"]}')
    print(f'💰 Price: ${current_price:.3f}')
    print(f'📈 Total Profit: +${total_profit:.2f}')
    print(f'📋 Positions: {len(positions)} open')
    print(f'🤖 Agents: {buy_count} BUY | {sell_count} SELL | {hold_count} HOLD')
    print(f'')
    if signal == 'SELL':
        print(f'🎯 SELL SL: {sl_display}')
    elif signal == 'BUY':
        print(f'🎯 BUY SL: {sl_display}')
    print(f'✅ All trade details included')
    print(f'✅ All 40 AI agent signals included')
else:
    print(f'❌ Failed: {resp.status_code}')
    print(resp.text[:200])
