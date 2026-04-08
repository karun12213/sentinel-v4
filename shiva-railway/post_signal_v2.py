#!/usr/bin/env python3
"""Post SHIVA signal + instructions to Discord using Vercel API data."""
import requests, json
from datetime import datetime, timezone

WEBHOOK = "https://discord.com/api/webhooks/1491123381645475873/nzQN0XmvxggQX0WInBTLqGvNkcqd5nCFuIt494Ib7cmv2q6_vPRlvbQmQ7xwmOjAbGMH"
VERCEL = "https://shiva-admin-dun.vercel.app/api/status"

# Get live data from Vercel (synced from Railway)
r = requests.get(VERCEL, timeout=10)
d = r.json() if r.ok else {}

price = d.get("price", 0)
cycle = d.get("cycle", 0)
equity = d.get("equity", 0)
pnl = d.get("pnl", 0)
pnl_pct = d.get("pnl_pct", 0)
wins = d.get("wins", 0)
losses = d.get("losses", 0)
buy_c = d.get("buy_count", 0)
sell_c = d.get("sell_count", 0)
hold_c = d.get("hold_count", 0)
signal = d.get("consensus", "HOLD")
strength = d.get("consensus_pct", 0)
agents = d.get("agents", [])

# Top 5 agents agreeing with signal
agreeing = [a for a in agents if a.get("signal") == signal][:5]
agent_lines = "\n".join([f"{a['emoji']} {a['name']}" for a in agreeing]) or "—"

# Calculate SL/TP
p = float(price) if price else 0
sl_buy = f"${p - 0.30:.3f}" if p else "—"
sl_sell = f"${p + 0.30:.3f}" if p else "—"
tp_text = "TRAIL — winners trail until momentum breaks"

# Colors & emoji
colors = {"BUY": 0x00FF88, "SELL": 0xFF4444, "HOLD": 0xFFAA00}
arrows = {"BUY": "\U0001F4C8", "SELL": "\U0001F4C9", "HOLD": "\u26A0\uFE0F"}
pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"

embed = {
    "title": f"{arrows.get(signal,'\u26A0\uFE0F')} SHIVA Signal \u2014 Cycle #{cycle}",
    "description": f"**{signal}** ({strength}%)" if signal != "HOLD" else "\u26AA **HOLD** \u2014 No new entries",
    "color": colors.get(signal, 0x4488FF),
    "fields": [
        {"name": "\U0001F4CA Market Data",
         "value": f"Price: `${price}` | Symbol: `SpotCrude`\nEquity: `${equity:.2f}` | PnL: `{pnl_str}` ({pnl_pct:.2f}%)",
         "inline": False},
        {"name": "\U0001F3AF Trade Plan",
         "value": (
             f"If **BUY**: Entry `${price}` | SL `{sl_buy}`\n"
             f"If **SELL**: Entry `${price}` | SL `{sl_sell}`\n"
             f"TP: `{tp_text}`"
         ),
         "inline": False},
        {"name": "\U0001F916 AI Consensus",
         "value": f"\U0001F7E2 BUY: `{buy_c}` | \U0001F534 SELL: `{sell_c}` | \u26AA HOLD: `{hold_c}`",
         "inline": False},
        {"name": "\u2705 Agents Agreeing",
         "value": agent_lines,
         "inline": False},
        {"name": "\U0001F4CB Instructions",
         "value": (
             "1. **Do nothing** \u2014 bot auto-executes all 6 positions\n"
             "2. **No manual close** \u2014 trail winners, cut losers at -$0.50\n"
             "3. **SL \u2192 breakeven** once position hits profit\n"
             "4. **Let winners run** \u2014 no fixed TP, trails automatically\n"
             "5. **Discord notifies** you on every open/close event\n"
             "6. **Mac can stay OFF** \u2014 running 24/7 on Railway"
         ),
         "inline": False},
        {"name": "\U0001F4A1 Note to You",
         "value": (
             "`SHIVA is fully automated. You don't need to watch or interfere. "
             "The 40 AI agents analyze the market every 30 seconds and open "
             "positions when consensus is strong enough. Risk is controlled: "
             "max loss per trade is $0.50, winners run until the trend breaks. "
             "Trust the system. Stay disciplined. The edge is in patience.`"
         ),
         "inline": False},
        {"name": "\U0001F4C8 Record",
         "value": f"W: `{wins}` | L: `{losses}`",
         "inline": True},
        {"name": "\u2699\uFE0F Config",
         "value": f"Lot: `0.03` | Max: `6` | Interval: `30s`",
         "inline": True},
    ],
    "footer": {"text": "\U0001F531 SHIVA Live Trading \u2014 Railway 24/7"},
    "timestamp": datetime.now(timezone.utc).isoformat(),
}

r = requests.post(WEBHOOK, json={"embeds": [embed]}, timeout=10)
if r.status_code in (200, 204):
    print(f"Posted: {signal} | Cycle #{cycle} | PnL: {pnl_str}")
else:
    print(f"Failed: {r.status_code}")
