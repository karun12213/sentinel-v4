#!/usr/bin/env python3
"""
SHIVA Signal + Instructions → Discord
Posts current market signal with entry, SL, TP, instructions, and notes.
"""
import requests, os, re
from datetime import datetime, timezone

WEBHOOK = os.getenv("DISCORD_TRADES", "https://discord.com/api/webhooks/1491123381645475873/nzQN0XmvxggQX0WInBTLqGvNkcqd5nCFuIt494Ib7cmv2q6_vPRlvbQmQ7xwmOjAbGMH")
LOG_FILE = os.getenv("SHIVA_LOG_FILE", "logs/shiva_live.log")

# Parse latest dashboard from log
try:
    with open(LOG_FILE) as f:
        content = f.read()
    blocks = content.split("\U0001F531 SHIVA LIVE TRADING BOT")
    text = blocks[-1] if len(blocks) > 1 else ""
except:
    text = ""

def rx(p):
    m = re.search(p, text)
    return m.group(1) if m else None

price = rx(r"Price:\s*\$([\d.]+)")
cycle = rx(r"Cycle:\s*#(\d+)")
equity = rx(r"EQUITY:\s*\$([\d,]+)")
pnl = rx(r"PnL:\s*([+-]?)\$([\d.]+)")
pnl_sign = rx(r"PnL:\s*([+-]?)") or "+"
pnl_pct = rx(r"\(([-+]?[\d.]+)%\)")
wins = rx(r"W:(\d+)")
losses = rx(r"L:(\d+)")
buy_c = rx(r"BUY:(\d+)") or "0"
sell_c = rx(r"SELL:(\d+)") or "0"
hold_c = rx(r"HOLD:(\d+)") or "0"

# Determine signal
buy_n, sell_n = int(buy_c), int(sell_c)
total = buy_n + sell_n
if total > 0:
    bp = round(buy_n / total * 100)
    signal = "BUY" if bp > 50 else "SELL" if bp < 50 else "HOLD"
    strength = max(bp, 100-bp)
else:
    signal, strength = "HOLD", 0

# Calculate SL/TP
if price:
    p = float(price)
    sl_buy = f"${p - 0.30:.3f}"
    sl_sell = f"${p + 0.30:.3f}"
    tp_text = "TRAIL (no fixed TP — winners trail until momentum drops)"
else:
    sl_buy, sl_sell = "—", "—"
    tp_text = "—"

# Colors
colors = {"BUY": 0x00FF88, "SELL": 0xFF4444, "HOLD": 0xFFAA00}
arrows = {"BUY": "\U0001F4C8", "SELL": "\U0001F4C9", "HOLD": "\u26A0\uFE0F"}

embed = {
    "title": f"{arrows.get(signal,'\u26A0\uFE0F')} SHIVA Signal — Cycle #{cycle or '?'}",
    "description": f"**{signal}** ({strength}%)" if signal != "HOLD" else "\u26AA **HOLD** — No new entries",
    "color": colors.get(signal, 0x4488FF),
    "fields": [
        {"name": "\U0001F4CA Market Data",
         "value": f"Price: `\\${price}` | Symbol: `SpotCrude`\nEquity: `\\${equity}` | PnL: `{pnl_sign}{pnl}` ({pnl_pct}%)",
         "inline": False},
        {"name": "\U0001F3AF Trade Plan",
         "value": (
             f"If **BUY**: Entry `\\${price}` | SL `{sl_buy}`\n"
             f"If **SELL**: Entry `\\${price}` | SL `{sl_sell}`\n"
             f"TP: `{tp_text}`"
         ),
         "inline": False},
        {"name": "\U0001F916 AI Consensus",
         "value": f"\\U0001F7E2 BUY: `{buy_n}` | \\U0001F534 SELL: `{sell_n}` | \\u26AA HOLD: `{hold_c}`",
         "inline": False},
        {"name": "\U0001F4CB Instructions",
         "value": (
             "1. **Wait for bot to auto-execute** — 6 positions max\n"
             "2. **Do NOT manually close** — bot trails winners & cuts losers\n"
             "3. **SL moves to breakeven** once position is in profit\n"
             "4. **Let winners run** — no fixed TP, trail system handles it\n"
             "5. **Losers cut at -$0.50** — small losses preserve capital\n"
             "6. **Monitor Discord** for auto-updates on every trade event"
         ),
         "inline": False},
        {"name": "\U0001F4A1 Note",
         "value": (
             "`SHIVA is fully automated on Railway (24/7). "
             "You don't need to do anything — the bot monitors 40 AI agents, "
             "opens positions on consensus, and manages risk automatically. "
             "Trust the system. Stay disciplined. Every loss is small and controlled. "
             "Every winner runs until the trend breaks. The edge is in the discipline.`"
         ),
         "inline": False},
        {"name": "\U0001F4C8 Record",
         "value": f"Wins: `{wins or 0}` | Losses: `{losses or 0}` | Win Rate: `—`",
         "inline": True},
        {"name": "\u2699\uFE0F Settings",
         "value": f"Lot: `0.03` | Max Pos: `6` | Check: `30s`",
         "inline": True},
    ],
    "footer": {"text": "\U0001F531 SHIVA Live Trading — Railway 24/7"},
    "timestamp": datetime.now(timezone.utc).isoformat(),
}

r = requests.post(WEBHOOK, json={"embeds": [embed]}, timeout=10)
if r.status_code in (200, 204):
    print(f"Posted: {signal} | Cycle #{cycle} | ${price}")
else:
    print(f"Failed: {r.status_code} {r.text[:200]}")
