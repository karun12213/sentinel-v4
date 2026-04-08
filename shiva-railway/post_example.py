#!/usr/bin/env python3
"""Post example position embed to Discord."""
import requests
from datetime import datetime, timezone

url = "https://discord.com/api/webhooks/1491123381645475873/nzQN0XmvxggQX0WInBTLqGvNkcqd5nCFuIt494Ib7cmv2q6_vPRlvbQmQ7xwmOjAbGMH"

embed = {
    "title": "\U0001F4C9 Position #3 Opened \u2014 SELL",
    "color": 0xFF4444,
    "fields": [
        {"name": "\U0001F3AF Entry", "value": "`$114.356`", "inline": True},
        {"name": "\U0001F6D1 Stop Loss", "value": "`$114.656`", "inline": True},
        {"name": "\U0001F4C8 Take Profit", "value": "`TRAIL (no fixed TP)`", "inline": True},
        {"name": "\U0001F4CB Direction", "value": "**SELL**", "inline": True},
        {"name": "\U0001F4CA Lot Size", "value": "`0.03`", "inline": True},
        {"name": "\U0001F4CA Symbol", "value": "`SpotCrude`", "inline": True},
        {"name": "\U0001F4A1 Note", "value": "`SELL signal from 23/40 agents. SL trails to breakeven once in profit. Let winners run!`", "inline": False},
    ],
    "footer": {"text": "\U0001F531 SHIVA Trading Bot"},
    "timestamp": datetime.now(timezone.utc).isoformat(),
}

r = requests.post(url, json={"embeds": [embed]}, timeout=10)
if r.status_code in (200, 204):
    print("Posted!")
else:
    print(f"Failed: {r.status_code}")
