#!/usr/bin/env python3
"""
SHIVA Godmode Overload → Discord Signal Poster
Posts signals with complete trade details from Vercel/Railway hosted bot.
Only includes SL or TP for ONE direction per signal (not both).
"""
import os
import re
import json
import requests
from datetime import datetime, timezone

# ============ CONFIG ============
DISCORD_WEBHOOK = os.getenv("DISCORD_TRADES", "")
SHIVA_LOG_FILE = os.getenv("SHIVA_LOG_FILE", "logs/shiva_live.log")
VERCEL_URL = os.getenv("VERCEL_URL", "").rstrip("/")

# Colors
COLORS = {
    "BUY": 0x00FF88,
    "SELL": 0xFF4444,
    "HOLD": 0xFFAA00,
    "WIN": 0x00FF88,
    "LOSS": 0xFF4444,
    "INFO": 0x4488FF,
}

# ============ LOG PARSER ============
def parse_latest_signal():
    """Parse the latest signal from SHIVA log file."""
    try:
        with open(SHIVA_LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except FileNotFoundError:
        return None

    # Split by dashboard blocks
    blocks = content.split("🔱 SHIVA LIVE TRADING BOT")
    if len(blocks) < 2:
        return None

    text = blocks[-1]

    # Extract fields
    def rx(pattern):
        m = re.search(pattern, text)
        return m.group(1) if m else None

    price = rx(r"Price:\s*\$([\d.]+)")
    cycle = rx(r"Cycle:\s*#(\d+)")
    equity = rx(r"EQUITY:\s*\$([\d,]+)")
    balance = rx(r"Balance:\s*\$([\d,]+)")
    pnl = rx(r"PnL:\s*([+-]?\$[\d.]+)")
    pnl_pct = rx(r"\(([-+]?[\d.]+)%\)")
    wins = rx(r"W:(\d+)")
    losses = rx(r"L:(\d+)")
    buy_c = rx(r"BUY:(\d+)")
    sell_c = rx(r"SELL:(\d+)")
    hold_c = rx(r"HOLD:(\d+)")
    positions = rx(r"(\d+)/(\d+) positions")
    max_positions = rx(r"\d+/(\d+) positions")
    symbol = rx(r"\|\s*(\w+)\s*\|")
    lot_size = rx(r"\|\s*([\d.]+)\s*lots")

    if not price:
        return None

    p = float(price)
    buy_n = int(buy_c or "0")
    sell_n = int(sell_c or "0")
    total = buy_n + sell_n

    if total > 0:
        buy_pct = round(buy_n / total * 100)
        signal = "BUY" if buy_pct > 60 else "SELL" if buy_pct < 40 else "HOLD"
        strength = max(buy_pct, 100 - buy_pct)
    else:
        signal = "HOLD"
        strength = 0

    # Calculate SL/TP for the signal direction ONLY
    sl_price = None
    tp_desc = None
    
    if signal == "BUY":
        sl_price = f"${p - 0.30:.3f}"
        tp_desc = "TRAIL — winners run until trend breaks"
    elif signal == "SELL":
        sl_price = f"${p + 0.30:.3f}"
        tp_desc = "TRAIL — winners run until trend breaks"

    return {
        "signal": signal,
        "strength": strength,
        "price": price,
        "symbol": symbol or "SpotCrude",
        "lot_size": lot_size or "0.03",
        "cycle": cycle or "?",
        "equity": equity or "0",
        "balance": balance or "0",
        "pnl": pnl or "$0.00",
        "pnl_pct": pnl_pct or "0.00",
        "wins": wins or "0",
        "losses": losses or "0",
        "buy_count": buy_n,
        "sell_count": sell_n,
        "hold_count": int(hold_c or "0"),
        "positions": positions or f"0/{max_positions or '6'}",
        "sl_price": sl_price,
        "tp_desc": tp_desc,
        "raw": text,
    }


def parse_agents(text):
    """Extract agent signals from dashboard text."""
    pattern = r"(?:✅\s*|  )\s*(\S+)\s+(\w+)\s+(BUY|SELL)"
    agents = []
    for m in re.finditer(pattern, text):
        agents.append({
            "emoji": m.group(1),
            "name": m.group(2),
            "signal": m.group(3),
        })
    return agents


# ============ DISCORD EMBED BUILDER ============
def build_signal_embed(data):
    """Build Discord embed for trading signal."""
    signal = data["signal"]
    strength = data["strength"]
    
    # Title and description
    arrows = {"BUY": "📈", "SELL": "📉", "HOLD": "⚠️"}
    title = f"{arrows[signal]} SHIVA Signal — Cycle #{data['cycle']}"
    
    if signal == "HOLD":
        description = "⚪ **HOLD** — No new entries"
    else:
        description = f"**{signal}** ({strength}% confidence)"

    # Build fields - ONLY include SL or TP for the signal direction
    fields = []

    # Market Data
    fields.append({
        "name": "📊 Market Data",
        "value": f"Price: `${data['price']}` | Symbol: `{data['symbol']}`\n"
                 f"Equity: `${data['equity']}` | Balance: `${data['balance']}`\n"
                 f"PnL: `{data['pnl']}` ({data['pnl_pct']}%)",
        "inline": False,
    })

    # Trade Plan - ONLY for the signal direction
    if signal == "BUY":
        fields.append({
            "name": "🎯 Trade Plan (BUY)",
            "value": f"Direction: **BUY**\n"
                     f"Entry: `${data['price']}`\n"
                     f"Stop Loss: `{data['sl_price']}`\n"
                     f"Take Profit: `{data['tp_desc']}`\n"
                     f"Lot Size: `{data['lot_size']}`",
            "inline": False,
        })
    elif signal == "SELL":
        fields.append({
            "name": "🎯 Trade Plan (SELL)",
            "value": f"Direction: **SELL**\n"
                     f"Entry: `${data['price']}`\n"
                     f"Stop Loss: `{data['sl_price']}`\n"
                     f"Take Profit: `{data['tp_desc']}`\n"
                     f"Lot Size: `{data['lot_size']}`",
            "inline": False,
        })
    else:
        fields.append({
            "name": "🎯 Trade Plan",
            "value": "No active signal — monitoring market",
            "inline": False,
        })

    # AI Consensus
    fields.append({
        "name": "🤖 AI Consensus",
        "value": f"🟢 BUY: `{data['buy_count']}` | 🔴 SELL: `{data['sell_count']}` | ⚪ HOLD: `{data['hold_count']}`",
        "inline": False,
    })

    # Positions
    fields.append({
        "name": "📋 Positions",
        "value": data["positions"],
        "inline": True,
    })

    # Record
    fields.append({
        "name": "📈 Record",
        "value": f"Wins: `{data['wins']}` | Losses: `{data['losses']}`",
        "inline": True,
    })

    # Instructions
    fields.append({
        "name": "📝 Instructions",
        "value": "1. **Bot auto-executes** — no manual intervention needed\n"
                 "2. **Do NOT manually close** — bot trails winners & cuts losers\n"
                 "3. **SL moves to breakeven** once position is in profit\n"
                 "4. **Let winners run** — trail system handles exits\n"
                 "5. **Losers cut at -$0.50** — small losses preserve capital\n"
                 "6. **Monitor Discord** for auto-updates on every trade event",
        "inline": False,
    })

    # Note
    fields.append({
        "name": "💡 Note",
        "value": "`SHIVA Godmode Overload is fully automated on Vercel/Railway (24/7). "
                 "40 AI agents scan every 30s. Max 6 positions. "
                 "Trust the system. Stay disciplined.`",
        "inline": False,
    })

    embed = {
        "title": title,
        "description": description,
        "color": COLORS.get(signal, 0x4488FF),
        "fields": fields,
        "footer": {"text": "🔱 SHIVA Godmode Overload — Vercel/Railway 24/7"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return {"embeds": [embed]}


def build_trade_closed_embed(trade_data):
    """Build embed for closed trade."""
    pnl = trade_data.get("pnl", 0)
    is_win = pnl > 0
    
    title = f"{'🟢' if is_win else '🔴'} Trade Closed — {'WIN' if is_win else 'LOSS'}"
    
    fields = [
        {
            "name": "📈 Signal",
            "value": trade_data.get("signal", "?"),
            "inline": True,
        },
        {
            "name": "💵 PnL",
            "value": f"`${pnl:+.2f}`",
            "inline": True,
        },
        {
            "name": "📝 Exit Reason",
            "value": trade_data.get("reason", "?"),
            "inline": True,
        },
        {
            "name": "💡 Note",
            "value": trade_data.get("note", "`Risk is controlled. Trust the system.`"),
            "inline": False,
        },
    ]

    embed = {
        "title": title,
        "color": COLORS["WIN" if is_win else "LOSS"],
        "fields": fields,
        "footer": {"text": "🔱 SHIVA Godmode Overload — Trade Closed"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return {"embeds": [embed]}


def build_position_opened_embed(pos_data):
    """Build embed for newly opened position."""
    direction = pos_data.get("direction", "BUY")
    
    title = f"{'📈' if direction == 'BUY' else '📉'} Position Opened — {direction}"
    
    fields = [
        {
            "name": "🎯 Entry",
            "value": f"`${pos_data.get('entry', '?')}`",
            "inline": True,
        },
        {
            "name": "🛑 Stop Loss",
            "value": f"`${pos_data.get('sl', '?')}`",
            "inline": True,
        },
        {
            "name": "📈 Take Profit",
            "value": pos_data.get("tp", "TRAIL"),
            "inline": True,
        },
        {
            "name": "📋 Direction",
            "value": f"**{direction}**",
            "inline": True,
        },
        {
            "name": "💰 Lot Size",
            "value": f"`{pos_data.get('lot_size', '0.03')}`",
            "inline": True,
        },
        {
            "name": "💡 Note",
            "value": f"`{direction}` signal from AI consensus. "
                     f"SL trails to breakeven once in profit. Let winners run!",
            "inline": False,
        },
    ]

    embed = {
        "title": title,
        "color": COLORS[direction],
        "fields": fields,
        "footer": {"text": "🔱 SHIVA Godmode Overload — Position Opened"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return {"embeds": [embed]}


# ============ POST TO DISCORD ============
def post_to_discord(webhook_url, payload):
    """Post embed to Discord webhook."""
    if not webhook_url:
        print("❌ No Discord webhook URL provided")
        return False

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code in (200, 204):
            print(f"✅ Posted to Discord successfully")
            return True
        else:
            print(f"❌ Discord post failed: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"❌ Discord error: {e}")
        return False


# ============ MAIN ============
def post_signal():
    """Main function to parse and post signal."""
    data = parse_latest_signal()
    if not data:
        print("❌ No signal data found in log file")
        return False

    print(f"📊 Signal: {data['signal']} | Cycle #{data['cycle']} | Price: ${data['price']}")

    # Build embed
    embed = build_signal_embed(data)

    # Post to Discord
    success = post_to_discord(DISCORD_WEBHOOK, embed)

    if success:
        print(f"✅ Posted {data['signal']} signal to Discord")
    else:
        print(f"❌ Failed to post signal")

    return success


def post_trade_closed(trade_data):
    """Post closed trade to Discord."""
    embed = build_trade_closed_embed(trade_data)
    return post_to_discord(DISCORD_WEBHOOK, embed)


def post_position_opened(pos_data):
    """Post opened position to Discord."""
    embed = build_position_opened_embed(pos_data)
    return post_to_discord(DISCORD_WEBHOOK, embed)


if __name__ == "__main__":
    post_signal()
