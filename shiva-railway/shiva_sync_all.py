#!/usr/bin/env python3
"""
SHIVA Sync → Vercel + Discord
Monitors log, pushes to Vercel admin panel AND posts trade signals to Discord.
"""

import os, re, json, time, logging, subprocess, requests
from datetime import datetime, timezone

LOG_FILE = os.path.expanduser(os.getenv("SHIVA_LOG_FILE", "~/logs/shiva_live.log"))
TRADE_HISTORY_FILE = os.path.expanduser("~/trade_history.json")
VERCEL_URL = os.getenv("VERCEL_URL", "").rstrip("/")
DISCORD_TRADES = os.getenv("DISCORD_TRADES", "")
DISCORD_ALERTS = os.getenv("DISCORD_ALERTS", "")
DISCORD_STATUS = os.getenv("DISCORD_STATUS", "")
POLL_INTERVAL = int(os.getenv("SYNC_INTERVAL", "10"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("shiva-sync")

# ============ DISCORD ============
C = {"green":0x00FF88,"red":0xFF4444,"orange":0xFFAA00,"blue":0x4488FF,"purple":0x9B59B6}

def discord_post(url, title, fields, color="blue", footer="🔱 SHIVA Trading Bot"):
    if not url: return
    try:
        requests.post(url, json={"embeds":[{
            "title": title, "color": C.get(color,0x4488FF), "fields": fields,
            "footer": {"text": footer},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]}, timeout=10)
    except Exception as e:
        log.error(f"Discord error: {e}")

# ============ PARSERS ============
RE_TS   = re.compile(r"🕐\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})")
RE_CYC  = re.compile(r"📊 Cycle:\s+#(\d+)\s*\|\s*Trades:\s*(\d+)\s*\|\s*W:(\d+)\s*L:(\d+)")
RE_EQ   = re.compile(r"💰\s*EQUITY:\s*\$([\d,]+\.?\d*)\s*\|\s*Balance:\s*\$([\d,]+\.?\d*)")
RE_PNL  = re.compile(r"💵\s*PnL:\s*([+-]?)\$([\d,]+\.?\d*)\s*\(([-+]?[\d,]+\.?\d*)%\)")
RE_PR   = re.compile(r"💹\s*Price:\s*\$([\d,]+\.?\d*)\s*\|\s*(\w+)\s*\|\s*([\d.]+)\s*lots")
RE_CON  = re.compile(r"BUY:(\d+)\s*SELL:(\d+)\s*HOLD:(\d+)")
RE_AGT  = re.compile(r"(?:✅\s*|  )\s*(\S+)\s+(\w+)\s+(BUY|SELL)")
RE_POS  = re.compile(r"📋\s*(\d+)/(\d+)\s*positions full")

# Trade & position events
RE_TRADE     = re.compile(r"📝\s*Trade logged:\s*(\S+)\s*\|\s*(\w+)\s*\|\s*PnL:\s*([+-]?)\$([\d.]+)\s*\|\s*(\w+)")
RE_POS_OPEN  = re.compile(r"🎯\s*ENTRY:\s*([\d.]+)\s*\|\s*🛑\s*SL:\s*([\d.]+)\s*\|\s*📈\s*TP:\s*(.+?)\s*\|\s*📋\s*(BUY|SELL)\s*\|\s*#(\d+)")
RE_CUT_LOSER = re.compile(r"🔴\s*CUTTING LOSER\s*\|\s*(\S+)\s*\|\s*PnL:\s*\$([-\d.]+)")
RE_TAKE_PRO  = re.compile(r"🟢\s*TAKING PROFIT\s*\|\s*(\S+)\s*\|\s*Peak:\s*\+\$([\d.]+)\s*\|\s*Now:\s*\+\$([\d.]+)")
RE_CONNECTED = re.compile(r"✅\s*CONNECTED\s*\|\s*Balance:\s*\$([\d.]+)\s*\|\s*Equity:\s*\$([\d.]+)")

def parse_dashboard(content):
    blocks = content.split("🔱 SHIVA LIVE TRADING BOT")
    if len(blocks) < 2: return None
    text = blocks[-1]
    d = {"agents": []}
    m = RE_TS.search(text)
    if m: d["timestamp"] = m.group(1)
    m = RE_CYC.search(text)
    if m: d["cycle"]=int(m.group(1)); d["total_trades"]=int(m.group(2)); d["wins"]=int(m.group(3)); d["losses"]=int(m.group(4))
    m = RE_EQ.search(text)
    if m: d["equity"]=float(m.group(1).replace(",","")); d["balance"]=float(m.group(2).replace(",",""))
    m = RE_PNL.search(text)
    if m: d["pnl"]=(1 if m.group(1) in ("+","") else -1)*float(m.group(2).replace(",","")); d["pnl_pct"]=float(m.group(3).replace(",",""))
    m = RE_PR.search(text)
    if m: d["price"]=float(m.group(1).replace(",","")); d["symbol"]=m.group(2); d["lot_size"]=float(m.group(3))
    m = RE_CON.search(text)
    if m:
        b,s,h = int(m.group(1)),int(m.group(2)),int(m.group(3))
        d["buy_count"]=b; d["sell_count"]=s; d["hold_count"]=h
        t=b+s
        if t>0:
            bp=round(b/t*100); d["consensus"]="BUY" if bp>50 else "SELL" if bp<50 else "HOLD"; d["consensus_pct"]=max(bp,100-bp)
    for m in RE_AGT.finditer(text):
        d["agents"].append({"emoji":m.group(1),"name":m.group(2),"signal":m.group(3)})
    m = RE_POS.search(text)
    if m: d["open_positions"]=int(m.group(1)); d["max_positions"]=int(m.group(2))
    return d

# ============ EVENT HANDLER ============
def handle_events(events, vercel_url, cycle_count):
    for ev in events:
        t = ev.get("type")
        m = ev.get("match", ())

        # ─── NEW POSITION OPENED ───
        if t == "pos_open":
            entry = m[0] if len(m) > 0 else "?"
            sl = m[1] if len(m) > 1 else "?"
            tp = m[2] if len(m) > 2 else "Trail"
            direction = m[3] if len(m) > 3 else "?"
            pos_num = m[4] if len(m) > 4 else "?"
            arrow = "📈" if direction == "BUY" else "📉"
            discord_post(DISCORD_ALERTS,
                f"{arrow} Position #{pos_num} Opened — {direction}",
                [
                    {"name": "🎯 Entry", "value": f"`${entry}`", "inline": True},
                    {"name": "🛑 Stop Loss", "value": f"`${sl}`", "inline": True},
                    {"name": "📈 Take Profit", "value": f"`{tp}`", "inline": True},
                    {"name": "📋 Direction", "value": f"**{direction}**", "inline": True},
                    {"name": "📊 Lot Size", "value": "`0.03`", "inline": True},
                    {"name": "💡 Note", "value": f"`{direction}` signal from {ev.get('buy_count','?')}/{ev.get('total_agents','40')} agents. SL trails to breakeven once in profit. Let winners run!", "inline": False},
                ],
                "green" if direction == "BUY" else "red")
            log.info(f"📤 Position #{pos_num} {direction} | Entry: ${entry} | SL: ${sl}")

        # ─── TRADE CLOSED (from trade log) ───
        elif t == "trade":
            trade_id = m[0] if len(m) > 0 else "?"
            signal = m[1] if len(m) > 1 else "?"
            pnl_sign = 1 if len(m) > 2 and m[2] in ("+","") else -1
            pnl = pnl_sign * float(m[3] if len(m) > 3 else 0)
            reason = m[4] if len(m) > 4 else "?"
            result = "WIN 🎉" if pnl > 0 else "LOSS"
            reason_map = {
                "take_profit": "🟢 Hit target / trailed to profit",
                "cut_loss": "🔴 Stop loss hit — risk managed",
                "cut_loss_timeout": "⏱ Time exit — position stalled",
            }
            note = reason_map.get(reason, f"Exit: {reason}")
            discord_post(DISCORD_TRADES,
                f"{'🟢' if pnl > 0 else '🔴'} {result} — ${abs(pnl):.2f}",
                [
                    {"name": "📈 Signal", "value": f"**{signal}**", "inline": True},
                    {"name": "💵 PnL", "value": f"`${pnl:+.2f}`", "inline": True},
                    {"name": "📝 Exit", "value": note, "inline": True},
                    {"name": "💡 Note to you", "value": "`Risk is controlled. Trust the system.`" if pnl > 0 else "`Losses are part of the edge. Stay disciplined.`", "inline": False},
                ],
                "green" if pnl > 0 else "red")
            log.info(f"📝 Trade: {signal} | PnL: ${pnl:+.2f} | {reason}")

        # ─── CUTTING LOSER ───
        elif t == "cut_loser":
            pos_id = m[0][:16] if len(m) > 0 else "?"
            pnl_val = float(m[1]) if len(m) > 1 else 0
            discord_post(DISCORD_ALERTS,
                f"🔴 Cutting Loser — ${pnl_val:.2f}",
                [
                    {"name": "🆔 Position", "value": f"`{pos_id}`", "inline": True},
                    {"name": "💵 Loss", "value": f"`${pnl_val:.2f}`", "inline": True},
                    {"name": "💡 Note", "value": "`Small loss saved. Capital preserved for the next setup. Stay focused.`", "inline": False},
                ], "red")

        # ─── TAKING PROFIT ───
        elif t == "take_profit":
            pos_id = m[0][:16] if len(m) > 0 else "?"
            peak = float(m[1]) if len(m) > 1 else 0
            exit_pnl = float(m[2]) if len(m) > 2 else 0
            discord_post(DISCORD_ALERTS,
                f"🟢 Taking Profit — +${exit_pnl:.2f}",
                [
                    {"name": "🆔 Position", "value": f"`{pos_id}`", "inline": True},
                    {"name": "🏔 Peak PnL", "value": f"`+${peak:.2f}`", "inline": True},
                    {"name": "💵 Exit PnL", "value": f"`+${exit_pnl:.2f}`", "inline": True},
                    {"name": "💡 Note", "value": "`Winner banked. Trail system worked. Let the next one run too.`", "inline": False},
                ], "green")

        # ─── CONNECTED ───
        elif t == "connected":
            bal = m[0] if len(m) > 0 else "?"
            eq = m[1] if len(m) > 1 else "?"
            discord_post(DISCORD_ALERTS, "✅ SHIVA Connected to MT4", [
                {"name": "💵 Balance", "value": f"`${float(bal):.2f}`", "inline": True},
                {"name": "💰 Equity", "value": f"`${float(eq):.2f}`", "inline": True},
                {"name": "💡 Note", "value": "`Bot is live. Monitoring 40 agents. Awaiting signals.`", "inline": False},
            ], "green")

        # ─── DASHBOARD (Vercel + periodic Discord) ───
        elif t == "dashboard":
            cycle_count += 1
            if vercel_url:
                try: requests.post(f"{vercel_url}/api/status", json=ev, timeout=10)
                except: pass
            # Full signal with Entry/SL/TP + instructions every 10 cycles - DISABLED
            if False and cycle_count % 10 == 0:
                pnl = ev.get("pnl", 0)
                con = ev.get("consensus", "HOLD")
                price = ev.get("price", 0)
                p = float(price) if price else 0
                # ONLY show SL for ONE direction
                if con == "BUY":
                    sl_buy = f"${p - 0.30:.3f}" if p else "—"
                    trade_plan = f"Entry: `${price}`\n**BUY SL**: `{sl_buy}`\nTP: `TRAIL — winners run until trend breaks`
                elif con == "SELL":
                    sl_sell = f"${p + 0.30:.3f}" if p else "—"
                    trade_plan = f"Entry: `${price}`\n**SELL SL**: `{sl_sell}`\nTP: `TRAIL — winners run until trend breaks`
                else:
                    trade_plan = "No active signal — monitoring market"
                agents = ev.get("agents", [])
                agreeing = [a for a in agents if a.get("signal") == con][:5]
                agent_str = "\n".join([f"{a.get('emoji','')} {a.get('name','')}" for a in agreeing]) or "—"
                colors_map = {"BUY": 0x00FF88, "SELL": 0xFF4444, "HOLD": 0xFFAA00}
                arrow = {"BUY": "📈", "SELL": "📉", "HOLD": "⚠️"}.get(con, "⚠️")
                pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
                discord_post(DISCORD_TRADES,
                    f"SHIVA Signal Cycle #{ev.get('cycle','?')}",
                    [
                        {"name": "📊 Market", "value": f"Price: `${price}` | Equity: `${ev.get('equity',0):.2f}`\nPnL: `{pnl_str}` ({ev.get('pnl_pct',0):.2f}%)", "inline": False},
                        {"name": "🎯 Trade Plan", "value": trade_plan, "inline": False},
                        {"name": "🤖 Consensus", "value": f"🟢:{ev.get('buy_count',0)} 🔴:{ev.get('sell_count',0)} ⚪:{ev.get('hold_count',0)}", "inline": True},
                        {"name": "✅ Top Agents", "value": agent_str, "inline": True},
                        {"name": "💡 Instructions", "value": "`1. Do nothing — bot auto-executes\n2. No manual close — trail & cut\n3. SL → breakeven in profit\n4. Let winners run\n5. Discord notifies on every event\n6. Mac can stay OFF — Railway 24/7`", "inline": False},
                        {"name": "💡 Note", "value": "`SHIVA is fully automated. 40 AI agents scan every 30s. Max loss per trade: $0.50. Winners run until trend breaks. Trust the system. Stay disciplined.`", "inline": False},
                    ],
                    "green" if pnl >= 0 else "red")                    ],
                    "green" if pnl >= 0 else "red")
            if vercel_url and cycle_count % 3 == 0:
                try:
                    trades = []
                    try:
                        with open(TRADE_HISTORY_FILE) as f: trades = json.load(f)[-200:]
                    except: pass
                    requests.post(f"{vercel_url}/api/trades", json=trades, timeout=10)
                except: pass
            if vercel_url:
                try:
                    r = subprocess.run(["tail","-n","500",LOG_FILE], capture_output=True, text=True)
                    requests.post(f"{vercel_url}/api/log", json={"log":r.stdout[-50000:]}, timeout=15)
                except: pass
            log.info(f"✅ Cycle #{ev.get('cycle','?')} | ${ev.get('equity',0):,.2f} | PnL: ${ev.get('pnl',0):+,.2f}")
    return cycle_count

# ============ MAIN ============
def main():
    log.info("🔱 SHIVA → Vercel + Discord")
    log.info(f"🌐 Vercel: {VERCEL_URL or 'off'} | 💬 Trades: {'✅' if DISCORD_TRADES else '❌'} | Alerts: {'✅' if DISCORD_ALERTS else '❌'}")

    offset = 0
    cycle_count = 0

    # Push current state on startup
    try:
        with open(LOG_FILE) as f: content = f.read()
        dash = parse_dashboard(content)
        if dash:
            log.info(f"📤 Initial: Cycle #{dash.get('cycle','?')} | Equity: ${dash.get('equity',0):,.2f}")
            cycle_count = handle_events([{"type":"dashboard",**dash}], VERCEL_URL, cycle_count)
            # Post current signal to Discord
            con = dash.get("consensus","HOLD")
            con_pct = dash.get("consensus_pct",0)
            agents = dash.get("agents",[])
            agreeing = [a for a in agents if a["signal"] == con][:5]
            agent_str = "\n".join([f"{a['emoji']} {a['name']} → **{a['signal']}**" for a in agreeing]) or "None"
            discord_post(DISCORD_TRADES,
                f"🔱 SHIVA Signal — Cycle #{dash.get('cycle','?')}",
                [
                    {"name": "🤖 Consensus", "value": f"**{con}** ({con_pct}%)", "inline": True},
                    {"name": "💰 Equity", "value": f"`${dash.get('equity',0):,.2f}`", "inline": True},
                    {"name": "📈 PnL", "value": f"`${dash.get('pnl',0):+,.2f}` ({dash.get('pnl_pct',0):.2f}%)", "inline": True},
                    {"name": "💹 Price", "value": f"`${dash.get('price',0):.3f}` {dash.get('symbol','')}", "inline": True},
                    {"name": "📋 Positions", "value": f"`{dash.get('open_positions',0)}/{dash.get('max_positions',6)}`", "inline": True},
                    {"name": "📊 Agents", "value": f"🟢:{dash.get('buy_count',0)} 🔴:{dash.get('sell_count',0)} ⚪:{dash.get('hold_count',0)}", "inline": True},
                    {"name": f"✅ Top → {con}", "value": agent_str, "inline": False},
                    {"name": "💡 Note", "value": "`Bot is monitoring. Trades will auto-post when they open/close.`", "inline": False},
                ], "green" if dash.get("pnl",0) >= 0 else "red")
            try:
                trades = []
                try:
                    with open(TRADE_HISTORY_FILE) as f: trades = json.load(f)[-200:]
                except: pass
                if VERCEL_URL:
                    requests.post(f"{VERCEL_URL}/api/trades", json=trades, timeout=10)
                    r = subprocess.run(["tail","-n","500",LOG_FILE], capture_output=True, text=True)
                    requests.post(f"{VERCEL_URL}/api/log", json={"log":r.stdout[-50000:]}, timeout=15)
            except: pass
        offset = os.path.getsize(LOG_FILE)
    except FileNotFoundError:
        log.warning(f"Log not found: {LOG_FILE}")

    # Monitor loop
    while True:
        try:
            if not os.path.exists(LOG_FILE):
                time.sleep(POLL_INTERVAL); continue
            size = os.path.getsize(LOG_FILE)
            if size < offset: offset = 0
            if size > offset:
                with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
                    f.seek(offset); new_lines = f.read().splitlines()
                offset = size

                events = []
                buf = []; in_dash = False
                total_agents = 0
                for line in new_lines:
                    line = line.strip()
                    if "🔱 SHIVA LIVE TRADING BOT" in line:
                        in_dash = True; buf = [line]; continue
                    if in_dash:
                        buf.append(line)
                        if RE_POS.search(line):
                            d = parse_dashboard("\n".join(buf))
                            if d:
                                d["total_agents"] = len(d.get("agents",[]))
                                events.append({"type":"dashboard",**d})
                            buf = []; in_dash = False
                    for etype, pat in [
                        ("trade", RE_TRADE),
                        ("pos_open", RE_POS_OPEN),
                        ("cut_loser", RE_CUT_LOSER),
                        ("take_profit", RE_TAKE_PRO),
                        ("connected", RE_CONNECTED),
                    ]:
                        m = pat.search(line)
                        if m:
                            events.append({"type":etype,"match":m.groups()})

                if events:
                    cycle_count = handle_events(events, VERCEL_URL, cycle_count)
        except KeyboardInterrupt:
            log.info("Shutting down..."); break
        except Exception as e:
            log.error(f"Error: {e}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
