#!/usr/bin/env python3
"""fp-copilot: unofficial, READ-ONLY risk tracker + auto-journal for MyFundedPerps.

  python fp_copilot.py accounts            list accounts with live risk numbers
  python fp_copilot.py watch               live tracker + alerts + auto-journal
  python fp_copilot.py summary [--date D]  end-of-day journal summary
  python fp_copilot.py size ...            position sizer (calculator only, places nothing)

Never places, modifies or cancels orders. Use a read-only API key.
Not official MyFundedPerps software.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

# ICT killzones in New York time: (name, start_hour, end_hour). Edit to taste.
NY = ZoneInfo("America/New_York")
KILLZONES = [
    ("Asia", 20, 24),
    ("London", 2, 5),
    ("NY AM", 7, 10),
    ("London close", 10, 12),
    ("NY PM", 13.5, 16),
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS fills(
  id TEXT PRIMARY KEY, account_id TEXT, order_id TEXT, market_id TEXT, symbol TEXT,
  side TEXT, order_type TEXT, price REAL, size REAL, notional REAL, fee REAL,
  realized_pnl REAL, time INTEGER, pos_side_after TEXT, pos_size_after REAL);
CREATE TABLE IF NOT EXISTS trades(
  id TEXT PRIMARY KEY, account_id TEXT, market_id TEXT, symbol TEXT, side TEXT,
  size REAL, entry_price REAL, exit_price REAL, opened_at INTEGER, closed_at INTEGER,
  realized_pnl REAL, fees REAL, funding REAL, close_reason TEXT,
  stop_price REAL, r_multiple REAL, killzone TEXT);
"""


class Fatal(Exception):
    """Unrecoverable error (bad key, revoked key...). Do not retry."""


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_env(path: str = ".env") -> None:
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


class Cfg:
    def __init__(self) -> None:
        self.key = os.environ.get("FP_API_KEY", "")
        if not self.key.startswith(("fp_live_", "fp_test_")):
            raise Fatal("Set FP_API_KEY in .env (fp_test_... = sandbox, fp_live_... = live).")
        sandbox = self.key.startswith("fp_test_")
        self.rest = os.environ.get("FP_REST_BASE") or (
            "https://sandbox.myfundedperpetuals.com" if sandbox
            else "https://developers.myfundedperpetuals.com")
        self.ws = os.environ.get("FP_WS_URL") or (
            "wss://account-stream.myfundedperpetuals.com/v1/sandbox/stream" if sandbox
            else "wss://account-stream.myfundedperpetuals.com/v1/stream")
        self.account_id = os.environ.get("FP_ACCOUNT_ID") or None
        self.db = os.environ.get("FP_DB", "journal.db")
        self.tz = ZoneInfo(os.environ.get("FP_TZ", "UTC"))
        self.warn = float(os.environ.get("FP_WARN_PCT", "70"))
        self.danger = float(os.environ.get("FP_DANGER_PCT", "90"))
        self.risk_secs = float(os.environ.get("FP_RISK_REFRESH_SECS", "10"))
        self.tg_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.tg_chat = os.environ.get("TELEGRAM_CHAT_ID")
        self.discord = os.environ.get("DISCORD_WEBHOOK_URL")
        self.debug = os.environ.get("FP_DEBUG") == "1"


# --------------------------------------------------------------------------- REST
class Api:
    def __init__(self, cfg: Cfg) -> None:
        self.c = httpx.AsyncClient(
            base_url=cfg.rest, timeout=15,
            headers={"Authorization": f"Bearer {cfg.key}"})

    async def get(self, path: str, **params):
        """GET with Retry-After handling on 429/503. Returns the `data` field."""
        for attempt in range(6):
            r = await self.c.get(path, params=params or None)
            if r.status_code in (429, 503):
                wait = float(r.headers.get("Retry-After", 2 ** attempt))
                log(f"API returned {r.status_code}; waiting {wait:.0f}s (Retry-After)")
                await asyncio.sleep(wait + random.random())
                continue
            if r.status_code >= 400:
                try:
                    msg = r.json()["error"]["message"]
                except Exception:
                    msg = "request failed"
                raise ApiError(r.status_code, msg)
            return r.json()["data"]
        raise ApiError(429, "rate limited; retries exhausted")

    async def close(self) -> None:
        await self.c.aclose()


# --------------------------------------------------------------------------- alerts
class Notifier:
    def __init__(self, cfg: Cfg) -> None:
        self.cfg = cfg

    async def send(self, text: str) -> None:
        log(f"ALERT: {text}")
        c = self.cfg
        async with httpx.AsyncClient(timeout=10) as http:
            for name, url, body in (
                ("telegram", f"https://api.telegram.org/bot{c.tg_token}/sendMessage",
                 {"chat_id": c.tg_chat, "text": text}) if c.tg_token and c.tg_chat else (None,) * 3,
                ("discord", c.discord, {"content": text}) if c.discord else (None,) * 3,
            ):
                if not name:
                    continue
                try:
                    r = await http.post(url, json=body)
                    if r.status_code >= 400:
                        log(f"{name} notify failed: HTTP {r.status_code}")
                except Exception as e:  # never log the exception text: it can contain the URL
                    log(f"{name} notify failed: {type(e).__name__}")


# --------------------------------------------------------------------------- journal
def killzone(ms: int) -> str:
    t = datetime.fromtimestamp(ms / 1000, NY)
    h = t.hour + t.minute / 60
    for name, a, b in KILLZONES:
        if a <= h < b:
            return name
    return "Off-hours"


class Journal:
    def __init__(self, path: str) -> None:
        self.db = sqlite3.connect(path)
        self.db.executescript(SCHEMA)

    def add_fill(self, f: dict, account_id: str) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO fills VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (f.get("id"), account_id, f.get("order_id"), f.get("market_id"), f.get("symbol"),
             f.get("side"), f.get("order_type"), f.get("price"), f.get("size"),
             f.get("notional"), f.get("fee"), f.get("realized_pnl"), f.get("time"),
             f.get("position_side_after"), f.get("position_size_after")))
        self.db.commit()

    def add_trade(self, p: dict, stop: float | None) -> None:
        entry, size = p.get("entry_price"), p.get("size")
        pnl = p.get("realized_pnl")
        r = None
        if stop and entry and size and pnl is not None:
            risk = abs(entry - stop) * size
            r = round(pnl / risk, 2) if risk else None
        opened = p.get("opened_at")
        self.db.execute(
            "INSERT OR IGNORE INTO trades VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (p.get("id"), p.get("account_id"), p.get("market_id"), p.get("symbol"), p.get("side"),
             size, entry, p.get("exit_price"), opened, p.get("closed_at"), pnl,
             p.get("fees"), p.get("funding"), p.get("close_reason"), stop, r,
             killzone(opened) if opened else None))
        self.db.commit()


# --------------------------------------------------------------------------- live tracker
class Copilot:
    def __init__(self, cfg: Cfg) -> None:
        self.cfg = cfg
        self.api = Api(cfg)
        self.notify = Notifier(cfg)
        self.journal = Journal(cfg.db)
        self.accounts: dict[str, dict] = {}
        self.positions: dict[str, dict[str, dict]] = {}
        self.stops: dict[tuple[str, str], float] = {}   # (account, market) -> initial stop
        self.levels: dict[tuple[str, str], int] = {}    # alert level per account+rule
        self.last_state: dict[str, tuple] = {}
        self.dirty: set[str] = set()
        self.kick = asyncio.Event()
        self.stream_id: str | None = None
        self.last_seq: int | None = None
        self.last_time: int | None = None

    def wanted(self, aid: str | None) -> bool:
        return bool(aid) and (not self.cfg.account_id or aid == self.cfg.account_id)

    async def run(self) -> None:
        try:
            await asyncio.gather(self.stream_loop(), self.risk_loop())
        finally:
            await self.api.close()

    # ---- websocket with resume, watchdog and backoff -------------------------------
    async def stream_loop(self) -> None:
        backoff = 1.0
        while True:
            url = self.cfg.ws
            if self.stream_id and self.last_seq is not None:
                q = {"resume": self.stream_id, "last_seq": self.last_seq}
                if self.last_time is not None:
                    q["since"] = self.last_time
                url += "?" + urlencode(q)
            wait = backoff
            try:
                async with websockets.connect(
                        url, additional_headers={"Authorization": f"Bearer {self.cfg.key}"},
                        open_timeout=15) as ws:
                    log("stream connected")
                    backoff = 1.0
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=45)
                        except asyncio.TimeoutError:
                            log("no frames for 45s; reconnecting")
                            await ws.close()
                            break
                        await self.on_frame(json.loads(raw))
                    wait = 0.5
            except InvalidStatus as e:
                code = e.response.status_code
                if code in (401, 403):
                    raise Fatal(f"Stream rejected with HTTP {code}: check the key, that it matches "
                                "the environment (fp_test_ vs fp_live_), and sandbox access.")
                wait = float(e.response.headers.get("Retry-After", backoff))
                log(f"stream upgrade rejected ({code}); retrying in {wait:.0f}s")
                backoff = min(backoff * 2, 60)
            except ConnectionClosed as e:
                code = e.rcvd.code if e.rcvd else None
                if code == 4001:
                    raise Fatal("Key revoked/expired or sandbox access lapsed (close 4001). Not retrying.")
                wait = 0.5 if code == 1012 else backoff
                if code != 1012:
                    backoff = min(backoff * 2, 60)
                log(f"stream closed (code {code}); reconnecting")
            except (OSError, asyncio.TimeoutError) as e:
                log(f"stream error: {type(e).__name__}; retrying in {backoff:.0f}s")
                backoff = min(backoff * 2, 60)
            await asyncio.sleep(wait + random.random())  # jitter

    # ---- frame handling ------------------------------------------------------------
    async def on_frame(self, f: dict) -> None:
        t = f.get("type")
        if f.get("seq") is not None:
            self.last_seq = f["seq"]
        if f.get("time") is not None:
            self.last_time = f["time"]
        if self.cfg.debug and t != "heartbeat":
            log(f"frame: {json.dumps(f)[:300]}")
        if t in ("heartbeat", "pong"):
            return
        if t == "hello":
            self.stream_id = f.get("stream_id", self.stream_id)
            log(f"hello: env={f.get('environment')} resumed={f.get('resumed')}")
        elif t == "snapshot":
            self.on_snapshot(f)
        elif t == "snapshot_end":
            log("snapshot complete; tracking " + (", ".join(self.accounts) or "no accounts"))
        elif t in ("notice", "error"):
            log(f"{t}: {f.get('code')} {f.get('message', '')}")
        elif t == "reconnect":
            log("server asked us to reconnect")
        elif t == "order":
            self.on_order(f)
        elif t == "fill":
            aid = f.get("account_id")
            if self.wanted(aid) and f.get("fill"):
                self.journal.add_fill(f["fill"], aid)
                self.dirty.add(aid)
                self.kick.set()
        elif t == "position":
            self.on_position(f)
        elif t == "account":
            await self.on_account(f)

    def on_snapshot(self, f: dict) -> None:
        acc = f.get("account") or {}
        aid = f.get("account_id") or acc.get("id")
        if not self.wanted(aid):
            return
        prev = self.accounts.get(aid, {})
        self.accounts[aid] = {**acc, "risk": prev.get("risk")}
        self.positions[aid] = {p["id"]: p for p in f.get("positions", [])}
        for o in f.get("orders", []):
            self.track_stop(aid, o)
        self.dirty.add(aid)
        self.kick.set()

    def track_stop(self, aid: str, o: dict) -> None:
        """Remember the FIRST resting stop seen on an open position (for R-multiples)."""
        if o.get("status") != "resting":
            return
        is_stop = o.get("bracket_role") == "stop_loss" or (
            o.get("reduce_only") and o.get("type") in ("stop_market", "stop_limit"))
        price = o.get("trigger_price") or o.get("limit_price")
        if is_stop and price:
            self.stops.setdefault((aid, o.get("market_id")), float(price))

    def on_order(self, f: dict) -> None:
        aid, o = f.get("account_id"), f.get("order") or {}
        if self.wanted(aid) and f.get("event") in ("created", "edited"):
            self.track_stop(aid, o)

    def on_position(self, f: dict) -> None:
        aid, p, ev = f.get("account_id"), f.get("position") or {}, f.get("event")
        if not self.wanted(aid) or not p:
            return
        book = self.positions.setdefault(aid, {})
        if ev == "closed":
            book.pop(p.get("id"), None)
            key = (aid, p.get("market_id"))
            self.journal.add_trade({**p, "account_id": aid}, self.stops.pop(key, None))
        else:
            book[p["id"]] = p
        self.dirty.add(aid)
        self.kick.set()

    async def on_account(self, f: dict) -> None:
        aid = f.get("account_id")
        if not self.wanted(aid):
            return
        if f.get("event") == "removed":
            self.accounts.pop(aid, None)
            return
        acc = f.get("account") or {}
        prev = self.accounts.get(aid, {})
        self.accounts[aid] = {**prev, **acc, "risk": prev.get("risk")}
        state = (acc.get("status"), bool(f.get("locked_out")))
        old = self.last_state.get(aid)
        self.last_state[aid] = state
        if old is not None and state != old:
            name = self.accounts[aid].get("display_name") or aid
            await self.notify.send(f"{name}: status={state[0]} locked_out={state[1]}")
        self.dirty.add(aid)
        self.kick.set()

    # ---- risk snapshots: stream says WHEN something changed, REST gives equity/room --
    async def risk_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self.kick.wait(), timeout=self.cfg.risk_secs)
                await asyncio.sleep(1)  # debounce bursts of events
            except asyncio.TimeoutError:
                pass
            self.kick.clear()
            targets = set(self.dirty) | {a for a, p in self.positions.items() if p}
            self.dirty.clear()
            for aid in targets:
                if aid not in self.accounts:
                    continue
                try:
                    acc = await self.api.get(f"/v1/accounts/{aid}")
                except ApiError as e:
                    if e.status in (401, 403):
                        raise Fatal(f"API rejected the key: {e}")
                    log(f"risk refresh failed: {e}")
                    continue
                self.accounts[aid]["risk"] = acc.get("risk")
                self.accounts[aid]["balance"] = acc.get("balance")
                await self.evaluate(aid, acc)

    async def evaluate(self, aid: str, acc: dict) -> None:
        r = acc.get("risk") or {}
        name = acc.get("display_name") or aid
        req = r.get("requirements") or {}
        parts = [f"{name}", f"equity {fmt(r.get('equity'))}"]
        if not r.get("marks_complete", True):
            parts.append("(marks incomplete)")
        for label, room_key, pct_key in (("daily", "daily_loss_room", "daily_loss_pct"),
                                         ("max DD", "max_drawdown_room", "max_drawdown_pct")):
            room, pct = r.get(room_key), req.get(pct_key)
            if room is None:
                continue
            used = None
            if pct:
                allowance = acc["starting_balance"] * pct / 100  # approximation, see README
                used = max(0.0, min(100.0, (1 - room / allowance) * 100))
            parts.append(f"{label} room {fmt(room)}" + (f" ({used:.0f}% used)" if used is not None else ""))
            if used is not None:
                await self.check_level(aid, name, label, used, room)
        log(" | ".join(parts))

    async def check_level(self, aid: str, name: str, label: str, used: float, room: float) -> None:
        k = (aid, label)
        old = self.levels.get(k, 0)
        new = 2 if used >= self.cfg.danger else 1 if used >= self.cfg.warn else 0
        if new > old:
            self.levels[k] = new
            tag = "DANGER" if new == 2 else "WARNING"
            await self.notify.send(f"{tag} {name}: {label} limit {used:.0f}% used, room left {fmt(room)}")
        elif new < old and used < (self.cfg.danger if old == 2 else self.cfg.warn) - 5:
            self.levels[k] = new


def fmt(x) -> str:
    return "n/a" if x is None else f"${x:,.2f}"


# --------------------------------------------------------------------------- commands
async def cmd_accounts(cfg: Cfg, _args) -> None:
    api = Api(cfg)
    try:
        for a in await api.get("/v1/accounts"):
            d = await api.get(f"/v1/accounts/{a['id']}")
            r = d.get("risk") or {}
            print(f"{d['id']}  {d.get('display_name')}  {d['stage']}/{d['status']}  "
                  f"balance {fmt(d['balance'])}  equity {fmt(r.get('equity'))}  "
                  f"daily room {fmt(r.get('daily_loss_room'))}  DD room {fmt(r.get('max_drawdown_room'))}  "
                  f"to target {fmt(r.get('remaining_profit_equity'))}")
    finally:
        await api.close()


async def cmd_watch(cfg: Cfg, _args) -> None:
    log(f"watching ({'sandbox' if cfg.key.startswith('fp_test_') else 'LIVE'}), read-only. Ctrl+C to stop.")
    await Copilot(cfg).run()


async def cmd_size(cfg: Cfg, a) -> None:
    api = Api(cfg)
    try:
        markets = await api.get("/v1/markets")
        want = a.market.lower()
        m = next((x for x in markets if want in (x["market_id"].lower(), x["symbol"].lower(),
                                                  x["coin"].lower())), None)
        if not m:
            raise Fatal(f"Market '{a.market}' not found. Try e.g. 'BTC' or 'binance|BTCUSDT'.")
        aid = a.account or cfg.account_id
        if not aid:
            accts = [x for x in await api.get("/v1/accounts") if x["status"] == "active"]
            if not accts:
                raise Fatal("No active account found.")
            aid = accts[0]["id"]
        acc = await api.get(f"/v1/accounts/{aid}")
        risk = acc.get("risk") or {}
        equity = risk.get("equity") or acc["balance"]
        risk_usd = Decimal(str(a.risk_usd)) if a.risk_usd else Decimal(str(equity)) * Decimal(str(a.risk_pct)) / 100
        entry, stop = Decimal(str(a.entry)), Decimal(str(a.stop))
        dist = abs(entry - stop)
        if dist == 0:
            raise Fatal("Entry and stop are the same price.")
        step = Decimal(str(m["size_step"]))
        size = (risk_usd / dist / step).to_integral_value(ROUND_DOWN) * step
        notional = size * entry
        real_risk = size * dist
        print(f"{acc.get('display_name')}  equity {fmt(float(equity))}")
        print(f"{m['market_id']}  {'LONG' if stop < entry else 'SHORT'}  entry {entry}  stop {stop}")
        print(f"target risk {fmt(float(risk_usd))}  ->  size {size}  (risk {fmt(float(real_risk))}, "
              f"notional {fmt(float(notional))}, leverage needed ~{float(notional) / float(equity):.1f}x "
              f"of max {m['max_leverage']:g}x)")
        if size < Decimal(str(m["min_size"])):
            print(f"! below min size {m['min_size']}")
        if notional < Decimal(str(m["min_notional"])):
            print(f"! below min notional {fmt(m['min_notional'])}")
        room = risk.get("daily_loss_room")
        if room is not None and float(real_risk) > room:
            print(f"! this risk exceeds your daily loss room ({fmt(room)})")
        print("Note: excludes fees and slippage.")
    finally:
        await api.close()


def cmd_summary(cfg: Cfg, a) -> None:
    day = datetime.strptime(a.date, "%Y-%m-%d") if a.date else datetime.now(cfg.tz)
    start = datetime(day.year, day.month, day.day, tzinfo=cfg.tz)
    lo, hi = int(start.timestamp() * 1000), int((start + timedelta(days=1)).timestamp() * 1000)
    db = sqlite3.connect(cfg.db)
    rows = db.execute("SELECT symbol, side, realized_pnl, fees, r_multiple, killzone, stop_price "
                      "FROM trades WHERE closed_at >= ? AND closed_at < ? ORDER BY closed_at",
                      (lo, hi)).fetchall()
    out = [f"Journal {start:%Y-%m-%d} ({cfg.tz.key})"]
    if not rows:
        out.append("No closed trades recorded.")
    else:
        pnls = [r[2] or 0 for r in rows]
        wins = sum(1 for p in pnls if p > 0)
        rs = [r[4] for r in rows if r[4] is not None]
        out += [f"Trades {len(rows)} | wins {wins} ({wins / len(rows):.0%}) | "
                f"P&L as reported {fmt(sum(pnls))} | fees {fmt(sum(r[3] or 0 for r in rows))}",
                f"Avg R {sum(rs) / len(rs):.2f} over {len(rs)} trades with a stop" if rs
                else "Avg R n/a (no stops seen)",
                f"Trades with no stop: {sum(1 for r in rows if not r[6])}",
                f"Best {fmt(max(pnls))} | Worst {fmt(min(pnls))}", "By killzone:"]
        zones: dict[str, list[float]] = {}
        for r in rows:
            zones.setdefault(r[5] or "?", []).append(r[2] or 0)
        for z, v in zones.items():
            out.append(f"  {z:<13} {len(v)} trades  {fmt(sum(v))}  win {sum(1 for p in v if p > 0) / len(v):.0%}")
    text = "\n".join(out)
    print(text)
    if a.send:
        asyncio.run(Notifier(cfg).send(text))


def main() -> None:
    load_env()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("accounts")
    sub.add_parser("watch")
    s = sub.add_parser("summary")
    s.add_argument("--date", help="YYYY-MM-DD in FP_TZ (default today)")
    s.add_argument("--send", action="store_true", help="also send via Telegram/Discord")
    z = sub.add_parser("size")
    z.add_argument("--market", required=True, help="e.g. BTC or binance|BTCUSDT")
    z.add_argument("--entry", type=float, required=True)
    z.add_argument("--stop", type=float, required=True)
    g = z.add_mutually_exclusive_group(required=True)
    g.add_argument("--risk-pct", type=float, help="percent of equity to risk")
    g.add_argument("--risk-usd", type=float, help="fixed USD risk")
    z.add_argument("--account")
    args = ap.parse_args()
    if not args.cmd:
        ap.print_help()
        return
    try:
        cfg = Cfg()
        if args.cmd == "summary":
            cmd_summary(cfg, args)
        else:
            asyncio.run({"accounts": cmd_accounts, "watch": cmd_watch, "size": cmd_size}[args.cmd](cfg, args))
    except Fatal as e:
        sys.exit(f"error: {e}")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
