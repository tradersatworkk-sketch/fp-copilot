"""Offline smoke test: mock REST + WebSocket server that follows the documented formats.

This checks OUR logic (journal, R-multiples, killzones, alerts, resume, sizer).
It does not prove the real API behaves the same: run against a fp_test_ sandbox key too.
"""
import asyncio
import json
import os
import sqlite3
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import websockets

ACCOUNT = {"id": "acc1", "display_name": "Prime $10K", "stage": "evaluation", "status": "active",
           "starting_balance": 10000, "balance": 10000}
RISK = {"observed_at": 1, "marks_complete": True, "missing_markets": [], "equity": 9500.0,
        "unrealized_pnl": -500.0, "margin_used": 100, "available_balance": 9000, "gross_exposure": 5000,
        "daily_loss_floor": 9200.0, "max_drawdown_floor": 9000.0, "daily_loss_room": 300.0,
        "max_drawdown_room": 500.0, "remaining_profit": 800, "remaining_profit_equity": 1300.0,
        "requirements": {"daily_loss_pct": 5, "max_drawdown_pct": 10, "profit_target_pct": 8,
                         "consistency_pct": None, "minimum_trading_days": None}}
MARKET = {"market_id": "binance|BTCUSDT", "provider": "binance", "symbol": "BTC", "coin": "BTCUSDT",
          "size_decimals": 3, "max_leverage": 50, "tick_size": 1e-8, "size_step": 0.001,
          "min_size": 0.001, "min_notional": 10, "default_venue": True, "contract_size": 1}


class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_GET(self):
        assert self.headers.get("Authorization", "").startswith("Bearer fp_test_") or self.path == "/v1/markets"
        if self.path == "/v1/markets":
            body = {"data": [MARKET]}
        elif self.path == "/v1/accounts":
            body = {"data": [ACCOUNT]}
        elif self.path == "/v1/accounts/acc1":
            body = {"data": {**ACCOUNT, "risk": RISK}}
        else:
            self.send_response(404); self.end_headers(); return
        raw = json.dumps(body).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.end_headers(); self.wfile.write(raw)


# 2026-11-24 13:00 UTC = 08:00 New York -> "NY AM" killzone
OPEN_MS = 1795525200000
POS = {"id": "pos1", "account_id": "acc1", "market_id": "binance|BTCUSDT", "symbol": "BTC",
       "side": "long", "size": 0.1, "entry_price": 100000.0, "leverage": 2, "margin_mode": "cross",
       "isolated_margin_extra": 0, "status": "open", "opened_at": OPEN_MS}
FILL = {"id": "fill1", "order_id": "o1", "price": 100000.0, "size": 0.1, "notional": 10000.0,
        "fee": 4.0, "realized_pnl": 0, "time": OPEN_MS, "position_side_after": "long",
        "position_size_after": 0.1, "side": "buy", "order_type": "market",
        "market_id": "binance|BTCUSDT", "symbol": "BTC"}
STOP = {"id": "o2", "status": "resting", "type": "stop_market", "bracket_role": "stop_loss",
        "reduce_only": True, "market_id": "binance|BTCUSDT", "trigger_price": 99000.0}
CLOSED = {**POS, "status": "closed", "exit_price": 101000.0, "closed_at": OPEN_MS + 600000,
          "realized_pnl": 100.0, "fees": 8.0, "funding": 0.0, "close_reason": "take_profit"}

seen_urls = []


async def ws_handler(ws):
    seen_urls.append(ws.request.path)
    frames = [
        {"type": "hello", "stream_id": "s1", "environment": "test", "resumed": False},
        {"type": "snapshot", "seq": 1, "account_id": "acc1", "account": ACCOUNT, "orders": [], "positions": []},
        {"type": "snapshot_end", "replay": {"complete": True}},
        {"seq": 2, "time": 1, "type": "position", "event": "opened", "account_id": "acc1", "position": POS},
        {"seq": 3, "time": 2, "type": "fill", "account_id": "acc1", "fill": FILL},
        {"seq": 4, "time": 3, "type": "order", "event": "created", "account_id": "acc1", "order": STOP},
        {"seq": 5, "time": 4, "type": "position", "event": "closed", "account_id": "acc1", "position": CLOSED},
        {"seq": 6, "time": 5, "type": "fill", "account_id": "acc1", "fill": FILL, "replay": True},  # duplicate
    ]
    for f in frames:
        await ws.send(json.dumps(f))
    if len(seen_urls) == 1:      # first connection: drop it to test resume
        await asyncio.sleep(0.5)
        await ws.close(code=1012)
        return
    await asyncio.sleep(30)


async def main():
    tmp = tempfile.mkdtemp()
    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    async with websockets.serve(ws_handler, "127.0.0.1", 0) as wss:
        wport = wss.sockets[0].getsockname()[1]
        os.environ.update(FP_API_KEY="fp_test_dummy", FP_REST_BASE=f"http://127.0.0.1:{srv.server_port}",
                          FP_WS_URL=f"ws://127.0.0.1:{wport}", FP_DB=f"{tmp}/j.db", FP_TZ="UTC",
                          FP_RISK_REFRESH_SECS="1", FP_WARN_PCT="35", FP_DANGER_PCT="90")
        import fp_copilot as fc
        cfg = fc.Cfg()
        cop = fc.Copilot(cfg)
        alerts = []

        async def fake_send(text): alerts.append(text)
        cop.notify.send = fake_send
        try:
            await asyncio.wait_for(cop.run(), 6)
        except asyncio.TimeoutError:
            pass

        db = sqlite3.connect(cfg.db)
        fills = db.execute("SELECT COUNT(*) FROM fills").fetchone()[0]
        trade = db.execute("SELECT stop_price, r_multiple, killzone, close_reason FROM trades").fetchall()
        assert fills == 1, f"fills deduped by id, got {fills}"
        assert trade == [(99000.0, 1.0, "NY AM", "take_profit")], trade   # pnl 100 / risk (1000*0.1)
        assert any("daily limit 40% used" in a for a in alerts), alerts      # allowance 500, room 300
        assert len(seen_urls) >= 2 and "resume=s1" in seen_urls[1] and "last_seq=" in seen_urls[1], seen_urls
        print("OK  journal dedupe, stop->R-multiple, killzone, alert level, resume URL")

        # summary + sizer against the same mock
        import argparse, io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            fc.cmd_summary(cfg, argparse.Namespace(date="2026-11-24", send=False))
        out = buf.getvalue()
        assert "Trades 1" in out and "NY AM" in out and "Avg R 1.00" in out, out
        print("OK  summary")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            await fc.cmd_size(cfg, argparse.Namespace(market="BTC", entry=100000, stop=99500,
                                                      risk_pct=0.5, risk_usd=None, account="acc1"))
        out = buf.getvalue()
        assert "size 0.095" in out, out      # 9500 * 0.5% = 47.5 / 500 = 0.095
        print("OK  sizer")
        print(out)


asyncio.run(main())
