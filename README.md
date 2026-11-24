# fp-copilot

Unofficial, **read-only** risk tracker and auto-journal for MyFundedPerps challenge accounts.
Not official MyFundedPerps software. It never places, modifies or cancels orders, and it
does not promise or imply that it helps anyone pass a challenge.

## What it does
- **watch**: connects to the account WebSocket stream, tracks equity and distance to the
  daily-loss and max-drawdown floors, and alerts (terminal, Telegram, Discord) at 70% / 90% of
  the allowance used (configurable).
- **Auto-journal**: every fill and every closed trade goes into a local SQLite file
  (`journal.db`) with R-multiple (from the first stop seen on the position) and ICT killzone tag
  (New York time; edit `KILLZONES` in the source).
- **summary**: end-of-day stats (win rate, P&L, fees, avg R, trades with no stop, per-killzone
  breakdown). `--send` pushes it to Telegram/Discord.
- **size**: position sizer. Give entry, stop and a risk % (or USD) and it returns a size rounded
  down to the market's `size_step`, checks `min_size` / `min_notional`, and warns if the risk is
  bigger than your remaining daily-loss room. A calculator only.
- **accounts**: lists your accounts with equity, loss-limit room and distance to target.

## Setup
```bash
pip install -r requirements.txt
cp .env.example .env        # add a READ-ONLY key; never commit .env
python fp_copilot.py accounts
python fp_copilot.py watch
python fp_copilot.py summary
python fp_copilot.py size --market BTC --entry 118000 --stop 117500 --risk-pct 0.5
```
Create keys under Settings > API Keys. `fp_test_` keys use the sandbox automatically.

## How it uses the API
- Stream: `wss://account-stream.myfundedperpetuals.com/v1/stream` (sandbox path for test keys).
  Snapshot first, then events; resume with `resume`/`last_seq`/`since`; 45s silence watchdog;
  exponential backoff with jitter; honors `Retry-After` on 429/503; stops on 401/403/close 4001.
- The stream carries balance, orders, fills and positions but **not marks or equity**. So equity
  and loss-limit room come from `GET /v1/accounts/{id}`, fetched when the stream reports a change
  and every `FP_RISK_REFRESH_SECS` seconds only while a position is open. Fills are deduplicated
  by id, so replayed events are safe.
- Alert percentages use allowance = `daily_loss_pct` (or `max_drawdown_pct`) x starting balance,
  and used = 1 - room / allowance. That is an approximation (for example a trailing drawdown may
  be defined differently): compare it with the dashboard.
- Trade `realized_pnl` is stored as reported by the API; check whether it is before or after fees.

## Testing
- `python test_smoke.py`: offline test with a mock REST + WebSocket server (journal dedupe,
  R-multiple, killzone tags, alert levels, resume URL, summary, sizer).
- Sandbox run: (fill in) date, what you traded, what matched the dashboard.
- Live read-only run: (fill in).

## Known limits
- Beta API: field names may change. `FP_DEBUG=1` prints raw stream frames (never keys).
- v1 does not use the market data stream, so unrealized P&L updates at the refresh interval.
