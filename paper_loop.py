#!/usr/bin/env python3
"""Persistent Kalshi paper lock loop. No orders. Clip-shaped: tight books, fast poll.

Steal from realfishsam/prediction-market-arbitrage-bot: running process, flatten-when-gone
is N/A for same-venue YES+NO (hold-to-$1 is the lock). We recycle via same-day sports.

Do not: YOLO, ignore fees, fuzzy Poly match, live fire, print keys.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scan_lib import (  # noqa: E402
    CAP_C,
    ET,
    Kalshi,
    combo_metrics,
    depth_ok,
    fmt_et,
    implied_asks_from_book,
    is_sportsy,
    skip_market,
    taker_fee,
    to_dollars,
    two_leg_metrics,
    close_dt,
)

DIR = Path("/workspace/kalshi-desk")
STATUS = DIR / "status.json"
ALERTS = DIR / "alerts.jsonl"
HEARTBEAT = DIR / "heartbeat"
PIDFILE = DIR / "loop.pid"
LOG = DIR / "loop.log"

MARKET_REFRESH_S = 90
BOOK_CYCLE_TARGET_S = 8
WATCH_CAP = 40
LIST_COST_CUT = 1.06
NEAR_LOCK = 0.0  # edge > 0 after fees is a lock
LIVE_FIRE = False  # never place orders from this process


def log(msg: str) -> None:
    line = f"{datetime.now(ET).strftime('%H:%M:%S')} {msg}"
    print(line, flush=True)
    with LOG.open("a") as f:
        f.write(line + "\n")


def write_status(payload: dict) -> None:
    payload["ts_et"] = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET")
    STATUS.write_text(json.dumps(payload, default=str, indent=2))
    HEARTBEAT.write_text(str(time.time()))


def cash_dollars(bal: dict):
    cash = bal.get("balance")
    if isinstance(cash, int) and cash > 1000:
        return cash / 100.0
    try:
        v = float(cash)
        return v / 100.0 if v > 50 else v
    except Exception:
        return cash


def refresh_universe(k: Kalshi):
    now = datetime.now(timezone.utc)
    today = now.astimezone(ET).date()
    tomorrow = today + timedelta(days=1)
    markets = k.paginate("/markets", "markets", params={"status": "open", "limit": 1000}, max_pages=40)
    screened = []
    for m in markets:
        if skip_market(m):
            continue
        ya = to_dollars(m.get("yes_ask_dollars") if m.get("yes_ask_dollars") is not None else m.get("yes_ask"))
        na = to_dollars(m.get("no_ask_dollars") if m.get("no_ask_dollars") is not None else m.get("no_ask"))
        m["_ya"], m["_na"] = ya, na
        m["_close"] = close_dt(m)
        m["_sports"] = is_sportsy(m)
        screened.append(m)

    def rank(m):
        ya, na = m["_ya"], m["_na"]
        if ya is None or na is None:
            return (9, 9, 9)
        cost = ya + na
        sports = 0 if m["_sports"] else 1
        cl = m["_close"]
        day = 3
        if cl:
            d = cl.astimezone(ET).date()
            if d == today:
                day = 0
            elif d == tomorrow:
                day = 1
            elif cl <= now + timedelta(days=3):
                day = 2
        return (0 if cost <= LIST_COST_CUT else 1, sports, day, cost)

    tight = [m for m in screened if m["_ya"] is not None and m["_na"] is not None and (m["_ya"] + m["_na"]) <= LIST_COST_CUT]
    tight.sort(key=rank)
    watch = tight[:WATCH_CAP]
    return markets, screened, watch


def eval_books(k: Kalshi, watch: list, series_m: dict):
    locks, near = [], []
    books_ok = 0
    for m in watch:
        t = m["ticker"]
        try:
            ob = k.get(f"/markets/{t}/orderbook", params={"depth": 10})
            b = implied_asks_from_book(ob)
            books_ok += 1
        except Exception as e:
            log(f"ob fail {t}: {e}")
            continue
        st = m.get("series_ticker")
        if st not in series_m:
            try:
                data = k.get(f"/series/{st}")
                ser = data.get("series") or data
                mv = ser.get("fee_multiplier")
                series_m[st] = float(mv) if mv is not None else 1.0
            except Exception:
                series_m[st] = 1.0
        mm = series_m.get(st, 1.0)
        met = two_leg_metrics(b["yes_ask"], b["no_ask"], b["yes_ask_depth"], b["no_ask_depth"], mm)
        if not met:
            continue
        rec = {
            "kind": "YES+NO",
            "ticker": t,
            "title": (m.get("title") or "")[:80],
            "close": fmt_et(m.get("_close")),
            "sports": bool(m.get("_sports")),
            "M": mm,
            **met,
            "yes_ask_d": b["yes_ask_depth"],
            "no_ask_d": b["no_ask_depth"],
        }
        if met["edge"] > NEAR_LOCK and met["depth_ok"]:
            locks.append(rec)
        else:
            near.append(rec)
    locks.sort(key=lambda r: -r["edge"])
    near.sort(key=lambda r: -r["edge"])
    return locks, near, books_ok


def alert_lock(rec: dict, seen: set):
    key = rec["ticker"]
    if key in seen:
        return
    seen.add(key)
    rec = dict(rec)
    rec["alert_et"] = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET")
    rec["live_fire"] = LIVE_FIRE
    with ALERTS.open("a") as f:
        f.write(json.dumps(rec, default=str) + "\n")
    log(
        f"LOCK {rec['ticker']} YES {rec['yes_ask']:.3f}x{rec['yes_ask_d']} "
        f"NO {rec['no_ask']:.3f}x{rec['no_ask_d']} all_in={rec['all_in']:.4f} "
        f"edge={rec['edge']*100:+.2f}c C={rec['c']} PAPER"
    )


def main():
    DIR.mkdir(parents=True, exist_ok=True)
    PIDFILE.write_text(str(os.getpid()))
    stop = {"n": False}

    def _stop(signum, frame):
        stop["n"] = True
        log(f"signal {signum} stopping")

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    k = Kalshi()
    k.min_interval = 0.10
    series_m = {}
    seen_locks = set()
    markets = screened = watch = []
    last_refresh = 0.0
    cycle = 0
    log(f"paper loop start pid={os.getpid()} live_fire={LIVE_FIRE} watch_cap={WATCH_CAP}")

    while not stop["n"]:
        t0 = time.monotonic()
        cycle += 1
        try:
            if time.monotonic() - last_refresh > MARKET_REFRESH_S or not watch:
                markets, screened, watch = refresh_universe(k)
                last_refresh = time.monotonic()
                log(f"universe open={len(markets)} screened={len(screened)} watch={len(watch)}")
            locks, near, books_ok = eval_books(k, watch, series_m)
            for rec in locks:
                alert_lock(rec, seen_locks)
            # drop seen if no longer a lock (spread closed)
            live_tickers = {r["ticker"] for r in locks}
            seen_locks &= live_tickers
            cash_d = None
            pos_n = None
            if cycle == 1 or cycle % 8 == 0:
                bal = k.get("/portfolio/balance")
                cash_d = cash_dollars(bal)
                pos = k.get("/portfolio/positions", params={"limit": 100, "count_filter": "position"})
                plist = pos.get("market_positions") or pos.get("positions") or []
                pos_n = sum(
                    1
                    for p in plist
                    if abs(float(str(p.get("position") or p.get("position_fp") or 0).replace(",", "") or 0)) > 1e-9
                )
            tightest = (locks + near)[:3]
            write_status(
                {
                    "alive": True,
                    "pid": os.getpid(),
                    "cycle": cycle,
                    "live_fire": LIVE_FIRE,
                    "cash": cash_d,
                    "positions": pos_n,
                    "n_watch": len(watch),
                    "n_books": books_ok,
                    "n_locks": len(locks),
                    "locks": locks[:5],
                    "near": near[:5],
                    "reqs": k.n_req,
                    "x429": k.n_429,
                    "cycle_s": round(time.monotonic() - t0, 2),
                }
            )
            if locks:
                log(f"cycle {cycle} LOCKS={len(locks)} books={books_ok}")
            elif cycle % 6 == 1:
                n0 = near[0] if near else None
                if n0:
                    log(
                        f"cycle {cycle} sit near {n0['ticker']} all_in={n0['all_in']:.4f} "
                        f"edge={n0['edge']*100:+.2f}c books={books_ok}"
                    )
                else:
                    log(f"cycle {cycle} sit no books={books_ok} watch={len(watch)}")
        except Exception as e:
            log(f"cycle {cycle} ERROR {type(e).__name__}: {e}")
            write_status({"alive": False, "pid": os.getpid(), "error": str(e)[:300], "cycle": cycle})
        elapsed = time.monotonic() - t0
        sleep_for = max(1.0, BOOK_CYCLE_TARGET_S - elapsed)
        # chunked sleep so SIGTERM lands
        end = time.monotonic() + sleep_for
        while time.monotonic() < end and not stop["n"]:
            time.sleep(min(0.5, end - time.monotonic()))

    PIDFILE.unlink(missing_ok=True)
    log("paper loop stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
