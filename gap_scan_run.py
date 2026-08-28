#!/usr/bin/env python3
"""PAPER-ONLY Kalshi maker-gap + 2-leg lock scan. No orders. Never print keys."""
from __future__ import annotations

import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scan_lib import (  # noqa: E402
    ET,
    Kalshi,
    close_dt,
    fmt_et,
    implied_asks_from_book,
    is_sportsy,
    parse_ts,
    skip_market,
    taker_fee,
    to_dollars,
    two_leg_metrics,
)

OUT = Path("/workspace/kalshi-desk/gap_scan.json")
LIVE_FIRE = False
C = 10
WIDE_N = 25
TOP_REPORT = 8
LOCK_LIST_CUT = 1.04
MARKET_CAP = 8000


def cash_dollars(bal: dict):
    cash = bal.get("balance")
    bd = bal.get("balance_dollars")
    if bd is not None:
        try:
            return float(bd)
        except Exception:
            pass
    if isinstance(cash, int) and cash > 1000:
        return cash / 100.0
    try:
        v = float(cash)
        return v / 100.0 if v > 50 else v
    except Exception:
        return cash


def list_px(m, *keys):
    for k in keys:
        if m.get(k) is not None:
            return to_dollars(m.get(k))
    return None


def ticker_date(t: str):
    for tok in (t or "").split("-"):
        if len(tok) >= 7 and tok[:2].isdigit():
            try:
                return datetime.strptime(tok[:7], "%y%b%d").date()
            except Exception:
                continue
    return None


def is_near(m, today, tomorrow) -> bool:
    t = m.get("ticker") or ""
    td = ticker_date(t)
    if td in (today, tomorrow):
        return True
    for k in ("expected_expiration_time", "close_time", "expiration_time"):
        dt = parse_ts(m.get(k))
        if dt and dt.astimezone(ET).date() in (today, tomorrow):
            return True
    return False


def display_close(m):
    for k in ("expected_expiration_time", "close_time", "expiration_time"):
        dt = parse_ts(m.get(k))
        if dt:
            return fmt_et(dt)
    return fmt_et(m.get("_close"))


def maker_join(yes_bid, yes_ask, c=C):
    """Join both sides size C. Conservative taker fee if both fill."""
    if yes_bid is None or yes_ask is None:
        return None
    if yes_bid <= 0 or yes_ask >= 1 or yes_ask - yes_bid < 0.01 - 1e-12:
        return None
    bid = round(float(yes_bid), 2)
    ask = round(float(yes_ask), 2)
    no_px = round(1.0 - ask, 2)
    if bid <= 0 or no_px <= 0 or bid >= 1 or no_px >= 1:
        return None
    fee = taker_fee(c, bid) + taker_fee(c, no_px)
    cost = bid + no_px
    all_in = cost + fee / c
    edge = 1.0 - all_in
    return {
        "join_yes_bid": bid,
        "join_yes_ask": ask,
        "join_no_px": no_px,
        "size": c,
        "fee": round(fee, 4),
        "cost": round(cost, 4),
        "all_in": round(all_in, 4),
        "edge": round(edge, 4),
        "edge_cents": round(edge * 100, 2),
        "live_fire": False,
    }


def extra_skip(m: dict) -> str | None:
    t = (m.get("ticker") or "").upper()
    series = (m.get("series_ticker") or "").upper()
    if t.startswith("KXMVE") or series.startswith("KXMVE") or "MVECROSS" in t:
        return "skip_mve"
    return skip_market(m)


def main():
    assert LIVE_FIRE is False
    t_start = time.monotonic()
    now = datetime.now(timezone.utc)
    today = now.astimezone(ET).date()
    tomorrow = today + timedelta(days=1)
    k = Kalshi()

    bal = k.get("/portfolio/balance")
    cash = cash_dollars(bal)
    print(f"CASH ${float(cash):.2f} bal_keys={sorted(bal.keys())}", flush=True)

    print("Fetching open markets (mve excluded)…", flush=True)
    markets = k.paginate(
        "/markets",
        "markets",
        params={"status": "open", "limit": 1000, "mve_filter": "exclude"},
        max_pages=10,
        max_items=MARKET_CAP,
    )
    print(f"open markets={len(markets)} reqs={k.n_req} 429s={k.n_429}", flush=True)

    skipped = defaultdict(int)
    screened = []
    for m in markets:
        reason = extra_skip(m)
        if reason:
            skipped[reason] += 1
            continue
        yb = list_px(m, "yes_bid_dollars", "yes_bid")
        ya = list_px(m, "yes_ask_dollars", "yes_ask")
        nb = list_px(m, "no_bid_dollars", "no_bid")
        na = list_px(m, "no_ask_dollars", "no_ask")
        m["_yb"] = yb
        m["_ya"] = ya
        m["_nb"] = nb
        m["_na"] = na
        m["_spread"] = (ya - yb) if (ya is not None and yb is not None) else None
        m["_cost"] = (ya + na) if (ya is not None and na is not None) else None
        m["_close"] = close_dt(m)
        m["_sports"] = is_sportsy(m)
        m["_near"] = is_near(m, today, tomorrow)
        screened.append(m)

    print(f"screened={len(screened)} skipped={dict(skipped)}", flush=True)

    # two-sided 47/50-style: both quotes inside (0,1), spread >= 3c
    wide = []
    for m in screened:
        yb, ya, sp = m["_yb"], m["_ya"], m["_spread"]
        if yb is None or ya is None or sp is None:
            continue
        # 47/50 neighborhood: two-sided mid-book, clip-shaped (not 2/98 empty)
        if yb < 0.15 or ya > 0.85:
            continue
        if sp + 1e-12 < 0.03 or sp > 0.20:
            continue
        ybs = float(m.get("yes_bid_size_fp") or 0)
        yas = float(m.get("yes_ask_size_fp") or 0)
        if ybs <= 0 and yas <= 0:
            continue
        wide.append(m)

    def wide_key(m):
        # prefer sports same/next day, then widest
        sports = 0 if m.get("_sports") else 1
        near = 0 if m.get("_near") else 1
        return (sports, near, -(m["_spread"] or 0))

    wide.sort(key=wide_key)
    # top 25 from preferred ranking (sports near first, then width)
    top25 = []
    seen = set()
    for m in wide:
        t = m.get("ticker")
        if not t or t in seen:
            continue
        seen.add(t)
        top25.append(m)
        if len(top25) >= WIDE_N:
            break
    # also stash a few purely-widest two-sided in case sports-pref hid them
    widest_abs = sorted(wide, key=lambda m: -(m["_spread"] or 0))
    for m in widest_abs[:8]:
        t = m.get("ticker")
        if t and t not in seen and len(top25) < WIDE_N + 5:
            seen.add(t)
            top25.append(m)

    lock_cands = []
    for m in screened:
        ya, na, cost = m["_ya"], m["_na"], m["_cost"]
        if cost is None or cost > LOCK_LIST_CUT:
            continue
        if ya is None or na is None:
            continue
        if not (0 < ya < 1 and 0 < na < 1):
            continue
        # skip empty-wing 1c books posing as 1.01 cost
        lock_cands.append(m)
    lock_cands.sort(
        key=lambda m: (
            0 if m.get("_sports") else 1,
            0 if m.get("_near") else 1,
            m["_cost"] or 9,
        )
    )
    lock_fetch = []
    for m in lock_cands:
        t = m.get("ticker")
        if not t or t in seen:
            continue
        seen.add(t)
        lock_fetch.append(m)
        if len(lock_fetch) >= 40:
            break

    fetch = top25 + lock_fetch
    print(
        f"wide>=3c two-sided={len(wide)} fetch_wide={len(top25)} "
        f"lock_cands={len(lock_cands)} books_to_fetch={len(fetch)}",
        flush=True,
    )

    books = {}
    fails = 0
    t0 = time.monotonic()
    for i, m in enumerate(fetch):
        t = m["ticker"]
        try:
            ob = k.get(f"/markets/{t}/orderbook", params={"depth": 10})
            books[t] = implied_asks_from_book(ob)
        except Exception as e:
            fails += 1
            if fails <= 5:
                print(f"  ob fail {t}: {type(e).__name__}: {e}", flush=True)
        if (i + 1) % 20 == 0:
            print(f"  books {i+1}/{len(fetch)} fails={fails} reqs={k.n_req}", flush=True)
    print(f"books ok={len(books)} fails={fails} elapsed={time.monotonic()-t0:.1f}s", flush=True)

    mkt_by_t = {m["ticker"]: m for m in screened}

    maker_rows = []
    taker_locks = []
    taker_near = []

    for t, b in books.items():
        m = mkt_by_t.get(t)
        if not m:
            continue
        yb = b.get("yes_bid")
        ya = b.get("yes_ask")
        na = b.get("no_ask")
        spread = (ya - yb) if (ya is not None and yb is not None) else None
        pq = maker_join(yb, ya, C)
        row = {
            "ticker": t,
            "title": (m.get("title") or "")[:100],
            "yes_sub": (m.get("yes_sub_title") or m.get("subtitle") or "")[:80],
            "event": m.get("event_ticker"),
            "series": m.get("series_ticker"),
            "yes_bid": yb,
            "yes_ask": ya,
            "no_bid": b.get("no_bid"),
            "no_ask": na,
            "spread": round(spread, 4) if spread is not None else None,
            "spread_cents": round(spread * 100, 2) if spread is not None else None,
            "yes_bid_depth": b.get("yes_bid_depth") or 0,
            "yes_ask_depth": b.get("yes_ask_depth") or 0,
            "no_bid_depth": b.get("no_bid_depth") or 0,
            "no_ask_depth": b.get("no_ask_depth") or 0,
            "close_et": display_close(m),
            "sports": bool(m.get("_sports")),
            "near": bool(m.get("_near")),
            "list_spread": round(m["_spread"], 4) if m.get("_spread") is not None else None,
            "list_cost": round(m["_cost"], 4) if m.get("_cost") is not None else None,
            "paper_quote": pq,
        }
        if spread is not None and 0.03-1e-12 <= spread <= 0.20 and pq and yb >= 0.15 and ya <= 0.85:
            maker_rows.append(row)

        yad = b.get("yes_ask_depth") or 0
        nad = b.get("no_ask_depth") or 0
        met = two_leg_metrics(ya, na, yad, nad)
        if met:
            rec = {
                "ticker": t,
                "title": (m.get("title") or "")[:100],
                "close_et": display_close(m),
                "sports": bool(m.get("_sports")),
                "near": bool(m.get("_near")),
                "yes_ask": ya,
                "no_ask": na,
                "yes_ask_depth": yad,
                "no_ask_depth": nad,
                "cost": round(met["cost"], 4),
                "fee": round(met["fee"], 4),
                "all_in": round(met["all_in"], 4),
                "edge": round(met["edge"], 4),
                "edge_cents": round(met["edge"] * 100, 2),
                "c": met["c"],
                "depth_ok": met["depth_ok"],
                "fillable_10": yad >= 10 and nad >= 10,
            }
            if met["edge"] > 0 and rec["fillable_10"]:
                taker_locks.append(rec)
            else:
                taker_near.append(rec)

    def maker_key(r):
        sports = 0 if r.get("sports") else 1
        near = 0 if r.get("near") else 1
        return (sports, near, -(r.get("spread") or 0))

    maker_rows.sort(key=maker_key)
    top8 = maker_rows[:TOP_REPORT]

    taker_locks.sort(key=lambda r: -r["edge"])
    taker_near.sort(key=lambda r: -r["edge"])
    fillable_locks = [r for r in taker_locks if r.get("fillable_10")]
    sit = len(fillable_locks) == 0
    sit_reason = (
        "SIT — no YES+NO taker lock after fees with size-10 depth on both asks"
        if sit
        else None
    )

    elapsed = time.monotonic() - t_start
    out = {
        "paper_only": True,
        "live_fire": False,
        "ts_et": now.astimezone(ET).strftime("%Y-%m-%d %H:%M:%S ET"),
        "elapsed_s": round(elapsed, 2),
        "cash": cash,
        "cash_raw": bal.get("balance"),
        "balance_dollars": bal.get("balance_dollars"),
        "n_markets": len(markets),
        "n_screened": len(screened),
        "skipped": dict(skipped),
        "n_wide_list": len(wide),
        "n_lock_list": len(lock_cands),
        "n_books": len(books),
        "book_fails": fails,
        "reqs": k.n_req,
        "x429": k.n_429,
        "sit": sit,
        "sit_reason": sit_reason,
        "top_maker_gaps": top8,
        "taker_locks": fillable_locks[:8],
        "taker_near": taker_near[:8],
        "fee_note": "paper quote joins both sides size 10; fee = taker_fee(C=10,P) both legs (conservative)",
        "list_wide_preview": [
            {
                "ticker": m.get("ticker"),
                "title": (m.get("title") or "")[:80],
                "yes_bid": m["_yb"],
                "yes_ask": m["_ya"],
                "spread_cents": round(m["_spread"] * 100, 2),
                "cost": None if m["_cost"] is None else round(m["_cost"], 4),
                "close_et": display_close(m),
                "sports": m["_sports"],
                "near": m["_near"],
            }
            for m in top25[:12]
        ],
    }
    OUT.write_text(json.dumps(out, default=str, indent=2))
    print(f"wrote {OUT}", flush=True)

    print("\n===== GAP SCAN =====", flush=True)
    print(
        f"CASH ${float(cash):.2f} | markets {len(markets)} screened {len(screened)} "
        f"books {len(books)} | {elapsed:.1f}s | 429s={k.n_429}",
        flush=True,
    )
    print(f"TOP {len(top8)} MAKER GAPS (join both sides C=10):", flush=True)
    for r in top8:
        pq = r.get("paper_quote") or {}
        sport = "SPORT" if r.get("sports") else "other"
        near = "NEAR" if r.get("near") else "later"
        print(
            f"  {r['ticker']}  {r.get('spread_cents')}c  "
            f"yb={r.get('yes_bid')} x{r.get('yes_bid_depth')}  "
            f"ya={r.get('yes_ask')} x{r.get('yes_ask_depth')}  "
            f"join {pq.get('join_yes_bid')}/{pq.get('join_yes_ask')}  "
            f"fee={pq.get('fee')} all_in={pq.get('all_in')} edge={pq.get('edge_cents')}c  "
            f"close={r.get('close_et')} {sport}/{near}  {(r.get('title') or '')[:50]}",
            flush=True,
        )
    print(f"TAKER LOCKS fillable@10: {len(fillable_locks)}", flush=True)
    for r in fillable_locks[:5]:
        sport = "SPORT" if r.get("sports") else "other"
        print(
            f"  LOCK {r['ticker']} YES {r['yes_ask']}x{r['yes_ask_depth']} "
            f"NO {r['no_ask']}x{r['no_ask_depth']} "
            f"cost={r['cost']} fee={r['fee']} all_in={r['all_in']} edge={r['edge_cents']}c "
            f"{r.get('close_et')} {sport} {(r.get('title') or '')[:50]}",
            flush=True,
        )
    if sit:
        print(sit_reason, flush=True)
        if taker_near:
            best = taker_near[0]
            print(
                f"NEAR {best['ticker']} cost={best['cost']} fee={best['fee']} "
                f"all_in={best['all_in']} edge={best['edge_cents']}c "
                f"depth Y{best['yes_ask_depth']}/N{best['no_ask_depth']} "
                f"fillable_10={best['fillable_10']}",
                flush=True,
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
