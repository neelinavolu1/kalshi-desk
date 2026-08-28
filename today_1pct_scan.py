#!/usr/bin/env python3
"""PAPER-ONLY today $1 path scan. No orders. Never print keys."""
from __future__ import annotations

import json
import math
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scan_lib import (  # noqa: E402
    CAP_C,
    ET,
    Kalshi,
    ceil_cent,
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

OUT = Path("/workspace/kalshi-desk/today_1pct.json")
LIVE_FIRE = False
C = 10
CAP_NOTIONAL = 10.0
MAX_SPREAD = 0.12
MIN_MAKER_SPREAD = 0.03
TODAY_TAG = "26AUG27"
NAMED_HINTS = ("LADATL", "AZSF", "PITBUF", "GSNY")

# Prioritize live / same-day sports. Clipper SPORT_SERIES plus NFL 1H.
SPORT_SERIES = (
    "KXMLBGAME", "KXMLBSPREAD", "KXMLBTOTAL", "KXMLBF5", "KXMLBF5TOTAL",
    "KXWNBAGAME", "KXWNBASPREAD", "KXWNBATOTAL", "KXWNBATEAMTOTAL", "KXWNBA1QTOTAL",
    "KXNFLGAME", "KXNFLSPREAD", "KXNFLTOTAL",
    "KXNFL1HTOTAL", "KXNFL1HGAME", "KXNFL1HSPREAD", "KXNFL1H",
    "KXNPBGAME", "KXNPBTOTAL",
    "KXNCAAFGAME", "KXNCAAFTOTAL",
)
# KXKBOGAME excluded: ties resolve 50/50, not a clean 1-and-0 two-way.
KBO_EXCLUDE_PREFIX = ("KXKBOGAME", "KXKBOTOTAL", "KXKBOSPREAD")

# User: KXMLBGAME same-game 2-ways use quadratic_with_maker_fees M=0.5
M_OVERRIDE = {
    "KXMLBGAME": 0.5,
}


def cash_dollars(bal: dict):
    bd = bal.get("balance_dollars")
    if bd is not None:
        try:
            return float(bd)
        except Exception:
            pass
    cash = bal.get("balance")
    if isinstance(cash, int) and cash > 1000:
        return cash / 100.0
    try:
        v = float(cash)
        return v / 100.0 if v > 50 else v
    except Exception:
        return cash


def list_px(obj, *keys):
    for k in keys:
        if obj.get(k) is not None:
            return to_dollars(obj.get(k))
    return None


def ticker_date(t: str):
    for tok in (t or "").split("-"):
        if len(tok) >= 7 and tok[:2].isdigit():
            try:
                return datetime.strptime(tok[:7], "%y%b%d").date()
            except Exception:
                continue
    return None


def series_of(t: str, series_ticker: str | None) -> str:
    if series_ticker:
        return series_ticker.upper()
    t = (t or "").upper()
    if "-" in t:
        return t.split("-", 1)[0]
    return t


def fee_m_for(series: str, series_cache: dict) -> float:
    s = (series or "").upper()
    if s in M_OVERRIDE:
        return M_OVERRIDE[s]
    if s in series_cache:
        return series_cache[s]
    return 1.0


def display_close(m):
    for k in ("expected_expiration_time", "expiration_time", "close_time", "latest_expiration_time"):
        dt = parse_ts(m.get(k))
        if dt:
            return fmt_et(dt)
    return "?"


def event_start(ev: dict, m: dict):
    for src in (ev, m):
        if not src:
            continue
        for k in (
            "target_datetime", "start_time", "scheduled_start", "game_start_time",
            "strike_date", "open_time", "event_start_time",
        ):
            dt = parse_ts(src.get(k))
            if dt:
                return dt
    return None


def classify_phase(m: dict, ev: dict, now: datetime) -> str:
    """Keep in-play even if close_time has passed, as long as status is open/active."""
    st = (m.get("status") or ev.get("status") or "").lower()
    if st and st not in ("open", "active", "initialized", ""):
        if st in ("closed", "settled", "determined", "finalized"):
            return "closed"
    start = event_start(ev, m)
    close = parse_ts(m.get("close_time"))
    exp = parse_ts(
        m.get("expected_expiration_time")
        or m.get("expiration_time")
        or m.get("latest_expiration_time")
    )
    # still tradable after listed close → in-play (clipper drops these)
    if close and close <= now:
        return "in_play"
    if start and start <= now:
        return "in_play"
    # expiration already passed but status still open
    if exp and exp <= now:
        return "in_play"
    return "pregame"


def maker_rest_both(yes_bid, no_bid, m_mult=1.0, c=C):
    """Rest YES bid + NO bid at touch. Conservative = taker fee both legs."""
    if yes_bid is None or no_bid is None:
        return None
    yb = round(float(yes_bid), 2)
    nb = round(float(no_bid), 2)
    if yb <= 0 or nb <= 0 or yb >= 1 or nb >= 1:
        return None
    spread = round(1.0 - (yb + nb), 4)
    if spread <= 0:
        return None
    cost = yb + nb
    qty = c
    if cost > 0:
        qty = min(qty, int(CAP_NOTIONAL / cost + 1e-9))
    qty = max(0, min(qty, CAP_C))
    if qty <= 0:
        return None
    fee_cons = taker_fee(qty, yb, m_mult) + taker_fee(qty, nb, m_mult)
    all_in = cost + fee_cons / qty
    edge = 1.0 - all_in
    # actual maker fee if quadratic_with_maker_fees: 0.0175 vs 0.07
    fee_mkr = ceil_cent(m_mult * 0.0175 * qty * yb * (1.0 - yb)) + ceil_cent(
        m_mult * 0.0175 * qty * nb * (1.0 - nb)
    )
    all_in_mkr = cost + fee_mkr / qty
    return {
        "yes_quote": yb,
        "no_quote": nb,
        "size": qty,
        "cost": round(cost, 4),
        "spread": spread,
        "fee_conservative": round(fee_cons, 4),
        "all_in": round(all_in, 4),
        "edge": round(edge, 4),
        "edge_per_10": round(edge * qty, 4),
        "fee_maker_actual": round(fee_mkr, 4),
        "all_in_maker_actual": round(all_in_mkr, 4),
        "edge_maker_actual": round(1.0 - all_in_mkr, 4),
        "M": m_mult,
    }


def paginate_events(k: Kalshi, series: str):
    out = []
    cursor = None
    for _ in range(8):
        q = {
            "series_ticker": series,
            "status": "open",
            "limit": 200,
            "with_nested_markets": True,
        }
        if cursor:
            q["cursor"] = cursor
        try:
            data = k.get("/events", q)
        except Exception as e:
            print(f"  events {series} fail: {type(e).__name__}: {e}", flush=True)
            break
        evs = data.get("events") or []
        out.extend(evs)
        cursor = data.get("cursor") or None
        if not cursor or not evs:
            break
    return out


def main():
    assert LIVE_FIRE is False
    t_start = time.monotonic()
    now = datetime.now(timezone.utc)
    today = now.astimezone(ET).date()
    k = Kalshi()

    # --- cash / positions ---
    bal = k.get("/portfolio/balance")
    cash = cash_dollars(bal)
    pos_raw = k.get("/portfolio/positions", params={"limit": 200, "count_filter": "position"})
    positions = pos_raw.get("market_positions") or pos_raw.get("positions") or []
    cursor = pos_raw.get("cursor")
    pages = 0
    while cursor and pages < 10:
        more = k.get("/portfolio/positions", params={"limit": 200, "cursor": cursor, "count_filter": "position"})
        positions.extend(more.get("market_positions") or more.get("positions") or [])
        cursor = more.get("cursor")
        pages += 1
    open_pos = []
    for p in positions:
        rp = p.get("position") or p.get("position_fp") or 0
        try:
            rp_n = float(str(rp).replace(",", ""))
        except Exception:
            rp_n = 0
        if abs(rp_n) < 1e-9:
            continue
        open_pos.append({"ticker": p.get("ticker") or p.get("market_ticker"), "position": rp})
    pos_s = ",".join(f"{x['ticker']}:{x['position']}" for x in open_pos[:12]) or "FLAT"
    print(
        f"CASH ${float(cash):.2f} | positions={len(open_pos)} {pos_s} | "
        f"ts={now.astimezone(ET).strftime('%Y-%m-%d %H:%M:%S ET')}",
        flush=True,
    )

    # --- series fee multipliers ---
    series_cache = dict(M_OVERRIDE)
    series_meta = {}
    for st in SPORT_SERIES:
        try:
            data = k.get(f"/series/{st}")
            ser = data.get("series") or data
            mv = ser.get("fee_multiplier")
            ft = ser.get("fee_type")
            series_meta[st] = {"fee_type": ft, "fee_multiplier": mv, "title": (ser.get("title") or "")[:60]}
            if st not in M_OVERRIDE and mv is not None:
                series_cache[st] = float(mv)
            print(f"  series {st} fee_type={ft} M={series_cache.get(st, 1.0)} title={series_meta[st]['title']}", flush=True)
        except Exception as e:
            print(f"  series {st} miss: {type(e).__name__}", flush=True)

    # --- events with nested markets (DO NOT drop close_time<=now) ---
    markets = []
    event_by_ticker = {}
    n_events = 0
    n_nested = 0
    skipped = defaultdict(int)
    for st in SPORT_SERIES:
        evs = paginate_events(k, st)
        n_events += len(evs)
        for ev in evs:
            event_by_ticker[ev.get("event_ticker") or ""] = ev
            for m in ev.get("markets") or []:
                n_nested += 1
                m["_event"] = ev
                m["_series"] = st
                if not m.get("series_ticker"):
                    m["series_ticker"] = st
                stt = (m.get("status") or "").lower()
                if stt and stt not in ("open", "active", "initialized", ""):
                    skipped[f"status:{stt}"] += 1
                    continue
                reason = skip_market(m)
                if reason:
                    skipped[reason] += 1
                    continue
                tu = (m.get("ticker") or "").upper()
                if any(x in tu for x in ("5M", "15M", "BTC5", "ETH5", "CROSSCATEGORY", "KXMVE")):
                    skipped["skip_short_or_mve"] += 1
                    continue
                if tu.startswith(KBO_EXCLUDE_PREFIX) or st.startswith("KXKBO"):
                    skipped["skip_kbo_not_1and0"] += 1
                    continue
                markets.append(m)
        print(f"  {st}: events={len(evs)} nested_kept_running={len(markets)}", flush=True)

    print(f"events={n_events} nested={n_nested} kept={len(markets)} skipped={dict(skipped)}", flush=True)

    # annotate
    today_mkts = []
    inplay_n = pregame_n = 0
    for m in markets:
        t = m.get("ticker") or ""
        ev = m.get("_event") or {}
        yb = list_px(m, "yes_bid_dollars", "yes_bid")
        ya = list_px(m, "yes_ask_dollars", "yes_ask")
        nb = list_px(m, "no_bid_dollars", "no_bid")
        na = list_px(m, "no_ask_dollars", "no_ask")
        if yb is None and na is not None:
            yb = round(1.0 - na, 4)
        if nb is None and ya is not None:
            nb = round(1.0 - ya, 4)
        if ya is None and nb is not None:
            ya = round(1.0 - nb, 4)
        if na is None and yb is not None:
            na = round(1.0 - yb, 4)
        spread = round(ya - yb, 4) if (ya is not None and yb is not None) else None
        if spread is None and yb is not None and nb is not None:
            spread = round(1.0 - (yb + nb), 4)
        cost = (ya + na) if (ya is not None and na is not None) else None
        phase = classify_phase(m, ev, now)
        td = ticker_date(t)
        cl = close_dt(m)
        cl_date = cl.astimezone(ET).date() if cl else None
        is_today = (
            td == today
            or TODAY_TAG in t.upper()
            or cl_date == today
            or phase == "in_play"
        )
        m["_yb"] = yb
        m["_ya"] = ya
        m["_nb"] = nb
        m["_na"] = na
        m["_spread"] = spread
        m["_cost"] = cost
        m["_phase"] = phase
        m["_today"] = is_today
        m["_close"] = cl
        m["_sports"] = True
        if phase == "in_play":
            inplay_n += 1
        else:
            pregame_n += 1
        if is_today:
            today_mkts.append(m)

    print(
        f"today_or_inplay={len(today_mkts)} in_play={inplay_n} pregame={pregame_n}",
        flush=True,
    )

    # dump a few in-play samples
    ip = [m for m in today_mkts if m["_phase"] == "in_play"]
    print(f"IN-PLAY sample n={len(ip)}", flush=True)
    for m in ip[:12]:
        print(
            f"  LIVE {m.get('ticker')} spr={m.get('_spread')} yb={m.get('_yb')} nb={m.get('_nb')} "
            f"ya={m.get('_ya')} na={m.get('_na')} close={display_close(m)} "
            f"{(m.get('title') or m.get('yes_sub_title') or '')[:50]}",
            flush=True,
        )

    # named games
    named_list = []
    for m in today_mkts:
        tu = (m.get("ticker") or "").upper()
        if any(h in tu for h in NAMED_HINTS) or "KXMLBGAME" in tu:
            named_list.append(m)
    mlb_games = sorted({
        (m.get("event_ticker") or "")
        for m in today_mkts
        if (m.get("_series") or "").startswith("KXMLB") or (m.get("ticker") or "").startswith("KXMLBGAME")
    })
    print(f"MLB events today: {mlb_games[:20]} n={len(mlb_games)}", flush=True)

    # --- choose books to fetch ---
    fetch = []
    seen = set()

    def add_m(m, why):
        t = m.get("ticker")
        if not t or t in seen:
            return
        seen.add(t)
        m["_why"] = why
        fetch.append(m)

    # always: KXMLBGAME 2-ways today/inplay
    for m in today_mkts:
        ser = m.get("_series") or series_of(m.get("ticker"), m.get("series_ticker"))
        tu = (m.get("ticker") or "").upper()
        if ser == "KXMLBGAME" or tu.startswith("KXMLBGAME"):
            add_m(m, "mlb_game")
        if any(h in tu for h in NAMED_HINTS):
            add_m(m, "named")

    # NFL 1H / moneyline
    for m in today_mkts:
        tu = (m.get("ticker") or "").upper()
        ser = m.get("_series") or ""
        if "NFL1H" in tu or "NFL1H" in ser or ser.startswith("KXNFL"):
            add_m(m, "nfl")
        if "WNBA" in tu or "WNBA" in ser:
            add_m(m, "wnba")
        if any(x in tu or x in ser for x in ("KBO", "NPB")):
            add_m(m, "kbo_npb")

    # all in-play with non-ghost list spread
    for m in today_mkts:
        if m["_phase"] != "in_play":
            continue
        sp = m.get("_spread")
        if sp is None:
            add_m(m, "inplay_nobook")
            continue
        if sp > MAX_SPREAD + 0.05:
            continue  # even list-side ghost
        add_m(m, "inplay")

    # pregame maker-shaped 3-12c two-sided, not empty wings
    for m in today_mkts:
        yb, ya, sp = m.get("_yb"), m.get("_ya"), m.get("_spread")
        if yb is None or ya is None or sp is None:
            continue
        if yb < 0.10 or ya > 0.90:
            continue
        if sp + 1e-12 < MIN_MAKER_SPREAD or sp > MAX_SPREAD:
            continue
        add_m(m, "maker_shape")

    # potential taker locks from list
    for m in today_mkts:
        cost = m.get("_cost")
        ya, na = m.get("_ya"), m.get("_na")
        if cost is None or ya is None or na is None:
            continue
        if cost <= 1.08 and 0 < ya < 1 and 0 < na < 1:
            sp = m.get("_spread")
            if sp is not None and sp > MAX_SPREAD:
                continue
            add_m(m, "taker_list")

    print(f"books_to_fetch={len(fetch)} (cap 220)", flush=True)
    if len(fetch) > 220:
        # keep all mlb/nfl/wnba/inplay, trim maker_shape
        pri = [m for m in fetch if m.get("_why") in ("mlb_game", "named", "nfl", "wnba", "kbo_npb", "inplay", "inplay_nobook", "taker_list")]
        rest = [m for m in fetch if m not in pri]
        fetch = pri + rest
        fetch = fetch[:220]
        print(f"  trimmed to {len(fetch)} pri={len(pri)}", flush=True)

    books = {}
    fails = 0
    t0 = time.monotonic()
    for i, m in enumerate(fetch):
        t = m["ticker"]
        try:
            ob = k.get(f"/markets/{t}/orderbook", params={"depth": 20})
            books[t] = implied_asks_from_book(ob)
        except Exception as e:
            fails += 1
            if fails <= 8:
                print(f"  ob fail {t}: {type(e).__name__}: {e}", flush=True)
        if (i + 1) % 30 == 0:
            print(f"  books {i+1}/{len(fetch)} fails={fails} reqs={k.n_req} 429s={k.n_429}", flush=True)
    print(f"books ok={len(books)} fails={fails} elapsed={time.monotonic()-t0:.1f}s", flush=True)

    mkt_by_t = {m["ticker"]: m for m in today_mkts}
    for m in fetch:
        mkt_by_t[m["ticker"]] = m

    taker_rows = []
    maker_rows = []
    named_tickers = {}

    for t, b in books.items():
        m = mkt_by_t.get(t)
        if not m:
            continue
        ser = m.get("_series") or series_of(t, m.get("series_ticker"))
        if (ser or "").upper().startswith("KXKBO") or t.upper().startswith("KXKBOGAME"):
            continue  # not a clean 1-and-0
        mm = fee_m_for(ser, series_cache)
        yb, ya = b.get("yes_bid"), b.get("yes_ask")
        nb, na = b.get("no_bid"), b.get("no_ask")
        yad = b.get("yes_ask_depth") or 0
        nad = b.get("no_ask_depth") or 0
        ybd = b.get("yes_bid_depth") or 0
        nbd = b.get("no_bid_depth") or 0
        spread = None
        if ya is not None and yb is not None:
            spread = round(ya - yb, 4)
        elif yb is not None and nb is not None:
            spread = round(1.0 - (yb + nb), 4)
        phase = m.get("_phase") or "pregame"
        close_et = display_close(m)
        title = (m.get("title") or "")[:100]
        yes_sub = (m.get("yes_sub_title") or m.get("subtitle") or "")[:80]

        ghost = spread is not None and spread > MAX_SPREAD + 1e-12
        depth10 = (yad >= 10 and nad >= 10)

        rec_common = {
            "ticker": t,
            "title": title,
            "yes_sub": yes_sub,
            "event": m.get("event_ticker"),
            "series": ser,
            "yes_bid": yb,
            "no_bid": nb,
            "yes_ask": ya,
            "no_ask": na,
            "spread": spread,
            "yes_bid_depth": ybd,
            "no_bid_depth": nbd,
            "yes_ask_depth": yad,
            "no_ask_depth": nad,
            "close_et": close_et,
            "phase": phase,
            "M": mm,
            "ghost": ghost,
            "depth10": depth10,
        }

        tu = t.upper()
        if any(h in tu for h in NAMED_HINTS) or ser == "KXMLBGAME" or tu.startswith("KXMLBGAME"):
            named_tickers[t] = {
                "yes_bid": yb,
                "no_bid": nb,
                "yes_ask": ya,
                "no_ask": na,
                "spread": spread,
                "close_et": close_et,
                "phase": phase,
                "series": ser,
                "title": title,
                "yes_sub": yes_sub,
            }

        if ghost:
            continue

        met = two_leg_metrics(ya, na, yad, nad, mm)
        if met:
            trow = {
                **rec_common,
                "cost": round(met["cost"], 4),
                "fee": round(met["fee"], 4),
                "all_in": round(met["all_in"], 4),
                "edge": round(met["edge"], 4),
                "edge_per_10": round(met["edge"] * met["c"], 4),
                "c": met["c"],
                "depth_ok": met["depth_ok"],
                "fillable_10": depth10,
            }
            taker_rows.append(trow)
            if t in named_tickers:
                named_tickers[t]["taker_edge"] = trow["edge"]
                named_tickers[t]["taker_all_in"] = trow["all_in"]
                named_tickers[t]["edge"] = trow["edge"]

        pq = maker_rest_both(yb, nb, mm, C)
        if pq and spread is not None and spread + 1e-12 >= MIN_MAKER_SPREAD:
            # two-sided, 3-12c, size 10; maker does not need 10 sitting on bid,
            # but require some book so it's not empty. Prefer depth10 on asks
            # (implies both sides quoted) OR bid depths >= 1 both sides.
            two_sided = yb is not None and nb is not None and yb > 0 and nb > 0
            if two_sided and (ybd >= 1 and nbd >= 1):
                mrow = {
                    **rec_common,
                    "maker": pq,
                    "edge": pq["edge"],
                    "edge_per_10": pq["edge_per_10"],
                    "all_in": pq["all_in"],
                    "fee": pq["fee_conservative"],
                }
                maker_rows.append(mrow)
                if t in named_tickers:
                    named_tickers[t]["maker_edge"] = pq["edge"]
                    named_tickers[t]["edge"] = named_tickers[t].get("edge", pq["edge"])
                    named_tickers[t]["maker_edge_per_10"] = pq["edge_per_10"]

    def taker_ok(r):
        return r.get("fillable_10") and r.get("edge", -9) > 0 and not r.get("ghost")

    def maker_ok(r):
        return r.get("edge", -9) > 0 and not r.get("ghost") and r.get("spread") is not None and r["spread"] <= MAX_SPREAD

    taker_locks = [r for r in taker_rows if taker_ok(r)]
    taker_locks.sort(key=lambda r: (-r["edge"], 0 if r.get("phase") == "in_play" else 1))
    taker_near = [r for r in taker_rows if not taker_ok(r)]
    taker_near.sort(key=lambda r: -r.get("edge", -9))

    maker_pos = [r for r in maker_rows if maker_ok(r)]
    maker_pos.sort(key=lambda r: (-r["edge_per_10"], 0 if r.get("phase") == "in_play" else 1))

    maker_ip = [r for r in maker_pos if r.get("phase") == "in_play"]
    maker_pg = [r for r in maker_pos if r.get("phase") != "in_play"]
    taker_ip = [r for r in taker_locks if r.get("phase") == "in_play"]
    taker_pg = [r for r in taker_locks if r.get("phase") != "in_play"]

    def slim_taker(r):
        return {
            "ticker": r["ticker"],
            "yes_ask": r.get("yes_ask"),
            "no_ask": r.get("no_ask"),
            "yes_ask_depth": r.get("yes_ask_depth"),
            "no_ask_depth": r.get("no_ask_depth"),
            "spread": r.get("spread"),
            "cost": r.get("cost"),
            "fee": r.get("fee"),
            "all_in": r.get("all_in"),
            "edge": r.get("edge"),
            "edge_per_10": r.get("edge_per_10"),
            "c": r.get("c"),
            "M": r.get("M"),
            "close_et": r.get("close_et"),
            "phase": r.get("phase"),
            "title": r.get("title"),
            "yes_sub": r.get("yes_sub"),
            "series": r.get("series"),
        }

    def slim_maker(r):
        pq = r.get("maker") or {}
        return {
            "ticker": r["ticker"],
            "yes_bid": r.get("yes_bid"),
            "no_bid": r.get("no_bid"),
            "yes_ask": r.get("yes_ask"),
            "no_ask": r.get("no_ask"),
            "spread": r.get("spread"),
            "yes_bid_depth": r.get("yes_bid_depth"),
            "no_bid_depth": r.get("no_bid_depth"),
            "edge": r.get("edge"),
            "edge_per_10": r.get("edge_per_10"),
            "all_in": r.get("all_in"),
            "fee": r.get("fee"),
            "M": r.get("M"),
            "close_et": r.get("close_et"),
            "phase": r.get("phase"),
            "title": r.get("title"),
            "yes_sub": r.get("yes_sub"),
            "series": r.get("series"),
            "quotes": {"yes": pq.get("yes_quote"), "no": pq.get("no_quote"), "size": pq.get("size")},
        }

    def path_from(rows):
        if not rows:
            return "none"
        best = rows[0]
        e10 = best.get("edge_per_10") or 0
        if e10 <= 0:
            return "none"
        n = int(math.ceil(1.0 / e10 - 1e-12))
        return {
            "clips": n,
            "edge_per_clip": e10,
            "ticker": best.get("ticker"),
            "phase": best.get("phase"),
            "note": f"{n} x $10 clips at {best.get('ticker')} edge=${e10:.3f}/10 lots (both sides fill, conservative taker fees)",
        }

    best_maker = [slim_maker(r) for r in maker_pos[:10]]
    best_taker = [slim_taker(r) for r in taker_locks[:5]]
    path = path_from(maker_pos)
    path_ip = path_from(maker_ip)

    # fill named tickers that we listed but maybe no book? already from books
    # also attach edge for named even if ghost (for honesty)
    for t, b in books.items():
        if t in named_tickers:
            continue
        tu = t.upper()
        if any(h in tu for h in NAMED_HINTS):
            m = mkt_by_t[t]
            named_tickers[t] = {
                "yes_bid": b.get("yes_bid"),
                "no_bid": b.get("no_bid"),
                "yes_ask": b.get("yes_ask"),
                "no_ask": b.get("no_ask"),
                "spread": (round(b["yes_ask"] - b["yes_bid"], 4) if b.get("yes_ask") is not None and b.get("yes_bid") is not None else None),
                "close_et": display_close(m),
                "phase": m.get("_phase"),
                "series": m.get("_series"),
                "title": (m.get("title") or "")[:100],
            }

    elapsed = time.monotonic() - t_start
    out = {
        "paper_only": True,
        "live_fire": False,
        "ts_et": now.astimezone(ET).strftime("%Y-%m-%d %H:%M:%S ET"),
        "elapsed_s": round(elapsed, 2),
        "cash": cash,
        "n_positions": len(open_pos),
        "positions": open_pos,
        "note": (
            "In-play included even if close_time<=now while status open/active. "
            "Ghost books (spread>0.12) dropped. Taker = buy both asks + official ceil_cent(M*0.07*C*P*(1-P)). "
            "KXMLBGAME M=0.5. Maker = rest both bids at touch, conservative taker fees, size 10 / $10. "
            "KXKBOGAME excluded (tie resolves 50/50, not clean 1-and-0). Focus: afternoon MLB 2-ways. "
            "No orders placed."
        ),
        "n_events": n_events,
        "n_nested": n_nested,
        "n_today": len(today_mkts),
        "n_in_play": inplay_n,
        "n_pregame": pregame_n,
        "n_books": len(books),
        "book_fails": fails,
        "reqs": k.n_req,
        "x429": k.n_429,
        "series_fees": series_meta,
        "skipped": dict(skipped),
        "best_taker": best_taker[0] if best_taker else {},
        "best_taker_all": best_taker,
        "best_taker_inplay": slim_taker(taker_ip[0]) if taker_ip else {},
        "best_taker_pregame": slim_taker(taker_pg[0]) if taker_pg else {},
        "best_maker": best_maker,
        "best_maker_inplay": [slim_maker(r) for r in maker_ip[:5]],
        "best_maker_pregame": [slim_maker(r) for r in maker_pg[:5]],
        "path_to_1_dollar": path,
        "path_to_1_dollar_inplay": path_ip,
        "named_tickers": named_tickers,
        "taker_near": [slim_taker(r) for r in taker_near[:8]],
        "inplay_tickers_scanned": [
            m.get("ticker") for m in today_mkts if m.get("_phase") == "in_play"
        ][:80],
        "mlb_events": mlb_games,
    }
    OUT.write_text(json.dumps(out, default=str, indent=2))
    print(f"wrote {OUT}", flush=True)

    print("\n===== TODAY $1 PATH =====", flush=True)
    print(
        f"CASH ${float(cash):.2f} FLAT={len(open_pos)==0} | books={len(books)} "
        f"in_play_mkts={inplay_n} | {elapsed:.1f}s | 429s={k.n_429}",
        flush=True,
    )
    print(f"TAKER LOCKS fillable@10 after fees: {len(taker_locks)}", flush=True)
    for r in taker_locks[:5]:
        print(
            f"  TAKER {r['phase']} {r['ticker']} YES {r.get('yes_ask')}x{r.get('yes_ask_depth')} "
            f"NO {r.get('no_ask')}x{r.get('no_ask_depth')} all_in={r.get('all_in')} "
            f"edge={r.get('edge')} M={r.get('M')} {r.get('close_et')}",
            flush=True,
        )
    if not taker_locks:
        print("  NO taker lock after fees with depth 10.", flush=True)
        if taker_near:
            b = taker_near[0]
            print(
                f"  NEAR {b.get('phase')} {b['ticker']} all_in={b.get('all_in')} edge={b.get('edge')} "
                f"Y{b.get('yes_ask')}x{b.get('yes_ask_depth')} N{b.get('no_ask')}x{b.get('no_ask_depth')} "
                f"fillable_10={b.get('fillable_10')} ghost={b.get('ghost')}",
                flush=True,
            )
    print(f"MAKER +edge n={len(maker_pos)} inplay={len(maker_ip)} pregame={len(maker_pg)}", flush=True)
    for r in maker_pos[:10]:
        print(
            f"  MAKER {r['phase']} {r['ticker']} yb={r.get('yes_bid')} nb={r.get('no_bid')} "
            f"spr={r.get('spread')} edge/10=${r.get('edge_per_10')} all_in={r.get('all_in')} "
            f"M={r.get('M')} {r.get('close_et')} {(r.get('yes_sub') or r.get('title') or '')[:40]}",
            flush=True,
        )
    print(f"PATH {path}", flush=True)
    print(f"PATH_INPLAY {path_ip}", flush=True)
    print(f"INPLAY taker={len(taker_ip)} maker={len(maker_ip)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
