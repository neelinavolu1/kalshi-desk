#!/usr/bin/env python3
"""Kalshi WS clipper. LIVE_FIRE post_only 2-way YES rests only.

Shard lesson (do not drop): Kalshi is sharded. Tennis+baseball = exchange_index=3
as of 2026-08-24 (combos=1, crypto=2, else 0). GET /markets/{ticker} has the index.
POST /portfolio/events/orders to a shard with $0 cash returns HTTP 404
{code: user_not_found} — unfunded/unprovisioned shard, NOT a bad key.
GET /portfolio/balance on shard 0 can succeed while shard-3 writes fail.
Fund first: GET /portfolio/balance?exchange_index=N, then
POST /portfolio/intra_exchange_instance_transfer (~$36 centicents, leftover stays
on shard 0). V1 POST /portfolio/orders is 410; V2 /portfolio/events/orders only.
Never lift (post_only=true). Never duplicate HARLLA. Do not cancel HARLLA.
Concurrent post-only 2-way rests = min(2, floor(shard_cash / 28.5)), but after a
filled lock the leftover free cash on that shard may still sit a smaller second
pair when free >= MIN_LEFTOVER_NOTIONAL (size by depth + free; do not require
another $28.50). Do NOT wait for a filled inventory 2-way before the second rest.
Do not spend the one live-attempt tick on a shard whose free cash is below that
min (Sep NFL on shard 0 with $0.50) while tennis cash sits idle. Prefer
independent events. Do not cancel a still-paying rest unless a candidate is a
full cent better all-in. If leftover rest_allin exceeds 0.99 (book walked off),
cancel those oids (never HARLLA) and rest a pair that still pays. Size leftover
by depth + free shard cash; never fund a second pair via transfer.
On a one-leg fill (exactly one inventory leg; matching 2-way locks skip),
KEEP the original paired yes-bid rest if it is still working and fill+rest
<=0.99 (ignore 2c stale and 35-65 — GTC can still print after the book moves).
Do not cancel, flatten, or oneleg_ban that game — lock completes only via
that original rest printing. Never place a new buy on the missing wing.
If fill+rest >0.99 while that rest is still working: keep waiting (do not
buy more, do not sell). Cancel that other buy only if fill+rest >1.00
(losing lock if it printed); if 0.99<sum<=1.00 wait for fees. True orphan
(no other buy on the event): wait 180s, then flatten the filled YES at
~1c under cost: target=round(cost-0.01, 2) in (0, 1).
FIRST flatten action is always post_only SELL (no reduce_only; Kalshi IOC-only): px=target if
live_bid is None or < target, else px=round(min(0.99, live_bid+0.01), 2)
(maker above bid; never post_only <= live_bid / no taker fee). If a flatten
ask already rests within 1c of that px, do not duplicate. IOC-sell at
live_bid ONLY as give-up: maker rest still open after ~45s AND live_bid
< target-0.02 (bid walked). If bid is still near cost after 45s, leave
the maker ask. Never flatten a locked pair. Never 101c. Drop banned and unpinned in-play lopsided games off
the watch so they cannot crowd restable 97c books.
Never print keys or PEM.
"""

# Strategy in plain English (for someone new to these markets):
# Each sports game is two YES contracts -- one per team. Exactly one team
# wins, so that YES pays $1 and the other pays $0.
# We sit a buy order on both teams at once, cheap enough that the two
# prices add to 99 cents or less. If both orders go through we own the $10 card:
# one side will be worth $1 per contract no matter who wins. Hold that pair.
# If only one order went through: keep the other buy if it is still sitting
# and the two prices still add to $1.00 or less (ignore a 2-cent market move).
# If the other buy is gone, wait 3 minutes after the first side went through
# before selling the leftover contract. Never sit a new buy on the missing
# team. Never pay $1.01 or more to complete the pair.
from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import signal
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scan_lib import (  # noqa: E402
    ET,
    Kalshi,
    UnfundedShard,
    close_dt,
    fund_shard,
    guess_exchange_index,
    is_sportsy,
    market_exchange_index,
    parse_ts,
    shard_balance_dollars,
    skip_market,
    taker_fee,
    to_dollars,
)

WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"
KEY_ID_PATH = Path("/home/box/.config/kalshi/key_id")
KEY_PATH = Path("/home/box/.config/kalshi/main.key")
DIR = Path("/workspace/kalshi-desk")
STATUS = DIR / "mm_status.json"
LOG = DIR / "mm.log"
PIDFILE = DIR / "mm.pid"
HEARTBEAT = DIR / "mm_heartbeat"
BAN_PATH = DIR / "oneleg_ban.json"

LIVE_FIRE = True  # post_only 2-way YES rests only; never lift
CAP_C = 30
CAP_NOTIONAL = 30.0
CLIP_NOTIONAL = 30.0  # first pair: skip live rest if dest shard cash below this; fund ~$36
FUND_DOLLARS = 36.0
PAIR_CASH_UNIT = 28.5  # concurrent rests = min(MAX_LIVE_PAIRS, floor(shard_cash / this))
MAX_LIVE_PAIRS = 2  # leftover cash may rest a second pair without filled inventory
MAX_STACKED_PAIRS = 2  # alias: max concurrent post-only 2-way rests
# Only try a game when each team's YES buy price is between 35 cents and 65 cents.
# Outside that band one team is a heavy favorite, and we often end up
# owning only one side instead of both.
LIVE_BID_LO = 0.35  # all live 2-way rests (first pair + leftover); was 0.18
LIVE_BID_HI = 0.65  # was 0.80; 25/70 and 63/34 one-legged
LIVE_REST_MAX = 0.99  # raw yes+yes cap
LIVE_REST_ALLIN_MAX = 0.99  # leftover second pair: >=~1c after M=0.5
LIVE_SPREAD_MAX = 0.03  # each leg; 7-10c books are not locks even at bid_sum~93c
# Raw take-rest gap. BROLOF 08:07 ET: 95c rest / 101c take (+3.2c all-in) one-legged in 20s.
LIVE_TAKE_REST_GAP_MAX = 0.04
MIN_LEFTOVER_NOTIONAL = 4.0  # sit leftover if sized clip notional below this
# Cancel a still-passing leftover rest if a new candidate is this much better (all-in $).
# Cross-matchup: 1.0c (VIDBOU 97c was stuck behind SVRROY/CWSMIN 98c for 10+ min).
# Same doubleheader stem (G1/G2): keep 1.5c so AZSFG1↔G2 does not thrash.
LEFTOVER_UPGRADE_ALLIN = 0.010
LEFTOVER_UPGRADE_SAME_STEM = 0.015
# Lopsided gap for ALL live 2-way rests (not only leftover in-play).
# 25/70 and 70/25 must not stack pregame either. LIVE_BID 35-65 is the wing gate.
LEFTOVER_INPLAY_BID_LO = 0.25
LEFTOVER_INPLAY_BID_HI = 0.75
LEFTOVER_INPLAY_BID_GAP = 0.35  # keep; applied to every 2-way live rest
ONELEG_STALE_C = 0.02  # keep unfilled rest only if live_bid - rest_px <= 2c
ONELEG_MAKER_WAIT_S = 45.0  # wait for 1c-under-cost maker flatten before give-up IOC
ONELEG_GIVEUP_UNDER = 0.02  # give-up IOC only if live_bid still < target - this
ONELEG_NEAR_C = 0.01  # existing flatten ask counts as at/near target
ONELEG_ORPHAN_WAIT_S = 180.0  # true orphan: wait before maker-sell leftover
# Already LIVE on book — do not duplicate, do not cancel.
KNOWN_LIVE = (
    ("KXATPMATCH-26AUG27HARLLA-HAR", "01a044d3-b7e8-7c4c-a7fc-a328d4559e62"),
    ("KXATPMATCH-26AUG27HARLLA-LLA", "01a044d3-bbd0-7360-ba6c-a7c15c9caed3"),
)
LIVE_SKIP_OIDS = {oid for _, oid in KNOWN_LIVE}
MIN_SPREAD = 0.03
MAX_SPREAD = 0.12  # wider than this is an empty/ghost book, not a clip
IMPROVE_SPREAD = 0.04
STALE_S = 2.0
WATCH_N = 20  # 16 dropped live ATP (FEAROD) once pins + MLB ate slots
UNIVERSE_S = 90.0
IN_PLAY_MAX_AGE = 18 * 3600  # occ older than this is not in-play (yesterday first-ball)
SKIP_TICKER_SUBSTR = ("CROSSCATEGORY", "KXMVE", "SHARD1", "SHARD2")

# Same-day sports live on these series; /markets open-list is flooded with MVE shards.
SPORT_SERIES = (
    "KXMLBGAME", "KXMLBSPREAD", "KXMLBTOTAL", "KXMLBF5", "KXMLBF5TOTAL",
    "KXWNBAGAME", "KXWNBASPREAD", "KXWNBATOTAL", "KXWNBATEAMTOTAL", "KXWNBA1QTOTAL",
    "KXATPMATCH", "KXWTAMATCH", "KXATPCHALLENGERMATCH", "KXWTACHALLENGERMATCH",
    "KXNCAAFGAME", "KXNCAAFTOTAL", "KXNFLGAME", "KXNFL1HTOTAL", "KXNFLTOTAL",
    "KXEPLGAME", "KXMLSGAME",
    "KXKBOGAME", "KXKBOTOTAL", "KXNPBGAME", "KXNPBTOTAL", "KXT20MATCH",
    "KXATP", "KXWTA",
)


class SeqGap(Exception):
    pass


def log(msg: str) -> None:
    print(f"{datetime.now(ET).strftime('%H:%M:%S')} {msg}", flush=True)


def load_key():
    key_id = KEY_ID_PATH.read_text().strip()
    pkey = serialization.load_pem_private_key(KEY_PATH.read_bytes(), password=None)
    return key_id, pkey


def ws_headers(key_id, pkey) -> dict:
    ts = str(int(time.time() * 1000))
    msg = f"{ts}GET{WS_PATH}".encode()
    sig = pkey.sign(
        msg,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }


def safe_err(e: BaseException) -> str:
    s = f"{type(e).__name__}: {e}"
    for needle in ("BEGIN ", "PRIVATE", "KEY", "KALSHI-ACCESS", "-----"):
        if needle in s:
            return f"{type(e).__name__}: <redacted>"
    return s[:400]


def event_dt(m: dict):
    """Game/expiry for ranking. close_time is often a multi-day settlement window."""
    for k in ("expected_expiration_time", "close_time", "expiration_time", "latest_expiration_time"):
        dt = parse_ts(m.get(k))
        if dt:
            return dt
    return close_dt(m)


_MON = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_TICKER_START = re.compile(
    r"-(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})(\d{4})?",
    re.I,
)


def ticker_start_et(ticker: str):
    """First-ball from ticker (YYMONDDHHMM). MLB 1915 = 7:15pm ET.

    T20 occ is often the ticker clock stored as UTC: TRITHR 0915 ET was live
    at 09:17 with +12c rest, but occurrence_datetime was 17:15Z (1:15pm ET).
    """
    m = _TICKER_START.search(ticker or "")
    if not m or not m.group(4):
        return None
    yy, mon, dd, hhmm = m.group(1), m.group(2).upper(), m.group(3), m.group(4)
    hh, mm = int(hhmm[:2]), int(hhmm[2:])
    if hh > 23 or mm > 59:
        return None
    try:
        return datetime(2000 + int(yy), _MON[mon], int(dd), hh, mm, tzinfo=ET)
    except ValueError:
        return None


def to_cents(px, *, dollars: bool | None = None) -> int | None:
    if px is None:
        return None
    if dollars is True:
        try:
            d = float(px)
        except (TypeError, ValueError):
            return None
        if d > 1.5:
            d = d / 100.0
        return int(round(d * 100.0))
    if dollars is False:
        try:
            if isinstance(px, str) and "." in str(px):
                return int(round(float(px) * 100.0))
            v = float(px)
        except (TypeError, ValueError):
            return None
        if isinstance(px, float) and 0.0 < v <= 1.5:
            return int(round(v * 100.0))
        return int(round(v))
    d = to_dollars(px)
    if d is None:
        return None
    return int(round(d * 100.0))


def parse_level(lv, *, dollars: bool | None = None):
    if isinstance(lv, (list, tuple)) and len(lv) >= 2:
        px, qty = lv[0], lv[1]
    elif isinstance(lv, dict):
        if lv.get("price_dollars") is not None:
            px, dollars = lv.get("price_dollars"), True
        else:
            px = lv.get("price")
        qty = lv.get("quantity") or lv.get("count") or lv.get("size") or lv.get("delta")
    else:
        return None, None
    c = to_cents(px, dollars=dollars)
    try:
        q = float(qty)
    except (TypeError, ValueError):
        return None, None
    return c, q


def extract_levels(msg: dict, side: str):
    for k in (f"{side}_dollars_fp", f"{side}_dollars", f"{side}_fp"):
        lv = msg.get(k)
        if lv:
            out = []
            for row in lv:
                c, q = parse_level(row, dollars=True)
                if c is not None and q is not None:
                    out.append((c, q))
            if out:
                return out
    lv = msg.get(side)
    if lv:
        out = []
        for row in lv:
            c, q = parse_level(row, dollars=False)
            if c is not None and q is not None:
                out.append((c, q))
        if out:
            return out
    return []


class Book:
    def __init__(self, ticker: str):
        self.ticker = ticker
        self.yes: dict[int, float] = {}
        self.no: dict[int, float] = {}
        self.local_mono = 0.0
        self.n_snap = 0
        self.n_delta = 0

    def touch(self):
        self.local_mono = time.monotonic()

    def snapshot(self, msg: dict):
        self.yes.clear()
        self.no.clear()
        for c, q in extract_levels(msg, "yes"):
            if q > 0 and 1 <= c <= 99:
                self.yes[c] = q
        for c, q in extract_levels(msg, "no"):
            if q > 0 and 1 <= c <= 99:
                self.no[c] = q
        self.n_snap += 1
        self.touch()

    def delta(self, msg: dict):
        side = (msg.get("side") or "").lower()
        if msg.get("price_dollars") is not None:
            c = to_cents(msg.get("price_dollars"), dollars=True)
        else:
            c = to_cents(msg.get("price"), dollars=False)
        raw_dq = msg.get("delta_fp")
        if raw_dq is None:
            raw_dq = msg.get("delta")
        if c is None or raw_dq is None or side not in ("yes", "no"):
            return
        try:
            dq = float(raw_dq)
        except (TypeError, ValueError):
            return
        bucket = self.yes if side == "yes" else self.no
        bucket[c] = bucket.get(c, 0.0) + dq
        if bucket[c] <= 1e-9:
            bucket.pop(c, None)
        self.n_delta += 1
        self.touch()

    def best(self, bucket: dict):
        if not bucket:
            return None, 0.0
        c = max(bucket)
        return c / 100.0, bucket[c]

    def quote(self) -> dict:
        yb, yq = self.best(self.yes)
        nb, nq = self.best(self.no)
        yes_ask = round(1.0 - nb, 4) if nb is not None else None
        no_ask = round(1.0 - yb, 4) if yb is not None else None
        if yb is not None and nb is not None:
            spread = round(1.0 - (yb + nb), 4)
        else:
            spread = None
        stale = (time.monotonic() - self.local_mono) if self.local_mono else 999.0
        return {
            "ticker": self.ticker,
            "yes_bid": yb,
            "no_bid": nb,
            "yes_ask": yes_ask,
            "no_ask": no_ask,
            "yes_bid_q": yq,
            "no_bid_q": nq,
            "spread": spread,
            "stale_s": round(stale, 3),
            "n_yes_lv": len(self.yes),
            "n_no_lv": len(self.no),
            "n_snap": self.n_snap,
            "n_delta": self.n_delta,
        }


SPORTS_FEE_M = 0.5  # Kalshi quadratic_with_maker_fees on sports 2-ways
GAME_KEEP = ("KXMLBGAME", "KXWNBAGAME", "KXNFLGAME", "KXT20MATCH")

def event_prefix(ticker: str) -> str:
    if not ticker or ticker.count("-") < 2:
        return ticker
    return ticker.rsplit("-", 1)[0]


def load_oneleg_ban() -> set[str]:
    """Disk ban survives supervise restarts. Flatten scripts must hit this file too."""
    try:
        raw = json.loads(BAN_PATH.read_text())
        if isinstance(raw, dict):
            raw = raw.get("events") or raw.get("oneleg_ban") or []
        return {str(x) for x in (raw or []) if x}
    except Exception:
        return set()


def save_oneleg_ban(ban) -> None:
    evs = sorted({str(x) for x in (ban or []) if x})
    BAN_PATH.write_text(json.dumps({"events": evs}, indent=2) + "\n")

def has_sibling(ticker: str, tickers) -> bool:
    pref = event_prefix(ticker)
    return any(x != ticker and event_prefix(x) == pref for x in tickers)


_GAME_G = re.compile(r"G\d+$", re.I)


def game_stem(ticker: str) -> str:
    """Matchup slug; G1/G2 of a doubleheader share a stem. Independent events differ."""
    pref = event_prefix(ticker or "")
    slug = pref.rsplit("-", 1)[-1] if pref else ""
    return _GAME_G.sub("", slug).upper()


def max_pair_cap(shard_cash: float) -> int:
    """min(2, floor(available_shard_cash / 9.5))."""
    try:
        c = float(shard_cash or 0.0)
    except (TypeError, ValueError):
        c = 0.0
    return max(0, min(MAX_LIVE_PAIRS, int(c // PAIR_CASH_UNIT)))


def order_remaining_qty(o: dict) -> float:
    for k in (
        "remaining_count_fp",
        "remaining_count",
        "remaining",
        "unfilled_count",
        "count_fp",
        "count",
        "quantity",
    ):
        v = o.get(k)
        if v is None or v == "":
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def order_price_dollars(o: dict) -> float:
    for k in ("yes_price_dollars", "price_dollars", "no_price_dollars"):
        d = to_dollars(o.get(k)) if o.get(k) is not None else None
        if d is not None:
            return float(d)
    for k in ("yes_price", "price", "no_price"):
        d = to_dollars(o.get(k)) if o.get(k) is not None else None
        if d is not None:
            return float(d)
    return 0.0


def order_exchange_index(o: dict, cache: dict | None = None) -> int:
    raw = o.get("exchange_index")
    if raw is not None and str(raw) != "":
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    t = order_ticker(o)
    if cache and t in cache:
        try:
            return int(cache[t])
        except (TypeError, ValueError, KeyError):
            pass
    return guess_exchange_index(t)


def _approx_reserved(pairs, state: dict) -> float:
    tw_by_ev = {}
    for tw in state.get("two_ways_snap") or []:
        evn = event_prefix(tw.get("a") or "")
        if evn:
            tw_by_ev[evn] = tw
    tot = 0.0
    for ev in pairs or []:
        tw = tw_by_ev.get(ev)
        if not tw:
            tot += PAIR_CASH_UNIT
            continue
        try:
            unit = float(tw["yes_a"]) + float(tw["yes_b"])
            q = float(tw.get("qty") or CAP_C)
        except (TypeError, ValueError, KeyError):
            tot += PAIR_CASH_UNIT
            continue
        tot += q * unit
    return tot


def reserved_dollars(orders, idx: int, cache: dict | None = None, state: dict | None = None) -> float:
    """Cash tied up in resting bids on one shard (qty * price). Flatten asks do not lock cash."""
    tot = 0.0
    n = 0
    shard_orders = []
    for o in orders or []:
        if order_exchange_index(o, cache) != int(idx):
            continue
        if not order_is_yes_bid(o):
            continue
        n += 1
        shard_orders.append(o)
        q = order_remaining_qty(o)
        px = order_price_dollars(o)
        if q > 0 and px > 0:
            tot += q * px
    if tot > 1e-9:
        return tot
    if n >= 2 and state is not None:
        pairs = two_way_resting_events(shard_orders)
        return _approx_reserved(pairs, state)
    if n >= 1:
        return float(n) * (PAIR_CASH_UNIT / 2.0)
    return 0.0


def two_way_paper(qa: dict, qb: dict):
    """Pair rest/take on two YES legs of a mx 2-way. Always paper-log. M=0.5."""
    ba, bb = qa.get("yes_bid"), qb.get("yes_bid")
    aa, ab = qa.get("yes_ask"), qb.get("yes_ask")
    if ba is None or bb is None or aa is None or ab is None:
        return None
    # Snapshot is the book. WS silence means no change (tennis points can sit
    # 10–60s). 8s was dropping live 2-ways from paper (n_paper=0 on a lull).
    if (qa.get("n_snap") or 0) < 1 or (qb.get("n_snap") or 0) < 1:
        return None
    if max(qa.get("stale_s") or 99, qb.get("stale_s") or 99) > 75.0:
        return None
    qty = CAP_C
    rest = round(ba + bb, 4)
    take = round(aa + ab, 4)
    fy_r = taker_fee(qty, ba, SPORTS_FEE_M)
    fn_r = taker_fee(qty, bb, SPORTS_FEE_M)
    fy_t = taker_fee(qty, aa, SPORTS_FEE_M)
    fn_t = taker_fee(qty, ab, SPORTS_FEE_M)
    rest_allin = round(rest + (fy_r + fn_r) / qty, 4)
    take_allin = round(take + (fy_t + fn_t) / qty, 4)
    return {
        "a": qa.get("ticker"),
        "b": qb.get("ticker"),
        "yes_a": ba,
        "yes_b": bb,
        "ask_a": aa,
        "ask_b": ab,
        "qty": qty,
        "rest": rest,
        "take": take,
        "rest_allin": rest_allin,
        "take_allin": take_allin,
        "rest_edge": round(1.0 - rest_allin, 4),
        "take_edge": round(1.0 - take_allin, 4),
        "spread_a": qa.get("spread"),
        "spread_b": qb.get("spread"),
        "yes_bid_q_a": qa.get("yes_bid_q"),
        "yes_bid_q_b": qb.get("yes_bid_q"),
        "paper": True,
        "live_fire": bool(LIVE_FIRE),
    }


def is_kbo(ticker: str) -> bool:
    return (ticker or "").upper().startswith("KXKBO")


def is_npb(ticker: str) -> bool:
    """NPB can tie; both-YES is not a clean 1-and-0 (same as KBO)."""
    return (ticker or "").upper().startswith("KXNPB")


def is_cricket(ticker: str) -> bool:
    """T20/IPL etc. Shard 0 cricket is not a proven 1-and-0; T20 also needs a transfer."""
    tu = (ticker or "").upper()
    return tu.startswith(("KXT20", "KXIPL", "KXCRICKET")) or "CRICKET" in tu


def is_harlla(s: str) -> bool:
    return "HARLLA" in (s or "").upper()


def is_lopsided_pair(ya, yb) -> bool:
    """Wing outside 25-75c or the two bids differ by more than ~35c."""
    if ya is None or yb is None:
        return False
    try:
        a, b = float(ya), float(yb)
    except (TypeError, ValueError):
        return False
    lo, hi = LEFTOVER_INPLAY_BID_LO, LEFTOVER_INPLAY_BID_HI
    if a < lo - 1e-12 or a > hi + 1e-12 or b < lo - 1e-12 or b > hi + 1e-12:
        return True
    return abs(a - b) > LEFTOVER_INPLAY_BID_GAP + 1e-12


# Should we sit buy orders on both teams of this game?
# Returns None if the prices look fair enough to try. Otherwise a short
# reason to skip: a team outside 35-65 cents, one team a huge favorite, the two
# prices adding up over 99 cents, or the bid/ask gap too wide.
def live_filter_reason(tw: dict, *, leftover: bool = False, placement: bool = True) -> str | None:
    """None => book filters pass for a live post_only 2-way rest.

    placement=True (maybe_live_rest / new candidates, and OUR resting quote
    in release_stale_leftover_rest): LIVE_BID 35-65 and is_lopsided_pair on
    every 2-way, pregame included.
    placement=False (touch-based leftover rotate): do not cancel solely because
    the touch walked off band; rest_allin>0.99 and leftover in-play lopsided
    still rotate. OUR posted prices are checked with placement=True separately.
    """
    a, b = tw.get("a") or "", tw.get("b") or ""
    if is_harlla(a) or is_harlla(b):
        return "skip HARLLA (already live; do not duplicate)"
    if is_kbo(a) or is_kbo(b):
        return "skip KBO"
    if is_npb(a) or is_npb(b):
        return "skip NPB"
    if is_cricket(a) or is_cricket(b):
        return "cricket skip"
    ba, bb = tw.get("yes_a"), tw.get("yes_b")
    if ba is None or bb is None:
        return "missing bids"
    if placement:
        if not (LIVE_BID_LO - 1e-12 <= float(ba) <= LIVE_BID_HI + 1e-12):
            return f"wing a={ba:.2f}"
        if not (LIVE_BID_LO - 1e-12 <= float(bb) <= LIVE_BID_HI + 1e-12):
            return f"wing b={bb:.2f}"
        # All 2-way live rests (first pair and leftover, pregame too).
        # 25/70 and 70/25 must not stack. Gap LEFTOVER_INPLAY_BID_GAP (0.35).
        if is_lopsided_pair(ba, bb):
            tag = "leftover " if leftover else ""
            return f"{tag}lopsided {float(ba):.2f}/{float(bb):.2f}"
    elif leftover and tw.get("in_play") and is_lopsided_pair(ba, bb):
        # Keep-working: leftover in-play 25-75 / gap 35 still rotates CWS/MIN.
        return f"leftover in-play lopsided {float(ba):.2f}/{float(bb):.2f}"
    if float(ba) + float(bb) > LIVE_REST_MAX + 1e-12:
        if leftover:
            return "idle cash but rest_allin>0.99"
        return f"rest {float(ba)+float(bb):.2f}>{LIVE_REST_MAX}"
    sa, sb = tw.get("spread_a"), tw.get("spread_b")
    # 7-10c books can print bid_sum~93c and are not locks.
    if sa is None or sb is None or float(sa) > LIVE_SPREAD_MAX + 1e-12 or float(sb) > LIVE_SPREAD_MAX + 1e-12:
        return "wide spread"
    tr, ta = tw.get("rest"), tw.get("take")
    # Fat take-rest = adverse-selection magnet (challenger upgrade one-legs).
    if (
        tr is not None
        and ta is not None
        and float(ta) - float(tr) > LIVE_TAKE_REST_GAP_MAX + 1e-12
    ):
        return "fat take-rest gap"
    ra = tw.get("rest_allin")
    # All live rests: rest_allin <= 0.99 (~1c after M=0.5). 0.99 raw is 1.008 all-in.
    if ra is None or float(ra) > LIVE_REST_ALLIN_MAX + 1e-12:
        return "idle cash but rest_allin>0.99" if leftover else f"rest_allin {ra}>0.99"
    return None


def live_qty(qa: dict, qb: dict, cash: float | None = None, yes_a=None, yes_b=None) -> int:
    """qty = min(CAP_C, floor(min yes_bid_q), floor(shard_cash / (yes_a+yes_b)))."""
    try:
        da = float(qa.get("yes_bid_q") or 0)
        db = float(qb.get("yes_bid_q") or 0)
        depth = int(min(da, db))  # floor for positive
    except (TypeError, ValueError):
        depth = 0
    qty = max(0, min(CAP_C, depth))
    try:
        ya = float(yes_a if yes_a is not None else qa.get("yes_bid") or 0)
        yb = float(yes_b if yes_b is not None else qb.get("yes_bid") or 0)
    except (TypeError, ValueError):
        ya, yb = 0.0, 0.0
    unit = ya + yb
    if cash is not None and unit > 0:
        qty = min(qty, int(float(cash) / unit))  # never size a clip leftover cannot fund
    return max(0, qty)


def order_ticker(o: dict) -> str:
    return o.get("ticker") or o.get("market_ticker") or ""


def order_oid(o: dict) -> str:
    return o.get("order_id") or o.get("id") or ""


def list_resting(k: Kalshi, exchange_index: int):
    try:
        data = k.get(
            "/portfolio/orders",
            params={
                "status": "resting",
                "exchange_index": int(exchange_index),
                "limit": 200,
                "subaccount": 0,
            },
        )
    except UnfundedShard:
        return None
    except RuntimeError as e:
        if "404" in str(e) or "user_not_found" in str(e).lower():
            return None
        raise
    return list(data.get("orders") or [])


def collect_resting(k: Kalshi, idxs):
    seen = set()
    out = []
    failed = []
    for idx in idxs:
        rows = list_resting(k, int(idx))
        if rows is None:
            failed.append(int(idx))
            continue
        for o in rows:
            oid = order_oid(o)
            key = oid or (order_ticker(o), str(o.get("created_time") or o.get("ts") or len(out)))
            if key in seen:
                continue
            seen.add(key)
            out.append(o)
    return out, failed


def two_way_resting_events(orders) -> list[str]:
    by_ev: dict[str, set[str]] = {}
    for o in orders or []:
        t = order_ticker(o)
        if not t:
            continue
        by_ev.setdefault(event_prefix(t), set()).add(t)
    return [ev for ev, ts in by_ev.items() if len(ts) >= 2]


def list_open_positions(k: Kalshi, exchange_index: int):
    """Unsettled market positions on one shard. None => lookup failed."""
    try:
        data = k.get(
            "/portfolio/positions",
            params={
                "exchange_index": int(exchange_index),
                "limit": 200,
                "settlement_status": "unsettled",
                "count_filter": "position",
            },
        )
    except UnfundedShard:
        return None
    except RuntimeError as e:
        if "404" in str(e) or "user_not_found" in str(e).lower():
            return None
        raise
    rows = data.get("market_positions") or data.get("positions") or []
    out = []
    for p in rows:
        t = p.get("ticker") or p.get("market_ticker") or ""
        raw = p.get("position_fp", p.get("position"))
        try:
            pv = float(raw)
        except (TypeError, ValueError):
            continue
        if abs(pv) <= 1e-9:
            continue
        out.append(
            {
                "ticker": t,
                "position": pv,
                "exposure": p.get("market_exposure_dollars") or p.get("market_exposure"),
            }
        )
    return out


def collect_open_positions(k: Kalshi, idxs):
    seen = set()
    out = []
    failed = []
    for idx in idxs:
        rows = list_open_positions(k, int(idx))
        if rows is None:
            failed.append(int(idx))
            continue
        for p in rows:
            t = p.get("ticker") or ""
            if not t or t in seen:
                continue
            seen.add(t)
            out.append(p)
    return out, failed


def open_position_events(positions) -> list[str]:
    by_ev: dict[str, set[str]] = {}
    for p in positions or []:
        t = p.get("ticker") or ""
        if not t:
            continue
        by_ev.setdefault(event_prefix(t), set()).add(t)
    return sorted(by_ev)


def open_pos_is_locked(positions) -> bool:
    """True when every open event has both YES legs at equal size (locked 2-way)."""
    by_ev: dict[str, list[float]] = {}
    for p in positions or []:
        t = p.get("ticker") or ""
        if not t:
            continue
        try:
            pv = abs(float(p.get("position") or 0))
        except (TypeError, ValueError):
            continue
        if pv <= 1e-9:
            continue
        by_ev.setdefault(event_prefix(t), []).append(pv)
    if not by_ev:
        return True
    for sizes in by_ev.values():
        if len(sizes) != 2:
            return False
        if abs(sizes[0] - sizes[1]) > 1e-6:
            return False
    return True


# Place a buy order for YES on one team and leave it sitting on the book.
# post_only=True means we only sit; we never take someone else's offer
# (that would cost extra).
def place_yes_post_only(k: Kalshi, ticker: str, price: float, count: int, exchange_index: int) -> dict:
    # NEVER lift: post_only must stay true. V2 events/orders only (V1 is 410).
    body = {
        "ticker": ticker,
        "client_order_id": str(uuid.uuid4()),
        "side": "bid",  # buy YES
        "count": f"{int(count):.2f}",
        "price": f"{float(price):.4f}",
        "time_in_force": "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
        "post_only": True,
        "subaccount": 0,
        "exchange_index": int(exchange_index),
    }
    return k.post("/portfolio/events/orders", body)


# Place a sell order for a leftover YES contract we already own, and leave
# it sitting on the book. Used after the 3-minute wait when the other
# team's buy is gone. Never sell at $1.00 or more.
def place_yes_post_only_sell(k: Kalshi, ticker: str, price: float, count: int, exchange_index: int) -> dict:
    """Maker flatten: sell YES. post_only stays true. No reduce_only (Kalshi: IOC only). Never >= $1.00 / 101c."""
    if float(price) >= 1.0 - 1e-12 or float(price) <= 0:
        raise RuntimeError("refuse sell px outside (0, 1.00) (no lift / no 101c)")
    body = {
        "ticker": ticker,
        "client_order_id": str(uuid.uuid4()),
        "side": "ask",  # sell YES
        "count": f"{int(count):.2f}",
        "price": f"{float(price):.4f}",
        "time_in_force": "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
        "post_only": True,
        "subaccount": 0,
        "exchange_index": int(exchange_index),
    }
    return k.post("/portfolio/events/orders", body)


# Sell a leftover YES contract immediately at the current best bid, instead
# of waiting on the book. Last resort after a sitting sell has waited ~45s
# and the bid has dropped. Never sell at $1.00 or more.
def place_yes_ioc_sell(k: Kalshi, ticker: str, price: float, count: int, exchange_index: int) -> dict:
    """IOC flatten: sell YES at the live bid. post_only=False. Never >= $1.00 / 101c."""
    if float(price) >= 1.0 - 1e-12 or float(price) <= 0:
        raise RuntimeError("refuse sell px outside (0, 1.00) (no lift / no 101c)")
    body = {
        "ticker": ticker,
        "client_order_id": str(uuid.uuid4()),
        "side": "ask",
        "action": "sell",
        "count": f"{int(count):.2f}",
        "price": f"{float(price):.4f}",
        "time_in_force": "immediate_or_cancel",
        "self_trade_prevention_type": "taker_at_cross",
        "post_only": False,
        "reduce_only": True,
        "subaccount": 0,
        "exchange_index": int(exchange_index),
    }
    try:
        return k.post("/portfolio/events/orders", body)
    except Exception:
        body2 = dict(body)
        body2.pop("reduce_only", None)
        body2.pop("action", None)
        try:
            return k.post("/portfolio/events/orders", body2)
        except Exception:
            body3 = {
                "ticker": ticker,
                "client_order_id": str(uuid.uuid4()),
                "side": "yes",
                "action": "sell",
                "count": f"{int(count):.2f}",
                "yes_price_dollars": f"{float(price):.4f}",
                "time_in_force": "immediate_or_cancel",
                "self_trade_prevention_type": "taker_at_cross",
                "post_only": False,
                "subaccount": 0,
                "exchange_index": int(exchange_index),
            }
            return k.post("/portfolio/events/orders", body3)


def order_is_yes_bid(o: dict) -> bool:
    side = str(o.get("side") or o.get("book_side") or "").lower()
    action = str(o.get("action") or "").lower()
    if side in ("ask", "no"):
        return False
    if action == "sell":
        return False
    return True


def flatten_lock_open(ev: str = "") -> list[str]:
    """Sibling flatten in flight (FLATTEN_CWS.lock). Do not race it."""
    hits = []
    evu = (ev or "").upper()
    for path in DIR.glob("FLATTEN_*.lock"):
        tag = path.stem[len("FLATTEN_"):].upper()
        if not tag:
            hits.append(path.name)
        elif not evu or tag in evu or evu in tag:
            hits.append(path.name)
    return hits


def maker_sell_px(yes_bid, yes_ask=None):
    """post_only SELL at/near bid. Selling at the bid takes — rest 1c above. Never >= $1."""
    if yes_bid is None:
        return None
    try:
        bid = round(float(yes_bid), 2)
    except (TypeError, ValueError):
        return None
    if bid <= 0:
        return None
    px = round(bid + 0.01, 2)
    if yes_ask is not None:
        try:
            ask = round(float(yes_ask), 2)
        except (TypeError, ValueError):
            ask = None
        else:
            if ask > bid + 1e-12:
                px = min(px, ask)
    if px >= 1.0 - 1e-12 or px <= 0:
        return None
    return px


def rest_is_stale(rest_px, live_bid) -> bool:
    """True when our maker rest is more than ~2c below the live bid."""
    if rest_px is None or live_bid is None:
        return False
    try:
        return float(live_bid) - float(rest_px) > ONELEG_STALE_C + 1e-12
    except (TypeError, ValueError):
        return False


def _yes_touch(ticker: str, state: dict, books=None):
    if not ticker:
        return None, None
    if books is not None:
        b = books.get(ticker)
        if b is not None:
            q = b.quote() if hasattr(b, "quote") else b
            if isinstance(q, dict) and q.get("yes_bid") is not None:
                ask = q.get("yes_ask")
                return float(q["yes_bid"]), (None if ask is None else float(ask))
    for tw in state.get("two_ways_snap") or []:
        if tw.get("a") == ticker and tw.get("yes_a") is not None:
            ask = tw.get("ask_a")
            return float(tw["yes_a"]), (None if ask is None else float(ask))
        if tw.get("b") == ticker and tw.get("yes_b") is not None:
            ask = tw.get("ask_b")
            return float(tw["yes_b"]), (None if ask is None else float(ask))
    hit = (state.get("yes_touch_cache") or {}).get(ticker)
    if hit and time.monotonic() - float(hit[2]) < 4.0:
        return hit[0], hit[1]
    return None, None


def _fetch_yes_touch(k: Kalshi, ticker: str, state: dict):
    bid, ask = None, None
    try:
        data = k.get(f"/markets/{ticker}")
        m = data.get("market") or data
        raw_b = m.get("yes_bid_dollars") if m.get("yes_bid_dollars") is not None else m.get("yes_bid")
        raw_a = m.get("yes_ask_dollars") if m.get("yes_ask_dollars") is not None else m.get("yes_ask")
        bid = to_dollars(raw_b)
        ask = to_dollars(raw_a)
    except Exception as e:
        log(f"ONELEG touch miss {ticker} {safe_err(e)}")
    state.setdefault("yes_touch_cache", {})[ticker] = (bid, ask, time.monotonic())
    return bid, ask


def _oneleg_log(state: dict, rec: dict) -> None:
    key = (
        f"{rec.get('event')}|{rec.get('action')}|{int(bool(rec.get('stale')))}|"
        f"{int(bool(rec.get('lopsided')))}"
    )
    ts = state.setdefault("oneleg_log_ts", {})
    now = time.monotonic()
    if now - float(ts.get(key) or 0) < 20.0:
        return
    ts[key] = now
    extra = f" {rec.get('flatten')}" if rec.get("flatten") else ""
    log(
        f"ONELEG {rec.get('event')} filled={rec.get('filled')} "
        f"{rec.get('filled_qty')}@{rec.get('filled_px')} "
        f"rest={rec.get('rest')} {rec.get('rest_qty')}@{rec.get('rest_px')} "
        f"live_bid={rec.get('live_rest_bid')} stale={int(bool(rec.get('stale')))} "
        f"lopsided={int(bool(rec.get('lopsided')))} → {rec.get('action')}{extra}"
    )


# One team's buy went through; we own YES on only one side of a game.
# Matching size on both teams already: hold (the $10 card) -- skip this.
# Other buy still sitting and prices add to $1.00 or less: keep that buy
# (ignore a 2-cent market move). Never a new buy on the missing team.
# Other buy gone: wait 3 minutes after the first order went through, then
# sell the leftover contract. Never pay $1.01+ to complete the pair.
def handle_oneleg_inventory(k: Kalshi, state: dict, books=None):
    positions = state.get("open_positions") or []
    orders = state.get("resting") or []
    by_ev: dict[str, list] = {}
    for p in positions:
        t = p.get("ticker") or ""
        if not t:
            continue
        try:
            pv = float(p.get("position") or 0)
        except (TypeError, ValueError):
            continue
        if abs(pv) <= 1e-9:
            continue
        by_ev.setdefault(event_prefix(t), []).append(p)
    if not by_ev:
        state["oneleg"] = None
        return None

    now = time.monotonic()
    recs = []
    acted = None
    for ev, plist in by_ev.items():
        if is_harlla(ev):
            continue
        sizes = []
        for p in plist:
            try:
                sizes.append(abs(float(p.get("position") or 0)))
            except (TypeError, ValueError):
                pass
        if len(plist) == 2 and len(sizes) == 2 and abs(sizes[0] - sizes[1]) <= 1e-6:
            (state.get("oneleg_seen_ts") or {}).pop(ev, None)
            continue
        # Flatten-price path only when exactly one leg has inventory.
        if len(plist) != 1:
            continue
        filled = max(plist, key=lambda p: abs(float(p.get("position") or 0)))
        ft = filled.get("ticker") or ""
        try:
            fqty = abs(float(filled.get("position") or 0))
            fsigned = float(filled.get("position") or 0)
        except (TypeError, ValueError):
            fqty, fsigned = 0.0, 0.0
        fill_px = None
        try:
            exp = filled.get("exposure")
            if exp is not None and fqty > 0:
                fill_px = abs(float(exp)) / fqty
        except (TypeError, ValueError):
            fill_px = None
        rest_legs = [
            o
            for o in orders
            if event_prefix(order_ticker(o)) == ev
            and order_ticker(o) != ft
            and order_is_yes_bid(o)
            and order_remaining_qty(o) > 0
            and order_oid(o)
        ]
        already_ask = [
            o
            for o in orders
            if order_ticker(o) == ft
            and not order_is_yes_bid(o)
            and order_remaining_qty(o) > 0
        ]
        rest_t = order_ticker(rest_legs[0]) if rest_legs else None
        rest_px = order_price_dollars(rest_legs[0]) if rest_legs else None
        rest_q = order_remaining_qty(rest_legs[0]) if rest_legs else 0.0
        live_rest, _ask_r = _yes_touch(rest_t, state, books) if rest_t else (None, None)
        live_fill, live_fill_ask = _yes_touch(ft, state, books)
        if rest_t and live_rest is None:
            live_rest, _ask_r = _fetch_yes_touch(k, rest_t, state)
        if live_fill is None:
            live_fill, live_fill_ask = _fetch_yes_touch(k, ft, state)
        if fill_px is None:
            fill_px = live_fill
        rec = {
            "event": ev,
            "filled": ft,
            "filled_qty": fqty,
            "filled_px": None if fill_px is None else round(float(fill_px), 4),
            "rest": rest_t,
            "rest_px": None if rest_px is None else round(float(rest_px), 4),
            "rest_qty": rest_q,
            "live_rest_bid": live_rest,
            "live_fill_bid": live_fill,
            "stale": False,
            "lopsided": False,
            "action": None,
            "ts_et": datetime.now(ET).strftime("%H:%M:%S ET"),
        }
        locks = flatten_lock_open(ev)
        if locks:
            rec["action"] = "ONELEG flatten-lock skip"
            recs.append(rec)
            leftover_note(state, None, 0, 0, f"ONELEG {ev} flatten lock {locks[0]}")
            _oneleg_log(state, rec)
            continue
        rec["stale"] = rest_is_stale(rest_px, live_rest)
        lock_if_prints = None
        if fill_px is not None and rest_px is not None:
            try:
                lock_if_prints = float(fill_px) + float(rest_px)
            except (TypeError, ValueError):
                lock_if_prints = None
        rec["lock_if_prints"] = (
            None if lock_if_prints is None else round(float(lock_if_prints), 4)
        )
        in_band = False
        try:
            in_band = (
                fill_px is not None
                and rest_px is not None
                and LIVE_BID_LO - 1e-12 <= float(fill_px) <= LIVE_BID_HI + 1e-12
                and LIVE_BID_LO - 1e-12 <= float(rest_px) <= LIVE_BID_HI + 1e-12
            )
        except (TypeError, ValueError):
            in_band = False
        rec["lopsided"] = bool(rest_legs) and not in_band
        seen_map = state.setdefault("oneleg_seen_ts", {})
        # Other team's buy is still sitting, and the two prices add to 99 cents
        # or less: keep it. Ignore a 2-cent market move -- that sitting buy can
        # still go through. Never a new buy on the missing team.
        keep_rest = (
            bool(rest_legs)
            and (lock_if_prints is None or lock_if_prints <= 0.99 + 1e-12)
        )
        if keep_rest:
            seen_map.pop(ev, None)
            rec["action"] = "ONELEG keep paired rest"
            recs.append(rec)
            leftover_note(
                state,
                None,
                0,
                0,
                (
                    f"ONELEG keep paired rest {ev} filled={ft} "
                    f"{fqty}@{rec.get('filled_px')} rest={rest_t} "
                    f"{rest_q}@{rec.get('rest_px')} "
                    f"lock_if_prints={rec['lock_if_prints']}"
                ),
            )
            _oneleg_log(state, rec)
            continue
        # Other buy still sitting and prices add to $1.00 or less: keep it
        # (fees can eat the last penny). Do not sell the leftover. Do not
        # sit a new buy on the missing team.
        if rest_legs and lock_if_prints is not None and lock_if_prints <= 1.00 + 1e-12:
            seen_map.pop(ev, None)
            rec["action"] = "ONELEG wait paired rest"
            recs.append(rec)
            leftover_note(
                state,
                None,
                0,
                0,
                (
                    f"ONELEG wait paired rest {ev} filled={ft} "
                    f"{fqty}@{rec.get('filled_px')} rest={rest_t} "
                    f"{rest_q}@{rec.get('rest_px')} "
                    f"lock_if_prints={rec['lock_if_prints']} (fees)"
                ),
            )
            _oneleg_log(state, rec)
            continue
        if rest_legs:
            # Prices would add to more than $1.00 if the other buy went
            # through -- we would lose. Cancel that sitting buy, then wait
            # 3 minutes (do not sell the leftover this pass). Never a new
            # buy on the missing team.
            cache = state.setdefault("idx_cache", {})
            for o in rest_legs:
                t = order_ticker(o)
                idx = cache.get(t)
                if idx is None:
                    idx = guess_exchange_index(t)
                cancel_our_order(k, order_oid(o), exchange_index=idx, market_ticker=t)
                log(
                    f"ONELEG cancel rest {t} px={order_price_dollars(o):.2f} "
                    f"live={live_rest} losing_lock={rec.get('lock_if_prints')}"
                )
            rest_legs = []
        # Other team's buy is gone. Wait 3 minutes after the first order
        # went through before selling the leftover contract. Never a new
        # buy on the missing team. Never pay $1.01+ to complete.
        seen = float(seen_map.get(ev) or 0.0)
        if seen > now + 1.0:
            seen = 0.0
        if seen <= 0:
            seen_map[ev] = now
            seen = now
        orphan_wait = now - seen
        if orphan_wait < ONELEG_ORPHAN_WAIT_S:
            rec["action"] = "ONELEG wait other side 180s"
            recs.append(rec)
            leftover_note(
                state,
                None,
                0,
                0,
                (
                    f"ONELEG wait other side 180s {ev} filled={ft} "
                    f"{fqty}@{rec.get('filled_px')} waited={orphan_wait:.0f}s"
                ),
            )
            _oneleg_log(state, rec)
            continue
        rec["action"] = "NAKED_FLATTEN"
        cache = state.setdefault("idx_cache", {})
        state.setdefault("oneleg_ban", set()).add(ev)
        save_oneleg_ban(state["oneleg_ban"])
        # 3 minutes are up and the other buy is gone: sell the leftover
        # contract we already own. Never a new buy on the missing team.
        # Never pay $1.01+ to complete.
        for o in rest_legs:
            t = order_ticker(o)
            idx = cache.get(t)
            if idx is None:
                idx = guess_exchange_index(t)
            cancel_our_order(k, order_oid(o), exchange_index=idx, market_ticker=t)
            log(
                f"ONELEG cancel rest {t} px={order_price_dollars(o):.2f} "
                f"live={live_rest}"
            )
        extra_bids = [
            o
            for o in orders
            if order_ticker(o) == ft
            and order_is_yes_bid(o)
            and order_remaining_qty(o) > 0
            and order_oid(o)
        ]
        for o in extra_bids:
            t = order_ticker(o)
            idx = cache.get(t)
            if idx is None:
                idx = guess_exchange_index(t)
            cancel_our_order(k, order_oid(o), exchange_index=idx, market_ticker=t)
            log(f"ONELEG cancel extra bid {t} oid={order_oid(o)}")
        sell_note = None
        if fsigned <= 0:
            sell_note = "ORCH short/NO inventory; no YES sell"
        else:
            qty = max(0, min(CAP_C, int(fqty)))
            try:
                cost = None if fill_px is None else float(fill_px)
            except (TypeError, ValueError):
                cost = None
            target = None if cost is None else round(cost - 0.01, 2)
            if target is not None and (target <= 0 or target >= 1.0 - 1e-12):
                target = None
            try:
                live_bid = None if live_fill is None else float(live_fill)
            except (TypeError, ValueError):
                live_bid = None
            idx = cache.get(ft)
            if idx is None:
                idx = market_exchange_index(k, ft, cache)
            placed_at = float(state.get("oneleg_action_ts") or 0.0)
            if placed_at > now + 1.0:
                placed_at = 0.0
            wait_s = (now - placed_at) if placed_at > 0 else None
            # Maker px: never post_only at/below live_bid (Kalshi rejects the cross).
            maker_px = target
            if target is not None and live_bid is not None and live_bid + 1e-12 >= target:
                maker_px = round(min(0.99, float(live_bid) + 0.01), 2)
            if maker_px is not None and (maker_px <= 0 or maker_px >= 1.0 - 1e-12):
                maker_px = None
            if (
                maker_px is not None
                and live_bid is not None
                and maker_px <= live_bid + 1e-12
            ):
                maker_px = None
            near_asks = []
            off_asks = []
            if maker_px is not None:
                for o in already_ask:
                    try:
                        opx = float(order_price_dollars(o))
                    except (TypeError, ValueError):
                        continue
                    if abs(opx - maker_px) <= ONELEG_NEAR_C + 1e-12:
                        near_asks.append(o)
                    else:
                        off_asks.append(o)
            else:
                off_asks = list(already_ask)

            def _cancel_asks(asks):
                for o in asks:
                    t = order_ticker(o)
                    oid = order_oid(o)
                    if not oid:
                        continue
                    cidx = cache.get(t)
                    if cidx is None:
                        cidx = guess_exchange_index(t)
                    cancel_our_order(k, oid, exchange_index=cidx, market_ticker=t)

            def _ioc_at_bid(why: str) -> str:
                if live_bid is None or live_bid <= 0 or live_bid >= 1.0 - 1e-12 or qty <= 0:
                    return "ORCH cannot IOC-sell (no bid / no 101c)"
                px = round(float(live_bid), 4)
                _cancel_asks(already_ask)
                try:
                    place_yes_ioc_sell(k, ft, px, qty, int(idx))
                    rec["action"] = why
                    log(f"{why} {ft} IOC sell YES {qty}@{px:.2f} cost={cost} target={target}")
                    return f"IOC SELL {qty}@{px:.2f}"
                except Exception as e:
                    log(f"{why} {ft} IOC sell fail {safe_err(e)} (no lift)")
                    return f"ORCH sell fail {safe_err(e)}"

            if qty <= 0:
                sell_note = "ORCH cannot sell (qty=0)"
            elif target is None:
                sell_note = "ORCH cannot flatten (bad cost/target / no 101c)"
            else:
                give_up = (
                    bool(already_ask)
                    and wait_s is not None
                    and wait_s >= ONELEG_MAKER_WAIT_S
                    and live_bid is not None
                    and live_bid < target - ONELEG_GIVEUP_UNDER
                )
                if give_up:
                    rec["action"] = "ONELEG give-up take"
                    sell_note = _ioc_at_bid("ONELEG give-up take")
                elif near_asks or (wait_s is not None and wait_s < ONELEG_MAKER_WAIT_S):
                    rec["action"] = "ONELEG maker-wait"
                    if placed_at <= 0:
                        state["oneleg_action_ts"] = now
                    px_s = (
                        f"{maker_px:.2f}" if maker_px is not None else f"{target:.2f}"
                    )
                    sell_note = (
                        f"maker-wait {qty}@{px_s} live_bid={live_bid} "
                        f"wait={0 if wait_s is None else wait_s:.0f}s"
                    )
                elif maker_px is None:
                    rec["action"] = "ONELEG maker-wait"
                    sell_note = (
                        f"maker-wait no-cross {qty} live_bid={live_bid} "
                        f"target={target}"
                    )
                    log(
                        f"ONELEG maker-wait {ft} skip post_only "
                        f"(would cross live_bid={live_bid} / no 101c)"
                    )
                else:
                    _cancel_asks(off_asks)
                    try:
                        place_yes_post_only_sell(k, ft, float(maker_px), qty, int(idx))
                        state["oneleg_action_ts"] = now
                        rec["action"] = "ONELEG maker-wait"
                        sell_note = f"maker rest SELL {qty}@{maker_px:.2f}"
                        log(
                            f"ONELEG maker-wait {ft} rest post_only SELL "
                            f"{qty}@{maker_px:.2f} live_bid={live_bid} target={target}"
                        )
                    except Exception as e:
                        sell_note = f"ORCH maker rest fail {safe_err(e)}"
                        log(f"ONELEG maker rest fail {ft} {safe_err(e)} (no lift)")
        rec["flatten"] = sell_note
        recs.append(rec)
        leftover_note(state, None, 0, 0, f"NAKED_FLATTEN {ev} {sell_note}")
        state["naked_flatten"] = rec
        _oneleg_log(state, rec)
        acted = rec
    state["oneleg"] = recs or None
    return acted


def cancel_our_order(
    k: Kalshi,
    oid: str,
    exchange_index: int | None = None,
    market_ticker: str | None = None,
) -> bool:
    """DELETE V2 order on the right shard. Shard 0 default 404s tennis/MLB."""
    if not oid or oid in LIVE_SKIP_OIDS:
        return False
    params = {"subaccount": 0}
    if exchange_index is not None:
        params["exchange_index"] = int(exchange_index)
    elif market_ticker:
        params["exchange_index"] = -1
    if market_ticker:
        params["market_ticker"] = market_ticker
    try:
        k.delete(f"/portfolio/events/orders/{oid}", params=params)
        return True
    except Exception as e:
        log(f"rollback cancel fail {safe_err(e)}")
        return False


def refresh_portfolio(k: Kalshi, watch: list, state: dict, books=None) -> None:
    cache = state.setdefault("idx_cache", {})
    idxs = {0, 3}
    for w in watch or []:
        t = w["ticker"] if isinstance(w, dict) else w
        if isinstance(w, dict) and w.get("exchange_index") is not None:
            cache[t] = int(w["exchange_index"])
        else:
            cache[t] = market_exchange_index(k, t, cache)
        idxs.add(int(cache[t]))
    bals = {}
    for i in sorted(idxs):
        bals[str(i)] = round(shard_balance_dollars(k, i), 4)
    orders, failed = collect_resting(k, sorted(idxs))
    # If shard 3 lookup failed, keep prior HARLLA seed so we never duplicate.
    if 3 in failed and state.get("resting"):
        have = {order_oid(o) for o in orders}
        for o in state["resting"]:
            if order_oid(o) not in have:
                orders.append(o)
    state["resting"] = orders
    state["shard_balances"] = bals
    state["leftover_cash"] = dict(bals)
    state["n_resting"] = len(orders)
    state["resting_tickers"] = sorted({order_ticker(o) for o in orders if order_ticker(o)})
    state["live_pair_events"] = two_way_resting_events(orders)
    state["n_live_pairs"] = len(state["live_pair_events"])
    free = {}
    for i_s, cash in bals.items():
        res = reserved_dollars(orders, int(i_s), cache, state)
        free[str(i_s)] = round(max(0.0, float(cash) - float(res)), 4)
    state["free_cash"] = free
    try:
        max_cash = max(float(x) for x in bals.values()) if bals else 0.0
    except (TypeError, ValueError):
        max_cash = 0.0
    n_allowed = max_pair_cap(max_cash)
    if state["n_live_pairs"] == 0:
        n_allowed = max(n_allowed, 1)
    state["pair_cap"] = n_allowed
    state["resting_failed_shards"] = failed
    positions, pos_failed = collect_open_positions(k, sorted(idxs))
    state["open_positions"] = positions
    state["n_open_pos"] = len(positions)
    state["open_pos_tickers"] = sorted({p["ticker"] for p in positions if p.get("ticker")})
    state["open_pos_events"] = open_position_events(positions)
    state["pos_failed_shards"] = pos_failed
    state["port_ts"] = time.monotonic()
    if LIVE_FIRE:
        try:
            handle_oneleg_inventory(k, state, books)
        except Exception as e:
            log(f"ONELEG handle {safe_err(e)}")



def resting_pair_quote(orders, ev: str) -> dict | None:
    """Build a filter tw from OUR resting YES prices (not the live touch).

    leftover rotate used to cancel a fresh 0.30+0.66 CINCHC rest one second
    after place because the touch flicked to 0.32+0.67 (rest_allin 1.006).
    Our bids were still a 0.96 lock; touch moving away is normal.
    """
    legs = [o for o in (orders or []) if event_prefix(order_ticker(o)) == ev]
    if len(legs) < 2:
        return None
    # Distinct tickers; take the best (highest) remaining yes price per ticker.
    by_t: dict[str, float] = {}
    qty_by_t: dict[str, float] = {}
    for o in legs:
        t = order_ticker(o)
        if not t:
            continue
        px = order_price_dollars(o)
        q = order_remaining_qty(o)
        if px <= 0 or q <= 0:
            continue
        if t not in by_t or px > by_t[t]:
            by_t[t] = px
            qty_by_t[t] = q
    if len(by_t) < 2:
        return None
    tickers = sorted(by_t.keys())
    a, b = tickers[0], tickers[1]
    ya, yb = float(by_t[a]), float(by_t[b])
    qty = max(1, int(min(qty_by_t[a], qty_by_t[b])))
    rest = round(ya + yb, 4)
    fy = taker_fee(qty, ya, SPORTS_FEE_M)
    fn = taker_fee(qty, yb, SPORTS_FEE_M)
    rest_allin = round(rest + (fy + fn) / qty, 4)
    return {
        "a": a,
        "b": b,
        "yes_a": ya,
        "yes_b": yb,
        "qty": qty,
        "rest": rest,
        "rest_allin": rest_allin,
        "rest_edge": round(1.0 - rest_allin, 4),
        # Our post_only prices; market spread is irrelevant to keeping them.
        "spread_a": 0.01,
        "spread_b": 0.01,
    }


def release_stale_leftover_rest(
    k: Kalshi,
    orders: list,
    state: dict,
    candidate_tw: dict | None = None,
    allow_upgrade: bool = True,
) -> bool:
    """Cancel leftover (non-HARLLA) rests that no longer pay, or lose to a better clip.

    AZSF was 48+49=97c/0.988 at 9:48 ET; later the touch was 50+49=99c/1.008 and
    the stacked slot blocked COLATL/SEMHUN 97c for ~10m while rotate cooldown /
    missing snap quotes left the slot stuck. Never touch HARLLA.

    Also upgrade: if candidate_tw clears leftover filters and beats the resting
    leftover by >= LEFTOVER_UPGRADE_ALLIN (or SAME_STEM), cancel the weaker
    rest so the better one can take the stacked slot.
    """
    now = time.monotonic()
    tw_by_ev = {}
    for tw in state.get("two_ways_snap") or []:
        evn = event_prefix(tw.get("a") or "")
        if evn:
            tw_by_ev[evn] = tw
    cand_ra = None
    cand_ev = None
    if candidate_tw is not None and live_filter_reason(candidate_tw, leftover=True) is None:
        try:
            cand_ra = float(candidate_tw.get("rest_allin"))
            cand_ev = event_prefix(candidate_tw.get("a") or "")
        except (TypeError, ValueError):
            cand_ra = None
    # Stale (fails filter / missing snap) ignores the 45s cooldown so a stuck
    # AZSF-style rest cannot block upgrades for a full minute. Pure upgrades keep it.
    cooling = now - float(state.get("leftover_rotate_ts") or 0) < 45.0
    freed = False
    last_why = None
    # Upgrade picks: (rest_allin, ev, tw). Cancel only the single worst leftover
    # for one candidate — 18:22 ET dumped BOTH BOSNYYG1+G2 for one BUSBON slot.
    upgrade_picks: list[tuple[float, str, dict]] = []
    pos_evs = set(state.get("open_pos_events") or [])
    # Only skip upgrades into inventory we already hold. Blanket in_play skip
    # froze BOSNYYG2 while HOUNYM cleared filters at 0.928 all-in (20:12:59 ET)
    # and only logged "cap" — live books still need the 1.5c upgrade gate +
    # spread/wing filters; they just must be allowed to compete for the slot.
    skip_up = bool(candidate_tw is not None and cand_ev in pos_evs)

    def _cancel_ev(ev: str, why: str, tw) -> bool:
        legs = [
            o
            for o in orders
            if event_prefix(order_ticker(o)) == ev and order_oid(o)
        ]
        if not legs:
            return False
        cache = state.setdefault("idx_cache", {})
        log(
            f"LIVE leftover rotate {ev} {why} rest_allin={None if not tw else tw.get('rest_allin')} "
            f"cancel {len(legs)} oids"
        )
        n_ok = 0
        for o in legs:
            t = order_ticker(o)
            idx = cache.get(t)
            if idx is None:
                idx = guess_exchange_index(t)
            if cancel_our_order(k, order_oid(o), exchange_index=idx, market_ticker=t):
                n_ok += 1
        return n_ok > 0

    for ev in two_way_resting_events(orders):
        if is_harlla(ev):
            continue
        if cand_ev and ev == cand_ev:
            continue
        tw = tw_by_ev.get(ev)
        why = None
        miss = state.setdefault("leftover_missing_since", {})
        our = resting_pair_quote(orders, ev)
        if not tw:
            # Watch rebuild blanks the live book for a couple seconds.
            # If our two buy orders are still sitting, check those prices
            # now. Waiting 30s left Svrcina/Royer at 99.8c all-in blocking
            # a 97c second game, and the upgrade dumped the other (better)
            # Sox/Twins buy instead. Only wait when we also have no orders.
            if our is not None:
                miss.pop(ev, None)
                tw = our
                why = live_filter_reason(our, leftover=True, placement=True)
            else:
                started = miss.get(ev)
                if started is None:
                    miss[ev] = now
                    continue
                if now - float(started) < 30.0:
                    continue
                why = "missing snap (cannot verify leftover)"
        else:
            miss.pop(ev, None)
            why = live_filter_reason(tw, leftover=True, placement=False)
            # Prefer OUR buy prices over the live tape. Tape flicking to 99c
            # must not cancel a still-valid 96-97c pair of buy orders.
            # OUR buy prices MUST stay in 35-65 (and not lopsided):
            # new buys reject 32/65, but keep-working used to leave
            # POTVAL 32+65 / VANNOR 33+65 sitting for hours and cap better
            # in-band books (VIDBOU 47/50, OLIBRA 48/49). Tape flick alone
            # still does not cancel; only our sitting buy prices do.
            if our is not None and tw is not None:
                our["in_play"] = tw.get("in_play")
            if our is not None:
                # Band/lopsided on OUR prices (placement=True); rest_allin /
                # spread / take-rest keep placement=False so a touch walk
                # off 99c does not dump a still-valid 96-97c post_only.
                our_band = live_filter_reason(our, leftover=True, placement=True)
                band_hit = (
                    our_band is not None
                    and (
                        our_band.startswith("wing ")
                        or "lopsided" in our_band
                    )
                )
                if band_hit:
                    why = f"our orders {our_band}"
                    tw = our
                elif why is not None:
                    our_why = live_filter_reason(our, leftover=True, placement=False)
                    if our_why is None:
                        why = None
                        tw = our  # upgrade compare uses our all-in
                    else:
                        why = f"our orders {our_why}"
                        tw = our
                else:
                    tw = our
            # Queue upgrades; cancel the single worst after the stale pass.
            if why is None and allow_upgrade and cand_ra is not None and not skip_up:
                try:
                    ra = float(tw.get("rest_allin"))
                except (TypeError, ValueError):
                    ra = None
                # Same stem (doubleheader) needs a wider gap; other matchups 1.0c.
                need = (
                    LEFTOVER_UPGRADE_SAME_STEM
                    if game_stem(cand_ev or "") == game_stem(ev)
                    else LEFTOVER_UPGRADE_ALLIN
                )
                if ra is not None and cand_ra + need <= ra + 1e-12:
                    upgrade_picks.append(
                        (
                            ra,
                            ev,
                            {
                                "why": (
                                    f"upgrade to {cand_ev} rest_allin={cand_ra:.3f} "
                                    f"beats {ra:.3f} need={need:.3f}"
                                ),
                                "tw": tw,
                            },
                        )
                    )
        if why is None:
            continue
        # Stale/missing always proceed (no cooldown). Upgrades handled below.
        if why.startswith("upgrade to "):
            continue
        if _cancel_ev(ev, why, tw):
            freed = True
            last_why = why
    if allow_upgrade and not cooling and upgrade_picks:
        # Highest rest_allin = weakest leftover; free exactly one stacked slot.
        upgrade_picks.sort(key=lambda x: (-x[0], x[1]))
        ra, ev, meta = upgrade_picks[0]
        if _cancel_ev(ev, meta["why"], meta["tw"]):
            freed = True
            last_why = meta["why"]
    if freed:
        state["leftover_rotate_ts"] = now
        leftover_note(state, None, 0, 0, f"rotated stale leftover: {last_why}")
    return freed


def leftover_note(state: dict, cash, qty, notional, reason: str | None = None) -> None:
    bals = state.setdefault("shard_balances", {})
    state["leftover_cash"] = dict(bals)
    if cash is not None:
        state["leftover_cash_used"] = round(float(cash), 4)
    state["leftover_size"] = int(qty or 0)
    state["leftover_notional"] = round(float(notional or 0), 4)
    if reason is not None:
        state["leftover_sit_reason"] = reason


# Try to sit buy orders on both teams of this game (wait on both sides).
# If we already own a leftover contract on one team of this same game,
# handle that instead -- never sit a new buy on the missing team.
def maybe_live_rest(k: Kalshi, tw: dict, qa: dict, qb: dict, state: dict) -> str:
    if event_prefix(tw["a"]) in (state.get("oneleg_ban") or set()):
        return "oneleg ban — already flattened, do not re-buy"
    cache = state.setdefault("idx_cache", {})
    idx = market_exchange_index(k, tw["a"], cache)
    idx_b = market_exchange_index(k, tw["b"], cache)
    if idx != idx_b:
        return f"shard mismatch {idx}/{idx_b}"
    # Fresh resting check on dest + 0 + 3 immediately before send.
    orders, failed = collect_resting(k, sorted({0, 3, int(idx)}))
    state["resting"] = orders
    state["n_resting"] = len(orders)
    if int(idx) in failed or 3 in failed:
        return f"resting GET miss failed={failed}; not placing"
    positions, pos_failed = collect_open_positions(k, sorted({0, 3, int(idx)}))
    state["open_positions"] = positions
    state["n_open_pos"] = len(positions)
    state["open_pos_tickers"] = sorted({p["ticker"] for p in positions if p.get("ticker")})
    state["open_pos_events"] = open_position_events(positions)
    if int(idx) in pos_failed or 3 in pos_failed:
        return f"positions GET miss failed={pos_failed}; not placing"
    pos_events = open_position_events(positions)
    locked = open_pos_is_locked(positions)
    if pos_events and not locked:
        # One-leg fill is not a completed lock. Keep original paired rest if still paying; else flatten. Never rest a NEW pair / buy missing wing.
        try:
            handle_oneleg_inventory(k, state, None)
        except Exception as e:
            log(f"ONELEG handle {safe_err(e)}")
        # Only abort THIS event (never buy the missing wing). Other 2-ways may
        # leftover-rest; DET flatten used to NAKED_FLATTEN-abort SVRROY 97.8c.
        evn = event_prefix(tw["a"])
        if evn in pos_events or evn in (state.get("oneleg_ban") or set()):
            why = state.get("leftover_sit_reason") or "ONELEG"
            leftover_note(state, None, 0, 0, why)
            return why
    if event_prefix(tw["a"]) in pos_events:
        return "already have inventory"
    tickers = {order_ticker(o) for o in orders}
    if tw["a"] in tickers or tw["b"] in tickers:
        return "leg already resting"
    pairs = two_way_resting_events(orders)
    n_rest_pairs = len(pairs)
    # Filled locked 2-ways leave n_rest_pairs=0 but cash is leftover — do NOT
    # treat as a first-pair fund ($12 from shard 0) or we LIVE-fail spam.
    n_inv_pairs = len(pos_events) if locked else 0
    leftover = n_rest_pairs >= 1 or n_inv_pairs >= 1
    reason = live_filter_reason(tw, leftover=leftover)
    if reason:
        if leftover:
            leftover_note(state, None, 0, 0, reason)
            log(
                f"LIVE leftover sit {event_prefix(tw['a'])} {reason} "
                f"n_live_pairs={n_rest_pairs}"
            )
        return reason
    stems = {game_stem(order_ticker(o)) for o in orders if order_ticker(o)}
    if leftover and game_stem(tw["a"]) in stems:
        msg = f"same game as resting {sorted(stems)[:4]}"
        leftover_note(state, None, 0, 0, msg)
        log(f"LIVE leftover sit {event_prefix(tw['a'])} {msg} n_live_pairs={n_rest_pairs}")
        return msg
    cash = shard_balance_dollars(k, idx)
    state.setdefault("shard_balances", {})[str(idx)] = round(cash, 4)
    reserved = reserved_dollars(orders, idx, cache, state)
    free = max(0.0, float(cash) - float(reserved))
    state.setdefault("free_cash", {})[str(idx)] = round(free, 4)
    n_allowed = max_pair_cap(cash)
    # Only force a first-pair slot when we have neither resting nor filled locks.
    if n_rest_pairs == 0 and n_inv_pairs == 0:
        n_allowed = max(n_allowed, 1)
    # After a filled 2-way, total cash can fall so floor(cash/28.5) equals the
    # remaining sitting pair (SDTB lock left $14.53 → cap 1, Svrcina/Royer
    # already sitting). ~$4.93 leftover could still buy a smaller second
    # pair on THIS shard. Count that extra slot when free cash is at least
    # the leftover minimum; live_qty will shrink the size.
    if (
        leftover
        and n_rest_pairs < MAX_LIVE_PAIRS
        and free + 1e-12 >= MIN_LEFTOVER_NOTIONAL
    ):
        n_allowed = max(n_allowed, n_rest_pairs + 1)
    state["pair_cap"] = n_allowed
    state["n_live_pairs"] = n_rest_pairs

    def _reload_resting():
        nonlocal orders, failed, tickers, pairs, n_rest_pairs, reserved, free
        orders, failed = collect_resting(k, sorted({0, 3, int(idx)}))
        state["resting"] = orders
        state["n_resting"] = len(orders)
        tickers = {order_ticker(o) for o in orders}
        pairs = two_way_resting_events(orders)
        n_rest_pairs = len(pairs)
        state["live_pair_events"] = pairs
        state["n_live_pairs"] = n_rest_pairs
        reserved = reserved_dollars(orders, idx, cache, state)
        free = max(0.0, float(cash) - float(reserved))
        state.setdefault("free_cash", {})[str(idx)] = round(free, 4)

    if leftover and n_allowed <= 0:
        msg = f"idle cash below pair unit free={free:.2f} inv={n_inv_pairs}"
        leftover_note(state, free, 0, 0, msg)
        log(f"LIVE leftover sit {event_prefix(tw['a'])} {msg}")
        return msg
    if leftover:
        at_cap = n_rest_pairs >= n_allowed
        if release_stale_leftover_rest(
            k, orders, state, candidate_tw=tw, allow_upgrade=at_cap
        ):
            _reload_resting()
            if int(idx) in failed or 3 in failed:
                return f"resting GET miss after rotate failed={failed}; not placing"
            if tw["a"] in tickers or tw["b"] in tickers:
                return "leg already resting"
        if n_rest_pairs >= n_allowed:
            msg = (
                f"cap: n_live_pairs={n_rest_pairs} cap={n_allowed} "
                f"resting={pairs[:3]} free={free:.2f}"
            )
            leftover_note(state, free, 0, 0, msg)
            log(f"LIVE leftover sit {event_prefix(tw['a'])} {msg}")
            return msg
    elif n_rest_pairs >= n_allowed:
        return f"cap: n_live_pairs={n_rest_pairs} cap={n_allowed} resting={pairs[:3]}"
    size_cash = free if leftover else cash
    qty = live_qty(qa, qb, cash=size_cash, yes_a=tw.get("yes_a"), yes_b=tw.get("yes_b"))
    try:
        unit = float(tw["yes_a"]) + float(tw["yes_b"])
    except (TypeError, ValueError, KeyError):
        unit = 0.0
    notional = qty * unit if qty and unit else 0.0
    if leftover:
        leftover_note(state, free, qty, notional)
        if qty <= 0 or notional + 1e-12 < MIN_LEFTOVER_NOTIONAL:
            msg = f"idle cash leftover notional <$4 free={free:.2f}"
            leftover_note(state, free, qty, notional, msg)
            log(
                f"LIVE leftover sit {event_prefix(tw['a'])} {msg} "
                f"n_live_pairs={n_rest_pairs} cap={n_allowed}"
            )
            return msg
        # Recompute all-in at the actual leftover size (small qty can worsen unit fee).
        fy = taker_fee(qty, float(tw["yes_a"]), SPORTS_FEE_M)
        fn = taker_fee(qty, float(tw["yes_b"]), SPORTS_FEE_M)
        rest_allin = round(unit + (fy + fn) / qty, 4)
        tw = {**tw, "qty": qty, "rest_allin": rest_allin, "rest_edge": round(1.0 - rest_allin, 4)}
        if rest_allin > LIVE_REST_ALLIN_MAX + 1e-12:
            leftover_note(state, free, qty, notional, "idle cash but rest_allin>0.99")
            log(
                f"LIVE leftover sit {event_prefix(tw['a'])} idle cash but rest_allin>0.99 "
                f"rest_allin={rest_allin} free={free:.2f}"
            )
            return "idle cash but rest_allin>0.99"
        # Idle money only: never transfer to fund a second pair (T20 shard 0 etc).
    else:
        if qty <= 0:
            return "no depth"
        if cash < CLIP_NOTIONAL:
            try:
                fund_shard(k, idx, FUND_DOLLARS, ticker=tw["a"], log=log)
            except RuntimeError as e:
                # Shard 0 only has the $0.50 keep after MLB fills — sit, don't raise.
                msg = f"unfunded idx={idx} cash={cash:.2f} ({e})"
                log(f"LIVE sit {event_prefix(tw['a'])} {msg}")
                return msg
            cash = shard_balance_dollars(k, idx)
            state["shard_balances"][str(idx)] = round(cash, 4)
            qty = live_qty(qa, qb, cash=cash, yes_a=tw.get("yes_a"), yes_b=tw.get("yes_b"))
            notional = qty * unit if qty and unit else 0.0
            if qty <= 0 or cash < CLIP_NOTIONAL:
                return f"unfunded idx={idx} cash={cash:.2f}"
    oid_a = None
    try:
        ra = place_yes_post_only(k, tw["a"], tw["yes_a"], qty, idx)
        oid_a = ra.get("order_id")
        rb = place_yes_post_only(k, tw["b"], tw["yes_b"], qty, idx)
    except UnfundedShard:
        if oid_a:
            cancel_our_order(k, oid_a, exchange_index=idx, market_ticker=tw["a"])
        if leftover:
            leftover_note(state, free, qty, notional, "leftover unfunded; sit no transfer")
            log(f"SHARD miss leftover: {tw['a']} idx={idx} cash={cash:g} sit (no transfer)")
            return "leftover unfunded; sit no transfer"
        log(f"SHARD miss: {tw['a']} idx={idx} cash={cash:g} → transfer ${FUND_DOLLARS:g}")
        try:
            fund_shard(k, idx, FUND_DOLLARS, ticker=tw["a"], log=log)
        except RuntimeError as e:
            return f"unfunded-404 fund failed ({e})"
        return "unfunded-404 retried fund; skip this tick"
    except Exception:
        if oid_a:
            cancel_our_order(k, oid_a, exchange_index=idx, market_ticker=tw["a"])
        raise
    if leftover:
        leftover_note(state, free, qty, notional)
        state["leftover_sit_reason"] = None
    log(
        f"LIVE REST {tw['a']} {qty}@{tw['yes_a']:.2f} + {tw['b']} {qty}@{tw['yes_b']:.2f} "
        f"idx={idx} post_only=1 leftover={int(leftover)} notional={notional:.2f} "
        f"free={free:.2f} n_live_pairs={n_rest_pairs + 1}/{n_allowed}"
    )
    return "placed"


def paper_quote(q: dict):
    """Same-contract maker: rest YES bid + NO bid. Never live."""
    if LIVE_FIRE:
        return None
    if q.get("spread") is None or q.get("yes_bid") is None or q.get("no_bid") is None:
        return None
    if q["stale_s"] > STALE_S:
        return None
    spread = q["spread"]
    if spread + 1e-12 < MIN_SPREAD:
        return None
    if spread > MAX_SPREAD:
        return None  # ghost 8¢/8¢ NCAAF-style books
    yb, nb = q["yes_bid"], q["no_bid"]
    if spread >= IMPROVE_SPREAD:
        y_quote = round(yb + 0.01, 2)
        n_quote = round(nb + 0.01, 2)
        action = "improve"
    else:
        y_quote = round(yb, 2)
        n_quote = round(nb, 2)
        action = "join"
    if y_quote <= 0 or n_quote <= 0 or y_quote >= 1 or n_quote >= 1:
        return None
    if y_quote + n_quote >= 1.0 - 1e-12:
        return None
    unit = y_quote + n_quote
    qty = CAP_C
    if unit > 0:
        qty = min(qty, int(CAP_NOTIONAL / unit + 1e-9))
    qty = max(0, min(qty, CAP_C))
    if qty <= 0:
        return None
    fy = taker_fee(qty, y_quote, SPORTS_FEE_M)
    fn = taker_fee(qty, n_quote, SPORTS_FEE_M)
    fee = fy + fn
    all_in = unit + (fee / qty)
    edge = 1.0 - all_in
    return {
        "yes_quote": y_quote,
        "no_quote": n_quote,
        "qty": qty,
        "action": action,
        "fee": round(fee, 4),
        "fee_yes": fy,
        "fee_no": fn,
        "all_in": round(all_in, 4),
        "edge": round(edge, 4),
        "notional": round(qty * unit, 4),
        "live_fire": False,
        "paper": True,
    }


def list_spread(m: dict):
    yb = to_dollars(m.get("yes_bid_dollars") if m.get("yes_bid_dollars") is not None else m.get("yes_bid"))
    nb = to_dollars(m.get("no_bid_dollars") if m.get("no_bid_dollars") is not None else m.get("no_bid"))
    ya = to_dollars(m.get("yes_ask_dollars") if m.get("yes_ask_dollars") is not None else m.get("yes_ask"))
    na = to_dollars(m.get("no_ask_dollars") if m.get("no_ask_dollars") is not None else m.get("no_ask"))
    if yb is None and na is not None:
        yb = round(1.0 - na, 4)
    if nb is None and ya is not None:
        nb = round(1.0 - ya, 4)
    if yb is None or nb is None:
        return None, yb, nb
    return round(1.0 - (yb + nb), 4), yb, nb


def pick_watch(k: Kalshi):
    now = datetime.now(timezone.utc)
    today = now.astimezone(ET).date()
    tomorrow = today + timedelta(days=1)
    leftover_mode = False
    ban = load_oneleg_ban()
    shard0_unfunded = False
    try:
        st = json.loads(STATUS.read_text())
        leftover_mode = (
            int(st.get("n_live_pairs") or 0) >= 1
            or int(st.get("n_resting") or 0) >= 2
            or (
                bool(st.get("open_pos_locked"))
                and int(st.get("n_open_pos") or 0) >= 2
            )
        )
        ban |= {str(x) for x in (st.get("oneleg_ban") or []) if x}
        fc = st.get("free_cash") or st.get("leftover_cash") or st.get("shard_balances") or {}
        try:
            cash0 = float(fc.get("0", fc.get(0, 0)) or 0)
        except (TypeError, ValueError):
            cash0 = 0.0
        # Already sitting tennis 2-ways: do not spend watch slots on shard-0
        # Bears/Titans or WNBA when that shard only has pocket change ($0.50)
        # and we will not move cash over for a second pair.
        shard0_unfunded = leftover_mode and cash0 < MIN_LEFTOVER_NOTIONAL
    except Exception:
        leftover_mode = False
        shard0_unfunded = False
    cands = []
    n_mkt = 0
    for st in SPORT_SERIES:
        cursor = None
        events = []
        for _page in range(4):  # ATP/WTA can exceed one 50-event page
            params = {
                "series_ticker": st,
                "status": "open",
                "limit": 50,
                "with_nested_markets": True,
            }
            if cursor:
                params["cursor"] = cursor
            try:
                data = k.get("/events", params)
            except Exception as e:
                log(f"events {st} fail {safe_err(e)}")
                break
            batch = data.get("events") or []
            events.extend(batch)
            cursor = data.get("cursor") or None
            if not cursor or not batch:
                break
        for e in events:
            cat = e.get("category") or "Sports"
            for m in e.get("markets") or []:
                n_mkt += 1
                if (m.get("status") or "active") not in ("open", "active"):
                    continue
                t = m.get("ticker") or ""
                tu = t.upper()
                if any(s in tu for s in SKIP_TICKER_SUBSTR):
                    continue
                if skip_market(m):
                    continue
                if any(x in tu for x in ("5M", "15M", "BTC5", "ETH5", "BTC15", "ETH15")):
                    continue
                cl = event_dt(m)  # often first-ball; use only for day rank
                close_t = parse_ts(m.get("close_time"))
                occ = parse_ts(m.get("occurrence_datetime"))
                exp = parse_ts(m.get("expected_expiration_time")) or parse_ts(m.get("expiration_time"))
                still_open = (m.get("status") or "active") in ("open", "active")
                vol = 0.0
                try:
                    vol = float(m.get("volume_fp") or m.get("volume_24h_fp") or 0)
                except (TypeError, ValueError):
                    vol = 0.0
                # Tennis expected_expiration_time is often first-ball (same as
                # occ), not settlement. Requiring exp>now ranked live FEAROD
                # (51/44 rest 95c, ~2M vol) behind 17k TAUPAR, and the old
                # drop used event_dt==exp so it vanished from the universe.
                # close_time is the trading window (often T+14d).
                # Volume>=5k used to mark Sep-9 NESEA "in_play" and steal a
                # tonight slot. occ<=now alone kept 8/27 tennis as in-play at
                # 8am 8/28 and ate 16/20 slots (today MLB/NFL 98c 2-ways missed).
                # Cap first-ball age. T20 occ is often ticker-ET stored as UTC
                # (TRITHR 0915 live at 09:17, occ 17:15Z). Prefer the earlier
                # of occ/event_dt and the ticker clock so live cricket is
                # in_play instead of day=0 behind tomorrow MLB volume.
                play_t = occ or cl
                ticker_t = ticker_start_et(t)
                if ticker_t is not None and (play_t is None or ticker_t < play_t):
                    play_t = ticker_t
                play_age = (now - play_t).total_seconds() if play_t is not None else None
                in_play = bool(still_open and (close_t is None or close_t > now) and (
                    (play_t is not None and play_t <= now and play_age is not None
                     and play_age <= IN_PLAY_MAX_AGE)
                    or (play_t is None and vol >= 5000 and (("MATCH" in tu) or ("GAME" in tu)))
                ))
                # drop only if we cannot trade; do not use first-ball exp
                if not still_open:
                    continue
                if not in_play and close_t is not None and close_t <= now:
                    continue
                spread, yb, nb = list_spread(m)
                if spread is None or yb is None or nb is None:
                    continue
                if yb < 0.01 or nb < 0.01:
                    continue
                if tu.startswith("KXKBO"):
                    continue  # KBO: skip (ties / not a clean 1-and-0)
                if tu.startswith("KXNPB"):
                    continue  # NPB: ties possible, same as KBO
                # Live T20 ranks in_play ahead of today MLB and cannot be the
                # leftover second pair (cricket skip + shard 0, no transfer).
                if leftover_mode and is_cricket(t):
                    continue
                ex_idx = m.get("exchange_index")
                if ex_idx is None or str(ex_idx) == "":
                    ex_idx = guess_exchange_index(t)
                else:
                    try:
                        ex_idx = int(ex_idx)
                    except (TypeError, ValueError):
                        ex_idx = guess_exchange_index(t)
                # EPL/MLS winner markets are 3-way (home/draw/away). Pairing
                # the first two YES legs looks like a 68c lock and is not.
                if tu.startswith(("KXEPL", "KXMLS")):
                    continue
                two_way = 0 if (
                    (("GAME" in tu) or ("MATCH" in tu))
                    and "TOTAL" not in tu and "SPREAD" not in tu
                ) else 1
                # CINCHC in-play TOTAL/SPREAD/F5 ranked as live and ate 4-8/20
                # GAME slots (PHILAA/SDTB/AZSFG2 97c dropped). Clipper is 2-way only.
                if two_way != 0:
                    continue
                min_sp = 0.01 if two_way == 0 else MIN_SPREAD
                # Live cricket/tennis REST books routinely print 13-20c; 0.12
                # dropped a TRITHR leg and broke the pair off the watch.
                max_sp = 0.25 if (in_play and two_way == 0) else MAX_SPREAD
                if spread < min_sp or spread > max_sp:
                    continue
                # Ticker calendar date (ET) beats occ/exp UTC. AUG292207 PHILAA
                # occ 05:07Z is 1:07am ET Aug 30, so day=2 dropped a 97c 2-way
                # behind today 98c MLB leftover will not rest. Same bug class as
                # T20 ticker-vs-UTC in_play.
                tm = _TICKER_START.search(tu)
                ticker_date = None
                if tm:
                    try:
                        ticker_date = datetime(
                            2000 + int(tm.group(1)),
                            _MON[tm.group(2).upper()],
                            int(tm.group(3)),
                            tzinfo=ET,
                        ).date()
                    except ValueError:
                        ticker_date = None
                ref = play_t or occ or cl or exp or now
                d = ticker_date if ticker_date is not None else ref.astimezone(ET).date()
                if in_play:
                    day = -1  # live first
                elif d == today:
                    day = 0
                elif d == tomorrow:
                    day = 1
                elif d <= today + timedelta(days=3):
                    day = 2
                else:
                    day = 3
                sports = 0 if (cat == "Sports" or is_sportsy(m) or is_sportsy(e)) else 1
                if tu.startswith(("KXMLB", "KXWNBA", "KXNFL", "KXATP", "KXWTA", "KXNPB", "KXT20")):
                    series_rank = 0
                elif tu.startswith("KXNCAAF"):
                    series_rank = 2
                else:
                    series_rank = 1
                cands.append(
                    {
                        "ticker": t,
                        "event": m.get("event_ticker") or e.get("event_ticker"),
                        "spread": spread,
                        "sports": sports == 0,
                        "day": day,
                        "title": (m.get("title") or e.get("title") or "")[:80],
                        "close": cl,
                        "yb": yb,
                        "nb": nb,
                        "in_play": in_play,
                        "two_way": two_way,
                        "vol": vol,
                        "series_rank": series_rank,
                        "exchange_index": int(ex_idx or 0),
                    }
                )
    # Full 2-way events (both legs). Live pairs first, then restable
    # 2-ways (both YES in LIVE_BID_LO..HI, yb_sum<=LIVE_REST_MAX, and
    # rest_allin<=LIVE_REST_ALLIN_MAX after M=0.5 sports fees), then
    # series_rank (MLB/NFL/ATP/WTA before NCAAF), then volume. Do NOT prefer
    # 50/50 over other in-band 2-ways. Band is LIVE_BID_LO..HI (35-65);
    # 19/80 is no longer restable. Volume-only rank dropped live ATP
    # FEAROD (52/45 rest 97c, ~2M vol) behind 100c NFL; unused series_rank let
    # ghost NCAAF steal slots from tennis. Raw<=0.99 without fee check let
    # DETIND/BOSNYY/PARMER hog slots while sitting rest_allin>0.99.
    by_ev = {}
    for c in cands:
        by_ev.setdefault(c.get("event") or c["ticker"], []).append(c)
    scored = []
    for ev, legs in by_ev.items():
        legs_tw = [x for x in legs if x.get("two_way") == 0]
        # Exactly two winner legs. >=2 grabbed EPL home+away and skipped draw.
        pair = legs_tw[:2] if len(legs_tw) == 2 else None
        use = pair if pair else legs[:1]
        vol = sum(float(x.get("vol") or 0) for x in use)
        in_play = any(x.get("in_play") for x in use)
        yb_sum = None
        ya = yb = None
        if pair and pair[0].get("yb") is not None and pair[1].get("yb") is not None:
            ya, yb = pair[0]["yb"], pair[1]["yb"]
            yb_sum = ya + yb
        # Align with live gate: raw yb_sum<=0.99 still marks DETIND/BOSNYY/
        # PARMER "actionable" while rest_allin 0.996-1.008 fails LIVE_REST_ALLIN_MAX
        # and sits forever, crowding real 97c ATP (SVRROY/BASSCH) off the watch.
        rest_allin_est = None
        if yb_sum is not None and ya is not None and yb is not None:
            qty_est = 10
            fy = taker_fee(qty_est, float(ya), SPORTS_FEE_M)
            fn = taker_fee(qty_est, float(yb), SPORTS_FEE_M)
            rest_allin_est = round(yb_sum + (fy + fn) / qty_est, 4)
        actionable = bool(
            pair
            and ya is not None
            and yb is not None
            and LIVE_BID_LO - 1e-12 <= ya <= LIVE_BID_HI + 1e-12
            and LIVE_BID_LO - 1e-12 <= yb <= LIVE_BID_HI + 1e-12
            and yb_sum <= LIVE_REST_MAX + 1e-12
            and yb_sum >= 0.90  # <0.90 is a missing outcome (3-way soccer)
            and rest_allin_est is not None
            and rest_allin_est <= LIVE_REST_ALLIN_MAX + 1e-12
        )
        # series_rank was computed per-leg but unused; NCAAF volume was
        # crowding ATP/MLB off the 20-slot watch (METOWS/LAFGTWN/UVAWPRE).
        series_rank = min(int(x.get("series_rank") or 1) for x in use)
        day = min(int(x.get("day") if x.get("day") is not None else 9) for x in use)
        # Skip unpinned pairs we will not sit anyway. Used to require
        # in_play, so pregame (or first-ball clock still in the future)
        # CHI/TEN 15/83 and TOR/PHX 78c still filled leftover GAME slots
        # and rotated GULMOL 96c tennis off the 20-name watch.
        skip_unpinned = bool(
            pair
            and ya is not None
            and yb is not None
            and (
                is_lopsided_pair(ya, yb)
                or ya < LIVE_BID_LO - 1e-12
                or ya > LIVE_BID_HI + 1e-12
                or yb < LIVE_BID_LO - 1e-12
                or yb > LIVE_BID_HI + 1e-12
            )
        )
        scored.append(
            {
                "ev": ev,
                "legs": use,
                "yb_sum": yb_sum,
                "rest_allin_est": rest_allin_est,
                "day": day,
                "actionable": actionable,
                "skip_unpinned": skip_unpinned,
                "rank": (
                    0 if pair else 1,  # 2-way GAME/MATCH before live props
                    0 if actionable else 1,
                    0 if in_play else 1,
                    max(day, 0),  # today (0) before Sep NFL (3)
                    series_rank,  # MLB/NFL/ATP/WTA=0 before NCAAF=2
                    # fee-adjusted tighter rest before volume (raw 0.99 looked
                    # tight but sits after M=0.5 sports fees)
                    rest_allin_est if rest_allin_est is not None else 9.0,
                    -vol,
                ),
            }
        )
    scored.sort(key=lambda x: x["rank"])
    watch = []
    have = set()

    def take(src, limit, *, keep_lopsided=False):
        for row in src:
            if len(watch) >= limit:
                return
            evn = row.get("ev") or ""
            if evn in ban:
                continue
            if (not keep_lopsided) and row.get("skip_unpinned"):
                continue
            if (not keep_lopsided) and shard0_unfunded:
                if any(int(leg.get("exchange_index") or 0) == 0 for leg in row["legs"]):
                    continue
            pending = [leg for leg in row["legs"] if leg["ticker"] not in have]
            if not pending:
                continue
            # Need both winner legs. 20th slot used to take MENHUA-MEN without HUA.
            if len(pending) >= 2 and len(watch) + len(pending) > limit:
                continue
            for leg in pending:
                if len(watch) >= limit:
                    return
                if event_prefix(leg["ticker"]) in ban:
                    continue
                have.add(leg["ticker"])
                watch.append(leg)

    def live_pin_tickers():
        # Pin working rests/inventory only. KNOWN_LIVE used to always prepend
        # HARLLA even after settlement, then the loop below invented 50/50
        # "pinned live rest" ghosts that ate 2/20 slots (PACAND 98c dropped).
        # LIVE_SKIP_OIDS still skips those oids so we never duplicate/cancel them.
        # Also pin recent take_lock flashes so BALATH-style 0.998 locks cannot
        # rotate off the 20-slot watch before the hourly (paper only; no auto-lift).
        pins = []
        try:
            st = json.loads(STATUS.read_text())
            live_have = set(st.get("resting_tickers") or []) | set(
                st.get("open_pos_tickers") or []
            )
            for t, _ in KNOWN_LIVE:
                if t in live_have and t not in pins:
                    pins.append(t)
            for t in st.get("resting_tickers") or []:
                if t and t not in pins:
                    pins.append(t)
            for t in st.get("open_pos_tickers") or []:
                if t and t not in pins:
                    pins.append(t)
            def _pin_leg(t):
                if t and t not in pins:
                    pins.append(t)
            for pk in st.get("take_locks") or []:
                _pin_leg(pk.get("a"))
                _pin_leg(pk.get("b"))
            tlb = st.get("take_lock_best") or {}
            if isinstance(tlb, dict):
                for pk in tlb.values():
                    if isinstance(pk, dict):
                        _pin_leg(pk.get("a"))
                        _pin_leg(pk.get("b"))
        except Exception:
            pass
        return pins

    # Keep working live orders on the watch. HARLLA was rotated off at 15:59 ET
    # while 10-lot post-only rests were still on the book.
    pin_ts = live_pin_tickers()
    # Take-lock *peaks* stay in status after the cheap take is gone.
    # Today's Sox/Twins 99.6c peak became 77/22 at 101c take and still
    # pinned 2 watch slots (keep_lopsided) in front of 97c tennis.
    # Only pin a take-lock flash if we still have buys sitting on it,
    # we already own it, or the live pair is not a lopsided in-play.
    rest_inv_pref = set()
    try:
        st_pin = json.loads(STATUS.read_text())
        rest_inv_pref = {
            event_prefix(t)
            for t in list(st_pin.get("resting_tickers") or [])
            + list(st_pin.get("open_pos_tickers") or [])
            if t
        }
    except Exception:
        rest_inv_pref = set()
    dead_lock_ev = {
        row.get("ev")
        for row in scored
        if row.get("skip_unpinned")
    }
    pin_ts = [
        t
        for t in pin_ts
        if event_prefix(t) in rest_inv_pref
        or event_prefix(t) not in dead_lock_ev
    ]
    pin_pref = {event_prefix(t) for t in pin_ts}
    by_ticker = {c["ticker"]: c for c in cands}
    pinned_rows = [
        row for row in scored
        if any(event_prefix(leg["ticker"]) in pin_pref for leg in row["legs"])
    ]
    take(pinned_rows, WATCH_N, keep_lopsided=True)
    for t in pin_ts:
        if t in have or len(watch) >= WATCH_N:
            continue
        if t in by_ticker:
            have.add(t)
            watch.insert(0, by_ticker[t])
            continue
        # Do not invent 50/50 ghosts for settled/unlisted pins.
        log(f"  skip ghost pin {t} not in universe")
    # Tight restable 2-ways (raw <= 0.98 still pays after M=0.5; 0.99 is 1.008).
    # Without this, aging yesterday tennis out of in_play dropped HAVHIB 95c
    # behind today 99c MLB that we skip on rest_allin.
    tight = [
        row for row in scored
        if row.get("actionable")
        and row.get("yb_sum") is not None
        and 0.90 <= row["yb_sum"] <= 0.9800001
        and all(
            (leg.get("spread") or 9) <= LIVE_SPREAD_MAX + 1e-12
            for leg in row["legs"]
        )
        and not any(
            (leg.get("ticker") or "").upper().startswith("KXNCAAF")
            for leg in row["legs"]
        )
    ]
    # Same-day first so Sep-20 94c NFL does not crowd out today 98c BOSNYY.
    tight.sort(
        key=lambda x: (
            0 if int(x.get("day") if x.get("day") is not None else 9) <= 1 else 1,
            x["yb_sum"] if x["yb_sum"] is not None else 9.0,
            x["rank"],
        )
    )
    take(tight, min(WATCH_N, len(watch) + 8))
    # Today NCAAF (day=0, series_rank=2) outranks tomorrow MLB/ATP (day=1,
    # series_rank=0) in the rank tuple. Tight already drops KXNCAAF; without
    # scored_noghost, take(scored) refilled the leftover 4 slots with ghost
    # 10-lot 4-6c NCAAF (ROOSCHS/ALSUFAMU) and rotated BALATH off watch.
    def _is_ghost_series(row):
        return any(
            (leg.get("ticker") or "").upper().startswith(("KXNCAAF", "KXKBO"))
            for leg in row["legs"]
        )
    scored_noghost = [row for row in scored if not _is_ghost_series(row)]
    take(scored_noghost, max(0, WATCH_N - 4))
    keep = GAME_KEEP if not leftover_mode else tuple(x for x in GAME_KEEP if x != "KXT20MATCH")
    game_scored = [
        row for row in scored
        if int(row.get("day") if row.get("day") is not None else 9) <= 1
        and any(
            any(leg["ticker"].startswith(pref) for pref in keep)
            for leg in row["legs"]
        )
    ]
    take(game_scored, WATCH_N)
    take(scored_noghost, WATCH_N)
    log(f"universe nested_mkt={n_mkt} cands={len(cands)} watch={len(watch)}")
    for w in watch[:6]:
        log(
            f"  pick {w['ticker']} spr={w['spread']:.3f} yb={w['yb']:.2f} nb={w['nb']:.2f} "
            f"sports={w['sports']} day={w['day']} {w['title'][:46]}"
        )
    evs = []
    for w in watch:
        pref = event_prefix(w["ticker"])
        if pref not in evs:
            evs.append(pref)
    log("  events " + " ".join(evs))
    return watch


def write_status(payload: dict):
    payload["ts_et"] = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET")
    payload["live_fire"] = LIVE_FIRE
    STATUS.write_text(json.dumps(payload, default=str, indent=2))
    HEARTBEAT.write_text(str(time.time()))


async def run_ws(tickers: list[str], books: dict, stop: dict, state: dict):
    key_id, pkey = load_key()
    headers = ws_headers(key_id, pkey)
    sid_seq: dict = {}
    dump_n = 0
    deadline = time.monotonic() + UNIVERSE_S
    try:
        async with websockets.connect(
            WS_URL,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=20,
            open_timeout=30,
        ) as ws:
            state["connected"] = True
            state["ws_error"] = None
            log(f"ws connected n={len(tickers)}")
            await ws.send(
                json.dumps(
                    {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["orderbook_delta"],
                            "market_tickers": tickers,
                        },
                    }
                )
            )
            while not stop["n"] and time.monotonic() <= deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue
                try:
                    data = json.loads(raw)
                except Exception:
                    log("ws non-json frame")
                    continue
                typ = data.get("type")
                seq = data.get("seq")
                sid = data.get("sid")
                msg = data.get("msg") or {}
                if dump_n < 5:
                    dump_n += 1
                    mk = list(msg.keys()) if isinstance(msg, dict) else type(msg).__name__
                    sample = None
                    if isinstance(msg, dict):
                        for k in ("yes", "yes_dollars", "yes_dollars_fp", "no", "no_dollars"):
                            if msg.get(k) is not None:
                                sample = f"{k}[:2]={str(msg.get(k)[:2])[:180]}"
                                break
                    log(f"ws sample type={typ} top={list(data.keys())} msg_keys={mk} {sample}")
                if sid is None and isinstance(msg, dict):
                    sid = msg.get("sid")
                # seq is per-sid across snapshots, deltas, AND command acks
                if sid is not None and seq is not None:
                    prev = sid_seq.get(sid)
                    if prev is not None and seq != prev + 1:
                        raise SeqGap(f"seq gap sid={sid} {prev}->{seq}")
                    sid_seq[sid] = seq
                if typ == "error":
                    em = msg if not isinstance(msg, dict) else {
                        kk: msg.get(kk) for kk in ("code", "message", "msg") if kk in msg
                    }
                    log(f"ws error {em}")
                    state["ws_error"] = str(em)[:200]
                    continue
                if typ in ("subscribed", "ok", "pong", "heartbeat"):
                    log(
                        f"ws {typ} id={data.get('id')} sid="
                        f"{sid or (msg.get('sid') if isinstance(msg, dict) else None)}"
                    )
                    continue
                t = None
                if isinstance(msg, dict):
                    t = msg.get("market_ticker") or msg.get("ticker")
                if not t:
                    continue
                b = books.setdefault(t, Book(t))
                if typ == "orderbook_snapshot":
                    b.snapshot(msg if isinstance(msg, dict) else {})
                elif typ == "orderbook_delta":
                    b.delta(msg if isinstance(msg, dict) else {})
                else:
                    continue
                state["last_msg_et"] = datetime.now(ET).strftime("%H:%M:%S ET")
                q = b.quote()
                pq = None if has_sibling(t, tickers) else paper_quote(q)
                key = (
                    q.get("ticker"),
                    None if not pq else (pq["yes_quote"], pq["no_quote"], pq["qty"], pq["action"]),
                )
                prev_key = state.setdefault("paper_keys", {}).get(t)
                if key != prev_key:
                    state["paper_keys"][t] = key
                    if pq:
                        log(
                            f"PAPER {t} spr={q['spread']:.3f} {pq['action']} "
                            f"YES {pq['yes_quote']:.2f} NO {pq['no_quote']:.2f} "
                            f"q={pq['qty']} fee={pq['fee']:.2f} all_in={pq['all_in']:.3f} "
                            f"edge={pq['edge']*100:+.1f}c"
                        )
                    elif prev_key and prev_key[1] is not None:
                        log(f"PAPER cancel {t} stale={q['stale_s']:.2f}s spr={q.get('spread')}")
    except SeqGap:
        raise
    except Exception as e:
        state["connected"] = False
        state["ws_error"] = safe_err(e)
        log(f"ws fail {safe_err(e)}")
        raise


async def status_loop(books: dict, watch: list, stop: dict, state: dict, k: Kalshi):
    n = 0
    while not stop["n"]:
        n += 1
        cache = state.setdefault("idx_cache", {})
        for w in watch:
            if isinstance(w, dict) and w.get("ticker") and w.get("exchange_index") is not None:
                cache[w["ticker"]] = int(w["exchange_index"])
        if LIVE_FIRE and (time.monotonic() - float(state.get("port_ts") or 0)) >= 8.0:
            try:
                await asyncio.to_thread(refresh_portfolio, k, watch, state, books)
            except Exception as e:
                log(f"portfolio refresh {safe_err(e)}")
        quotes = []
        papers = []
        for w in watch:
            t = w["ticker"] if isinstance(w, dict) else w
            b = books.get(t)
            if not b:
                continue
            q = b.quote()
            quotes.append(q)
        # 2-way YES+YES pairs (the actual clip), grouped by event
        two_ways = []
        by_ev = {}
        for w in watch:
            if not isinstance(w, dict):
                continue
            by_ev.setdefault(w.get("event") or w["ticker"], []).append(w)
        tw_keys = state.setdefault("tw_keys", {})
        for ev, legs in by_ev.items():
            if len(legs) < 2:
                continue
            ba = books.get(legs[0]["ticker"])
            bb = books.get(legs[1]["ticker"])
            if not ba or not bb:
                continue
            tw = two_way_paper(ba.quote(), bb.quote())
            if not tw:
                continue
            tw["in_play"] = any(bool(x.get("in_play")) for x in legs)
            two_ways.append(tw)
            key = (tw["rest"], tw["take"], tw["rest_allin"], tw["take_allin"])
            if tw_keys.get(ev) != key:
                tw_keys[ev] = key
                log(
                    f"2WAY {ev} rest {tw['yes_a']:.2f}+{tw['yes_b']:.2f}={tw['rest']:.2f} "
                    f"allin={tw['rest_allin']:.3f} e={tw['rest_edge']*100:+.1f}c | "
                    f"take {tw['ask_a']:.2f}+{tw['ask_b']:.2f}={tw['take']:.2f} "
                    f"allin={tw['take_allin']:.3f} e={tw['take_edge']*100:+.1f}c"
                )
            # Fee-adjusted ask lock (take_allin<1): never auto-lift; surface loudly.
            if tw.get("take_allin") is not None and float(tw["take_allin"]) < 1.0 - 1e-12:
                lock_ts = state.setdefault("take_lock_log_ts", {})
                lock_best = state.setdefault("take_lock_best", {})
                now = time.monotonic()
                prev_best = lock_best.get(ev)
                better = prev_best is None or float(tw["take_edge"]) > float(prev_best.get("take_edge") or -99)
                if better or now - float(lock_ts.get(ev) or 0) > 30.0:
                    lock_ts[ev] = now
                    if better:
                        lock_best[ev] = {
                            "a": tw["a"],
                            "b": tw["b"],
                            "take": tw["take"],
                            "take_allin": tw["take_allin"],
                            "take_edge": tw["take_edge"],
                            "rest_allin": tw["rest_allin"],
                            "ask_a": tw["ask_a"],
                            "ask_b": tw["ask_b"],
                            "ts_mono": now,
                        }
                    log(
                        f"TAKE LOCK {ev} asks {tw['ask_a']:.2f}+{tw['ask_b']:.2f}="
                        f"{tw['take']:.2f} allin={tw['take_allin']:.3f} "
                        f"e={tw['take_edge']*100:+.1f}c (paper only; no auto-lift)"
                    )
        # Peak positive rest_edge this watch window (status was flickering n_paper=0).
        peaks = state.setdefault("edge_peaks", {})
        for tw in two_ways:
            evn = event_prefix(tw["a"])
            if tw.get("rest_edge") is not None and tw["rest_edge"] > 0:
                prev = peaks.get(evn)
                if prev is None or tw["rest_edge"] > prev.get("rest_edge", -99):
                    peaks[evn] = {
                        "a": tw["a"],
                        "b": tw["b"],
                        "rest": tw["rest"],
                        "rest_allin": tw["rest_allin"],
                        "rest_edge": tw["rest_edge"],
                        "take_allin": tw["take_allin"],
                        "ts_mono": time.monotonic(),
                    }
        # Keep peaks ~65m so the hourly miss-pass still sees them (10m was
        # wiping TAKE LOCKs like SFLV +1.3c before :07/:23 status).
        for evn in list(peaks):
            if time.monotonic() - float(peaks[evn].get("ts_mono") or 0) > 3900:
                peaks.pop(evn, None)
        for evn in list(state.get("take_lock_best") or {}):
            if time.monotonic() - float(state["take_lock_best"][evn].get("ts_mono") or 0) > 3900:
                state["take_lock_best"].pop(evn, None)

        n_live = int(state.get("n_live_pairs") or len(state.get("live_pair_events") or []))
        leftover_now = n_live >= 1
        state["two_ways_snap"] = two_ways
        if LIVE_FIRE and not state.get("live_busy"):
            resting_evs = set(state.get("live_pair_events") or [])
            resting_stems = {game_stem(t) for t in (state.get("resting_tickers") or [])}

            def _live_rank(t):
                a = t.get("a") or ""
                independent = (
                    event_prefix(a) not in resting_evs
                    and game_stem(a) not in resting_stems
                )
                mlb = a.upper().startswith("KXMLB")
                try:
                    ra = float(t.get("rest_allin"))
                except (TypeError, ValueError):
                    ra = 9.0
                edge = t.get("rest_edge") if t.get("rest_edge") is not None else -99
                if leftover_now:
                    # Independent events first; today/tomorrow MLB 97c over 98c tennis +0.2c.
                    # Pregame leftover before in-play flap (CINCHC 97c dumped BOSNYYG2).
                    return (
                        0 if independent else 1,
                        0 if mlb else 1,
                        0 if not t.get("in_play") else 1,
                        ra,
                        -edge,
                    )
                return (
                    0 if t.get("in_play") else 1,
                    0 if mlb else 1,
                    ra,
                    -edge,
                )

            ranked = sorted(two_ways, key=_live_rank)
            skip_log = state.setdefault("live_skip_log_ts", {})
            at_cap = n_live >= int(state.get("pair_cap") or MAX_LIVE_PAIRS)
            sit_why = None
            attempted = False
            for tw in ranked:
                why = live_filter_reason(tw, leftover=leftover_now)
                if why:
                    evn = event_prefix(tw["a"])
                    key = f"{evn}|{why.split('(')[0][:48]}"
                    now = time.monotonic()
                    unused = evn not in resting_evs and game_stem(tw["a"]) not in resting_stems
                    if leftover_now and unused and sit_why is None:
                        sit_why = f"{evn}: {why}"
                    if now - skip_log.get(key, 0) > 60:
                        skip_log[key] = now
                        tag = "leftover sit" if leftover_now else "skip"
                        log(f"LIVE {tag} {evn} {why} rest_allin={tw.get('rest_allin')}")
                    continue
                evn = event_prefix(tw["a"])
                # Leftover only: skip a 2-way whose shard has too little free
                # cash (Bears/Vikings on shard 0 with $0.50) so this tick can
                # try Gold/Parry on the tennis shard that still has leftover.
                if leftover_now:
                    cache = state.get("idx_cache") or {}
                    try:
                        dest = int(cache.get(tw["a"], guess_exchange_index(tw["a"])))
                    except (TypeError, ValueError):
                        dest = guess_exchange_index(tw["a"])
                    free_d = (state.get("free_cash") or {}).get(str(dest))
                    if free_d is None:
                        free_d = (state.get("shard_balances") or {}).get(str(dest))
                    try:
                        if free_d is not None and float(free_d) + 1e-12 < MIN_LEFTOVER_NOTIONAL:
                            if sit_why is None:
                                sit_why = (
                                    f"{evn}: dest shard free={float(free_d):.2f} "
                                    "below leftover min"
                                )
                            key = f"{evn}|dest-free"
                            now = time.monotonic()
                            if now - skip_log.get(key, 0) > 60:
                                skip_log[key] = now
                                log(
                                    f"LIVE leftover sit {evn} dest shard "
                                    f"free={float(free_d):.2f} below leftover min"
                                )
                            continue
                    except (TypeError, ValueError):
                        pass
                last = state.setdefault("live_attempt_ts", {})
                if time.monotonic() - last.get(evn, 0) < 40:
                    continue
                last[evn] = time.monotonic()
                ba = books.get(tw["a"])
                bb = books.get(tw["b"])
                if not ba or not bb:
                    continue
                state["live_busy"] = True
                state["live_last_try"] = {
                    "event": evn,
                    "rest_allin": tw.get("rest_allin"),
                    "rest_edge": tw.get("rest_edge"),
                }
                attempted = True

                async def _job(tw=tw, qa=ba.quote(), qb=bb.quote()):
                    try:
                        msg = await asyncio.to_thread(maybe_live_rest, k, tw, qa, qb, state)
                        # Always log outcome (cap/HARLLA was silent and hid real sits).
                        if msg:
                            log(f"LIVE {event_prefix(tw['a'])} {msg}")
                        state["live_last_result"] = msg
                    except Exception as e:
                        log(f"LIVE fail {safe_err(e)}")
                        state["live_last_result"] = f"fail {safe_err(e)}"
                    finally:
                        state["live_busy"] = False

                asyncio.create_task(_job())
                break
            if leftover_now and not attempted and sit_why and not at_cap:
                leftover_note(
                    state,
                    None,
                    state.get("leftover_size") or 0,
                    state.get("leftover_notional") or 0,
                    sit_why,
                )
        paired = {tw["a"] for tw in two_ways} | {tw["b"] for tw in two_ways}
        take_locks = []
        for tw in two_ways:
            if tw["rest_allin"] < 1.0:
                papers.append({**tw, "ticker": tw["a"] + "+" + tw["b"], "action": "pair_rest", "yes_quote": tw["yes_a"], "no_quote": tw["yes_b"], "all_in": tw["rest_allin"], "edge": tw["rest_edge"], "qty": tw["qty"], "fee": round((tw["rest_allin"] - tw["rest"]) * tw["qty"], 4)})
            if tw.get("take_allin") is not None and float(tw["take_allin"]) < 1.0 - 1e-12:
                take_locks.append({
                    **tw,
                    "ticker": tw["a"] + "+" + tw["b"],
                    "action": "take_lock",
                    "yes_quote": tw["ask_a"],
                    "no_quote": tw["ask_b"],
                    "all_in": tw["take_allin"],
                    "edge": tw["take_edge"],
                    "qty": tw["qty"],
                    "fee": round((tw["take_allin"] - tw["take"]) * tw["qty"], 4),
                })
                papers.append(take_locks[-1])
        # Keep recent take-lock peaks visible even if the flash already closed.
        if not take_locks and state.get("take_lock_best"):
            for evn, pk in sorted(
                state["take_lock_best"].items(),
                key=lambda kv: -float(kv[1].get("take_edge") or 0),
            )[:4]:
                take_locks.append({
                    "ticker": f"{pk['a']}+{pk['b']}",
                    "action": "take_lock_peak",
                    "a": pk["a"],
                    "b": pk["b"],
                    "ask_a": pk.get("ask_a"),
                    "ask_b": pk.get("ask_b"),
                    "take": pk.get("take"),
                    "take_allin": pk["take_allin"],
                    "take_edge": pk["take_edge"],
                    "rest_allin": pk.get("rest_allin"),
                    "all_in": pk["take_allin"],
                    "edge": pk["take_edge"],
                    "qty": CAP_C,
                    "peak": True,
                })
                papers.append(take_locks[-1])
        # If snapshot lull wiped papers, surface recent peaks so status is not a lie.
        if not papers and peaks:
            for evn, pk in sorted(peaks.items(), key=lambda kv: -float(kv[1].get("rest_edge") or 0))[:4]:
                papers.append({
                    "ticker": f"{pk['a']}+{pk['b']}",
                    "action": "pair_rest_peak",
                    "yes_quote": None,
                    "no_quote": None,
                    "all_in": pk["rest_allin"],
                    "edge": pk["rest_edge"],
                    "qty": CAP_C,
                    "fee": None,
                    "peak": True,
                    "a": pk["a"],
                    "b": pk["b"],
                    "rest": pk["rest"],
                    "rest_allin": pk["rest_allin"],
                    "rest_edge": pk["rest_edge"],
                    "take_allin": pk["take_allin"],
                })
        wt = [w["ticker"] if isinstance(w, dict) else w for w in watch]
        for t in wt:
            if t in paired or has_sibling(t, wt):
                continue
            b = books.get(t)
            if not b:
                continue
            q = b.quote()
            pq = paper_quote(q)
            if pq:
                papers.append({**q, **pq})
        quotes.sort(key=lambda x: (-(x.get("spread") or -1), x.get("ticker") or ""))
        sample = []
        for q in quotes[:3]:
            sample.append(
                {
                    "ticker": q["ticker"],
                    "yes_bid": q["yes_bid"],
                    "no_bid": q["no_bid"],
                    "yes_ask": q["yes_ask"],
                    "no_ask": q["no_ask"],
                    "spread": q["spread"],
                    "stale_s": q["stale_s"],
                }
            )
        write_status(
            {
                "alive": True,
                "connected": bool(state.get("connected")),
                "pid": os.getpid(),
                "cycle": n,
                "n_watch": len(watch),
                "n_books": sum(1 for b in books.values() if b.n_snap or b.n_delta),
                "n_paper": len(papers),
                "sample_books": sample,
                "papers": papers[:8],
                "top": quotes[:8],
                "watch": [w["ticker"] if isinstance(w, dict) else w for w in watch],
                "two_ways": two_ways[:8],
                "ws_error": state.get("ws_error"),
                "last_msg_et": state.get("last_msg_et"),
                "exchange_index": state.get("idx_cache") or {},
                "shard_balances": state.get("shard_balances") or {},
                "leftover_cash": state.get("leftover_cash") or (state.get("shard_balances") or {}),
                "leftover_cash_used": state.get("leftover_cash_used"),
                "leftover_size": int(state.get("leftover_size") or 0),
                "leftover_notional": state.get("leftover_notional"),
                "leftover_sit_reason": state.get("leftover_sit_reason"),
                "n_resting": int(state.get("n_resting") or 0),
                "resting_tickers": state.get("resting_tickers") or [],
                "live_pair_events": state.get("live_pair_events") or [],
                "n_live_pairs": int(state.get("n_live_pairs") or len(state.get("live_pair_events") or [])),
                "free_cash": state.get("free_cash") or {},
                "pair_cap": int(state.get("pair_cap") if state.get("pair_cap") is not None else MAX_LIVE_PAIRS),
                "n_open_pos": int(state.get("n_open_pos") or 0),
                "open_pos_tickers": state.get("open_pos_tickers") or [],
                "open_pos_events": state.get("open_pos_events") or [],
                "open_positions": state.get("open_positions") or [],
                "open_pos_locked": open_pos_is_locked(state.get("open_positions") or []),
                "oneleg": state.get("oneleg"),
                "oneleg_ban": sorted(state.get("oneleg_ban") or []),
                "naked_flatten": state.get("naked_flatten"),
                "max_live_pairs": MAX_LIVE_PAIRS,
                "max_stacked_pairs": MAX_STACKED_PAIRS,
                "edge_peaks": list((state.get("edge_peaks") or {}).values())[:6],
                "take_locks": take_locks[:6],
                "live_last_try": state.get("live_last_try"),
                "live_last_result": state.get("live_last_result"),
            }
        )
        await asyncio.sleep(0.5)


async def main_async():
    DIR.mkdir(parents=True, exist_ok=True)
    PIDFILE.write_text(str(os.getpid()))
    stop = {"n": False}

    def _stop(signum, frame):
        stop["n"] = True
        log(f"signal {signum}")

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    log(
        f"ws clipper start pid={os.getpid()} live_fire={LIVE_FIRE} "
        f"post_only=1 max_live_pairs={MAX_LIVE_PAIRS} "
        f"pair_cash_unit={PAIR_CASH_UNIT} leftover_allin<={LIVE_REST_ALLIN_MAX}"
    )
    k = Kalshi()
    # Survive 90s watch refresh / seq-gap reconnects. Rebuilding state used to
    # wipe take_lock_best + edge_peaks so hourly miss-pass never saw flashes
    # like PACAND take_allin 0.981 (+1.9c) that closed before the next status.
    persist = {
        "take_lock_best": {},
        "take_lock_log_ts": {},
        "edge_peaks": {},
        "leftover_rotate_ts": 0.0,
        "leftover_missing_since": {},
        "live_skip_log_ts": {},
        "live_attempt_ts": {},
        "tw_keys": {},
        "oneleg": None,
        "naked_flatten": None,
        "oneleg_action_ts": 0.0,
        "oneleg_seen_ts": {},
        "oneleg_log_ts": {},
        "oneleg_ban": list(load_oneleg_ban()),
    }

    while not stop["n"]:
        try:
            watch = pick_watch(k)
            tickers = [w["ticker"] for w in watch]
            if not tickers:
                write_status(
                    {
                        "alive": False,
                        "connected": False,
                        "error": "empty watchlist",
                        "pid": os.getpid(),
                    }
                )
                log("empty watchlist, retry")
                await asyncio.sleep(8)
                continue
            books = {t: Book(t) for t in tickers}
            state = {
                "connected": False,
                "paper_keys": {},
                "ws_error": None,
                # HARLLA settled. Do not seed fake inventory or resting.
                # Pin via KNOWN_LIVE / live_pin_tickers only. Never duplicate / cancel those oids.
                "resting": [],
                "n_resting": 0,
                "resting_tickers": [],
                "live_pair_events": [],
                "n_live_pairs": 0,
                "free_cash": {},
                "pair_cap": MAX_LIVE_PAIRS,
                "open_positions": [],
                "n_open_pos": 0,
                "open_pos_tickers": [],
                "open_pos_events": [],
                "idx_cache": {},
                "shard_balances": {},
                "leftover_cash": {},
                "leftover_size": 0,
                "leftover_notional": 0,
                "leftover_sit_reason": None,
                "oneleg": persist.get("oneleg"),
                "naked_flatten": persist.get("naked_flatten"),
                "oneleg_action_ts": float(persist.get("oneleg_action_ts") or 0.0),
                "oneleg_seen_ts": persist.setdefault("oneleg_seen_ts", {}),
                "oneleg_log_ts": persist.setdefault("oneleg_log_ts", {}),
                "oneleg_ban": set(persist.get("oneleg_ban") or []),
                "port_ts": 0.0,
                "live_busy": False,
                "take_lock_best": persist["take_lock_best"],
                "take_lock_log_ts": persist["take_lock_log_ts"],
                "edge_peaks": persist["edge_peaks"],
                "leftover_rotate_ts": persist["leftover_rotate_ts"],
                "leftover_missing_since": persist["leftover_missing_since"],
                "live_skip_log_ts": persist["live_skip_log_ts"],
                "live_attempt_ts": persist["live_attempt_ts"],
                "tw_keys": persist["tw_keys"],
            }
            ws_task = asyncio.create_task(run_ws(tickers, books, stop, state))
            st_task = asyncio.create_task(status_loop(books, watch, stop, state, k))
            try:
                await ws_task
            finally:
                st_task.cancel()
                try:
                    await st_task
                except asyncio.CancelledError:
                    pass
                # Scalars assigned on state don't share identity; copy back.
                persist["leftover_rotate_ts"] = float(state.get("leftover_rotate_ts") or 0.0)
                persist["oneleg"] = state.get("oneleg")
                persist["naked_flatten"] = state.get("naked_flatten")
                persist["oneleg_action_ts"] = float(state.get("oneleg_action_ts") or 0.0)
                persist["oneleg_seen_ts"] = dict(state.get("oneleg_seen_ts") or {})
                persist["oneleg_ban"] = list(state.get("oneleg_ban") or [])
                save_oneleg_ban(persist["oneleg_ban"])
        except SeqGap as e:
            log(f"{e}; reconnect")
            await asyncio.sleep(1.0)
        except Exception as e:
            log(f"ws loop error {safe_err(e)}")
            write_status(
                {
                    "alive": True,
                    "connected": False,
                    "pid": os.getpid(),
                    "error": safe_err(e),
                    "live_fire": LIVE_FIRE,
                }
            )
            await asyncio.sleep(2.0)

    PIDFILE.unlink(missing_ok=True)
    log("ws clipper stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
