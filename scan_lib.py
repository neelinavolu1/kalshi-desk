#!/usr/bin/env python3
"""Kalshi REST helpers + paper lock scan. Never print keys or PEM."""
from __future__ import annotations

import base64
import json
import math
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

BASE = "https://external-api.kalshi.com/trade-api/v2"
KEY_ID_PATH = Path("/home/box/.config/kalshi/key_id")
KEY_PATH = Path("/home/box/.config/kalshi/main.key")
CAP_C = 30
M_DEFAULT = 1.0
NEAR_MISS_WINDOW = 0.08  # report near-misses within 8c of lock after fees

# series/ticker skip
SKIP_SUBSTR = (
    "GDPYEAR", "KXGDPYEAR", "CPIYEAR", "UNRATEYEAR", "PAYROLLSYEAR",
    "5M", "5MIN", "15M", "KXBTCD", "KXETHD", "KXBTC15", "KXETH15",
    "KXBTC5", "KXETH5", "INX5", "INX15", "KXNBA5", "CRYPTO5",
)
# 5-min / intra-hour crypto-like series prefixes
SKIP_SERIES_PREFIX = (
    "KXBTC", "KXETH", "KXSOL", "KXDOGE", "KXCRYPTO", "KXINX", "KXDJI",
    "KXNASDAQ", "KXXRP",
)

# prefer sports
SPORTS_HINTS = (
    "NFL", "NBA", "MLB", "NHL", "WNBA", "MLS", "EPL", "UCL", "SOCCER",
    "FOOTBALL", "TENNIS", "GOLF", "UFC", "MMA", "F1", "NASCAR", "CFB",
    "NCAAF", "NCAAB", "LALIGA", "SERIE", "BUNDES", "LIGUE", "WC",
    "AFCON", "ATP", "WTA", "PGA", "NHL", "MLB", "FIFA", "UEFA",
    "CRICKET", "RUGBY", "BOXING", "WWE", "FUTURES", "SPREAD", "TOTAL",
    "MONEYLINE", "GAME", "MATCH", "CFL", "KBO", "NPB", "WNBA",
    "LA LIGA", "PREMIER", "CHAMPIONS", "LIGUE 1", "SERIE A",
)

ET = timezone(timedelta(hours=-4))  # America/New_York late Aug = EDT


def ceil_cent(x: float) -> float:
    return math.ceil(x * 100.0 - 1e-12) / 100.0


def taker_fee(c: int, p: float, m: float = M_DEFAULT) -> float:
    """Official: round_up(M * 0.07 * C * P * (1-P)) to next cent."""
    if c <= 0 or p <= 0 or p >= 1:
        return 0.0
    return ceil_cent(m * 0.07 * c * p * (1.0 - p))


def load_auth():
    key_id = KEY_ID_PATH.read_text().strip()
    pem = KEY_PATH.read_bytes()
    pkey = serialization.load_pem_private_key(pem, password=None)
    return key_id, pkey


class UnfundedShard(RuntimeError):
    """HTTP 404 {code: user_not_found} on a shard write/balance.

    Kalshi is sharded. POST /portfolio/events/orders to a shard with $0 cash
    returns user_not_found — unfunded/unprovisioned shard, NOT a bad key.
    GET /portfolio/balance on shard 0 can succeed while shard-3 writes 404.
    """


class Kalshi:
    def __init__(self):
        self.key_id, self.pkey = load_auth()
        self.s = requests.Session()
        self.s.headers.update({
            "Accept": "application/json",
            "User-Agent": "desk-paper-scan/1.0",
        })
        self.last = 0.0
        self.min_interval = 0.12
        self.n_req = 0
        self.n_429 = 0

    def _sign(self, method: str, path: str) -> dict:
        ts = str(int(time.time() * 1000))
        msg = f"{ts}{method}{path}".encode("utf-8")
        sig = self.pkey.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        }

    def _safe_body(self, r: requests.Response, n: int = 240) -> str:
        body = (r.text or "")[:n]
        for needle in ("BEGIN ", "PRIVATE", "KEY", "KALSHI-ACCESS", "-----", "ssh-rsa"):
            if needle in body:
                return "<redacted>"
        return body.replace("\n", " ")

    def _is_unfunded(self, r: requests.Response) -> bool:
        if r.status_code != 404:
            return False
        try:
            code = str((r.json() or {}).get("code") or "").lower()
        except Exception:
            code = (r.text or "").lower()
        return "user_not_found" in code

    def _raise_http(self, path: str, r: requests.Response):
        if r.status_code == 401:
            raise RuntimeError(f"401 on {path} (session/auth failed; keys not printed)")
        if self._is_unfunded(r):
            raise UnfundedShard(f"HTTP 404 user_not_found on {path} (unfunded/unprovisioned shard, not a bad key)")
        raise RuntimeError(f"HTTP {r.status_code} on {path}: {self._safe_body(r)}")

    def get(self, path: str, params=None, signed=True, retries=6):
        # path like /portfolio/balance  (we prepend /trade-api/v2 for signing)
        api_path = "/trade-api/v2" + path
        url = BASE + path
        backoff = 1.5
        last_err = None
        for attempt in range(retries):
            elapsed = time.monotonic() - self.last
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            headers = self._sign("GET", api_path) if signed else {}
            self.n_req += 1
            try:
                r = self.s.get(url, params=params, headers=headers, timeout=30)
            except requests.RequestException as e:
                last_err = e
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue
            self.last = time.monotonic()
            if r.status_code == 429:
                self.n_429 += 1
                ra = r.headers.get("Retry-After")
                try:
                    wait = float(ra) if ra else backoff
                except ValueError:
                    wait = backoff
                time.sleep(min(max(wait, 1.0), 30))
                backoff = min(backoff * 1.7, 20)
                continue
            if r.status_code >= 400:
                self._raise_http(path, r)
            if not r.content:
                return {}
            try:
                return r.json()
            except Exception:
                return {"_raw": self._safe_body(r, 120)}
        raise RuntimeError(f"exhausted retries on {path}: {last_err}")

    def post(self, path: str, body=None, retries=6):
        """V2 POST. Sign timestamp+POST+/trade-api/v2+path. Never print keys on 4xx."""
        api_path = "/trade-api/v2" + path
        url = BASE + path
        backoff = 1.5
        last_err = None
        for attempt in range(retries):
            elapsed = time.monotonic() - self.last
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            headers = self._sign("POST", api_path)
            headers["Content-Type"] = "application/json"
            self.n_req += 1
            try:
                r = self.s.post(url, json=body if body is not None else {}, headers=headers, timeout=30)
            except requests.RequestException as e:
                last_err = e
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue
            self.last = time.monotonic()
            if r.status_code == 429:
                self.n_429 += 1
                ra = r.headers.get("Retry-After")
                try:
                    wait = float(ra) if ra else backoff
                except ValueError:
                    wait = backoff
                time.sleep(min(max(wait, 1.0), 30))
                backoff = min(backoff * 1.7, 20)
                continue
            if r.status_code in (200, 201, 202, 204):
                if not r.content:
                    return {}
                try:
                    return r.json()
                except Exception:
                    return {"ok": True}
            if r.status_code >= 500:
                last_err = RuntimeError(self._safe_body(r))
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue
            self._raise_http(path, r)
        raise RuntimeError(f"exhausted retries POST {path}: {last_err}")

    def delete(self, path: str, params=None, retries=4):
        """V2 DELETE (rollback a one-leg rest). Never print keys.

        Query params (exchange_index, market_ticker, subaccount) are NOT in
        the signed path, same as GET. Omit exchange_index and Kalshi defaults
        to shard 0, which 404s tennis/MLB orders on shard 3.
        """
        api_path = "/trade-api/v2" + path
        url = BASE + path
        backoff = 1.5
        last_err = None
        for attempt in range(retries):
            elapsed = time.monotonic() - self.last
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            headers = self._sign("DELETE", api_path)
            self.n_req += 1
            try:
                r = self.s.delete(url, params=params, headers=headers, timeout=30)
            except requests.RequestException as e:
                last_err = e
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue
            self.last = time.monotonic()
            if r.status_code == 429:
                self.n_429 += 1
                time.sleep(min(backoff, 30))
                backoff = min(backoff * 1.7, 20)
                continue
            if r.status_code in (200, 201, 202, 204):
                if not r.content:
                    return {}
                try:
                    return r.json()
                except Exception:
                    return {"ok": True}
            if r.status_code >= 500:
                last_err = RuntimeError(self._safe_body(r))
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)
                continue
            self._raise_http(path, r)
        raise RuntimeError(f"exhausted retries DELETE {path}: {last_err}")

    def paginate(self, path, list_key, params=None, max_pages=80, max_items=20000):
        params = dict(params or {})
        cursor = None
        pages = 0
        n = 0
        out = []
        while pages < max_pages and n < max_items:
            q = dict(params)
            if cursor:
                q["cursor"] = cursor
            data = self.get(path, q, signed=True)
            items = data.get(list_key) or []
            out.extend(items)
            n += len(items)
            pages += 1
            cursor = data.get("cursor") or None
            if not cursor or not items:
                break
        return out



def guess_exchange_index(ticker: str) -> int:
    """Fallback shard map as of 2026-08-24. Authoritative: GET /markets/{ticker}."""
    tu = (ticker or "").upper()
    if tu.startswith("KXMVE") or "CROSSCATEGORY" in tu or "MULTIGAME" in tu or "COMBO" in tu:
        return 1
    if tu.startswith(("KXBTC", "KXETH", "KXSOL", "KXDOGE", "KXCRYPTO", "KXXRP", "KXINX")):
        return 2
    # tennis + baseball
    if tu.startswith(("KXATP", "KXWTA", "KXMLB", "KXNPB", "KXKBO")):
        return 3
    return 0


def parse_balance_dollars(data: dict | None) -> float:
    if not data:
        return 0.0
    bd = data.get("balance_dollars")
    if bd is not None and str(bd) != "":
        try:
            return float(bd)
        except (TypeError, ValueError):
            pass
    b = data.get("balance")
    if isinstance(b, str) and b.strip():
        try:
            return float(b)
        except (TypeError, ValueError):
            return 0.0
    if isinstance(b, (int, float)):
        v = float(b)
        # GET /portfolio/balance.balance is cents
        return v / 100.0
    return 0.0


def shard_balance_dollars(k: Kalshi, idx: int) -> float:
    try:
        data = k.get("/portfolio/balance", params={"exchange_index": int(idx), "subaccount": 0})
    except UnfundedShard:
        return 0.0
    except RuntimeError as e:
        if "404" in str(e) or "user_not_found" in str(e).lower():
            return 0.0
        raise
    return parse_balance_dollars(data)


def market_exchange_index(k: Kalshi, ticker: str, cache: dict | None = None) -> int:
    if cache is not None and ticker in cache:
        return cache[ticker]
    idx = guess_exchange_index(ticker)
    try:
        data = k.get(f"/markets/{ticker}")
        m = data.get("market") or data
        raw = m.get("exchange_index")
        if raw is not None and str(raw) != "":
            idx = int(raw)
    except Exception:
        pass
    if cache is not None:
        cache[ticker] = idx
    return idx


def fund_shard(k: Kalshi, dest_idx: int, dollars: float, *, source_idx: int = 0, ticker: str = "", log=None) -> float:
    """Ensure dest shard has at least `dollars`. Transfer a slice from shard 0; leftover stays on 0.

    amount is centicents ($1=10000, $12=120000). source=destination=event_contract, subaccounts 0.
    """
    dest_idx = int(dest_idx)
    source_idx = int(source_idx)
    need = float(dollars)
    dest_cash = shard_balance_dollars(k, dest_idx)
    if dest_cash + 1e-9 >= need:
        return dest_cash
    if dest_idx == source_idx:
        return dest_cash
    src_cash = shard_balance_dollars(k, source_idx)
    short = max(0.0, need - dest_cash)
    # typical clip fund is $12; never dump the whole bankroll
    xfer = max(short, 0.01)
    keep = 0.50
    max_xfer = max(0.0, src_cash - keep)
    if max_xfer < 0.01:
        raise RuntimeError(
            f"shard {source_idx} cash=${src_cash:.2f} cannot fund idx={dest_idx} need=${need:.2f}"
        )
    xfer = min(xfer, max_xfer)
    amount = int(round(xfer * 10000))  # centicents
    if amount < 1:
        return dest_cash
    if log:
        log(f"SHARD miss: {ticker or '—'} idx={dest_idx} cash={dest_cash:g} → transfer ${xfer:g}")
    body = {
        "source": "event_contract",
        "destination": "event_contract",
        "amount": amount,
        "source_exchange_shard": source_idx,
        "destination_exchange_shard": dest_idx,
        "source_subaccount": 0,
        "destination_subaccount": 0,
    }
    k.post("/portfolio/intra_exchange_instance_transfer", body)
    deadline = time.monotonic() + 12.0
    while time.monotonic() < deadline:
        time.sleep(0.45)
        dest_cash = shard_balance_dollars(k, dest_idx)
        if dest_cash + 1e-9 >= need * 0.9:
            return dest_cash
    dest_cash = shard_balance_dollars(k, dest_idx)
    if dest_cash + 1e-9 < need * 0.5:
        raise RuntimeError(
            f"fund_shard idx={dest_idx} still ${dest_cash:.2f} after ${xfer:.2f} transfer"
        )
    return dest_cash


def to_dollars(x):
    """Kalshi prices may be int cents, str dollars, or float dollars."""
    if x is None:
        return None
    if isinstance(x, str):
        x = x.strip()
        if not x:
            return None
        v = float(x)
        return v if v <= 1.5 else v / 100.0
    if isinstance(x, (int, float)):
        v = float(x)
        # 0-1.5 treated as dollars; 2-99 as cents (Kalshi yes_ask is 1-99)
        if v <= 1.5:
            return v
        return v / 100.0
    return None


def parse_ob_level(level):
    """Return (price_dollars, qty). Level is [price, qty] or {price, count}."""
    if isinstance(level, (list, tuple)) and len(level) >= 2:
        px, qty = level[0], level[1]
    elif isinstance(level, dict):
        px = level.get("price") or level.get("price_dollars")
        qty = level.get("quantity") or level.get("count") or level.get("size")
    else:
        return None, None
    p = to_dollars(px)
    try:
        q = int(float(qty))
    except (TypeError, ValueError):
        q = None
    return p, q


def best_bid_and_depth(levels):
    """Highest bid + qty at that price. levels may be unsorted."""
    best_p, best_q = None, 0
    parsed = []
    for lv in levels or []:
        p, q = parse_ob_level(lv)
        if p is None or q is None or q <= 0:
            continue
        parsed.append((p, q))
        if best_p is None or p > best_p:
            best_p, best_q = p, q
        elif p == best_p:
            best_q += q
    return best_p, best_q, parsed


def implied_asks_from_book(ob: dict):
    """
    Kalshi book: yes = YES bids, no = NO bids (cents or dollars).
    YES ask = 1 - best NO bid; NO ask = 1 - best YES bid.
    Depth for YES ask is qty at best NO bid (and vice versa).
    Also honor explicit yes_ask arrays if present.
    """
    book = ob.get("orderbook") or ob
    yes_lv = book.get("yes") or book.get("yes_dollars") or []
    no_lv = book.get("no") or book.get("no_dollars") or []
    # some payloads nest orderbook_fp
    fp = ob.get("orderbook_fp") or book.get("orderbook_fp") or {}
    if not yes_lv and fp:
        yes_lv = fp.get("yes_dollars") or fp.get("yes") or []
        no_lv = fp.get("no_dollars") or fp.get("no") or []

    yb, yb_q, yes_parsed = best_bid_and_depth(yes_lv)
    nb, nb_q, no_parsed = best_bid_and_depth(no_lv)

    yes_ask = round(1.0 - nb, 4) if nb is not None else None
    no_ask = round(1.0 - yb, 4) if yb is not None else None
    yes_ask_depth = nb_q if nb is not None else 0
    no_ask_depth = yb_q if yb is not None else 0

    return {
        "yes_bid": yb,
        "no_bid": nb,
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "yes_ask_depth": yes_ask_depth,
        "no_ask_depth": no_ask_depth,
        "yes_bid_depth": yb_q,
        "no_bid_depth": nb_q,
        "n_yes_lv": len(yes_parsed),
        "n_no_lv": len(no_parsed),
    }


def parse_ts(s):
    if not s:
        return None
    try:
        if isinstance(s, (int, float)):
            return datetime.fromtimestamp(float(s), tz=timezone.utc)
        s = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def skip_market(m: dict) -> str | None:
    t = (m.get("ticker") or "").upper()
    series = (m.get("series_ticker") or "").upper()
    title = ((m.get("title") or "") + " " + (m.get("subtitle") or "") + " " + (m.get("yes_sub_title") or "")).upper()
    blob = t + " " + series + " " + title
    for s in SKIP_SUBSTR:
        if s in blob:
            return f"skip_substr:{s}"
    # 5-min crypto / index
    if any(x in blob for x in ("5 MIN", "5-MIN", "5MINUTE", "EVERY 5")):
        return "skip_5min"
    # year-long economics fields
    if any(x in blob for x in ("GDP YEAR", "ANNUAL GDP", "YEAR-END", "CALENDAR YEAR")) and any(
        x in blob for x in ("GDP", "CPI", "UNEMPLOY", "PAYROLL", "INFLATION")
    ):
        return "skip_year_econ"
    if t.startswith("KXGDPYEAR") or series.startswith("KXGDPYEAR"):
        return "skip_gdpyear"
    # crypto 5/15m series
    for pref in SKIP_SERIES_PREFIX:
        if series.startswith(pref) or t.startswith(pref):
            # keep longer-dated BTC year? user said skip 5-min crypto specifically.
            # skip short-dated crypto entirely for this desk (5m/15m/hourly).
            if any(x in t for x in ("5M", "15M", "1H", "1HR", "60M")) or any(
                x in blob for x in ("5 MIN", "15 MIN", "HOURLY", "1 HOUR")
            ):
                return "skip_crypto_short"
    return None


def is_sportsy(m: dict) -> bool:
    blob = " ".join(
        str(m.get(k) or "")
        for k in (
            "ticker", "series_ticker", "title", "subtitle", "yes_sub_title",
            "event_ticker", "category", "sports_league",
        )
    ).upper()
    return any(h in blob for h in SPORTS_HINTS)


def close_dt(m: dict):
    for k in ("close_time", "expected_expiration_time", "expiration_time", "latest_expiration_time"):
        dt = parse_ts(m.get(k))
        if dt:
            return dt
    return None


def depth_ok(c: int, px_sum: float) -> bool:
    if c <= 0:
        return False
    notional = c * px_sum
    return c >= 10 or notional >= 10.0


def two_leg_metrics(yes_ask, no_ask, yes_d, no_d, m=M_DEFAULT):
    if yes_ask is None or no_ask is None:
        return None
    if yes_ask <= 0 or no_ask <= 0 or yes_ask >= 1 or no_ask >= 1:
        return None
    c = min(int(yes_d or 0), int(no_d or 0), CAP_C)
    if c <= 0:
        return None
    fy = taker_fee(c, yes_ask, m)
    fn = taker_fee(c, no_ask, m)
    fee = fy + fn
    cost = yes_ask + no_ask
    unit_fee = fee / c
    all_in = cost + unit_fee
    edge = 1.0 - all_in  # per contract
    return {
        "legs": 2,
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "cost": cost,
        "fee": fee,
        "unit_fee": unit_fee,
        "all_in": all_in,
        "edge": edge,
        "c": c,
        "notional": c * cost,
        "depth_ok": depth_ok(c, cost),
    }


def combo_metrics(asks_depths, m=M_DEFAULT):
    """asks_depths: list of (ticker, yes_ask, depth)."""
    if not asks_depths or len(asks_depths) < 2:
        return None
    if any(a is None or a <= 0 or a >= 1 for _, a, _ in asks_depths):
        return None
    c = min([d for _, _, d in asks_depths] + [CAP_C])
    if c <= 0:
        return None
    cost = sum(a for _, a, _ in asks_depths)
    fee = sum(taker_fee(c, a, m) for _, a, _ in asks_depths)
    unit_fee = fee / c
    all_in = cost + unit_fee
    edge = 1.0 - all_in
    return {
        "legs": len(asks_depths),
        "asks": [(t, a, d) for t, a, d in asks_depths],
        "cost": cost,
        "fee": fee,
        "unit_fee": unit_fee,
        "all_in": all_in,
        "edge": edge,
        "c": c,
        "notional": c * cost,
        "depth_ok": depth_ok(c, cost),
    }


def fmt_px(p):
    if p is None:
        return "—"
    return f"{p:.2f}"


def fmt_et(dt):
    if not dt:
        return "?"
    return dt.astimezone(ET).strftime("%Y-%m-%d %H:%M ET")


def main():
    k = Kalshi()
    now = datetime.now(timezone.utc)
    today_et = now.astimezone(ET).date()
    tomorrow_et = today_et + timedelta(days=1)
    horizon = now + timedelta(days=2)  # same-day / next-day window with slack

    # --- cash / positions ---
    bal = k.get("/portfolio/balance")
    pos_raw = k.get("/portfolio/positions", params={"limit": 200, "count_filter": "position"})
    # try pagination if needed
    positions = pos_raw.get("market_positions") or pos_raw.get("positions") or []
    event_positions = pos_raw.get("event_positions") or []
    cursor = pos_raw.get("cursor")
    pages = 0
    while cursor and pages < 10:
        more = k.get("/portfolio/positions", params={"limit": 200, "cursor": cursor, "count_filter": "position"})
        positions.extend(more.get("market_positions") or more.get("positions") or [])
        event_positions.extend(more.get("event_positions") or [])
        cursor = more.get("cursor")
        pages += 1

    cash = bal.get("balance")
    # balance often in cents
    if isinstance(cash, int) and cash > 1000:
        cash_d = cash / 100.0
    else:
        try:
            cash_d = float(cash) / 100.0 if float(cash) > 50 else float(cash)
        except Exception:
            cash_d = cash
    # portfolio_value / payout etc
    extras = {kk: bal.get(kk) for kk in ("portfolio_value", "payout", "updated_ts") if kk in bal}

    open_pos = []
    for p in positions:
        # position in contracts; 0 skip
        rp = p.get("position") or p.get("position_fp") or 0
        try:
            rp_n = float(str(rp).replace(",", ""))
        except Exception:
            rp_n = 0
        if abs(rp_n) < 1e-9:
            continue
        open_pos.append(p)

    pos_bits = []
    for p in open_pos[:12]:
        t = p.get("ticker") or p.get("market_ticker")
        rp = p.get("position")
        pos_bits.append(f"{t}:{rp}")
    pos_s = ",".join(pos_bits) if pos_bits else "FLAT"
    print(
        f"CASH ${cash_d:.2f} | positions={len(open_pos)} {pos_s} | "
        f"bal_keys={sorted(bal.keys())} | ts={now.astimezone(ET).strftime('%Y-%m-%d %H:%M:%S ET')}"
    )
    sys.stdout.flush()

    # --- markets ---
    print("Fetching open markets…", flush=True)
    markets = k.paginate("/markets", "markets", params={"status": "open", "limit": 1000}, max_pages=40)
    print(f"open markets: {len(markets)} reqs={k.n_req} 429s={k.n_429}", flush=True)

    # schema peek
    if markets:
        sample = markets[0]
        print("sample keys:", sorted(sample.keys())[:40], flush=True)
        peek = {kk: sample.get(kk) for kk in (
            "ticker", "event_ticker", "series_ticker", "yes_ask", "no_ask",
            "yes_bid", "no_bid", "yes_ask_dollars", "no_ask_dollars",
            "close_time", "status", "category", "title", "volume",
            "yes_ask_size", "no_ask_size", "last_price",
        ) if kk in sample}
        print("sample peek:", json.dumps(peek, default=str)[:800], flush=True)

    # series fee multipliers cache
    series_m = {}

    def fee_m(series_ticker):
        if not series_ticker:
            return M_DEFAULT
        if series_ticker in series_m:
            return series_m[series_ticker]
        try:
            data = k.get(f"/series/{series_ticker}")
            ser = data.get("series") or data
            mv = ser.get("fee_multiplier") or ser.get("fee_rate_multiplier") or ser.get("taker_fee_multiplier")
            if mv is None:
                # some series nest fee_structure
                fs = ser.get("fee_structure") or {}
                mv = fs.get("multiplier") or fs.get("taker_multiplier")
            series_m[series_ticker] = float(mv) if mv is not None else M_DEFAULT
        except Exception:
            series_m[series_ticker] = M_DEFAULT
        return series_m[series_ticker]

    # screen
    screened = []
    skipped = defaultdict(int)
    by_event = defaultdict(list)
    for m in markets:
        reason = skip_market(m)
        if reason:
            skipped[reason] += 1
            continue
        t = m.get("ticker")
        if not t:
            continue
        ya = to_dollars(m.get("yes_ask_dollars") if m.get("yes_ask_dollars") is not None else m.get("yes_ask"))
        na = to_dollars(m.get("no_ask_dollars") if m.get("no_ask_dollars") is not None else m.get("no_ask"))
        m["_ya"] = ya
        m["_na"] = na
        m["_close"] = close_dt(m)
        m["_sports"] = is_sportsy(m)
        screened.append(m)
        et = m.get("event_ticker")
        if et:
            by_event[et].append(m)

    print(f"screened={len(screened)} skipped={dict(skipped)} events={len(by_event)}", flush=True)

    # Prefer sports closing today/tomorrow; still scan other short-dated binaries
    def recency_rank(m):
        cl = m.get("_close")
        sports = 0 if m.get("_sports") else 1
        if cl is None:
            return (sports, 2, 9e9)
        d = cl.astimezone(ET).date()
        if d == today_et:
            day = 0
        elif d == tomorrow_et:
            day = 1
        elif cl <= now + timedelta(days=3):
            day = 2
        else:
            day = 3
        return (sports, day, (cl - now).total_seconds())

    # --- 2-leg candidates from list prices (tight first) ---
    two_cand = []
    for m in screened:
        ya, na = m["_ya"], m["_na"]
        if ya is None or na is None:
            continue
        cost = ya + na
        # only fetch books if potentially interesting
        if cost <= 1.0 + NEAR_MISS_WINDOW + 0.04:
            two_cand.append(m)
    two_cand.sort(key=lambda m: (m["_ya"] or 9) + (m["_na"] or 9))
    # bias sports same/next day to front but keep all tight
    two_cand.sort(key=lambda m: (recency_rank(m)[0], recency_rank(m)[1], (m["_ya"] or 9) + (m["_na"] or 9)))

    print(f"2-leg list-price cands (cost<=1.12): {len(two_cand)}", flush=True)

    # 3-leg soccer / mutually exclusive events (2-3 markets)
    combo_events = []
    for et, ms in by_event.items():
        if not (2 <= len(ms) <= 3):
            continue
        # skip if not sportsy and not clearly partition
        if not any(x.get("_sports") for x in ms):
            # still allow if titles look like mutually exclusive outcomes (win/draw)
            blob = " ".join((x.get("yes_sub_title") or x.get("title") or "") for x in ms).upper()
            if not any(w in blob for w in ("DRAW", "TIE", "HOME", "AWAY", "WIN", "1X2")):
                continue
        combo_events.append((et, ms))
    print(f"2-3 market events: {len(combo_events)}", flush=True)

    # Fetch orderbooks for 2-leg cands (cap to keep live/fresh)
    MAX_OB = 180
    # take the tightest 120 plus sports same/next day extras
    fetch_tickers = []
    seen_t = set()
    for m in two_cand:
        t = m["ticker"]
        if t in seen_t:
            continue
        seen_t.add(t)
        fetch_tickers.append(t)
        if len(fetch_tickers) >= MAX_OB:
            break
    # add combo legs
    for et, ms in combo_events:
        for m in ms:
            t = m["ticker"]
            if t not in seen_t:
                seen_t.add(t)
                fetch_tickers.append(t)

    print(f"Fetching {len(fetch_tickers)} orderbooks…", flush=True)
    books = {}
    fails = 0
    t0 = time.monotonic()
    for i, t in enumerate(fetch_tickers):
        try:
            ob = k.get(f"/markets/{t}/orderbook", params={"depth": 20})
            books[t] = implied_asks_from_book(ob)
        except Exception as e:
            fails += 1
            if fails <= 5:
                print(f"  ob fail {t}: {e}", flush=True)
        if (i + 1) % 40 == 0:
            print(f"  books {i+1}/{len(fetch_tickers)} fails={fails} reqs={k.n_req}", flush=True)

    print(f"books ok={len(books)} fails={fails} elapsed={time.monotonic()-t0:.1f}s", flush=True)
    # dump one book structure sample
    if fetch_tickers:
        t0t = fetch_tickers[0]
        try:
            raw = k.get(f"/markets/{t0t}/orderbook", params={"depth": 5})
            # redact nothing needed; no keys. Truncate.
            print("raw ob keys:", list(raw.keys()), flush=True)
            inner = raw.get("orderbook") or raw
            print("inner keys:", list(inner.keys()) if isinstance(inner, dict) else type(inner), flush=True)
            for side in ("yes", "no", "yes_dollars", "no_dollars"):
                if isinstance(inner, dict) and inner.get(side) is not None:
                    print(f"  {side} sample:", inner.get(side)[:3], flush=True)
            print("parsed:", books.get(t0t), flush=True)
        except Exception as e:
            print("raw ob peek fail", e, flush=True)

    # Evaluate 2-leg from live books
    locks = []
    near = []
    mkt_by_t = {m["ticker"]: m for m in screened}

    for t, b in books.items():
        m = mkt_by_t.get(t)
        if not m:
            continue
        # skip combo-only evaluation here; 2-leg is always valid on a single binary
        mm = fee_m(m.get("series_ticker"))
        met = two_leg_metrics(b["yes_ask"], b["no_ask"], b["yes_ask_depth"], b["no_ask_depth"], mm)
        if not met:
            continue
        rec = {
            "kind": "YES+NO",
            "ticker": t,
            "event": m.get("event_ticker"),
            "title": (m.get("title") or "")[:80],
            "yes_sub": m.get("yes_sub_title") or m.get("subtitle") or "",
            "close": m.get("_close"),
            "sports": m.get("_sports"),
            "M": mm,
            **met,
            "yes_ask_d": b["yes_ask_depth"],
            "no_ask_d": b["no_ask_depth"],
        }
        if met["edge"] > 0 and met["depth_ok"]:
            locks.append(rec)
        else:
            near.append(rec)

    # Evaluate 2-3 leg combos
    for et, ms in combo_events:
        asks = []
        ok = True
        for m in ms:
            b = books.get(m["ticker"])
            if not b or b["yes_ask"] is None:
                ok = False
                break
            asks.append((m["ticker"], b["yes_ask"], b["yes_ask_depth"]))
        if not ok or len(asks) < 2 or len(asks) > 3:
            continue
        mm = fee_m(ms[0].get("series_ticker"))
        met = combo_metrics(asks, mm)
        if not met:
            continue
        titles = " | ".join(
            f"{(m.get('yes_sub_title') or m.get('ticker'))}@{books[m['ticker']]['yes_ask']:.2f}x{books[m['ticker']]['yes_ask_depth']}"
            for m in ms if m["ticker"] in books
        )
        rec = {
            "kind": f"{met['legs']}-LEG",
            "ticker": et,
            "event": et,
            "title": (ms[0].get("title") or et)[:80],
            "yes_sub": titles,
            "close": ms[0].get("_close"),
            "sports": any(x.get("_sports") for x in ms),
            "M": mm,
            **met,
        }
        if met["edge"] > 0 and met["depth_ok"] and met["legs"] <= 3:
            locks.append(rec)
        else:
            near.append(rec)

    locks.sort(key=lambda r: (-r["edge"], 0 if r.get("sports") else 1))
    # near-miss: closest to lock (highest edge, even if slightly negative), with a book
    near.sort(key=lambda r: -r["edge"])

    print("\n===== RESULTS =====")
    print(
        f"CASH ${cash_d:.2f} | positions={len(open_pos)} {pos_s} | "
        f"scan {len(markets)} open / {len(books)} books / {k.n_req} reqs / {k.n_429} x429"
    )
    if open_pos:
        for p in open_pos:
            print("  POS", {kk: p.get(kk) for kk in ("ticker", "position", "market_exposure", "realized_pnl", "unrealized_pnl", "resting_orders_count") if kk in p})

    def line(r):
        cl = fmt_et(r.get("close"))
        sport = "SPORT" if r.get("sports") else "other"
        if r["kind"] == "YES+NO":
            return (
                f"{r['kind']} {r['ticker']}  YES {fmt_px(r['yes_ask'])} x{r.get('yes_ask_d')}  "
                f"NO {fmt_px(r['no_ask'])} x{r.get('no_ask_d')}  "
                f"cost={r['cost']:.3f} fee={r['fee']:.2f} (C={r['c']} M={r['M']}) "
                f"all_in={r['all_in']:.4f} edge={r['edge']*100:+.2f}c  "
                f"notional=${r['notional']:.2f}  close={cl} {sport}  {r.get('title','')[:60]}"
            )
        asks_s = " + ".join(f"{t}@{a:.2f}x{d}" for t, a, d in r.get("asks") or [])
        return (
            f"{r['kind']} {r['ticker']}  {asks_s}  "
            f"cost={r['cost']:.3f} fee={r['fee']:.2f} (C={r['c']} M={r['M']}) "
            f"all_in={r['all_in']:.4f} edge={r['edge']*100:+.2f}c  "
            f"notional=${r['notional']:.2f}  close={cl} {sport}  {r.get('title','')[:60]}"
        )

    print(f"LOCKS: {len(locks)}")
    for r in locks[:5]:
        print("LOCK", line(r))
    if not locks:
        print("SIT — no same-venue lock after fees with depth.")
        # tightest near-miss among sports same/next day first, else global
        def nm_key(r):
            cl = r.get("close")
            sports = 0 if r.get("sports") else 1
            day = 9
            if cl:
                d = cl.astimezone(ET).date()
                if d <= tomorrow_et:
                    day = 0
                elif cl < now + timedelta(days=3):
                    day = 1
            return (sports, day, -r["edge"])
        near_f = [r for r in near if r["c"] > 0]
        near_f.sort(key=nm_key)
        if near_f:
            best = near_f[0]
            # also show the mathematically tightest regardless of sports
            tightest = max(near_f, key=lambda r: r["edge"])
            print("NEAR(sports/date)", line(best))
            if tightest is not best:
                print("NEAR(tightest)", line(tightest))
            # extra top 4 tightest
            print("TOP NEAR:")
            for r in sorted(near_f, key=lambda r: -r["edge"])[:8]:
                print(" ", line(r))

    # dump json for parent
    out = {
        "cash": cash_d,
        "n_positions": len(open_pos),
        "positions": pos_bits,
        "n_markets": len(markets),
        "n_books": len(books),
        "n_locks": len(locks),
        "locks": locks[:5],
        "near": sorted(near, key=lambda r: -r["edge"])[:8],
        "skipped": dict(skipped),
        "series_m_sample": dict(list(series_m.items())[:15]),
        "ts_et": now.astimezone(ET).strftime("%Y-%m-%d %H:%M:%S ET"),
        "reqs": k.n_req,
        "x429": k.n_429,
        "balance_raw_keys": sorted(bal.keys()),
        "cash_raw": cash,
    }
    Path("/tmp/kalshi_lock_scan_out.json").write_text(json.dumps(out, default=str, indent=2))
    print("wrote /tmp/kalshi_lock_scan_out.json", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
