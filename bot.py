import csv
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("poly-bot")


@dataclass
class Settings:
    webhook_secret: str
    stake_usd: float
    max_limit_price: float
    fee_buffer: float
    post_only: bool
    order_ttl_seconds: int
    one_trade_per_signal: bool
    max_trades_per_day: int
    cooldown_seconds: int

    market_mode: str
    yes_token_id: str
    no_token_id: str

    clob_host: str
    chain_id: int
    signature_type: int
    private_key: str
    funder: str
    api_key: str
    api_secret: str
    api_passphrase: str
    auto_derive_api_creds: bool
    trade_log_csv: str


class State:
    def __init__(self) -> None:
        self.last_trade_time: Optional[datetime] = None
        self.last_signal_key: Optional[str] = None
        self.trade_count_by_day: Dict[str, int] = {}

    def can_trade(self, s: Settings, signal_key: str, now: datetime) -> Tuple[bool, str]:
        day_key = now.strftime("%Y-%m-%d")
        count = self.trade_count_by_day.get(day_key, 0)

        if count >= s.max_trades_per_day:
            return False, f"max trades/day reached ({count})"

        if s.one_trade_per_signal and self.last_signal_key == signal_key:
            return False, "already traded this signal"

        if self.last_trade_time and s.cooldown_seconds > 0:
            delta = (now - self.last_trade_time).total_seconds()
            if delta < s.cooldown_seconds:
                return False, f"cooldown active ({delta:.0f}s/{s.cooldown_seconds}s)"

        return True, "ok"

    def mark_trade(self, signal_key: str, now: datetime) -> None:
        day_key = now.strftime("%Y-%m-%d")
        self.trade_count_by_day[day_key] = self.trade_count_by_day.get(day_key, 0) + 1
        self.last_trade_time = now
        self.last_signal_key = signal_key


state = State()
state_lock = threading.Lock()
trade_log_lock = threading.Lock()


def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "on"}


def load_settings() -> Settings:
    return Settings(
        webhook_secret=os.getenv("WEBHOOK_SECRET", "change-me"),
        stake_usd=float(os.getenv("STAKE_USD", "5")),
        max_limit_price=float(os.getenv("MAX_LIMIT_PRICE", "0.50")),
        fee_buffer=float(os.getenv("FEE_BUFFER", "0.002")),
        post_only=_env_bool("POST_ONLY", False),
        order_ttl_seconds=int(os.getenv("ORDER_TTL_SECONDS", "720")),
        one_trade_per_signal=_env_bool("ONE_TRADE_PER_SIGNAL", True),
        max_trades_per_day=int(os.getenv("MAX_TRADES_PER_DAY", "96")),
        cooldown_seconds=int(os.getenv("COOLDOWN_SECONDS", "0")),
        market_mode=os.getenv("MARKET_MODE", "AUTO_BTC_UPDOWN_15M"),
        yes_token_id=os.getenv("YES_TOKEN_ID", "AUTO"),
        no_token_id=os.getenv("NO_TOKEN_ID", "AUTO"),
        clob_host=os.getenv("CLOB_HOST", "https://clob.polymarket.com"),
        chain_id=int(os.getenv("CHAIN_ID", "137")),
        signature_type=int(os.getenv("SIGNATURE_TYPE", "0")),
        private_key=os.getenv("POLY_PRIVATE_KEY", ""),
        funder=os.getenv("POLY_FUNDER", ""),
        api_key=os.getenv("POLY_API_KEY", ""),
        api_secret=os.getenv("POLY_API_SECRET", ""),
        api_passphrase=os.getenv("POLY_API_PASSPHRASE", ""),
        auto_derive_api_creds=_env_bool("AUTO_DERIVE_API_CREDS", True),
        trade_log_csv=os.getenv("TRADE_LOG_CSV", "trades_log.csv"),
    )


TRADE_LOG_HEADERS = [
    "ts_utc",
    "status",
    "reason",
    "direction",
    "signal_key",
    "market_slug",
    "market_question",
    "token_id",
    "best_ask",
    "effective_max_price",
    "stake_usd",
    "order_type",
    "order_status",
    "order_id",
    "tx_hash",
    "payload_time",
    "payload_symbol",
    "payload_msg",
]


def append_trade_log(s: Settings, row: Dict[str, Any]) -> None:
    path = s.trade_log_csv
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    out: Dict[str, Any] = {k: row.get(k, "") for k in TRADE_LOG_HEADERS}
    with trade_log_lock:
        write_header = (not os.path.exists(path)) or os.path.getsize(path) == 0
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=TRADE_LOG_HEADERS)
            if write_header:
                w.writeheader()
            w.writerow(out)


def get_recent_trades_from_csv(path: str, limit: int) -> list:
    if not os.path.exists(path):
        return []
    rows: list = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows[-limit:]


def parse_payload(body: Any) -> Dict[str, Any]:
    if isinstance(body, dict):
        return body

    if isinstance(body, str):
        raw = body.strip()
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            direction = "UP" if "UP" in raw.upper() else "DOWN" if "DOWN" in raw.upper() else ""
            return {"direction": direction, "raw": raw}

    raise HTTPException(status_code=400, detail="Unsupported payload format")


def parse_direction(payload: Dict[str, Any]) -> str:
    direction = str(payload.get("direction", "")).upper().strip()
    if direction in {"UP", "DOWN"}:
        return direction

    for key in ("msg", "message", "raw"):
        v = str(payload.get(key, "")).upper()
        if "UP" in v:
            return "UP"
        if "DOWN" in v:
            return "DOWN"

    raise HTTPException(status_code=400, detail="Direction not found. Send direction=UP or DOWN")


def signal_key_for_payload(payload: Dict[str, Any], now: datetime) -> str:
    t = payload.get("time")
    direction = parse_direction(payload)
    if isinstance(t, str) and t:
        return f"{direction}:{t}"
    rounded = now.replace(second=0, microsecond=0)
    return f"{direction}:{rounded.isoformat()}"


def parse_json_list_field(v: Any) -> list:
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            x = json.loads(v)
            return x if isinstance(x, list) else []
        except json.JSONDecodeError:
            return []
    return []


def parse_dt_utc(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def now_utc_from_clob(host: str, chain_id: int) -> datetime:
    # Use exchange server time when available to reduce local clock drift.
    try:
        pub = ClobClient(host, chain_id=chain_id)
        ts = None
        if hasattr(pub, "get_server_time"):
            ts = pub.get_server_time()
        elif hasattr(pub, "getServerTime"):
            ts = pub.getServerTime()
        if ts is not None:
            return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception as e:
        logger.warning("server time fallback to local clock: %s", e)
    return datetime.now(timezone.utc)


def fetch_btc_15m_markets_from_events() -> list:
    events_url = "https://gamma-api.polymarket.com/events"
    markets_url = "https://gamma-api.polymarket.com/markets"

    # Pull open events with pagination, then resolve event slug -> market object.
    events: list = []
    offset = 0
    page_size = 500
    while True:
        r = requests.get(
            events_url,
            params={"closed": "false", "active": "true", "limit": page_size, "offset": offset},
            timeout=20,
        )
        r.raise_for_status()
        page = r.json()
        if not isinstance(page, list) or not page:
            break
        events.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    markets: list = []
    seen_ids = set()

    for ev in events:
        slug = str(ev.get("slug", "") or "")
        title = str(ev.get("title", "") or "")
        series_slug = str(ev.get("seriesSlug", "") or "")
        text = f"{slug} {title} {series_slug}".lower()

        is_btc = ("btc" in text) or ("bitcoin" in text)
        is_15m = ("15m" in text) or ("up-or-down-15m" in text) or ("updown-15m" in text)
        if not (is_btc and is_15m):
            continue

        # Some event payloads embed markets directly.
        embedded = ev.get("markets")
        if isinstance(embedded, list):
            for m in embedded:
                mid = str(m.get("id", ""))
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    markets.append(m)

        # Resolve slug to full market object with clobTokenIds.
        if slug:
            rm = requests.get(markets_url, params={"slug": slug}, timeout=20)
            rm.raise_for_status()
            data = rm.json()
            if isinstance(data, list):
                for m in data:
                    mid = str(m.get("id", ""))
                    if mid and mid not in seen_ids:
                        seen_ids.add(mid)
                        markets.append(m)

    return markets


def find_active_btc_15m_market() -> Tuple[str, str, Dict[str, Any]]:
    url = "https://gamma-api.polymarket.com/markets"

    markets = fetch_btc_15m_markets_from_events()
    if not markets:
        # Fallback: broader market scan in case events endpoint misses fresh listings.
        r = requests.get(
            url,
            params={"closed": "false", "enableOrderBook": "true", "limit": 2000},
            timeout=20,
        )
        r.raise_for_status()
        markets = r.json()

    if not isinstance(markets, list):
        raise RuntimeError("Gamma response is not a list")

    now = now_utc_from_clob("https://clob.polymarket.com", 137)
    candidates = []

    for m in markets:
        text = " ".join(
            [
                str(m.get("slug", "")),
                str(m.get("question", "")),
                str(m.get("groupItemTitle", "")),
            ]
        ).lower()

        if "btc" not in text and "bitcoin" not in text:
            continue
        # Some markets are not labeled explicitly as "15m up/down".
        # Accept BTC binary markets and infer 15m behavior from time-to-end.
        is_updown_like = ("up" in text and "down" in text) or ("higher" in text and "lower" in text)

        # Prefer eventStartTime when present; fallback to startDate.
        start_dt = parse_dt_utc(m.get("eventStartTime") or m.get("startDate"))
        end_dt = parse_dt_utc(m.get("endDate") or m.get("endDateIso"))
        if start_dt is None or end_dt is None:
            continue

        # Market must be live now.
        if start_dt > now or end_dt <= now:
            continue

        seconds_to_end = (end_dt - now).total_seconds()
        if seconds_to_end <= 0:
            continue

        outcomes = parse_json_list_field(m.get("outcomes"))
        token_ids = parse_json_list_field(m.get("clobTokenIds"))
        if len(outcomes) != 2 or len(token_ids) != 2:
            continue

        # Skip only explicit non-tradeable markets. Some payloads omit this field.
        if m.get("acceptingOrders") is False:
            continue

        # Target current/next 15m rotation window with tolerance.
        if not (30 <= seconds_to_end <= 30 * 60):
            continue

        # Prefer explicit up/down naming if available, otherwise fallback to
        # nearest active BTC 15m yes/no market.
        priority = 0 if is_updown_like else 1
        candidates.append((priority, seconds_to_end, token_ids, outcomes, m))

    if not candidates:
        raise RuntimeError("No active BTC 15m up/down market found in Gamma")

    candidates.sort(key=lambda x: (x[0], x[1]))
    _, _, token_ids, outcomes, market = candidates[0]

    out0 = str(outcomes[0]).strip().lower()
    out1 = str(outcomes[1]).strip().lower()
    if out0 in {"yes", "up"} and out1 in {"no", "down"}:
        up_token = str(token_ids[0])
        down_token = str(token_ids[1])
    elif out0 in {"no", "down"} and out1 in {"yes", "up"}:
        up_token = str(token_ids[1])
        down_token = str(token_ids[0])
    else:
        raise RuntimeError(f"Unexpected outcomes for BTC 15m market: {outcomes}")

    return up_token, down_token, market


def build_client(s: Settings) -> ClobClient:
    if not s.private_key:
        raise RuntimeError("Missing POLY_PRIVATE_KEY")

    kwargs: Dict[str, Any] = {
        "key": s.private_key,
        "chain_id": s.chain_id,
        "signature_type": s.signature_type,
    }
    if s.funder:
        kwargs["funder"] = s.funder

    client = ClobClient(s.clob_host, **kwargs)

    if (
        s.api_key
        and s.api_secret
        and s.api_passphrase
        and s.api_key != "AUTO"
        and s.api_secret != "AUTO"
        and s.api_passphrase != "AUTO"
    ):
        creds = {
            "key": s.api_key,
            "secret": s.api_secret,
            "passphrase": s.api_passphrase,
        }
        client.set_api_creds(creds)
    elif s.auto_derive_api_creds:
        client.set_api_creds(client.create_or_derive_api_creds())
    else:
        raise RuntimeError("Missing API creds and AUTO_DERIVE_API_CREDS=false")

    return client


def extract_best_ask(book: Any) -> Optional[float]:
    # dict shape
    if isinstance(book, dict):
        asks = book.get("asks", [])
        if asks:
            prices = []
            for a in asks:
                if isinstance(a, dict) and "price" in a:
                    try:
                        prices.append(float(a["price"]))
                    except (TypeError, ValueError):
                        pass
            if prices:
                return min(prices)

    # object shape
    asks = getattr(book, "asks", None)
    if asks:
        prices = []
        for a in asks:
            p = getattr(a, "price", None)
            if p is None and isinstance(a, dict):
                p = a.get("price")
            try:
                prices.append(float(p))
            except (TypeError, ValueError):
                pass
        if prices:
            return min(prices)

    return None


def choose_token_ids(s: Settings) -> Tuple[str, str, Dict[str, Any]]:
    if s.market_mode == "AUTO_BTC_UPDOWN_15M" or s.yes_token_id.upper() == "AUTO" or s.no_token_id.upper() == "AUTO":
        yes_token, no_token, m = find_active_btc_15m_market()
        return yes_token, no_token, m

    return s.yes_token_id, s.no_token_id, {"slug": "manual", "question": "manual token ids"}


def place_limit_buy(
    client: ClobClient,
    token_id: str,
    stake_usd: float,
    max_limit_price: float,
    effective_max_price: float,
    post_only: bool,
) -> Dict[str, Any]:
    # We use a limit order at effective_max_price.
    # Shares = dollar stake / price
    price = round(effective_max_price, 4)
    if price <= 0 or price >= 1:
        raise RuntimeError(f"Invalid limit price {price}")

    size = round(stake_usd / price, 4)
    if size <= 0:
        raise RuntimeError("Calculated size is zero")

    order = OrderArgs(token_id=token_id, price=price, size=size, side=BUY)
    signed = client.create_order(order)

    # FOK = immediate fill at this price or better, else no trade.
    # GTC = resting maker order (if post_only workflow desired).
    order_type = OrderType.GTC if post_only else OrderType.FOK
    resp = client.post_order(signed, order_type)

    return {
        "token_id": token_id,
        "price": price,
        "size": size,
        "stake_usd": stake_usd,
        "max_limit_price": max_limit_price,
        "order_type": "GTC" if post_only else "FOK",
        "response": resp,
    }


def place_fok_buy_usd(
    client: ClobClient,
    token_id: str,
    stake_usd: float,
    effective_max_price: float,
) -> Dict[str, Any]:
    # FOK BUY should be modeled as market-order args with amount in USD.
    # We try SDK market-order methods first; fallback to limit+FOK if unavailable.
    if hasattr(client, "create_market_order"):
        try:
            mo_args = MarketOrderArgs(
                token_id=token_id,
                amount=stake_usd,
                side=BUY,
                price=effective_max_price,
                order_type=OrderType.FOK,
            )
            mo = client.create_market_order(mo_args)
            resp = client.post_order(mo, OrderType.FOK)
            return {
                "token_id": token_id,
                "amount_usd": stake_usd,
                "worst_price": effective_max_price,
                "order_type": "FOK",
                "response": resp,
            }
        except Exception as e:
            logger.warning("create_market_order path failed, fallback to limit+FOK: %s", e)
    if hasattr(client, "createMarketOrder"):
        try:
            mo = client.createMarketOrder(
                {"tokenID": token_id, "amount": stake_usd, "side": BUY, "price": effective_max_price}
            )
            resp = client.post_order(mo, OrderType.FOK)
            return {
                "token_id": token_id,
                "amount_usd": stake_usd,
                "worst_price": effective_max_price,
                "order_type": "FOK",
                "response": resp,
            }
        except Exception as e:
            logger.warning("createMarketOrder path failed, fallback to limit+FOK: %s", e)

    # Compatibility fallback
    return place_limit_buy(
        client=client,
        token_id=token_id,
        stake_usd=stake_usd,
        max_limit_price=effective_max_price,
        effective_max_price=effective_max_price,
        post_only=False,
    )


def extract_order_id(resp: Any) -> Optional[str]:
    candidates = []
    if isinstance(resp, dict):
        candidates.extend(
            [
                resp.get("orderID"),
                resp.get("orderId"),
                resp.get("id"),
            ]
        )
        order_obj = resp.get("order")
        if isinstance(order_obj, dict):
            candidates.extend(
                [
                    order_obj.get("orderID"),
                    order_obj.get("orderId"),
                    order_obj.get("id"),
                ]
            )

    for c in candidates:
        if c is not None:
            s = str(c).strip()
            if s:
                return s
    return None


def cancel_order_best_effort(client: ClobClient, order_id: str) -> Any:
    # API method names differ across py-clob-client versions.
    if hasattr(client, "cancel"):
        return client.cancel(order_id)
    if hasattr(client, "cancel_order"):
        return client.cancel_order(order_id)
    if hasattr(client, "cancel_orders"):
        return client.cancel_orders([order_id])
    raise RuntimeError("No cancel method available in client version")


def schedule_cancel_if_open(s: Settings, order_id: str) -> None:
    def _worker() -> None:
        try:
            time.sleep(max(1, s.order_ttl_seconds))
            client = build_client(s)
            resp = cancel_order_best_effort(client, order_id)
            logger.info("TTL cancel attempted for order_id=%s resp=%s", order_id, resp)
        except Exception as e:
            logger.warning("TTL cancel failed for order_id=%s err=%s", order_id, e)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


app = FastAPI(title="Polymarket BTC 15m Auto Trader")


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/trades/recent")
def trades_recent(limit: int = 20) -> Dict[str, Any]:
    s = load_settings()
    n = max(1, min(limit, 200))
    return {"ok": True, "count": n, "rows": get_recent_trades_from_csv(s.trade_log_csv, n)}


@app.post("/webhook")
async def webhook(req: Request) -> Dict[str, Any]:
    s = load_settings()
    now = datetime.now(timezone.utc)
    direction = ""
    signal_key = ""
    payload: Dict[str, Any] = {}
    market: Dict[str, Any] = {}
    token_id = ""
    best_ask = None
    effective_max_price = s.max_limit_price - s.fee_buffer

    try:
        content_type = (req.headers.get("content-type") or "").lower()
        if "application/json" in content_type:
            body = await req.json()
        else:
            raw = (await req.body()).decode("utf-8", errors="ignore")
            body = raw

        payload = parse_payload(body)

        if payload.get("secret") != s.webhook_secret:
            raise HTTPException(status_code=401, detail="Invalid secret")

        direction = parse_direction(payload)
        signal_key = signal_key_for_payload(payload, now)

        with state_lock:
            can, reason = state.can_trade(s, signal_key, now)
            if not can:
                append_trade_log(
                    s,
                    {
                        "ts_utc": now.isoformat(),
                        "status": "skipped",
                        "reason": reason,
                        "direction": direction,
                        "signal_key": signal_key,
                        "stake_usd": s.stake_usd,
                        "payload_time": payload.get("time", ""),
                        "payload_symbol": payload.get("symbol", ""),
                        "payload_msg": payload.get("msg", ""),
                    },
                )
                return {"ok": False, "skipped": True, "reason": reason}

        yes_token, no_token, market = choose_token_ids(s)
        token_id = yes_token if direction == "UP" else no_token

        if effective_max_price <= 0:
            raise HTTPException(status_code=400, detail="effective_max_price <= 0. Lower FEE_BUFFER")

        client = build_client(s)

        # Optional pre-check using current best ask. If no ask, we still try FOK limit.
        try:
            if hasattr(client, "get_order_book"):
                book = client.get_order_book(token_id)
                best_ask = extract_best_ask(book)
        except Exception as e:
            logger.warning("book pre-check failed: %s", e)

        if best_ask is not None and best_ask > effective_max_price:
            append_trade_log(
                s,
                {
                    "ts_utc": now.isoformat(),
                    "status": "skipped",
                    "reason": f"best_ask {best_ask:.4f} > effective_max_price {effective_max_price:.4f}",
                    "direction": direction,
                    "signal_key": signal_key,
                    "market_slug": market.get("slug", ""),
                    "market_question": market.get("question", ""),
                    "token_id": token_id,
                    "best_ask": best_ask,
                    "effective_max_price": round(effective_max_price, 4),
                    "stake_usd": s.stake_usd,
                    "payload_time": payload.get("time", ""),
                    "payload_symbol": payload.get("symbol", ""),
                    "payload_msg": payload.get("msg", ""),
                },
            )
            return {
                "ok": False,
                "skipped": True,
                "reason": f"best_ask {best_ask:.4f} > effective_max_price {effective_max_price:.4f}",
                "direction": direction,
                "market_slug": market.get("slug"),
                "token_id": token_id,
            }

        if s.post_only:
            order_info = place_limit_buy(
                client=client,
                token_id=token_id,
                stake_usd=s.stake_usd,
                max_limit_price=s.max_limit_price,
                effective_max_price=effective_max_price,
                post_only=True,
            )
        else:
            order_info = place_fok_buy_usd(
                client=client,
                token_id=token_id,
                stake_usd=s.stake_usd,
                effective_max_price=effective_max_price,
            )

        with state_lock:
            state.mark_trade(signal_key, now)

        if s.post_only and s.order_ttl_seconds > 0:
            order_id = extract_order_id(order_info.get("response"))
            if order_id:
                schedule_cancel_if_open(s, order_id)
            else:
                logger.warning("post_only=true but order_id not found; TTL cancel skipped")

        resp_obj = order_info.get("response")
        order_status = ""
        tx_hash = ""
        order_id = ""
        if isinstance(resp_obj, dict):
            order_status = str(resp_obj.get("status", ""))
            order_id = str(resp_obj.get("orderID") or resp_obj.get("orderId") or "")
            txs = resp_obj.get("transactionsHashes")
            if isinstance(txs, list) and txs:
                tx_hash = str(txs[0])

        append_trade_log(
            s,
            {
                "ts_utc": now.isoformat(),
                "status": "ok",
                "reason": "",
                "direction": direction,
                "signal_key": signal_key,
                "market_slug": market.get("slug", ""),
                "market_question": market.get("question", ""),
                "token_id": token_id,
                "best_ask": best_ask,
                "effective_max_price": round(effective_max_price, 4),
                "stake_usd": s.stake_usd,
                "order_type": order_info.get("order_type", ""),
                "order_status": order_status,
                "order_id": order_id,
                "tx_hash": tx_hash,
                "payload_time": payload.get("time", ""),
                "payload_symbol": payload.get("symbol", ""),
                "payload_msg": payload.get("msg", ""),
            },
        )

        return {
            "ok": True,
            "direction": direction,
            "market_slug": market.get("slug"),
            "market_question": market.get("question"),
            "token_id": token_id,
            "best_ask": best_ask,
            "effective_max_price": round(effective_max_price, 4),
            "order": order_info,
        }

    except HTTPException as e:
        append_trade_log(
            s,
            {
                "ts_utc": now.isoformat(),
                "status": "error",
                "reason": str(e.detail),
                "direction": direction,
                "signal_key": signal_key,
                "market_slug": market.get("slug", ""),
                "token_id": token_id,
                "best_ask": best_ask,
                "effective_max_price": round(effective_max_price, 4),
                "stake_usd": s.stake_usd,
                "payload_time": payload.get("time", ""),
                "payload_symbol": payload.get("symbol", ""),
                "payload_msg": payload.get("msg", ""),
            },
        )
        raise
    except Exception as e:
        logger.exception("webhook failed")
        append_trade_log(
            s,
            {
                "ts_utc": now.isoformat(),
                "status": "error",
                "reason": str(e),
                "direction": direction,
                "signal_key": signal_key,
                "market_slug": market.get("slug", ""),
                "token_id": token_id,
                "best_ask": best_ask,
                "effective_max_price": round(effective_max_price, 4),
                "stake_usd": s.stake_usd,
                "payload_time": payload.get("time", ""),
                "payload_symbol": payload.get("symbol", ""),
                "payload_msg": payload.get("msg", ""),
            },
        )
        raise HTTPException(status_code=500, detail=str(e))
