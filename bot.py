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
from py_clob_client.clob_types import OrderArgs, OrderType
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
    )


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


def find_active_btc_15m_market() -> Tuple[str, str, Dict[str, Any]]:
    url = "https://gamma-api.polymarket.com/markets"
    params = {
        "closed": "false",
        "active": "true",
        "enableOrderBook": "true",
        "limit": 1000,
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    markets = r.json()
    if not isinstance(markets, list):
        raise RuntimeError("Gamma response is not a list")

    now = datetime.now(timezone.utc)
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

        end_s = m.get("endDateIso") or m.get("endDate")
        if not end_s:
            continue
        try:
            end_dt = datetime.fromisoformat(str(end_s).replace("Z", "+00:00"))
        except ValueError:
            continue
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)

        if end_dt <= now:
            continue

        outcomes = parse_json_list_field(m.get("outcomes"))
        token_ids = parse_json_list_field(m.get("clobTokenIds"))

        if len(outcomes) != 2 or len(token_ids) != 2:
            continue

        # Map YES/NO token positions
        # For up/down binary markets outcomes are typically ["Yes","No"]
        out0 = str(outcomes[0]).strip().lower()
        out1 = str(outcomes[1]).strip().lower()

        if out0 == "yes" and out1 == "no":
            yes_token = str(token_ids[0])
            no_token = str(token_ids[1])
        elif out0 == "no" and out1 == "yes":
            yes_token = str(token_ids[1])
            no_token = str(token_ids[0])
        else:
            continue

        seconds_to_end = (end_dt - now).total_seconds()
        if seconds_to_end <= 0:
            continue

        # Keep only short-horizon markets likely to be 15m contracts.
        # We allow 2..30 minutes to tolerate listing/rollover timing.
        if seconds_to_end < 120 or seconds_to_end > 1800:
            continue

        # Prefer explicit up/down naming if available, otherwise fallback to
        # nearest active BTC 15m yes/no market.
        priority = 0 if is_updown_like else 1
        candidates.append((priority, seconds_to_end, yes_token, no_token, m))

    if not candidates:
        raise RuntimeError("No active BTC 15m up/down market found in Gamma")

    candidates.sort(key=lambda x: (x[0], x[1]))
    _, _, yes_token, no_token, market = candidates[0]
    return yes_token, no_token, market


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

    if s.api_key and s.api_secret and s.api_passphrase:
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


@app.post("/webhook")
async def webhook(req: Request) -> Dict[str, Any]:
    s = load_settings()

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
        now = datetime.now(timezone.utc)
        signal_key = signal_key_for_payload(payload, now)

        can, reason = state.can_trade(s, signal_key, now)
        if not can:
            return {"ok": False, "skipped": True, "reason": reason}

        yes_token, no_token, market = choose_token_ids(s)
        token_id = yes_token if direction == "UP" else no_token

        effective_max_price = s.max_limit_price - s.fee_buffer
        if effective_max_price <= 0:
            raise HTTPException(status_code=400, detail="effective_max_price <= 0. Lower FEE_BUFFER")

        client = build_client(s)

        # Optional pre-check using current best ask. If no ask, we still try FOK limit.
        best_ask = None
        try:
            book = client.get_book(token_id)
            best_ask = extract_best_ask(book)
        except Exception as e:
            logger.warning("book pre-check failed: %s", e)

        if best_ask is not None and best_ask > effective_max_price:
            return {
                "ok": False,
                "skipped": True,
                "reason": f"best_ask {best_ask:.4f} > effective_max_price {effective_max_price:.4f}",
                "direction": direction,
                "market_slug": market.get("slug"),
                "token_id": token_id,
            }

        order_info = place_limit_buy(
            client=client,
            token_id=token_id,
            stake_usd=s.stake_usd,
            max_limit_price=s.max_limit_price,
            effective_max_price=effective_max_price,
            post_only=s.post_only,
        )

        state.mark_trade(signal_key, now)

        if s.post_only and s.order_ttl_seconds > 0:
            order_id = extract_order_id(order_info.get("response"))
            if order_id:
                schedule_cancel_if_open(s, order_id)
            else:
                logger.warning("post_only=true but order_id not found; TTL cancel skipped")

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

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("webhook failed")
        raise HTTPException(status_code=500, detail=str(e))
