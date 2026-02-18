# Polymarket BTC 15m Auto Trader (TradingView Webhook)

Automates YES/NO limit buys for BTC up/down 15m markets from TradingView alerts.

## What it does
- Receives TradingView webhook with prediction `UP` or `DOWN`.
- Resolves active BTC 15m market in Gamma (`gamma-api.polymarket.com`).
- Chooses YES token for `UP`, NO token for `DOWN`.
- Places limit buy with stake `$5` (configurable).
- Enforces policy: effective price must be `<= 0.50`.
- Uses `post_only` and risk limits.

## Install
```bash
cd /Users/trabajo/Documents/Poly/polymarket_bot
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Configure `.env`
Set:
- `WEBHOOK_SECRET`
- `POLY_PRIVATE_KEY`
- `POLY_FUNDER` (if your setup needs it)
- `POLY_API_KEY`, `POLY_API_SECRET`, `POLY_API_PASSPHRASE` (or enable derive)

Default trading policy already matches your request:
- stake `$5`
- buy only if effective price `<= 0.50`
- `FOK` limit (immediate execution or skip)

## Run
```bash
uvicorn bot:app --host 0.0.0.0 --port 8000
```

## TradingView alert
- Condition: `Any alert() function call`
- Webhook URL: `https://YOUR_PUBLIC_URL/webhook`
- Message JSON:
```json
{
  "secret": "change-me",
  "direction": "UP",
  "symbol": "BTCUSDT",
  "time": "{{time}}",
  "close": "{{close}}"
}
```
For DOWN signal send `"direction":"DOWN"`.

## Safety notes
- Test in paper/small size first.
- Geolocation/regulatory restrictions may apply.
- Make sure token allowances and auth are configured for your wallet type.
