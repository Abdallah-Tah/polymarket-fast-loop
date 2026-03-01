#!/usr/bin/env python3
"""
Simmer FastLoop Trading Skill

Trades Polymarket BTC 5-minute fast markets using CEX price momentum.
Default signal: Binance BTCUSDT candles. Agents can customize signal source.

Usage:
    python fast_trader.py              # Dry run (show opportunities, no trades)
    python fast_trader.py --live       # Execute real trades
    python fast_trader.py --positions  # Show current fast market positions
    python fast_trader.py --quiet      # Only output on trades/errors

Requires:
    SIMMER_API_KEY environment variable (get from simmer.markets/dashboard)
"""

import os
import sys
import json
import math
import argparse
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, quote

from paper_broker import broker_from_env

# Force line-buffered stdout for non-TTY environments (cron, Docker, OpenClaw)
sys.stdout.reconfigure(line_buffering=True)

# Optional: Trade Journal integration
try:
    from tradejournal import log_trade
    JOURNAL_AVAILABLE = True
except ImportError:
    try:
        from skills.tradejournal import log_trade
        JOURNAL_AVAILABLE = True
    except ImportError:
        JOURNAL_AVAILABLE = False
        def log_trade(*args, **kwargs):
            pass

# =============================================================================
# Configuration (config.json > env vars > defaults)
# =============================================================================

CONFIG_SCHEMA = {
    "entry_threshold": {"default": 0.05, "env": "SIMMER_SPRINT_ENTRY", "type": float,
                        "help": "Min price divergence from 50¢ to trigger trade"},
    "min_momentum_pct": {"default": 0.5, "env": "SIMMER_SPRINT_MOMENTUM", "type": float,
                         "help": "Min BTC % move in lookback window to trigger"},
    "max_position": {"default": 5.0, "env": "SIMMER_SPRINT_MAX_POSITION", "type": float,
                     "help": "Max $ per trade"},
    "signal_source": {"default": "coinbase", "env": "SIMMER_SPRINT_SIGNAL", "type": str,
                      "help": "Price feed source (coinbase|binance)"},
    "lookback_minutes": {"default": 5, "env": "SIMMER_SPRINT_LOOKBACK", "type": int,
                         "help": "Minutes of price history for momentum calc"},
    "min_time_remaining": {"default": 0, "env": "SIMMER_SPRINT_MIN_TIME", "type": int,
                           "help": "Skip fast_markets with less than this many seconds remaining (0 = auto: 10%% of window)"},
    "asset": {"default": "BTC", "env": "SIMMER_SPRINT_ASSET", "type": str,
              "help": "Asset to trade (BTC, ETH, SOL)"},
    "window": {"default": "5m", "env": "SIMMER_SPRINT_WINDOW", "type": str,
               "help": "Market window duration (5m or 15m)"},
    "volume_confidence": {"default": True, "env": "SIMMER_SPRINT_VOL_CONF", "type": bool,
                          "help": "Weight signal by volume (higher volume = more confident)"},
    "daily_budget": {"default": 10.0, "env": "SIMMER_SPRINT_DAILY_BUDGET", "type": float,
                     "help": "Max total spend per UTC day"},
}

TRADE_SOURCE = "sdk:fastloop"
SKILL_SLUG = "polymarket-fast-loop"
_automaton_reported = False
SMART_SIZING_PCT = 0.05  # 5% of balance per trade
MIN_SHARES_PER_ORDER = 5  # Polymarket minimum
MAX_SPREAD_PCT = 0.10     # Skip if CLOB bid-ask spread exceeds this

# Asset → Binance symbol mapping
ASSET_SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
}

# Asset → Gamma API search patterns
ASSET_PATTERNS = {
    "BTC": ["bitcoin up or down"],
    "ETH": ["ethereum up or down"],
    "SOL": ["solana up or down"],
}


from simmer_sdk.skill import load_config, update_config, get_config_path

# Load config
cfg = load_config(CONFIG_SCHEMA, __file__, slug="polymarket-fast-loop")
ENTRY_THRESHOLD = cfg["entry_threshold"]
MIN_MOMENTUM_PCT = cfg["min_momentum_pct"]
MAX_POSITION_USD = cfg["max_position"]
_automaton_max = os.environ.get("AUTOMATON_MAX_BET")
if _automaton_max:
    MAX_POSITION_USD = min(MAX_POSITION_USD, float(_automaton_max))
SIGNAL_SOURCE = cfg["signal_source"]
LOOKBACK_MINUTES = cfg["lookback_minutes"]
ASSET = cfg["asset"].upper()
WINDOW = cfg["window"]  # "5m" or "15m"

# Multi-scan (paper-only)
FASTLOOP_ASSETS = os.environ.get("FASTLOOP_ASSETS", "BTC,ETH,SOL").split(",")
FASTLOOP_WINDOWS = os.environ.get("FASTLOOP_WINDOWS", "5m,15m").split(",")

# Dynamic min_time_remaining: 0 = auto (10% of window duration)
_window_seconds = {"5m": 300, "15m": 900, "1h": 3600}
_configured_min_time = cfg["min_time_remaining"]
if _configured_min_time > 0:
    MIN_TIME_REMAINING = _configured_min_time
else:
    MIN_TIME_REMAINING = max(30, _window_seconds.get(WINDOW, 300) // 10)
VOLUME_CONFIDENCE = cfg["volume_confidence"]
DAILY_BUDGET = cfg["daily_budget"]


# =============================================================================
# Daily Budget Tracking
# =============================================================================

def _get_spend_path(skill_file):
    from pathlib import Path
    return Path(skill_file).parent / "daily_spend.json"


def _load_daily_spend(skill_file):
    """Load today's spend. Resets if date != today (UTC)."""
    spend_path = _get_spend_path(skill_file)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if spend_path.exists():
        try:
            with open(spend_path) as f:
                data = json.load(f)
            if data.get("date") == today:
                return data
        except (json.JSONDecodeError, IOError):
            pass
    return {"date": today, "spent": 0.0, "trades": 0}


def _save_daily_spend(skill_file, spend_data):
    """Save daily spend to file."""
    spend_path = _get_spend_path(skill_file)
    with open(spend_path, "w") as f:
        json.dump(spend_data, f, indent=2)


# =============================================================================
# API Helpers
# =============================================================================

_client = None

def get_client(live=True):
    """Lazy-init SimmerClient singleton."""
    global _client
    if _client is None:
        try:
            from simmer_sdk import SimmerClient
        except ImportError:
            print("Error: simmer-sdk not installed. Run: pip install simmer-sdk")
            sys.exit(1)
        api_key = os.environ.get("SIMMER_API_KEY")
        if not api_key:
            print("Error: SIMMER_API_KEY environment variable not set")
            print("Get your API key from: simmer.markets/dashboard → SDK tab")
            sys.exit(1)
        venue = os.environ.get("TRADING_VENUE", "polymarket")
        _client = SimmerClient(api_key=api_key, venue=venue, live=live)
    return _client


def _api_request(url, method="GET", data=None, headers=None, timeout=15):
    """Make an HTTP request to external APIs (Binance, CoinGecko, Gamma). Returns parsed JSON or None on error."""
    try:
        req_headers = headers or {}
        if "User-Agent" not in req_headers:
            req_headers["User-Agent"] = "simmer-fastloop_market/1.0"
        body = None
        if data:
            body = json.dumps(data).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        req = Request(url, data=body, headers=req_headers, method=method)
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        try:
            error_body = json.loads(e.read().decode("utf-8"))
            return {"error": error_body.get("detail", str(e)), "status_code": e.code}
        except Exception:
            return {"error": str(e), "status_code": e.code}
    except URLError as e:
        return {"error": f"Connection error: {e.reason}"}
    except Exception as e:
        return {"error": str(e)}


CLOB_API = "https://clob.polymarket.com"


def fetch_live_midpoint(token_id):
    """Fetch live midpoint price from Polymarket CLOB for a single token."""
    result = _api_request(f"{CLOB_API}/midpoint?token_id={quote(str(token_id))}", timeout=5)
    if not result or not isinstance(result, dict) or result.get("error"):
        return None
    try:
        return float(result["mid"])
    except (KeyError, ValueError, TypeError):
        return None


def _parse_outcome_prices(market: dict):
    raw = market.get("outcome_prices") or market.get("outcomePrices") or "[]"
    try:
        if isinstance(raw, str):
            return [float(x) for x in json.loads(raw)]
        if isinstance(raw, list):
            return [float(x) for x in raw]
    except Exception:
        pass
    return []


def select_yes_token_id(market: dict):
    """Best-effort mapping of Gamma clobTokenIds -> YES token id.

    Gamma often returns clobTokenIds as [tokenA, tokenB] but order isn't guaranteed.
    We'll choose the token whose midpoint is closest to the market's YES price.

    Returns: (yes_token_id, no_token_id) or (None, None)
    """
    clob = market.get("clob_token_ids") or market.get("clobTokenIds") or []
    if isinstance(clob, str):
        try:
            clob = json.loads(clob)
        except Exception:
            clob = []
    if not clob or len(clob) < 2:
        return None, None

    t0, t1 = clob[0], clob[1]

    # Approx YES price from Gamma snapshot (fallback)
    prices = _parse_outcome_prices(market)
    gamma_yes = prices[0] if prices else None

    m0 = fetch_live_midpoint(t0)
    m1 = fetch_live_midpoint(t1)

    # If we can't fetch midpoints, fall back to original ordering.
    if m0 is None or m1 is None or gamma_yes is None:
        return t0, t1

    d0 = abs(m0 - gamma_yes)
    d1 = abs(m1 - gamma_yes)
    if d0 <= d1:
        return t0, t1
    return t1, t0


def fetch_live_prices(market_or_clob):
    """Fetch live YES midpoint from Polymarket CLOB.

    Accepts either:
      - market dict (preferred)
      - clob token id list (legacy)
    """
    if isinstance(market_or_clob, dict):
        yes_token, _ = select_yes_token_id(market_or_clob)
        if not yes_token:
            return None
        return fetch_live_midpoint(yes_token)

    clob_token_ids = market_or_clob
    if not clob_token_ids or len(clob_token_ids) < 1:
        return None
    return fetch_live_midpoint(clob_token_ids[0])


def fetch_orderbook(token_id):
    """Fetch Polymarket CLOB orderbook for a token id."""
    result = _api_request(f"{CLOB_API}/book?token_id={quote(str(token_id))}", timeout=5)
    if not result or not isinstance(result, dict):
        return None
    bids = result.get("bids", [])
    asks = result.get("asks", [])
    if not bids or not asks:
        return None
    return {"bids": bids, "asks": asks}


def fetch_orderbook_summary(market_or_clob):
    """Fetch order book for YES token and return spread + depth summary.

    Accepts either market dict (preferred) or clob token list.

    Args:
        clob_token_ids: List of [yes_token_id, no_token_id] from Gamma.

    Returns:
        dict with spread_pct, best_bid, best_ask, bid_depth_usd, ask_depth_usd
        or None on failure.
    """
    if isinstance(market_or_clob, dict):
        yes_token, _ = select_yes_token_id(market_or_clob)
        if not yes_token:
            return None
        clob_token_ids = market_or_clob.get("clob_token_ids") or market_or_clob.get("clobTokenIds") or []
    else:
        clob_token_ids = market_or_clob
        if not clob_token_ids or len(clob_token_ids) < 1:
            return None
        yes_token = clob_token_ids[0]

    book = fetch_orderbook(yes_token)
    if not book:
        return None

    bids = book.get("bids", [])
    asks = book.get("asks", [])
    if not bids or not asks:
        return None

    try:
        best_bid = float(bids[0]["price"])
        best_ask = float(asks[0]["price"])
        spread = best_ask - best_bid
        mid = (best_ask + best_bid) / 2
        spread_pct = spread / mid if mid > 0 else 0

        # Sum depth (top 5 levels)
        bid_depth = sum(float(b.get("size", 0)) * float(b.get("price", 0)) for b in bids[:5])
        ask_depth = sum(float(a.get("size", 0)) * float(a.get("price", 0)) for a in asks[:5])

        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread_pct": spread_pct,
            "bid_depth_usd": bid_depth,
            "ask_depth_usd": ask_depth,
        }
    except (KeyError, ValueError, IndexError, TypeError):
        return None


# =============================================================================
# Sprint Market Discovery
# =============================================================================

def _parse_iso_dt(dt: str):
    """Parse ISO8601 datetime like 2025-10-31T00:00:00Z -> aware UTC datetime."""
    if not dt or not isinstance(dt, str):
        return None
    try:
        # Gamma uses Z suffix
        if dt.endswith("Z"):
            dt = dt.replace("Z", "+00:00")
        return datetime.fromisoformat(dt).astimezone(timezone.utc)
    except Exception:
        return None


def discover_fast_market_markets(asset="BTC", window="5m"):
    """Find active fast markets on Polymarket via Gamma API."""
    patterns = ASSET_PATTERNS.get(asset, ASSET_PATTERNS["BTC"])
    # Gamma pagination: we page forward by endDate (ascending) until we reach "around now".
    # We filter to a tight time band around now to avoid stale/old fast markets.
    now = datetime.now(timezone.utc)
    band_past = now - timedelta(days=1)
    band_future = now + timedelta(days=1)

    limit = 200
    max_pages = 40  # up to 8k records

    markets = []
    for page in range(max_pages):
        offset = page * limit
        url = (
            "https://gamma-api.polymarket.com/markets"
            f"?limit={limit}&offset={offset}&closed=false&tag=crypto&order=endDate&ascending=true"
        )
        result = _api_request(url)
        if not result or (isinstance(result, dict) and result.get("error")):
            break

        # Determine page time range (best-effort)
        page_end_dates = [
            _parse_iso_dt(m.get("endDate") or m.get("end_date"))
            for m in result
        ]
        page_end_dates = [d for d in page_end_dates if d]
        if page_end_dates:
            page_min = min(page_end_dates)
            page_max = max(page_end_dates)
            # If we've paged beyond our future band, we're done.
            if page_min > band_future:
                break
            # If this entire page is still before our past band, keep paging.
            if page_max < band_past:
                continue

        for m in result:
            q = (m.get("question") or "").lower()
            slug = m.get("slug", "")
            matches_window = f"-{window}-" in slug
            if any(p in q for p in patterns) and matches_window:
                condition_id = m.get("conditionId", "")
                closed = m.get("closed", False)
                if not closed and slug:
                    # Prefer Gamma endDate when present; it's authoritative.
                    end_time = _parse_iso_dt(m.get("endDate") or m.get("end_date"))
                    if not end_time:
                        # Fallback: parse from question (best effort)
                        end_time = _parse_fast_market_end_time(m.get("question", ""))

                    # Only keep markets in our time band around now.
                    if end_time and (end_time < band_past or end_time > band_future):
                        continue

                    # Capture CLOB token IDs for live price fetching
                    clob_tokens_raw = m.get("clobTokenIds", "[]")
                    if isinstance(clob_tokens_raw, str):
                        try:
                            clob_tokens = json.loads(clob_tokens_raw)
                        except (json.JSONDecodeError, ValueError):
                            clob_tokens = []
                    else:
                        clob_tokens = clob_tokens_raw or []

                    markets.append({
                        "question": m.get("question", ""),
                        "slug": slug,
                        "condition_id": condition_id,
                        "end_time": end_time,
                        "outcomes": m.get("outcomes", []),
                        "outcome_prices": m.get("outcomePrices", "[]"),
                        "clob_token_ids": clob_tokens,
                        "fee_rate_bps": int(m.get("fee_rate_bps") or m.get("feeRateBps") or 0),
                    })
    return markets


def _parse_fast_market_end_time(question):
    """Parse end time from fast market question.
    e.g., 'Bitcoin Up or Down - February 15, 5:30AM-5:35AM ET' → datetime
    """
    import re
    # Match pattern: "Month Day, StartTime-EndTime ET"
    pattern = r'(\w+ \d+),.*?-\s*(\d{1,2}:\d{2}(?:AM|PM))\s*ET'
    match = re.search(pattern, question)
    if not match:
        return None
    try:
        from zoneinfo import ZoneInfo
        date_str = match.group(1)
        time_str = match.group(2)
        year = datetime.now(timezone.utc).year
        dt_str = f"{date_str} {year} {time_str}"
        dt = datetime.strptime(dt_str, "%B %d %Y %I:%M%p")
        # Proper ET → UTC (handles EST/EDT automatically)
        et = ZoneInfo("America/New_York")
        dt = dt.replace(tzinfo=et).astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def find_best_fast_market(markets):
    """Pick the best fast market to trade.

    Selection goals:
      1) market is live now (ends soon, but not too soon)
      2) liquidity is reasonable (tight spread)
      3) depth isn't tiny

    We prefer the best (lowest spread) market among live candidates.
    """
    now = datetime.now(timezone.utc)
    max_remaining = _window_seconds.get(WINDOW, 300) * 2

    scored = []
    for m in markets:
        end_time = m.get("end_time")
        if not end_time:
            continue
        remaining = (end_time - now).total_seconds()
        if remaining <= 0:
            continue
        if not (remaining > MIN_TIME_REMAINING and remaining < max_remaining):
            continue

        clob_tokens = m.get("clob_token_ids") or []
        book = fetch_orderbook_summary(m) if clob_tokens else None
        if not book:
            continue

        spread = float(book.get("spread_pct") or 999)
        bid_depth = float(book.get("bid_depth_usd") or 0)
        ask_depth = float(book.get("ask_depth_usd") or 0)
        depth = min(bid_depth, ask_depth)

        # Skip obviously broken books (or ultra-wide)
        if spread <= 0 or spread > 5:
            continue

        # Score: prefer low spread; then more depth; then closer expiry
        # (lower score = better)
        score = spread * 100.0 - (depth / 1000.0) + (remaining / 10000.0)
        scored.append((score, m, book))

    if not scored:
        return None

    scored.sort(key=lambda x: x[0])
    best_score, best_market, best_book = scored[0]
    # attach best book so main loop doesn't have to refetch
    best_market["_book"] = best_book
    best_market["_score"] = best_score
    return best_market




def select_best_market_across(assets, windows):
    """Scan multiple assets/windows and return the best market by liquidity."""
    best = None
    for asset in assets:
        for win in windows:
            markets = discover_fast_market_markets(asset.strip().upper(), win.strip())
            candidate = find_best_fast_market(markets)
            if not candidate:
                continue
            # Prefer lower score if present
            score = candidate.get("_score", 1e9)
            if best is None or score < best.get("_score", 1e9):
                candidate["_asset"] = asset.strip().upper()
                candidate["_window"] = win.strip()
                best = candidate
    return best

# =============================================================================
# CEX Price Signal
# =============================================================================

def get_binance_momentum(symbol="BTCUSDT", lookback_minutes=5):
    """Get price momentum from Binance public API.
    Returns: {momentum_pct, direction, price_now, price_then, avg_volume, candles}
    """
    url = (
        f"https://api.binance.com/api/v3/klines"
        f"?symbol={symbol}&interval=1m&limit={lookback_minutes}"
    )
    result = _api_request(url)
    if not result or isinstance(result, dict):
        return None

    try:
        # Kline format: [open_time, open, high, low, close, volume, ...]
        candles = result
        if len(candles) < 2:
            return None

        price_then = float(candles[0][1])   # open of oldest candle
        price_now = float(candles[-1][4])    # close of newest candle
        momentum_pct = ((price_now - price_then) / price_then) * 100
        direction = "up" if momentum_pct > 0 else "down"

        volumes = [float(c[5]) for c in candles]
        avg_volume = sum(volumes) / len(volumes)
        latest_volume = volumes[-1]

        # Volume ratio: latest vs average (>1 = above average activity)
        volume_ratio = latest_volume / avg_volume if avg_volume > 0 else 1.0

        return {
            "momentum_pct": momentum_pct,
            "direction": direction,
            "price_now": price_now,
            "price_then": price_then,
            "avg_volume": avg_volume,
            "latest_volume": latest_volume,
            "volume_ratio": volume_ratio,
            "candles": len(candles),
        }
    except (IndexError, ValueError, KeyError):
        return None


COINGECKO_ASSETS = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana"}


def get_coinbase_momentum(product="BTC-USD", lookback_minutes=5):
    """Get price momentum from Coinbase Exchange public candles.

    Endpoint returns candles newest-first: [time, low, high, open, close, volume]
    """
    # Coinbase limits: max 300 candles per request.
    url = (
        f"https://api.exchange.coinbase.com/products/{product}/candles"
        f"?granularity=60&limit={lookback_minutes}"
    )
    result = _api_request(url)
    if not result or isinstance(result, dict):
        return None

    try:
        candles = result
        if len(candles) < 2:
            return None
        # newest-first -> reverse to oldest-first
        candles = list(reversed(candles))
        price_then = float(candles[0][3])  # open
        price_now = float(candles[-1][4])  # close
        momentum_pct = ((price_now - price_then) / price_then) * 100
        direction = "up" if momentum_pct > 0 else "down"
        volumes = [float(c[5]) for c in candles]
        avg_volume = sum(volumes) / len(volumes) if volumes else 0
        latest_volume = volumes[-1] if volumes else 0
        volume_ratio = latest_volume / avg_volume if avg_volume > 0 else 1.0
        return {
            "momentum_pct": momentum_pct,
            "direction": direction,
            "price_now": price_now,
            "price_then": price_then,
            "avg_volume": avg_volume,
            "latest_volume": latest_volume,
            "volume_ratio": volume_ratio,
            "candles": len(candles),
        }
    except Exception:
        return None


def get_momentum(asset="BTC", source="binance", lookback=5):
    """Get price momentum from configured source."""
    if source == "binance":
        symbol = ASSET_SYMBOLS.get(asset, "BTCUSDT")
        return get_binance_momentum(symbol, lookback)
    if source == "coinbase":
        product = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD"}.get(asset, "BTC-USD")
        return get_coinbase_momentum(product, lookback)
    elif source == "coingecko":
        print("  ⚠️  CoinGecko free tier doesn't provide candle data — switch to coinbase")
        print("  Run: python fastloop_trader.py --set signal_source=coinbase")
        return None
    else:
        return None


# =============================================================================
# Import & Trade
# =============================================================================

def import_fast_market_market(slug):
    """Import a fast market to Simmer. Returns market_id or None."""
    url = f"https://polymarket.com/event/{slug}"
    try:
        result = get_client().import_market(url)
    except Exception as e:
        return None, str(e)

    if not result:
        return None, "No response from import endpoint"

    if result.get("error"):
        return None, result.get("error", "Unknown error")

    status = result.get("status")
    market_id = result.get("market_id")

    if status == "resolved":
        alternatives = result.get("active_alternatives", [])
        if alternatives:
            return None, f"Market resolved. Try alternative: {alternatives[0].get('id')}"
        return None, "Market resolved, no alternatives found"

    if status in ("imported", "already_exists"):
        return market_id, None

    return None, f"Unexpected status: {status}"


def get_market_details(market_id):
    """Fetch market details by ID."""
    try:
        market = get_client().get_market_by_id(market_id)
        if not market:
            return None
        from dataclasses import asdict
        return asdict(market)
    except Exception:
        return None


def get_portfolio():
    """Get portfolio summary."""
    try:
        return get_client().get_portfolio()
    except Exception as e:
        return {"error": str(e)}


def get_positions():
    """Get current positions as list of dicts."""
    try:
        positions = get_client().get_positions()
        from dataclasses import asdict
        return [asdict(p) for p in positions]
    except Exception:
        return []


def execute_trade(market_id, side, amount, *, paper_mode=False, paper_ctx=None):
    """Execute a trade.

    - live (default): uses Simmer SDK
    - paper_mode: uses local PaperBroker ledger (no wallet, no real trades)

    paper_ctx expects: {market_slug, question, clob_token_ids}
    """

    if paper_mode:
        try:
            if not paper_ctx:
                return {"success": False, "error": "paper_ctx missing", "simulated": True}
            market_slug = paper_ctx.get("market_slug")
            question = paper_ctx.get("question", "")
            clob_token_ids = paper_ctx.get("clob_token_ids") or []

            if side not in ("yes", "no"):
                return {"success": False, "error": f"invalid side: {side}", "simulated": True}
            if len(clob_token_ids) < 2:
                return {"success": False, "error": "missing clob token ids", "simulated": True}

            # Resolve YES/NO token IDs reliably
            yes_token, no_token = select_yes_token_id({"clob_token_ids": clob_token_ids, "outcome_prices": paper_ctx.get("outcome_prices")})
            if not yes_token or not no_token:
                yes_token, no_token = clob_token_ids[0], clob_token_ids[1]

            token_id = yes_token if side == "yes" else no_token

            book = fetch_orderbook(token_id)
            if not book:
                return {"success": False, "error": "failed to fetch orderbook", "simulated": True}
            asks = book.get("asks") or []
            if not asks:
                return {"success": False, "error": "empty orderbook asks", "simulated": True}
            best_ask = float(asks[0].get("price"))
            midpoint = fetch_live_midpoint(token_id)

            broker = broker_from_env(Path(__file__).parent)
            return broker.place_market_buy(
                market_slug=market_slug,
                question=question,
                token_id=str(token_id),
                side=side,
                amount_usd=float(amount),
                best_ask=best_ask,
                midpoint=midpoint,
                metadata={"strategy": "fastloop", "market_id": market_id},
            )
        except Exception as e:
            return {"success": False, "error": str(e), "simulated": True}

    # Live/simmer path
    try:
        result = get_client().trade(
            market_id=market_id,
            side=side,
            amount=amount,
            source=TRADE_SOURCE, skill_slug=SKILL_SLUG,
        )
        return {
            "success": result.success,
            "trade_id": result.trade_id,
            "shares_bought": result.shares_bought,
            "shares": result.shares_bought,
            "error": result.error,
            "simulated": result.simulated,
        }
    except Exception as e:
        return {"error": str(e)}


def calculate_position_size(max_size, smart_sizing=False):
    """Calculate position size, optionally based on portfolio."""
    if not smart_sizing:
        return max_size
    portfolio = get_portfolio()
    if not portfolio or portfolio.get("error"):
        return max_size
    balance = portfolio.get("balance_usdc", 0)
    if balance <= 0:
        return max_size
    smart_size = balance * SMART_SIZING_PCT
    return min(smart_size, max_size)


# =============================================================================
# Main Strategy Logic
# =============================================================================

def run_fast_market_strategy(dry_run=True, positions_only=False, show_config=False,
                        smart_sizing=False, quiet=False, paper_mode=False):
    """Run one cycle of the fast_market trading strategy."""

    def log(msg, force=False):
        """Print unless quiet mode is on. force=True always prints."""
        if not quiet or force:
            print(msg)

    log("⚡ Simmer FastLoop Trading Skill")
    log("=" * 50)

    if paper_mode:
        broker = broker_from_env(Path(__file__).parent)
        cash = float(broker.snapshot().get("cash_usd", 0.0))
        log("\n  [PAPER BROKER] Trades will be simulated with a local $ balance + real orderbooks. Use --live for real trades.")
        log(f"  Paper cash:       ${cash:.2f} (PAPER_START_USD)")
    elif dry_run:
        log("\n  [DRY RUN] No trades will be executed. Use --live for real trades.")

    log(f"\n⚙️  Configuration:")
    log(f"  Asset:            {ASSET}")
    log(f"  Window:           {WINDOW}")
    log(f"  Entry threshold:  {ENTRY_THRESHOLD} (min divergence from 50¢)")
    log(f"  Min momentum:     {MIN_MOMENTUM_PCT}% (min price move)")
    log(f"  Max position:     ${MAX_POSITION_USD:.2f}")
    log(f"  Signal source:    {SIGNAL_SOURCE}")
    log(f"  Lookback:         {LOOKBACK_MINUTES} minutes")
    log(f"  Min time left:    {MIN_TIME_REMAINING}s")
    log(f"  Volume weighting: {'✓' if VOLUME_CONFIDENCE else '✗'}")
    daily_spend = _load_daily_spend(__file__)
    log(f"  Daily budget:     ${DAILY_BUDGET:.2f} (${daily_spend['spent']:.2f} spent today, {daily_spend['trades']} trades)")

    if show_config:
        config_path = get_config_path(__file__)
        log(f"\n  Config file: {config_path}")
        log(f"\n  To change settings:")
        log(f'    python fast_trader.py --set entry_threshold=0.08')
        log(f'    python fast_trader.py --set asset=ETH')
        log(f'    Or edit config.json directly')
        return

    # Initialize client early to validate API key (only needed when using Simmer)
    if not paper_mode:
        get_client(live=not dry_run)

    # Show positions if requested
    if positions_only:
        log("\n📊 Sprint Positions:")
        positions = get_positions()
        fast_market_positions = [p for p in positions if "up or down" in (p.get("question", "") or "").lower()]
        if not fast_market_positions:
            log("  No open fast market positions")
        else:
            for pos in fast_market_positions:
                log(f"  • {pos.get('question', 'Unknown')[:60]}")
                log(f"    YES: {pos.get('shares_yes', 0):.1f} | NO: {pos.get('shares_no', 0):.1f} | P&L: ${pos.get('pnl', 0):.2f}")
        return

    # Show portfolio if smart sizing
    if smart_sizing:
        log("\n💰 Portfolio:")
        portfolio = get_portfolio()
        if portfolio and not portfolio.get("error"):
            log(f"  Balance: ${portfolio.get('balance_usdc', 0):.2f}")

    # Step 1: Discover fast markets
    log(f"\n🔍 Discovering {ASSET} fast markets...")
    markets = discover_fast_market_markets(ASSET, WINDOW)
    log(f"  Found {len(markets)} active fast markets")

    if not markets:
        log("  No active fast markets found — may be outside market hours or wrong asset/window")
        log(f"  Check: asset={ASSET}, window={WINDOW}")
        if not quiet:
            print("📊 Summary: No markets available")
        return

    # Step 2: Find best fast_market to trade
    best = find_best_fast_market(markets)
    if not best:
        # Show what we skipped so users understand the gap
        now = datetime.now(timezone.utc)
        for m in markets:
            end_time = m.get("end_time")
            if end_time:
                secs_left = (end_time - now).total_seconds()
                log(f"  Skipped: {m['question'][:50]}... ({secs_left:.0f}s left < {MIN_TIME_REMAINING}s min)")
        log(f"  All {len(markets)} markets have <{MIN_TIME_REMAINING}s remaining — waiting for next window")
        if not quiet:
            print(f"📊 Summary: No tradeable markets (all {len(markets)} too close to expiry)")
        return

    end_time = best.get("end_time")
    remaining = (end_time - datetime.now(timezone.utc)).total_seconds() if end_time else 0
    log(f"\n🎯 Selected: {best['question']}")
    log(f"  Expires in: {remaining:.0f}s")

    # Fetch live CLOB price (falls back to stale Gamma snapshot)
    clob_tokens = best.get("clob_token_ids", [])
    live_price = fetch_live_prices(best) if clob_tokens else None
    if live_price is not None:
        market_yes_price = live_price
        log(f"  Current YES price: ${market_yes_price:.3f} (live CLOB)")
    else:
        try:
            prices = json.loads(best.get("outcome_prices", "[]"))
            market_yes_price = float(prices[0]) if prices else 0.5
        except (json.JSONDecodeError, IndexError, ValueError):
            market_yes_price = 0.5
        log(f"  Current YES price: ${market_yes_price:.3f} (Gamma snapshot ⚠️)")

    # Fee info (fast markets charge 10% on winnings)
    fee_rate_bps = best.get("fee_rate_bps", 0)
    fee_rate = fee_rate_bps / 10000  # 1000 bps -> 0.10
    if fee_rate > 0:
        log(f"  Fee rate:         {fee_rate:.0%} (Polymarket fast market fee)")

    # Step 3: Get CEX price momentum
    log(f"\n📈 Fetching {ASSET} price signal ({SIGNAL_SOURCE})...")
    momentum = get_momentum(ASSET, SIGNAL_SOURCE, LOOKBACK_MINUTES)

    if not momentum:
        log("  ❌ Failed to fetch price data", force=True)
        return

    log(f"  Price: ${momentum['price_now']:,.2f} (was ${momentum['price_then']:,.2f})")
    log(f"  Momentum: {momentum['momentum_pct']:+.3f}%")
    log(f"  Direction: {momentum['direction']}")
    if VOLUME_CONFIDENCE:
        log(f"  Volume ratio: {momentum['volume_ratio']:.2f}x avg")

    # Step 4: Decision logic
    log(f"\n🧠 Analyzing...")

    momentum_pct = abs(momentum["momentum_pct"])
    direction = momentum["direction"]
    skip_reasons = []

    def _emit_skip_report(signals=1, attempted=0):
        """Emit automaton JSON with skip_reason before early return."""
        global _automaton_reported
        if os.environ.get("AUTOMATON_MANAGED") and skip_reasons:
            report = {"signals": signals, "trades_attempted": attempted, "trades_executed": 0,
                      "skip_reason": ", ".join(dict.fromkeys(skip_reasons))}
            print(json.dumps({"automaton": report}))
            _automaton_reported = True

    # Check order book spread and depth
    book = best.get("_book") or (fetch_orderbook_summary(best) if clob_tokens else None)
    if book:
        log(f"  Spread: {book['spread_pct']:.1%} (bid ${book['best_bid']:.3f} / ask ${book['best_ask']:.3f})")
        log(f"  Depth: ${book['bid_depth_usd']:.0f} bid / ${book['ask_depth_usd']:.0f} ask (top 5)")
        if book["spread_pct"] > MAX_SPREAD_PCT:
            log(f"  ⏸️  Spread {book['spread_pct']:.1%} > 10% — illiquid, skip")
            if not quiet:
                print(f"📊 Summary: No trade (wide spread: {book['spread_pct']:.1%})")
            skip_reasons.append("wide spread")
            _emit_skip_report()
            return

    # Check minimum momentum
    if momentum_pct < MIN_MOMENTUM_PCT:
        log(f"  ⏸️  Momentum {momentum_pct:.3f}% < minimum {MIN_MOMENTUM_PCT}% — skip")
        if not quiet:
            print(f"📊 Summary: No trade (momentum too weak: {momentum_pct:.3f}%)")
        return

    # Calculate expected fair price based on momentum direction
    # Simple model: strong momentum → higher probability of continuation
    if direction == "up":
        side = "yes"
        divergence = 0.50 + ENTRY_THRESHOLD - market_yes_price
        trade_rationale = f"{ASSET} up {momentum['momentum_pct']:+.3f}% but YES only ${market_yes_price:.3f}"
    else:
        side = "no"
        divergence = market_yes_price - (0.50 - ENTRY_THRESHOLD)
        trade_rationale = f"{ASSET} down {momentum['momentum_pct']:+.3f}% but YES still ${market_yes_price:.3f}"

    # Volume confidence adjustment
    vol_note = ""
    if VOLUME_CONFIDENCE and momentum["volume_ratio"] < 0.5:
        log(f"  ⏸️  Low volume ({momentum['volume_ratio']:.2f}x avg) — weak signal, skip")
        if not quiet:
            print(f"📊 Summary: No trade (low volume)")
        skip_reasons.append("low volume")
        _emit_skip_report()
        return
    elif VOLUME_CONFIDENCE and momentum["volume_ratio"] > 2.0:
        vol_note = f" 📊 (high volume: {momentum['volume_ratio']:.1f}x avg)"

    # Check divergence threshold
    if divergence <= 0:
        log(f"  ⏸️  Market already priced in: divergence {divergence:.3f} ≤ 0 — skip")
        if not quiet:
            print(f"📊 Summary: No trade (market already priced in)")
        skip_reasons.append("market already priced in")
        _emit_skip_report()
        return

    # Fee-aware EV check: require enough divergence to cover fees
    # EV = win_prob * payout_after_fees - (1 - win_prob) * cost
    # At the buy price, win_prob ≈ buy_price (market-implied).
    # We need our edge (divergence) to overcome the fee drag.
    if fee_rate > 0:
        buy_price = market_yes_price if side == "yes" else (1 - market_yes_price)
        # Fee cost per share in price terms: fee applies to winnings (1 - buy_price)
        fee_cost = (1 - buy_price) * fee_rate
        # Minimum divergence must exceed fee cost + buffer
        min_divergence = fee_cost + 0.02
        log(f"  Fee cost/share:   ${fee_cost:.3f} (min divergence {min_divergence:.3f})")
        if divergence < min_divergence:
            log(f"  ⏸️  Divergence {divergence:.3f} < fee-adjusted minimum {min_divergence:.3f} — skip")
            if not quiet:
                print(f"📊 Summary: No trade (fees eat the edge)")
            skip_reasons.append("fees eat the edge")
            _emit_skip_report()
            return

    # We have a signal!
    position_size = calculate_position_size(MAX_POSITION_USD, smart_sizing)
    price = market_yes_price if side == "yes" else (1 - market_yes_price)

    # Daily budget check
    remaining_budget = DAILY_BUDGET - daily_spend["spent"]
    if remaining_budget <= 0:
        log(f"  ⏸️  Daily budget exhausted (${daily_spend['spent']:.2f}/${DAILY_BUDGET:.2f} spent) — skip")
        if not quiet:
            print(f"📊 Summary: No trade (daily budget exhausted)")
        skip_reasons.append("daily budget exhausted")
        _emit_skip_report()
        return
    if position_size > remaining_budget:
        position_size = remaining_budget
        log(f"  Budget cap: trade capped at ${position_size:.2f} (${daily_spend['spent']:.2f}/${DAILY_BUDGET:.2f} spent)")
    if position_size < 0.50:
        log(f"  ⏸️  Remaining budget ${position_size:.2f} < $0.50 — skip")
        if not quiet:
            print(f"📊 Summary: No trade (remaining budget too small)")
        skip_reasons.append("budget too small")
        _emit_skip_report()
        return

    # Check minimum order size
    if price > 0:
        min_cost = MIN_SHARES_PER_ORDER * price
        if min_cost > position_size:
            log(f"  ⚠️  Position ${position_size:.2f} too small for {MIN_SHARES_PER_ORDER} shares at ${price:.2f}")
            skip_reasons.append("position too small")
            _emit_skip_report(attempted=1)
            return

    log(f"  ✅ Signal: {side.upper()} — {trade_rationale}{vol_note}", force=True)
    log(f"  Divergence: {divergence:.3f}", force=True)

    # Step 5: Import & Trade
    execution_error = None

    if paper_mode:
        # No Simmer import or wallet interaction in paper mode.
        market_id = f"paper:{best['slug']}"
        tag = "PAPER"
        log(f"\n🧾 Paper trade (no real orders)...", force=True)
        log(f"  Executing {side.upper()} paper trade for ${position_size:.2f} ({tag})...", force=True)
        result = execute_trade(
            market_id,
            side,
            position_size,
            paper_mode=True,
            paper_ctx={
                "market_slug": best["slug"],
                "question": best.get("question", ""),
                "clob_token_ids": clob_tokens,
                "outcome_prices": best.get("outcome_prices"),
            },
        )
    else:
        log(f"\n🔗 Importing to Simmer...", force=True)
        market_id, import_error = import_fast_market_market(best["slug"])

        if not market_id:
            log(f"  ❌ Import failed: {import_error}", force=True)
            return

        log(f"  ✅ Market ID: {market_id[:16]}...", force=True)

        tag = "SIMULATED" if dry_run else "LIVE"
        log(f"  Executing {side.upper()} trade for ${position_size:.2f} ({tag})...", force=True)
        result = execute_trade(market_id, side, position_size)

    if result and result.get("success"):
        shares = result.get("shares_bought") or result.get("shares") or 0
        trade_id = result.get("trade_id")
        log(f"  ✅ {'[PAPER] ' if result.get('simulated') else ''}Bought {shares:.1f} {side.upper()} shares @ ${price:.3f}", force=True)

        # Update daily spend (track live and paper separately)
        if paper_mode or not result.get("simulated"):
            daily_spend["spent"] += position_size
            daily_spend["trades"] += 1
            _save_daily_spend(__file__, daily_spend)

        # Log to trade journal (skip for paper trades)
        if trade_id and JOURNAL_AVAILABLE and not result.get("simulated"):
            confidence = min(0.9, 0.5 + divergence + (momentum_pct / 100))
            log_trade(
                trade_id=trade_id,
                source=TRADE_SOURCE, skill_slug=SKILL_SLUG,
                thesis=trade_rationale,
                confidence=round(confidence, 2),
                asset=ASSET,
                momentum_pct=round(momentum["momentum_pct"], 3),
                volume_ratio=round(momentum["volume_ratio"], 2),
                signal_source=SIGNAL_SOURCE,
            )
    else:
        error = result.get("error", "Unknown error") if result else "No response"
        log(f"  ❌ Trade failed: {error}", force=True)
        execution_error = error[:120]

    # Summary
    total_trades = 1 if result and result.get("success") else 0
    show_summary = not quiet or total_trades > 0
    if show_summary:
        print(f"\n📊 Summary:")
        print(f"  Sprint: {best['question'][:50]}")
        print(f"  Signal: {direction} {momentum_pct:.3f}% | YES ${market_yes_price:.3f}")
        print(f"  Action: {'PAPER' if dry_run else ('TRADED' if total_trades else 'FAILED')}")

    # Structured report for automaton (takes priority over fallback in __main__)
    if os.environ.get("AUTOMATON_MANAGED"):
        global _automaton_reported
        amount = round(position_size, 2) if total_trades > 0 else 0
        report = {"signals": 1, "trades_attempted": 1, "trades_executed": total_trades, "amount_usd": amount}
        if execution_error:
            report["execution_errors"] = [execution_error]
        print(json.dumps({"automaton": report}))
        _automaton_reported = True


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simmer FastLoop Trading Skill")
    parser.add_argument("--live", action="store_true", help="Execute real trades")
    parser.add_argument("--paper", action="store_true", help="Paper trade with a local $100 ledger (no real orders)")
    parser.add_argument("--dry-run", action="store_true", help="Show opportunities without trading (no orders)")
    parser.add_argument("--positions", action="store_true", help="Show current fast market positions")
    parser.add_argument("--paper-report", action="store_true", help="Print paper P&L report (no trading)")
    parser.add_argument("--config", action="store_true", help="Show current config")
    parser.add_argument("--set", action="append", metavar="KEY=VALUE",
                        help="Update config (e.g., --set entry_threshold=0.08)")
    parser.add_argument("--smart-sizing", action="store_true", help="Use portfolio-based position sizing")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="Only output on trades/errors (ideal for high-frequency runs)")
    args = parser.parse_args()

    if args.set:
        updates = {}
        for item in args.set:
            if "=" not in item:
                print(f"Invalid --set format: {item}. Use KEY=VALUE")
                sys.exit(1)
            key, val = item.split("=", 1)
            if key in CONFIG_SCHEMA:
                type_fn = CONFIG_SCHEMA[key].get("type", str)
                try:
                    if type_fn == bool:
                        updates[key] = val.lower() in ("true", "1", "yes")
                    else:
                        updates[key] = type_fn(val)
                except ValueError:
                    print(f"Invalid value for {key}: {val}")
                    sys.exit(1)
            else:
                print(f"Unknown config key: {key}")
                print(f"Valid keys: {', '.join(CONFIG_SCHEMA.keys())}")
                sys.exit(1)
        result = update_config(updates, __file__)
        print(f"✅ Config updated: {json.dumps(updates)}")
        sys.exit(0)

    def paper_report():
        skill_dir = Path(__file__).parent
        broker = broker_from_env(skill_dir)
        st = broker.snapshot()
        cash = float(st.get("cash_usd", 0.0))
        realized = float(st.get("realized_pnl_usd", 0.0))
        positions = st.get("positions", {}) or {}

        unreal = 0.0
        for p in positions.values():
            try:
                token_id = str(p.get("token_id"))
                shares = float(p.get("shares", 0))
                avg_entry = float(p.get("avg_entry", 0))
                mid = fetch_live_midpoint(token_id)
                if mid is None:
                    continue
                unreal += shares * (mid - avg_entry)
            except Exception:
                continue

        equity = cash + unreal

        # Hourly delta based on equity history
        hist_path = skill_dir / "paper_equity_history.jsonl"
        now_ts = time.time()
        past_equity = None
        if hist_path.exists():
            try:
                lines = hist_path.read_text().splitlines()
                for line in reversed(lines[-500:]):
                    row = json.loads(line)
                    if now_ts - float(row.get("ts", 0)) >= 3600:
                        past_equity = float(row.get("equity", 0))
                        break
            except Exception:
                pass
        delta = None if past_equity is None else (equity - past_equity)

        # append snapshot
        try:
            with hist_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": now_ts, "equity": equity, "cash": cash, "unreal": unreal, "realized": realized}) + "\n")
        except Exception:
            pass

        if delta is None:
            delta_str = "n/a"
        else:
            delta_str = f"{delta:+.2f}"

        return (
            "PAPER TRADING REPORT\n"
            f"• Equity: ${equity:.2f}\n"
            f"• Δ1h: {delta_str}\n"
            f"• Cash: ${cash:.2f}\n"
            f"• Unrealized P/L: ${unreal:.2f}\n"
            f"• Open Positions: {len(positions)}"
        )

    def market_health_report():
        now = datetime.now(timezone.utc)
        markets = []
        for a in FASTLOOP_ASSETS:
            for w in FASTLOOP_WINDOWS:
                markets.extend(discover_fast_market_markets(a.strip().upper(), w.strip()))
        scanned = len(markets)

        live = []
        max_remaining = _window_seconds.get(WINDOW, 300) * 2
        for m in markets:
            end_time = m.get("end_time")
            if not end_time:
                continue
            rem = (end_time - now).total_seconds()
            if rem <= 0:
                continue
            if not (rem > MIN_TIME_REMAINING and rem < max_remaining):
                continue
            book = fetch_orderbook_summary(m)
            if not book:
                continue
            spread = float(book.get("spread_pct") or 999)
            bid = float(book.get("best_bid") or 0)
            ask = float(book.get("best_ask") or 1)
            depth = min(float(book.get("bid_depth_usd") or 0), float(book.get("ask_depth_usd") or 0))
            live.append((spread, depth, bid, ask, rem, m.get("slug"), m.get("question", "")))

        # Liquidity filter
        ok = [x for x in live if x[0] <= MAX_SPREAD_PCT and x[2] > 0.05 and x[3] < 0.95 and x[1] >= 200]
        live.sort(key=lambda x: (x[0], -x[1]))
        top = live[:3]

        lines = [
            "MARKET HEALTH\n"
            f"• Scanned: {scanned}\n"
            f"• Live Now: {len(live)}\n"
            f"• Liquid (spread≤{MAX_SPREAD_PCT:.0%}, depth≥200): {len(ok)}\n"
            f"• Assets: {', '.join([a.strip().upper() for a in FASTLOOP_ASSETS])}\n"
            f"• Windows: {', '.join([w.strip() for w in FASTLOOP_WINDOWS])}\n"
            "• Top Liquidity Samples:"
        ]
        for spread, depth, bid, ask, rem, slug, q in top:
            lines.append(
                f"  - Spread {spread:.1%} | Bid {bid:.3f} | Ask {ask:.3f} | Depth ~${depth:.0f} | T- {int(rem)}s"
            )
        return "\n".join(lines)

    # Mode selection
    if args.live and args.paper:
        print("Error: choose only one: --live or --paper")
        sys.exit(2)

    if args.paper_report:
        print(paper_report())
        print(market_health_report())
        sys.exit(0)

    paper_mode = bool(args.paper)
    dry_run = (not args.live) and (not paper_mode)

    run_fast_market_strategy(
        dry_run=dry_run,
        positions_only=args.positions,
        show_config=args.config,
        smart_sizing=args.smart_sizing,
        quiet=args.quiet,
        paper_mode=paper_mode,
    )

    # Fallback report for automaton if the strategy returned early (no signal)
    # The function emits its own report when it reaches a trade; this covers early exits.
    if os.environ.get("AUTOMATON_MANAGED") and not _automaton_reported:
        print(json.dumps({"automaton": {"signals": 0, "trades_attempted": 0, "trades_executed": 0, "skip_reason": "no_signal"}}))
