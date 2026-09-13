import os
import csv
import gzip
import json
import math
import time
import threading
from copy import deepcopy
from datetime import datetime, timedelta, time as dtime, date
from pathlib import Path
from zoneinfo import ZoneInfo

from nse_calendar import is_nse_trading_day, next_nse_trading_day, holiday_name

import requests
from dotenv import load_dotenv
import upstox_client
import psycopg
from psycopg.types.json import Jsonb

# ============================================================
# UPSTOX MONEY-FLOW F&O MARKET ENGINE v3.7 — TOP 50 @ 09:20 IST
# ============================================================
# - Multi-symbol watchlist
# - Automatic NSE instrument discovery from Upstox instrument master
# - Automatic nearest FUTSTK / nearest OPTSTK expiry discovery
# - Automatic previous-day OHLC + Wilder ATR(14)
# - Opening ATM frozen at first 3-minute snapshot
# - 3 OTM call / 3 OTM put IV baskets
# - Independent T0 / prior-snapshot state per symbol
# - CSV backup + shared Neon market_snapshots table
#
# IMPORTANT:
# Keep bse_engine_neon.py unchanged while BSE v1 forward-test is running.
# This v2 file is the generic production architecture.
# ============================================================

# Portable runtime paths:
# - Windows/local: defaults to C:\\upstox_dashboard
# - Railway: defaults to /tmp/upstox_dashboard
IS_RAILWAY = bool(
    os.getenv("RAILWAY_ENVIRONMENT")
    or os.getenv("RAILWAY_PROJECT_ID")
    or os.getenv("RAILWAY_SERVICE_ID")
)

DEFAULT_BASE_DIR = "/tmp/upstox_dashboard" if IS_RAILWAY else r"C:\upstox_dashboard"
BASE_DIR = Path(os.getenv("BASE_DIR", DEFAULT_BASE_DIR))
BASE_DIR.mkdir(parents=True, exist_ok=True)

ENV_FILE = os.getenv("ENV_FILE", str(BASE_DIR / ".env"))
if Path(ENV_FILE).exists():
    load_dotenv(ENV_FILE)
else:
    # Railway injects variables directly; no .env file is required there.
    load_dotenv()

TOKEN = os.getenv("UPSTOX_TOKEN", "").strip()
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL", "").strip()

# Benchmark used for intraday relative-strength confirmation.
# Override in .env if Upstox changes the index instrument key.
NIFTY_INSTRUMENT_KEY = os.getenv("NIFTY_INSTRUMENT_KEY", "NSE_INDEX|Nifty 50").strip()
NIFTY_DAY_OPEN = None

# Daily universe is selected automatically from NSE stock F&O money flow.
MONEY_FLOW_TOP_N = int(os.getenv("MONEY_FLOW_TOP_N", "50"))
MONEY_FLOW_OPTION_WINGS = int(os.getenv("MONEY_FLOW_OPTION_WINGS", "3"))

# Exclude mega-cap/index-heavy names from the discovery ranking so they do not
# crowd out emerging money-flow stocks. Override freely from .env.
DEFAULT_EXCLUDED_SYMBOLS = (
    "RELIANCE,HDFCBANK,ICICIBANK,INFY,TCS,"
    "SBIN,BHARTIARTL,LT,HINDUNILVR,ITC"
)
EXCLUDED_SYMBOLS = {
    s.strip().upper()
    for s in os.getenv("EXCLUDED_SYMBOLS", DEFAULT_EXCLUDED_SYMBOLS).split(",")
    if s.strip()
}
MONEY_FLOW_FREEZE_HOUR = int(os.getenv("MONEY_FLOW_FREEZE_HOUR", "9"))
MONEY_FLOW_FREEZE_MINUTE = int(os.getenv("MONEY_FLOW_FREEZE_MINUTE", "20"))
MONEY_FLOW_FREEZE_TIME = dtime(MONEY_FLOW_FREEZE_HOUR, MONEY_FLOW_FREEZE_MINUTE)
ALLOW_UNIVERSE_REBUILD = os.getenv("ALLOW_UNIVERSE_REBUILD", "false").lower() == "true"
MAX_FULL_FEED_INSTRUMENTS = int(os.getenv("MAX_FULL_FEED_INSTRUMENTS", "2000"))
QUOTE_BATCH_SIZE = 500
FULL_QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"

# Upstox full-quote volume/vtt is treated as traded units.
# If your account/feed proves otherwise, set these to false and the script
# will multiply by lot size when estimating traded notional.
FUT_VOLUME_IS_UNITS = os.getenv("FUT_VOLUME_IS_UNITS", "true").lower() == "true"
OPT_VOLUME_IS_UNITS = os.getenv("OPT_VOLUME_IS_UNITS", "true").lower() == "true"

if not TOKEN:
    raise RuntimeError("UPSTOX_TOKEN not found in .env")

if not NEON_DATABASE_URL:
    print("WARNING: NEON_DATABASE_URL missing. CSV will work; Neon writes disabled.")

IST = ZoneInfo("Asia/Kolkata")
MARKET_START = dtime(9, 15)
MARKET_END = dtime(15, 15)
SNAPSHOT_MINUTES = 3
SAME_STRIKE_DROP_PCT = float(os.getenv("SAME_STRIKE_DROP_PCT", "20"))
SAME_STRIKE_STRONG_INCREASE_PCT = float(os.getenv("SAME_STRIKE_STRONG_INCREASE_PCT", "20"))
SAME_STRIKE_PERSIST_SNAPSHOTS = int(os.getenv("SAME_STRIKE_PERSIST_SNAPSHOTS", "3"))

# Railway should restart the process after the market session so the next
# trading day rebuilds the instrument master, money-flow universe, opening
# parameters and T0 from scratch. Local Windows behaviour remains unchanged
# unless AUTO_RESTART_DAILY=true is explicitly supplied.
AUTO_RESTART_DAILY = (
    os.getenv("AUTO_RESTART_DAILY", "true" if IS_RAILWAY else "false").lower() == "true"
)
ATR_PERIOD = 14
D_SLOPE = 0.69
D_INTERCEPT = 0.0
OI_IS_UNITS = True
PHI = 1.61803398875
SQRT_2 = math.sqrt(2)
SQRT_252 = math.sqrt(252)

# We subscribe to a buffer around previous-close ATM so opening gaps are tolerated.
# The actual frozen IV baskets are selected from the opening ATM when T0 is created.
OPTION_WINGS_EACH_SIDE = 8

UPSTOX_NSE_MASTER_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
)
UPSTOX_V3_HISTORY_BASE = "https://api.upstox.com/v3/historical-candle"
MASTER_CACHE = BASE_DIR / "NSE_instruments.json.gz"
MASTER_CACHE_MAX_AGE_HOURS = 12

CSV_FILE = BASE_DIR / "market_engine_snapshots.csv"
CSV_HEADERS = [
    "timestamp", "symbol", "money_flow_rank", "futures_value_cr",
    "options_value_cr", "total_money_flow_cr", "spot", "future",
    "future_basis", "future_oi", "future_oi_change_t0",
    "future_oi_change_pct_t0", "future_oi_change_3m", "call_oi", "put_oi",
    "call_oi_change_t0", "put_oi_change_t0", "call_oi_change_3m",
    "put_oi_change_3m", "pcr", "pcr_change_3m", "pcr_acceleration",
    "call_iv", "put_iv", "call_iv_change_3m", "put_iv_change_3m",
    "call_iv_acceleration", "put_iv_acceleration", "call_gamma", "put_gamma",
    "call_fresh_value_cr", "put_fresh_value_cr", "atm_call_oi_change_3m",
    "atm_put_oi_change_3m", "atm_call_unwinding", "atm_put_unwinding",
    "zone_state", "next_zone", "stock_change_pct", "nifty_change_pct",
    "relative_strength_vs_nifty", "relative_strength_acceleration_3m",
    "zone_changed", "previous_zone_state"
]


def safe_number(value):
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def fmt(value, digits=2):
    if value is None:
        return "-"
    try:
        return f"{value:.{digits}f}"
    except Exception:
        return str(value)


def pct_change(current, base):
    current = safe_number(current)
    base = safe_number(base)
    if not base:
        return None
    return (current - base) / base * 100.0


def current_ist():
    return datetime.now(IST)


def in_market_window(now_ist):
    return MARKET_START <= now_ist.time() <= MARKET_END


def after_market_window(now_ist):
    return now_ist.time() > MARKET_END


def next_session_date(d):
    """Return d if it is an NSE F&O trading day, otherwise the next trading day."""
    return next_nse_trading_day(d)


def wait_for_valid_session_start():
    """Keep Railway alive but never start a live session on an NSE F&O holiday."""
    last_notice = None
    while True:
        now_ist = current_ist()
        today = now_ist.date()

        if not is_nse_trading_day(today):
            target_date = next_nse_trading_day(today + timedelta(days=1))
            target = datetime.combine(target_date, MONEY_FLOW_FREEZE_TIME, tzinfo=IST)
            reason = holiday_name(today) or "Weekend"
        elif now_ist.time() < MONEY_FLOW_FREEZE_TIME:
            target = datetime.combine(today, MONEY_FLOW_FREEZE_TIME, tzinfo=IST)
            reason = "Before 09:20 freeze"
        elif after_market_window(now_ist):
            target_date = next_nse_trading_day(today + timedelta(days=1))
            target = datetime.combine(target_date, MONEY_FLOW_FREEZE_TIME, tzinfo=IST)
            reason = "Market closed"
        else:
            return

        remaining = (target - now_ist).total_seconds()
        if remaining <= 0:
            continue
        notice = (today, reason, target.date())
        if notice != last_notice:
            if holiday_name(today):
                print(f"NSE F&O HOLIDAY: {today} - {holiday_name(today)}. No live collection today.")
            print(f"{reason}. Waiting until next valid session: {target.strftime('%Y-%m-%d %H:%M:%S')} IST...")
            last_notice = notice
        time.sleep(min(60, max(1, remaining)))

def parse_expiry(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        x = float(value)
        if x > 10_000_000_000:
            x /= 1000.0
        return datetime.fromtimestamp(x, tz=IST).date()
    text = str(value).strip()
    if not text:
        return None
    for parser in (
        lambda s: datetime.fromisoformat(s.replace("Z", "+00:00")).date(),
        lambda s: datetime.strptime(s[:10], "%Y-%m-%d").date(),
    ):
        try:
            return parser(text)
        except Exception:
            pass
    return None


def instrument_type(row):
    return str(row.get("instrument_type") or row.get("instrumentType") or "").upper()


def trading_symbol(row):
    return str(row.get("trading_symbol") or row.get("tradingsymbol") or "").upper()


def underlying_symbol(row):
    return str(row.get("underlying_symbol") or row.get("underlyingSymbol") or "").upper()


def strike_price(row):
    return safe_number(row.get("strike_price", row.get("strike", 0)))


def download_instrument_master():
    refresh = True
    if MASTER_CACHE.exists():
        age = datetime.now().timestamp() - MASTER_CACHE.stat().st_mtime
        refresh = age > MASTER_CACHE_MAX_AGE_HOURS * 3600

    if refresh:
        print("Downloading Upstox NSE instrument master...")
        r = requests.get(UPSTOX_NSE_MASTER_URL, timeout=30)
        r.raise_for_status()
        MASTER_CACHE.write_bytes(r.content)
    else:
        print(f"Using cached instrument master: {MASTER_CACHE}")

    with gzip.open(MASTER_CACHE, "rt", encoding="utf-8") as f:
        rows = json.load(f)

    print(f"Instrument master loaded: {len(rows):,} rows")
    return rows


def find_spot(master, symbol):
    candidates = [
        r for r in master
        if str(r.get("segment", "")).upper() == "NSE_EQ"
        and instrument_type(r) in {"EQ", "BE"}
        and trading_symbol(r) == symbol
    ]
    if not candidates:
        # fallback because some masters expose the symbol through short_name/name
        candidates = [
            r for r in master
            if str(r.get("segment", "")).upper() == "NSE_EQ"
            and instrument_type(r) in {"EQ", "BE"}
            and str(r.get("short_name", "")).upper() == symbol
        ]
    if not candidates:
        raise ValueError(f"{symbol}: NSE equity instrument not found")
    return candidates[0]


def find_fo_rows(master, symbol):
    rows = []
    for r in master:
        if str(r.get("segment", "")).upper() != "NSE_FO":
            continue
        us = underlying_symbol(r)
        name = str(r.get("name", "")).upper()
        ts = trading_symbol(r)
        if us == symbol or ts.startswith(symbol) or name == symbol:
            rows.append(r)
    return rows


def nearest_expiry(rows, allowed_types):
    today = current_ist().date()
    expiries = sorted({
        parse_expiry(r.get("expiry"))
        for r in rows
        if instrument_type(r) in allowed_types
        and parse_expiry(r.get("expiry")) is not None
        and parse_expiry(r.get("expiry")) >= today
    })
    return expiries[0] if expiries else None


def find_future(fo_rows):
    exp = nearest_expiry(fo_rows, {"FUT", "FUTSTK"})
    if not exp:
        raise ValueError("No live stock future found")
    cands = [
        r for r in fo_rows
        if instrument_type(r) in {"FUT", "FUTSTK"}
        and parse_expiry(r.get("expiry")) == exp
    ]
    if not cands:
        raise ValueError("Nearest stock future row not found")
    return cands[0], exp


def find_option_chain(fo_rows):
    exp = nearest_expiry(fo_rows, {"CE", "PE", "OPTSTK"})
    if not exp:
        raise ValueError("No live stock options expiry found")
    rows = [
        r for r in fo_rows
        if parse_expiry(r.get("expiry")) == exp
        and (
            instrument_type(r) in {"CE", "PE", "OPTSTK"}
            or str(r.get("option_type", "")).upper() in {"CE", "PE"}
        )
    ]
    return rows, exp


def option_side(row):
    side = str(row.get("option_type", "")).upper()
    if side in {"CE", "PE"}:
        return side
    it = instrument_type(row)
    if it in {"CE", "PE"}:
        return it
    ts = trading_symbol(row)
    if ts.endswith("CE"):
        return "CE"
    if ts.endswith("PE"):
        return "PE"
    return ""


def nearest_strike(strikes, price):
    if not strikes:
        raise ValueError("No strikes available")
    return min(strikes, key=lambda s: abs(s - price))


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def get_full_quotes(instrument_keys):
    """Fetch Upstox full quotes in batches and map by instrument_token."""
    result = {}
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }
    keys = list(dict.fromkeys(instrument_keys))
    for batch in chunks(keys, QUOTE_BATCH_SIZE):
        r = requests.get(
            FULL_QUOTE_URL,
            params={"instrument_key": ",".join(batch)},
            headers=headers,
            timeout=30,
        )
        r.raise_for_status()
        payload = r.json()
        for q in payload.get("data", {}).values():
            token = q.get("instrument_token")
            if token:
                result[token] = q
        time.sleep(0.08)
    return result


def quote_value_cr(quote, lot_size=1, volume_is_units=True):
    if not quote:
        return 0.0
    volume = safe_number(quote.get("volume"))
    px = safe_number(quote.get("average_price")) or safe_number(quote.get("last_price"))
    multiplier = 1 if volume_is_units else max(1, int(lot_size or 1))
    return volume * px * multiplier / 10_000_000


def discover_stock_fo_universe(master):
    """Resolve every live NSE stock future with its equity spot and nearest option expiry."""
    today = current_ist().date()
    by_symbol = {}

    future_rows = []
    for r in master:
        if str(r.get("segment", "")).upper() != "NSE_FO":
            continue
        it = instrument_type(r)
        if it not in {"FUT", "FUTSTK"}:
            continue
        exp = parse_expiry(r.get("expiry"))
        if exp is None or exp < today:
            continue
        sym = underlying_symbol(r) or str(r.get("name", "")).upper().strip()
        if sym:
            future_rows.append((sym, exp, r))

    grouped = {}
    for sym, exp, row in future_rows:
        grouped.setdefault(sym, []).append((exp, row))

    for sym, rows in grouped.items():
        try:
            spot = find_spot(master, sym)
        except Exception:
            continue  # filters indices/non-equity underlyings
        rows.sort(key=lambda x: x[0])
        fut_exp, fut_row = rows[0]
        fo_rows = find_fo_rows(master, sym)
        try:
            option_rows, opt_exp = find_option_chain(fo_rows)
        except Exception:
            continue
        if not option_rows:
            continue
        by_symbol[sym] = {
            "symbol": sym,
            "spot_row": spot,
            "future_row": fut_row,
            "future_expiry": fut_exp,
            "option_rows": option_rows,
            "option_expiry": opt_exp,
            "lot_size": int(safe_number(fut_row.get("lot_size")) or 1),
        }

    return by_symbol


def select_money_flow_option_rows(option_rows, spot_price):
    strikes = sorted({strike_price(r) for r in option_rows if strike_price(r) > 0})
    if not strikes:
        return []
    atm = nearest_strike(strikes, spot_price)
    i = strikes.index(atm)
    lo = max(0, i - MONEY_FLOW_OPTION_WINGS)
    hi = min(len(strikes), i + MONEY_FLOW_OPTION_WINGS + 1)
    chosen = set(strikes[lo:hi])
    return [
        r for r in option_rows
        if strike_price(r) in chosen and option_side(r) in {"CE", "PE"}
    ]


def scan_money_flow_universe(master):
    """
    Rank the complete live NSE stock-F&O universe by:
      futures traded value + near-ATM option traded value.
    Option value uses ATM +/- MONEY_FLOW_OPTION_WINGS strikes for both CE and PE.
    """
    universe = discover_stock_fo_universe(master)
    if not universe:
        raise RuntimeError("No live NSE stock F&O universe could be resolved")

    total_before_exclusion = len(universe)
    universe = {
        symbol: data
        for symbol, data in universe.items()
        if symbol.upper() not in EXCLUDED_SYMBOLS
    }
    excluded_present = total_before_exclusion - len(universe)

    if not universe:
        raise RuntimeError("All resolved stock F&O names were excluded")

    print(
        f"Money-flow scan universe: {len(universe)} eligible stock F&O names "
        f"({excluded_present} excluded mega-cap names present)"
    )

    base_keys = []
    for u in universe.values():
        base_keys.extend([u["spot_row"]["instrument_key"], u["future_row"]["instrument_key"]])
    base_quotes = get_full_quotes(base_keys)

    option_keys = []
    selected_option_rows = {}
    valid = {}
    for symbol, u in universe.items():
        spot_key = u["spot_row"]["instrument_key"]
        fut_key = u["future_row"]["instrument_key"]
        sq = base_quotes.get(spot_key)
        fq = base_quotes.get(fut_key)
        spot_price = safe_number((sq or {}).get("last_price"))
        if not sq or not fq or spot_price <= 0:
            continue
        opts = select_money_flow_option_rows(u["option_rows"], spot_price)
        selected_option_rows[symbol] = opts
        option_keys.extend(r["instrument_key"] for r in opts)
        valid[symbol] = u

    option_quotes = get_full_quotes(option_keys) if option_keys else {}

    rankings = []
    for symbol, u in valid.items():
        spot_key = u["spot_row"]["instrument_key"]
        fut_key = u["future_row"]["instrument_key"]
        sq = base_quotes.get(spot_key, {})
        fq = base_quotes.get(fut_key, {})
        futures_value = quote_value_cr(fq, u["lot_size"], FUT_VOLUME_IS_UNITS)
        options_value = 0.0
        for r in selected_option_rows.get(symbol, []):
            options_value += quote_value_cr(
                option_quotes.get(r["instrument_key"], {}),
                u["lot_size"],
                OPT_VOLUME_IS_UNITS,
            )
        rankings.append({
            "symbol": symbol,
            "spot_key": spot_key,
            "future_key": fut_key,
            "spot_price": safe_number(sq.get("last_price")),
            "day_open": safe_number((sq.get("ohlc") or {}).get("open")),
            "future_price": safe_number(fq.get("last_price")),
            "future_volume": int(safe_number(fq.get("volume"))),
            "future_oi": int(safe_number(fq.get("oi"))),
            "futures_value_cr": futures_value,
            "options_value_cr": options_value,
            "total_money_flow_cr": futures_value + options_value,
        })

    rankings.sort(key=lambda x: x["total_money_flow_cr"], reverse=True)
    for i, row in enumerate(rankings, 1):
        row["rank"] = i
    return rankings


def load_frozen_universe_from_neon(trading_date):
    if not NEON_DATABASE_URL:
        return []
    sql = """
        SELECT rank, symbol, spot_instrument_key AS spot_key,
               future_instrument_key AS future_key, spot_price, future_price,
               future_volume, future_oi, futures_value_cr, options_value_cr,
               total_money_flow_cr, freeze_ts
        FROM public.money_flow_universe
        WHERE trading_date = %s
        ORDER BY rank
    """
    with psycopg.connect(NEON_DATABASE_URL, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (trading_date,))
            names = [d.name for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]


def is_verified_frozen_universe(rows):
    if len(rows) != MONEY_FLOW_TOP_N:
        return False
    ranks = [int(row["rank"]) for row in rows]
    symbols = {str(row["symbol"]).upper() for row in rows}
    return ranks == list(range(1, MONEY_FLOW_TOP_N + 1)) and len(symbols) == MONEY_FLOW_TOP_N


def save_money_flow_universe_to_neon(rows, freeze_ts):
    if not NEON_DATABASE_URL:
        return
    sql = """
        INSERT INTO public.money_flow_universe (
            trading_date, freeze_ts, rank, symbol, future_instrument_key,
            spot_instrument_key, futures_value_cr, options_value_cr,
            total_money_flow_cr, future_volume, future_oi, future_price, spot_price,
            selection_method
        ) VALUES (
            %(trading_date)s, %(freeze_ts)s, %(rank)s, %(symbol)s, %(future_key)s,
            %(spot_key)s, %(futures_value_cr)s, %(options_value_cr)s,
            %(total_money_flow_cr)s, %(future_volume)s, %(future_oi)s,
            %(future_price)s, %(spot_price)s, %(selection_method)s
        )
        ON CONFLICT (trading_date, symbol) DO UPDATE SET
            freeze_ts = EXCLUDED.freeze_ts,
            rank = EXCLUDED.rank,
            futures_value_cr = EXCLUDED.futures_value_cr,
            options_value_cr = EXCLUDED.options_value_cr,
            total_money_flow_cr = EXCLUDED.total_money_flow_cr,
            future_volume = EXCLUDED.future_volume,
            future_oi = EXCLUDED.future_oi,
            future_price = EXCLUDED.future_price,
            spot_price = EXCLUDED.spot_price,
            selection_method = EXCLUDED.selection_method
    """
    try:
        with psycopg.connect(NEON_DATABASE_URL, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                # One authoritative universe per date. This also removes stale
                # rows if an explicitly authorized same-day rebuild is run.
                cur.execute(
                    "DELETE FROM public.money_flow_universe WHERE trading_date = %s",
                    (freeze_ts.date(),),
                )
                for row in rows:
                    payload = dict(row)
                    payload["trading_date"] = freeze_ts.date()
                    payload["freeze_ts"] = freeze_ts
                    payload["selection_method"] = "FUTURES_PLUS_NEAR_ATM_OPTIONS_TOP50_0920"
                    cur.execute(sql, payload)
                cur.execute(
                    """SELECT COUNT(*), MIN(rank), MAX(rank), COUNT(DISTINCT symbol)
                       FROM public.money_flow_universe WHERE trading_date = %s""",
                    (freeze_ts.date(),),
                )
                count, min_rank, max_rank, symbols = cur.fetchone()
                if (count, min_rank, max_rank, symbols) != (
                    MONEY_FLOW_TOP_N, 1, MONEY_FLOW_TOP_N, MONEY_FLOW_TOP_N
                ):
                    raise RuntimeError(
                        f"Universe verification failed: rows={count}, ranks={min_rank}-{max_rank}, "
                        f"symbols={symbols}; expected exactly {MONEY_FLOW_TOP_N}."
                    )
            conn.commit()
        print(f"Money-flow universe written to Neon: {len(rows)} rows")
    except Exception as e:
        print("MONEY FLOW NEON ERROR:", e)
        raise


def historical_daily_candles(instrument_key, days=40):
    to_date = current_ist().date() - timedelta(days=1)
    from_date = to_date - timedelta(days=days * 2)
    enc_key = requests.utils.quote(instrument_key, safe="")
    url = (
        f"{UPSTOX_V3_HISTORY_BASE}/{enc_key}/days/1/"
        f"{to_date.isoformat()}/{from_date.isoformat()}"
    )
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    payload = r.json()
    candles = payload.get("data", {}).get("candles", [])

    parsed = []
    for c in candles:
        if len(c) < 5:
            continue
        parsed.append({
            "timestamp": c[0],
            "open": safe_number(c[1]),
            "high": safe_number(c[2]),
            "low": safe_number(c[3]),
            "close": safe_number(c[4]),
        })
    parsed.sort(key=lambda x: x["timestamp"])
    return parsed[-days:]


def wilder_atr(candles, period=14):
    if len(candles) < period + 1:
        raise ValueError(f"Need at least {period + 1} daily candles for ATR")
    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = ((atr * (period - 1)) + tr) / period
    return atr


def calculate_zones(day_open, day_atr, prev_high, prev_low, prev_close):
    atr_ann_pct = day_atr / prev_close * SQRT_252 * 100
    effective_volatility = D_SLOPE * atr_ann_pct + D_INTERCEPT
    p = round(day_open)
    sigma = p * effective_volatility / (100 * SQRT_252)
    dist_strong = sigma
    dist_weak = sigma / (2 * SQRT_2)
    ws = round(sigma / 4)
    ww = round(sigma / (4 * PHI))
    return {
        "strong_demand_low": round(p - dist_strong - ws / 2),
        "strong_demand_high": round(p - dist_strong + ws / 2),
        "weak_demand_low": round(p - dist_weak - ww / 2),
        "weak_demand_high": round(p - dist_weak + ww / 2),
        "weak_supply_low": round(p + dist_weak - ww / 2),
        "weak_supply_high": round(p + dist_weak + ww / 2),
        "strong_supply_low": round(p + dist_strong - ws / 2),
        "strong_supply_high": round(p + dist_strong + ws / 2),
        "dpoc": round((prev_high + prev_low + prev_close) / 3),
    }


def select_option_subscription_rows(option_rows, reference_price):
    strikes = sorted({strike_price(r) for r in option_rows if strike_price(r) > 0})
    atm = nearest_strike(strikes, reference_price)
    atm_i = strikes.index(atm)
    lo = max(0, atm_i - OPTION_WINGS_EACH_SIDE)
    hi = min(len(strikes), atm_i + OPTION_WINGS_EACH_SIDE + 1)
    selected_strikes = set(strikes[lo:hi])
    selected = [r for r in option_rows if strike_price(r) in selected_strikes and option_side(r) in {"CE", "PE"}]
    return selected, selected_strikes


def build_symbol_context(master, symbol, reference_price=None, selection_info=None):
    spot_row = find_spot(master, symbol)
    spot_key = spot_row["instrument_key"]
    daily = historical_daily_candles(spot_key, days=40)
    if len(daily) < ATR_PERIOD + 1:
        raise ValueError(f"{symbol}: insufficient daily history")
    prev = daily[-1]
    atr = wilder_atr(daily, ATR_PERIOD)

    fo_rows = find_fo_rows(master, symbol)
    if not fo_rows:
        raise ValueError(f"{symbol}: no NSE F&O contracts found")

    fut_row, fut_expiry = find_future(fo_rows)
    option_rows, opt_expiry = find_option_chain(fo_rows)
    selected_opts, selected_strikes = select_option_subscription_rows(option_rows, reference_price or prev["close"])

    option_meta = {}
    for r in selected_opts:
        key = r["instrument_key"]
        option_meta[key] = {
            "name": trading_symbol(r),
            "strike": strike_price(r),
            "type": option_side(r),
        }

    lot_size = int(safe_number(fut_row.get("lot_size")) or 1)

    return {
        "symbol": symbol,
        "spot_key": spot_key,
        "future_key": fut_row["instrument_key"],
        "future_expiry": fut_expiry,
        "option_expiry": opt_expiry,
        "option_meta": option_meta,
        "subscribed_strikes": selected_strikes,
        "lot_size": lot_size,
        "prev_high": prev["high"],
        "prev_low": prev["low"],
        "prev_close": prev["close"],
        "day_atr": atr,
        "day_open": (selection_info or {}).get("day_open") or None,
        "opening_atm": None,
        "call_iv_strikes": set(),
        "put_iv_strikes": set(),
        "option_opening_ltp": {},
        "option_oi_baseline": {},
        "option_oi_baseline_ts": None,
        "same_strike_persistence": {},
        "zones": None,
        "t0_snapshot": None,
        "previous_snapshot": None,
        "previous_call_iv_change": None,
        "previous_put_iv_change": None,
        "previous_pcr_change": None,
        "previous_relative_strength": None,
        "previous_zone_state": None,
        "last_snapshot_key": None,
        "money_flow_rank": (selection_info or {}).get("rank"),
        "futures_value_cr": (selection_info or {}).get("futures_value_cr", 0.0),
        "options_value_cr": (selection_info or {}).get("options_value_cr", 0.0),
        "total_money_flow_cr": (selection_info or {}).get("total_money_flow_cr", 0.0),
    }


def freeze_opening_parameters(ctx, spot):
    ctx["day_open"] = spot
    strikes = sorted({m["strike"] for m in ctx["option_meta"].values()})
    opening_atm = nearest_strike(strikes, spot)
    ctx["opening_atm"] = opening_atm
    atm_i = strikes.index(opening_atm)

    call_candidates = strikes[atm_i + 1: atm_i + 4]
    put_candidates = list(reversed(strikes[max(0, atm_i - 3):atm_i]))

    if len(call_candidates) < 3 or len(put_candidates) < 3:
        raise ValueError(
            f"{ctx['symbol']}: insufficient subscribed strikes around opening ATM {opening_atm}; "
            "increase OPTION_WINGS_EACH_SIDE"
        )

    ctx["call_iv_strikes"] = set(call_candidates[:3])
    ctx["put_iv_strikes"] = set(put_candidates[:3])
    ctx["zones"] = calculate_zones(
        ctx["day_open"], ctx["day_atr"],
        ctx["prev_high"], ctx["prev_low"], ctx["prev_close"]
    )

    z = ctx["zones"]
    print("\n" + "#" * 100)
    print(f"{ctx['symbol']} OPENING PARAMETERS FROZEN")
    print(f"Day Open          : {ctx['day_open']:.2f}")
    print(f"Opening ATM       : {ctx['opening_atm']}")
    print(f"Prev H/L/C        : {ctx['prev_high']:.2f} / {ctx['prev_low']:.2f} / {ctx['prev_close']:.2f}")
    print(f"ATR({ATR_PERIOD})          : {ctx['day_atr']:.2f}")
    print(f"DPOC              : {z['dpoc']}")
    print(f"Strong Demand     : {z['strong_demand_low']} - {z['strong_demand_high']}")
    print(f"Weak Demand       : {z['weak_demand_low']} - {z['weak_demand_high']}")
    print(f"Weak Supply       : {z['weak_supply_low']} - {z['weak_supply_high']}")
    print(f"Strong Supply     : {z['strong_supply_low']} - {z['strong_supply_high']}")
    print(f"3 OTM Calls       : {sorted(ctx['call_iv_strikes'])}")
    print(f"3 OTM Puts        : {sorted(ctx['put_iv_strikes'], reverse=True)}")
    print("#" * 100)


def initialize_csv():
    if not CSV_FILE.exists():
        with CSV_FILE.open("w", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()


def save_csv(row):
    try:
        with CSV_FILE.open("a", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writerow(row)
        return True
    except PermissionError:
        print(f"{row.get('symbol',''):<18} CSV WARNING: file is locked; Neon collection continues")
        return False
    except Exception as e:
        print(f"{row.get('symbol',''):<18} CSV ERROR: {e}")
        return False



def ensure_money_flow_option_snapshot_table():
    """Create the additive research table used for the frozen 3CE + 3PE basket."""
    if not NEON_DATABASE_URL:
        return False

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS public.money_flow_option_snapshots (
            id BIGSERIAL PRIMARY KEY,
            trading_date DATE NOT NULL,
            ts TIMESTAMPTZ NOT NULL,
            money_flow_rank INTEGER,
            symbol TEXT NOT NULL,
            opening_atm NUMERIC,
            expiry DATE,
            instrument_key TEXT,
            option_type TEXT NOT NULL,
            wing_no INTEGER NOT NULL,
            strike NUMERIC NOT NULL,
            ltp NUMERIC,
            opening_ltp NUMERIC,
            price_multiple NUMERIC,
            doubled BOOLEAN,
            oi BIGINT,
            oi_change_3m BIGINT,
            baseline_oi BIGINT,
            baseline_ts TIMESTAMPTZ,
            paired_option_type TEXT,
            paired_instrument_key TEXT,
            paired_oi BIGINT,
            paired_baseline_oi BIGINT,
            own_oi_change_pct_0920 NUMERIC,
            paired_oi_change_pct_0920 NUMERIC,
            same_strike_signal TEXT,
            same_strike_persistence INTEGER NOT NULL DEFAULT 0,
            same_strike_contribution NUMERIC NOT NULL DEFAULT 0,
            iv NUMERIC,
            delta NUMERIC,
            gamma NUMERIC,
            theta NUMERIC,
            vega NUMERIC,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (symbol, ts, option_type, wing_no)
        )
    """
    index_date_sql = """
        CREATE INDEX IF NOT EXISTS idx_mf_option_snapshots_date_symbol
            ON public.money_flow_option_snapshots (trading_date, symbol, ts)
    """
    index_signal_sql = """
        CREATE INDEX IF NOT EXISTS idx_mf_option_snapshots_signal
            ON public.money_flow_option_snapshots (trading_date, doubled, option_type, symbol)
    """
    alter_sql = """
        ALTER TABLE public.money_flow_option_snapshots ADD COLUMN IF NOT EXISTS baseline_oi BIGINT;
        ALTER TABLE public.money_flow_option_snapshots ADD COLUMN IF NOT EXISTS baseline_ts TIMESTAMPTZ;
        ALTER TABLE public.money_flow_option_snapshots ADD COLUMN IF NOT EXISTS paired_option_type TEXT;
        ALTER TABLE public.money_flow_option_snapshots ADD COLUMN IF NOT EXISTS paired_instrument_key TEXT;
        ALTER TABLE public.money_flow_option_snapshots ADD COLUMN IF NOT EXISTS paired_oi BIGINT;
        ALTER TABLE public.money_flow_option_snapshots ADD COLUMN IF NOT EXISTS paired_baseline_oi BIGINT;
        ALTER TABLE public.money_flow_option_snapshots ADD COLUMN IF NOT EXISTS own_oi_change_pct_0920 NUMERIC;
        ALTER TABLE public.money_flow_option_snapshots ADD COLUMN IF NOT EXISTS paired_oi_change_pct_0920 NUMERIC;
        ALTER TABLE public.money_flow_option_snapshots ADD COLUMN IF NOT EXISTS same_strike_signal TEXT;
        ALTER TABLE public.money_flow_option_snapshots ADD COLUMN IF NOT EXISTS same_strike_persistence INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE public.money_flow_option_snapshots ADD COLUMN IF NOT EXISTS same_strike_contribution NUMERIC NOT NULL DEFAULT 0;
    """

    try:
        with psycopg.connect(NEON_DATABASE_URL, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                cur.execute(alter_sql)
                cur.execute(index_date_sql)
                cur.execute(index_signal_sql)
            conn.commit()
        print("MONEY-FLOW OPTION SNAPSHOT TABLE : READY")
        return True
    except Exception as e:
        print(f"OPTION SNAPSHOT TABLE ERROR : {e}")
        return False


def frozen_option_contracts(ctx):
    """Return the exact frozen 3 OTM calls and 3 OTM puts with stable wing numbers."""
    calls = sorted(ctx.get("call_iv_strikes", set()))
    puts = sorted(ctx.get("put_iv_strikes", set()), reverse=True)
    wanted = [("CE", i + 1, strike) for i, strike in enumerate(calls[:3])]
    wanted += [("PE", i + 1, strike) for i, strike in enumerate(puts[:3])]

    result = []
    for option_type, wing_no, strike in wanted:
        for key, meta in ctx["option_meta"].items():
            if meta.get("type") == option_type and meta.get("strike") == strike:
                result.append((key, option_type, wing_no, strike))
                break
    return result


def option_key_for_strike(ctx, strike, option_type):
    for key, meta in ctx["option_meta"].items():
        if meta.get("type") == option_type and meta.get("strike") == strike:
            return key
    return None


def oi_pct_from_baseline(current_oi, baseline_oi):
    current_oi = safe_number(current_oi)
    baseline_oi = safe_number(baseline_oi)
    if baseline_oi <= 0:
        return None
    return (current_oi / baseline_oi - 1.0) * 100.0


def same_strike_confirmation(ctx, strike, anchor_type, snapshot):
    own_key = option_key_for_strike(ctx, strike, anchor_type)
    paired_type = "PE" if anchor_type == "CE" else "CE"
    paired_key = option_key_for_strike(ctx, strike, paired_type)
    own = snapshot.get(own_key, {}) if own_key else {}
    paired = snapshot.get(paired_key, {}) if paired_key else {}
    own_oi = safe_number(own.get("oi"))
    paired_oi = safe_number(paired.get("oi"))
    own_base = safe_number(ctx.get("option_oi_baseline", {}).get(own_key))
    paired_base = safe_number(ctx.get("option_oi_baseline", {}).get(paired_key))
    own_pct = oi_pct_from_baseline(own_oi, own_base)
    paired_pct = oi_pct_from_baseline(paired_oi, paired_base)
    signal = None
    if own_pct is not None and paired_pct is not None:
        if anchor_type == "CE" and own_pct <= -SAME_STRIKE_DROP_PCT and paired_pct > 0:
            signal = "BULLISH"
        elif anchor_type == "PE" and own_pct <= -SAME_STRIKE_DROP_PCT and paired_pct > 0:
            signal = "BEARISH"
    persistence_key = (strike, anchor_type)
    if signal:
        ctx["same_strike_persistence"][persistence_key] = ctx["same_strike_persistence"].get(persistence_key, 0) + 1
    else:
        ctx["same_strike_persistence"][persistence_key] = 0
    persistence = ctx["same_strike_persistence"].get(persistence_key, 0)
    contribution = 0.0
    if signal:
        contribution = 0.5
        if paired_pct is not None and paired_pct >= SAME_STRIKE_STRONG_INCREASE_PCT:
            contribution += 0.5
        if persistence >= SAME_STRIKE_PERSIST_SNAPSHOTS:
            contribution += 0.5
    return {
        "paired_option_type": paired_type, "paired_instrument_key": paired_key,
        "paired_oi": int(paired_oi) if paired_key and paired_oi else None,
        "paired_baseline_oi": int(paired_base) if paired_base else None,
        "own_oi_change_pct_0920": own_pct, "paired_oi_change_pct_0920": paired_pct,
        "same_strike_signal": signal, "same_strike_persistence": persistence,
        "same_strike_contribution": min(1.5, contribution),
    }


def save_money_flow_option_snapshots_to_neon(ctx, timestamp, snapshot, previous_snapshot):
    """Persist the six frozen OTM option contracts for this stock at each 3-minute snapshot."""
    if not NEON_DATABASE_URL or not ctx.get("opening_atm"):
        return False

    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
    rows = []

    for key, option_type, wing_no, strike in frozen_option_contracts(ctx):
        now = snapshot.get(key)
        if not now:
            continue

        ltp = safe_number(now.get("ltp"))
        oi = safe_number(now.get("oi"))

        # First valid positive LTP becomes the fixed opening/reference premium for the day.
        if ltp > 0 and key not in ctx["option_opening_ltp"]:
            ctx["option_opening_ltp"][key] = ltp

        opening_ltp = safe_number(ctx["option_opening_ltp"].get(key))
        price_multiple = (ltp / opening_ltp) if opening_ltp > 0 else None

        old = previous_snapshot.get(key) if previous_snapshot else None
        old_oi = safe_number(old.get("oi")) if old else oi
        oi_change_3m = oi - old_oi

        pair = same_strike_confirmation(ctx, strike, option_type, snapshot)

        rows.append({
            "trading_date": dt.date(),
            "ts": dt,
            "money_flow_rank": ctx.get("money_flow_rank"),
            "symbol": ctx["symbol"],
            "opening_atm": ctx.get("opening_atm"),
            "expiry": ctx.get("option_expiry"),
            "instrument_key": key,
            "option_type": option_type,
            "wing_no": wing_no,
            "strike": strike,
            "ltp": ltp,
            "opening_ltp": opening_ltp if opening_ltp > 0 else None,
            "price_multiple": price_multiple,
            "doubled": bool(price_multiple is not None and price_multiple >= 2.0),
            "oi": int(oi) if oi is not None else None,
            "oi_change_3m": int(oi_change_3m) if oi_change_3m is not None else None,
            "baseline_oi": int(safe_number(ctx.get("option_oi_baseline", {}).get(key))) or None,
            "baseline_ts": ctx.get("option_oi_baseline_ts"),
            **pair,
            "iv": now.get("iv"),
            "delta": now.get("delta"),
            "gamma": now.get("gamma"),
            "theta": now.get("theta"),
            "vega": now.get("vega"),
        })

    if not rows:
        return False

    sql = """
        INSERT INTO public.money_flow_option_snapshots (
            trading_date, ts, money_flow_rank, symbol, opening_atm, expiry,
            instrument_key, option_type, wing_no, strike, ltp, opening_ltp,
            price_multiple, doubled, oi, oi_change_3m, baseline_oi, baseline_ts,
            paired_option_type, paired_instrument_key, paired_oi, paired_baseline_oi,
            own_oi_change_pct_0920, paired_oi_change_pct_0920, same_strike_signal,
            same_strike_persistence, same_strike_contribution, iv, delta, gamma, theta, vega
        ) VALUES (
            %(trading_date)s, %(ts)s, %(money_flow_rank)s, %(symbol)s, %(opening_atm)s, %(expiry)s,
            %(instrument_key)s, %(option_type)s, %(wing_no)s, %(strike)s, %(ltp)s, %(opening_ltp)s,
            %(price_multiple)s, %(doubled)s, %(oi)s, %(oi_change_3m)s, %(baseline_oi)s, %(baseline_ts)s,
            %(paired_option_type)s, %(paired_instrument_key)s, %(paired_oi)s, %(paired_baseline_oi)s,
            %(own_oi_change_pct_0920)s, %(paired_oi_change_pct_0920)s, %(same_strike_signal)s,
            %(same_strike_persistence)s, %(same_strike_contribution)s, %(iv)s, %(delta)s, %(gamma)s,
            %(theta)s, %(vega)s
        )
        ON CONFLICT (symbol, ts, option_type, wing_no) DO UPDATE SET
            ltp = EXCLUDED.ltp,
            opening_ltp = COALESCE(public.money_flow_option_snapshots.opening_ltp, EXCLUDED.opening_ltp),
            price_multiple = EXCLUDED.price_multiple,
            doubled = EXCLUDED.doubled,
            oi = EXCLUDED.oi,
            oi_change_3m = EXCLUDED.oi_change_3m,
            baseline_oi = COALESCE(public.money_flow_option_snapshots.baseline_oi, EXCLUDED.baseline_oi),
            baseline_ts = COALESCE(public.money_flow_option_snapshots.baseline_ts, EXCLUDED.baseline_ts),
            paired_option_type = EXCLUDED.paired_option_type,
            paired_instrument_key = EXCLUDED.paired_instrument_key,
            paired_oi = EXCLUDED.paired_oi,
            paired_baseline_oi = COALESCE(public.money_flow_option_snapshots.paired_baseline_oi, EXCLUDED.paired_baseline_oi),
            own_oi_change_pct_0920 = EXCLUDED.own_oi_change_pct_0920,
            paired_oi_change_pct_0920 = EXCLUDED.paired_oi_change_pct_0920,
            same_strike_signal = EXCLUDED.same_strike_signal,
            same_strike_persistence = EXCLUDED.same_strike_persistence,
            same_strike_contribution = EXCLUDED.same_strike_contribution,
            iv = EXCLUDED.iv,
            delta = EXCLUDED.delta,
            gamma = EXCLUDED.gamma,
            theta = EXCLUDED.theta,
            vega = EXCLUDED.vega
    """

    try:
        with psycopg.connect(NEON_DATABASE_URL, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        return True
    except Exception as e:
        print(f"{ctx['symbol']:<18} OPTION SNAPSHOT NEON ERROR : {e}")
        return False


def save_snapshot_to_neon(ctx, row):
    if not NEON_DATABASE_URL:
        return False

    sql = """
        INSERT INTO public.stock_engine_snapshots (
            ts, symbol, money_flow_rank, futures_value_cr, options_value_cr, total_money_flow_cr,
            day_open, opening_atm, dpoc, spot, future, future_basis, future_oi,
            future_oi_change_t0, future_oi_change_pct_t0, future_oi_change_3m,
            call_oi, put_oi, call_oi_change_t0, put_oi_change_t0,
            call_oi_change_3m, put_oi_change_3m, pcr, pcr_change_3m, pcr_acceleration,
            call_iv, put_iv, call_iv_change_3m, put_iv_change_3m,
            call_iv_acceleration, put_iv_acceleration, call_gamma, put_gamma,
            call_fresh_value_cr, put_fresh_value_cr,
            atm_call_oi_change_3m, atm_put_oi_change_3m,
            atm_call_unwinding, atm_put_unwinding, zone_state, next_zone
        ) VALUES (
            %(ts)s, %(symbol)s, %(money_flow_rank)s, %(futures_value_cr)s, %(options_value_cr)s, %(total_money_flow_cr)s,
            %(day_open)s, %(opening_atm)s, %(dpoc)s, %(spot)s, %(future)s, %(future_basis)s, %(future_oi)s,
            %(future_oi_change_t0)s, %(future_oi_change_pct_t0)s, %(future_oi_change_3m)s,
            %(call_oi)s, %(put_oi)s, %(call_oi_change_t0)s, %(put_oi_change_t0)s,
            %(call_oi_change_3m)s, %(put_oi_change_3m)s, %(pcr)s, %(pcr_change_3m)s, %(pcr_acceleration)s,
            %(call_iv)s, %(put_iv)s, %(call_iv_change_3m)s, %(put_iv_change_3m)s,
            %(call_iv_acceleration)s, %(put_iv_acceleration)s, %(call_gamma)s, %(put_gamma)s,
            %(call_fresh_value_cr)s, %(put_fresh_value_cr)s,
            %(atm_call_oi_change_3m)s, %(atm_put_oi_change_3m)s,
            %(atm_call_unwinding)s, %(atm_put_unwinding)s, %(zone_state)s, %(next_zone)s
        )
        ON CONFLICT (symbol, ts) DO NOTHING
    """

    dt = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
    payload = dict(row)
    payload.update({
        "ts": dt,
        "symbol": ctx["symbol"],
        "money_flow_rank": ctx.get("money_flow_rank"),
        "futures_value_cr": ctx.get("futures_value_cr", 0.0),
        "options_value_cr": ctx.get("options_value_cr", 0.0),
        "total_money_flow_cr": ctx.get("total_money_flow_cr", 0.0),
        "day_open": ctx["day_open"],
        "opening_atm": ctx["opening_atm"],
        "dpoc": ctx["zones"]["dpoc"],
    })

    try:
        with psycopg.connect(NEON_DATABASE_URL, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, payload)
            conn.commit()
        print(f"{ctx['symbol']:<18} NEON WRITE : OK")
        return True
    except Exception as e:
        print(f"{ctx['symbol']:<18} NEON ERROR : {e}")
        return False


def save_dashboard_snapshot_to_neon(ctx, row):
    """Write a research-friendly snapshot using only columns that already exist in Neon."""
    if not NEON_DATABASE_URL:
        return False

    dt = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
    basis = safe_number(row.get("future_basis"))
    if basis > 0:
        basis_state = "PREMIUM"
    elif basis < 0:
        basis_state = "DISCOUNT"
    else:
        basis_state = "FLAT"

    details = {
        "money_flow_rank": row.get("money_flow_rank"),
        "futures_value_cr": row.get("futures_value_cr"),
        "options_value_cr": row.get("options_value_cr"),
        "total_money_flow_cr": row.get("total_money_flow_cr"),
        "spot": row.get("spot"),
        "future": row.get("future"),
        "future_basis": row.get("future_basis"),
        "future_oi_change_t0": row.get("future_oi_change_t0"),
        "future_oi_change_3m": row.get("future_oi_change_3m"),
        "call_oi_change_3m": row.get("call_oi_change_3m"),
        "put_oi_change_3m": row.get("put_oi_change_3m"),
        "pcr": row.get("pcr"),
        "pcr_change_3m": row.get("pcr_change_3m"),
        "pcr_acceleration": row.get("pcr_acceleration"),
        "call_iv": row.get("call_iv"),
        "put_iv": row.get("put_iv"),
        "call_iv_change_3m": row.get("call_iv_change_3m"),
        "put_iv_change_3m": row.get("put_iv_change_3m"),
        "call_iv_acceleration": row.get("call_iv_acceleration"),
        "put_iv_acceleration": row.get("put_iv_acceleration"),
        "call_fresh_value_cr": row.get("call_fresh_value_cr"),
        "put_fresh_value_cr": row.get("put_fresh_value_cr"),
        "atm_call_oi_change_3m": row.get("atm_call_oi_change_3m"),
        "atm_put_oi_change_3m": row.get("atm_put_oi_change_3m"),
        "atm_call_unwinding": row.get("atm_call_unwinding"),
        "atm_put_unwinding": row.get("atm_put_unwinding"),
        "stock_change_pct": row.get("stock_change_pct"),
        "nifty_change_pct": row.get("nifty_change_pct"),
        "relative_strength_vs_nifty": row.get("relative_strength_vs_nifty"),
        "relative_strength_acceleration_3m": row.get("relative_strength_acceleration_3m"),
        "zone_changed": row.get("zone_changed"),
        "previous_zone_state": row.get("previous_zone_state"),
        "current_zone": row.get("zone_state"),
        "next_zone": row.get("next_zone"),
    }

    sql = """
        INSERT INTO public.dashboard_snapshots (
            ts, symbol, asset_type, engine,
            status, classification, current_zone, target_zone,
            relative_strength_score, sector_confirmation,
            futures_basis_state, news_bias, details,
            engine_version, parameters_frozen,
            parameter_freeze_label, parameter_freeze_date
        )
        SELECT
            %(ts)s, %(symbol)s, 'STOCK', 'MONEY_FLOW',
            %(status)s, %(classification)s, %(current_zone)s, %(target_zone)s,
            %(relative_strength_score)s, 'NOT_CONFIGURED',
            %(futures_basis_state)s, 'NOT_EVALUATED', %(details)s,
            '3.2', TRUE,
            '09:20 MONEY FLOW + OPENING PARAMETERS', %(parameter_freeze_date)s
        WHERE NOT EXISTS (
            SELECT 1 FROM public.dashboard_snapshots
            WHERE symbol = %(symbol)s AND ts = %(ts)s AND engine = 'MONEY_FLOW'
        )
    """

    payload = {
        "ts": dt,
        "symbol": ctx["symbol"],
        "status": "ZONE_TRANSITION" if row.get("zone_changed") else "TRACKING",
        "classification": "RESEARCH_CAPTURE",
        "current_zone": row.get("zone_state"),
        "target_zone": row.get("next_zone"),
        "relative_strength_score": row.get("relative_strength_vs_nifty"),
        "futures_basis_state": basis_state,
        "details": Jsonb(details),
        "parameter_freeze_date": dt.date(),
    }

    try:
        with psycopg.connect(NEON_DATABASE_URL, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, payload)
            conn.commit()
        return True
    except Exception as e:
        print(f"{ctx['symbol']:<18} DASHBOARD NEON ERROR : {e}")
        return False



def get_option_by_strike(ctx, snapshot, strike, option_type):
    for key, meta in ctx["option_meta"].items():
        if meta["strike"] == strike and meta["type"] == option_type:
            return snapshot.get(key)
    return None


def total_option_oi(ctx, snapshot, option_type):
    total = 0.0
    for key, meta in ctx["option_meta"].items():
        if meta["type"] != option_type:
            continue
        row = snapshot.get(key)
        if row:
            total += safe_number(row.get("oi"))
    return total


def average_iv(ctx, snapshot, option_type, allowed_strikes):
    vals = []
    for key, meta in ctx["option_meta"].items():
        if meta["type"] != option_type or meta["strike"] not in allowed_strikes:
            continue
        row = snapshot.get(key)
        if row and row.get("iv") is not None:
            vals.append(safe_number(row.get("iv")))
    return sum(vals) / len(vals) if vals else None


def average_gamma(ctx, snapshot, option_type, allowed_strikes):
    vals = []
    for key, meta in ctx["option_meta"].items():
        if meta["type"] != option_type or meta["strike"] not in allowed_strikes:
            continue
        row = snapshot.get(key)
        if row and row.get("gamma") is not None:
            vals.append(abs(safe_number(row.get("gamma"))))
    return sum(vals) / len(vals) if vals else None


def fresh_position_value(ctx, current, previous, option_type):
    total_rupees = 0.0
    for key, meta in ctx["option_meta"].items():
        if meta["type"] != option_type:
            continue
        now = current.get(key)
        old = previous.get(key)
        if not now or not old:
            continue
        delta_oi = safe_number(now.get("oi")) - safe_number(old.get("oi"))
        if delta_oi <= 0:
            continue
        premium = safe_number(now.get("ltp"))
        if OI_IS_UNITS:
            total_rupees += delta_oi * premium
        else:
            total_rupees += delta_oi * premium * ctx["lot_size"]
    return total_rupees / 10_000_000


def get_zone_state(ctx, price):
    if price is None or ctx["zones"] is None:
        return "NO PRICE", "-"
    z = ctx["zones"]
    p = float(price)
    if p > z["strong_supply_high"]:
        return "ABOVE STRONG SUPPLY", "UPSIDE EXPANSION"
    if z["strong_supply_low"] <= p <= z["strong_supply_high"]:
        return "STRONG SUPPLY", "BREAK / REJECT"
    if p > z["weak_supply_high"]:
        return "WEAK SUPPLY BROKEN", f"STRONG SUPPLY {z['strong_supply_low']}-{z['strong_supply_high']}"
    if z["weak_supply_low"] <= p <= z["weak_supply_high"]:
        return "WEAK SUPPLY", f"STRONG SUPPLY {z['strong_supply_low']}-{z['strong_supply_high']}"
    if p > z["dpoc"]:
        return "ABOVE DPOC", f"WEAK SUPPLY {z['weak_supply_low']}-{z['weak_supply_high']}"
    if p == z["dpoc"]:
        return "AT DPOC", "WAIT"
    if p >= z["weak_demand_high"]:
        return "BELOW DPOC", f"WEAK DEMAND {z['weak_demand_low']}-{z['weak_demand_high']}"
    if z["weak_demand_low"] <= p <= z["weak_demand_high"]:
        return "WEAK DEMAND", f"STRONG DEMAND {z['strong_demand_low']}-{z['strong_demand_high']}"
    if p > z["strong_demand_high"]:
        return "WEAK DEMAND BROKEN", f"STRONG DEMAND {z['strong_demand_low']}-{z['strong_demand_high']}"
    if z["strong_demand_low"] <= p <= z["strong_demand_high"]:
        return "STRONG DEMAND", "BREAK / REJECT"
    return "BELOW STRONG DEMAND", "DOWNSIDE EXPANSION"


def process_symbol_snapshot(ctx, timestamp, snapshot):
    spot_row = snapshot.get(ctx["spot_key"])
    fut_row = snapshot.get(ctx["future_key"])
    if not spot_row or not fut_row:
        print(f"{ctx['symbol']:<18} snapshot skipped: spot/future incomplete")
        return

    spot = safe_number(spot_row.get("ltp"))
    future = safe_number(fut_row.get("ltp"))
    future_oi = safe_number(fut_row.get("oi"))

    if ctx["day_open"] is None:
        freeze_opening_parameters(ctx, spot)

    if ctx["t0_snapshot"] is None:
        ctx["t0_snapshot"] = deepcopy(snapshot)
        print(f"{ctx['symbol']:<18} T0 LOCKED AT {timestamp}")

    t0 = ctx["t0_snapshot"]
    prev = ctx["previous_snapshot"]

    t0_future_oi = safe_number(t0.get(ctx["future_key"], {}).get("oi"))
    future_oi_change_t0 = future_oi - t0_future_oi
    future_oi_pct_t0 = (future_oi_change_t0 / t0_future_oi * 100) if t0_future_oi else 0
    future_oi_change_3m = (
        future_oi - safe_number(prev.get(ctx["future_key"], {}).get("oi")) if prev else 0
    )

    call_oi = total_option_oi(ctx, snapshot, "CE")
    put_oi = total_option_oi(ctx, snapshot, "PE")
    t0_call_oi = total_option_oi(ctx, t0, "CE")
    t0_put_oi = total_option_oi(ctx, t0, "PE")
    call_oi_change_t0 = call_oi - t0_call_oi
    put_oi_change_t0 = put_oi - t0_put_oi

    if prev:
        prev_call_oi = total_option_oi(ctx, prev, "CE")
        prev_put_oi = total_option_oi(ctx, prev, "PE")
        call_oi_change_3m = call_oi - prev_call_oi
        put_oi_change_3m = put_oi - prev_put_oi
    else:
        call_oi_change_3m = put_oi_change_3m = 0

    pcr = put_oi / call_oi if call_oi else None
    if prev:
        prev_call = total_option_oi(ctx, prev, "CE")
        prev_put = total_option_oi(ctx, prev, "PE")
        prev_pcr = prev_put / prev_call if prev_call else None
        pcr_change = pcr - prev_pcr if pcr is not None and prev_pcr is not None else 0
    else:
        pcr_change = 0
    pcr_acceleration = (
        pcr_change - ctx["previous_pcr_change"]
        if ctx["previous_pcr_change"] is not None else 0
    )

    call_iv = average_iv(ctx, snapshot, "CE", ctx["call_iv_strikes"])
    put_iv = average_iv(ctx, snapshot, "PE", ctx["put_iv_strikes"])
    if prev:
        prev_call_iv = average_iv(ctx, prev, "CE", ctx["call_iv_strikes"])
        prev_put_iv = average_iv(ctx, prev, "PE", ctx["put_iv_strikes"])
        call_iv_change = call_iv - prev_call_iv if call_iv is not None and prev_call_iv is not None else 0
        put_iv_change = put_iv - prev_put_iv if put_iv is not None and prev_put_iv is not None else 0
    else:
        call_iv_change = put_iv_change = 0

    call_iv_acceleration = (
        call_iv_change - ctx["previous_call_iv_change"]
        if ctx["previous_call_iv_change"] is not None else 0
    )
    put_iv_acceleration = (
        put_iv_change - ctx["previous_put_iv_change"]
        if ctx["previous_put_iv_change"] is not None else 0
    )

    call_gamma = average_gamma(ctx, snapshot, "CE", ctx["call_iv_strikes"])
    put_gamma = average_gamma(ctx, snapshot, "PE", ctx["put_iv_strikes"])

    atm_call_oi_change_3m = 0
    atm_put_oi_change_3m = 0
    atm_call_now = get_option_by_strike(ctx, snapshot, ctx["opening_atm"], "CE")
    atm_put_now = get_option_by_strike(ctx, snapshot, ctx["opening_atm"], "PE")
    if prev:
        atm_call_prev = get_option_by_strike(ctx, prev, ctx["opening_atm"], "CE")
        atm_put_prev = get_option_by_strike(ctx, prev, ctx["opening_atm"], "PE")
        if atm_call_now and atm_call_prev:
            atm_call_oi_change_3m = safe_number(atm_call_now.get("oi")) - safe_number(atm_call_prev.get("oi"))
        if atm_put_now and atm_put_prev:
            atm_put_oi_change_3m = safe_number(atm_put_now.get("oi")) - safe_number(atm_put_prev.get("oi"))

    call_fresh_value = fresh_position_value(ctx, snapshot, prev, "CE") if prev else 0
    put_fresh_value = fresh_position_value(ctx, snapshot, prev, "PE") if prev else 0
    zone_state, next_zone = get_zone_state(ctx, spot)

    # Relative strength is measured in percentage points versus NIFTY 50.
    nifty_row = snapshot.get(NIFTY_INSTRUMENT_KEY, {}) if NIFTY_INSTRUMENT_KEY else {}
    nifty_ltp = safe_number(nifty_row.get("ltp")) if nifty_row else 0.0
    stock_change_pct = pct_change(spot, ctx.get("day_open"))
    nifty_change_pct = pct_change(nifty_ltp, NIFTY_DAY_OPEN) if NIFTY_DAY_OPEN else None
    relative_strength = (
        stock_change_pct - nifty_change_pct
        if stock_change_pct is not None and nifty_change_pct is not None
        else None
    )
    rs_acceleration = (
        relative_strength - ctx["previous_relative_strength"]
        if relative_strength is not None and ctx["previous_relative_strength"] is not None
        else 0.0
    )
    previous_zone_state = ctx.get("previous_zone_state")
    zone_changed = previous_zone_state is not None and previous_zone_state != zone_state

    row = {
        "timestamp": timestamp,
        "symbol": ctx["symbol"],
        "money_flow_rank": ctx.get("money_flow_rank"),
        "futures_value_cr": ctx.get("futures_value_cr", 0.0),
        "options_value_cr": ctx.get("options_value_cr", 0.0),
        "total_money_flow_cr": ctx.get("total_money_flow_cr", 0.0),
        "spot": spot,
        "future": future,
        "future_basis": future - spot,
        "future_oi": future_oi,
        "future_oi_change_t0": future_oi_change_t0,
        "future_oi_change_pct_t0": future_oi_pct_t0,
        "future_oi_change_3m": future_oi_change_3m,
        "call_oi": call_oi,
        "put_oi": put_oi,
        "call_oi_change_t0": call_oi_change_t0,
        "put_oi_change_t0": put_oi_change_t0,
        "call_oi_change_3m": call_oi_change_3m,
        "put_oi_change_3m": put_oi_change_3m,
        "pcr": pcr,
        "pcr_change_3m": pcr_change,
        "pcr_acceleration": pcr_acceleration,
        "call_iv": call_iv,
        "put_iv": put_iv,
        "call_iv_change_3m": call_iv_change,
        "put_iv_change_3m": put_iv_change,
        "call_iv_acceleration": call_iv_acceleration,
        "put_iv_acceleration": put_iv_acceleration,
        "call_gamma": call_gamma,
        "put_gamma": put_gamma,
        "call_fresh_value_cr": call_fresh_value,
        "put_fresh_value_cr": put_fresh_value,
        "atm_call_oi_change_3m": atm_call_oi_change_3m,
        "atm_put_oi_change_3m": atm_put_oi_change_3m,
        "atm_call_unwinding": atm_call_oi_change_3m < 0,
        "atm_put_unwinding": atm_put_oi_change_3m < 0,
        "zone_state": zone_state,
        "next_zone": next_zone,
        "stock_change_pct": stock_change_pct,
        "nifty_change_pct": nifty_change_pct,
        "relative_strength_vs_nifty": relative_strength,
        "relative_strength_acceleration_3m": rs_acceleration,
        "zone_changed": zone_changed,
        "previous_zone_state": previous_zone_state,
    }

    print("\n" + "=" * 100)
    print(f"ENGINE v3.5 | #{ctx.get('money_flow_rank','-')} {ctx['symbol']} | {timestamp}")
    print("=" * 100)
    print(f"Money Flow (Cr)   : Fut {ctx.get('futures_value_cr',0):.2f} + Opt {ctx.get('options_value_cr',0):.2f} = {ctx.get('total_money_flow_cr',0):.2f}")
    print(f"Spot/Future/Basis : {spot:.2f} / {future:.2f} / {future - spot:+.2f}")
    print(f"Fut OI ΔT0 / 3m  : {future_oi_change_t0:+,.0f} / {future_oi_change_3m:+,.0f}")
    print(f"Call/Put ΔOI 3m  : {call_oi_change_3m:+,.0f} / {put_oi_change_3m:+,.0f}")
    print(f"PCR / Δ / Accel  : {fmt(pcr,3)} / {pcr_change:+.4f} / {pcr_acceleration:+.4f}")
    print(f"Call/Put IV      : {fmt(call_iv * 100 if call_iv is not None else None,2)}% / {fmt(put_iv * 100 if put_iv is not None else None,2)}%")
    print(f"Zone             : {zone_state} -> {next_zone}")
    if relative_strength is not None:
        print(f"RS vs NIFTY      : {relative_strength:+.3f} pp | accel {rs_acceleration:+.3f} pp/3m")
    if zone_changed:
        print(f"ZONE TRANSITION  : {previous_zone_state} -> {zone_state}")

    # Database first: a locked CSV must never block the primary history.
    save_snapshot_to_neon(ctx, row)
    save_dashboard_snapshot_to_neon(ctx, row)
    save_money_flow_option_snapshots_to_neon(ctx, timestamp, snapshot, prev)
    save_csv(row)

    ctx["previous_snapshot"] = deepcopy(snapshot)
    ctx["previous_call_iv_change"] = call_iv_change
    ctx["previous_put_iv_change"] = put_iv_change
    ctx["previous_pcr_change"] = pcr_change
    ctx["previous_relative_strength"] = relative_strength
    ctx["previous_zone_state"] = zone_state


ensure_money_flow_option_snapshot_table()
initialize_csv()

# ============================================================
# DAILY MONEY-FLOW UNIVERSE SELECTION
# ============================================================

master = download_instrument_master()

# On Railway, a service may restart at any hour. Do not accidentally perform
# a second "daily" scan after the current session has already closed.
wait_for_valid_session_start()

existing_rankings = load_frozen_universe_from_neon(current_ist().date())
if existing_rankings and is_verified_frozen_universe(existing_rankings) and not ALLOW_UNIVERSE_REBUILD:
    selected_rankings = existing_rankings
    freeze_ts = existing_rankings[0]["freeze_ts"]
    print(f"Reusing today's verified frozen universe: {len(selected_rankings)} stocks")
elif existing_rankings and not ALLOW_UNIVERSE_REBUILD:
    raise RuntimeError(
        f"Today's universe contains {len(existing_rankings)} rows, expected {MONEY_FLOW_TOP_N}. "
        "Set ALLOW_UNIVERSE_REBUILD=true once to replace it, then return it to false."
    )
else:
    freeze_ts = current_ist()
print("\n" + "#" * 100)
print(f"MONEY-FLOW SCAN STARTED | {freeze_ts.strftime('%Y-%m-%d %H:%M:%S')} IST")
print("Ranking = futures traded value + ATM +/- "
      f"{MONEY_FLOW_OPTION_WINGS} option-strike traded value")
print(f"Top N              : {MONEY_FLOW_TOP_N}")
print("Excluded symbols   : " + (", ".join(sorted(EXCLUDED_SYMBOLS)) if EXCLUDED_SYMBOLS else "NONE"))
print("#" * 100)

if not existing_rankings or ALLOW_UNIVERSE_REBUILD:
    all_rankings = scan_money_flow_universe(master)
    if len(all_rankings) < MONEY_FLOW_TOP_N:
        raise RuntimeError(
            f"Only {len(all_rankings)} eligible ranked stocks were available; "
            f"refusing to freeze a partial Top {MONEY_FLOW_TOP_N}."
        )
    selected_rankings = all_rankings[:MONEY_FLOW_TOP_N]

if not selected_rankings:
    raise RuntimeError("Money-flow scan returned no eligible stocks")

print("\nTOP MONEY-FLOW F&O STOCKS - FROZEN FOR TODAY")
print("-" * 100)
for row in selected_rankings:
    print(
        f"#{row['rank']:<3} {row['symbol']:<15} "
        f"Fut ₹{row['futures_value_cr']:>10.2f} Cr | "
        f"Opt ₹{row['options_value_cr']:>10.2f} Cr | "
        f"Total ₹{row['total_money_flow_cr']:>10.2f} Cr"
    )
print("-" * 100)

if not existing_rankings or ALLOW_UNIVERSE_REBUILD:
    save_money_flow_universe_to_neon(selected_rankings, freeze_ts)

# ============================================================
# BUILD DETAILED ENGINE CONTEXTS ONLY FOR FROZEN TOP N
# ============================================================

contexts = {}
failed_symbols = {}
ranking_by_symbol = {r["symbol"]: r for r in selected_rankings}

for item in selected_rankings:
    symbol = item["symbol"]
    try:
        ctx = build_symbol_context(
            master,
            symbol,
            reference_price=item.get("spot_price"),
            selection_info=item,
        )
        # Freeze liquidity zones and opening ATM from the exchange-reported day open,
        # not from the first post-selection tick. T0 itself starts when streaming begins.
        if ctx.get("day_open"):
            freeze_opening_parameters(ctx, ctx["day_open"])
        contexts[symbol] = ctx
        print(
            f"READY #{item['rank']:<3} {symbol:<15} spot={ctx['spot_key']} "
            f"fut={ctx['future_key']} opt_exp={ctx['option_expiry']} "
            f"lot={ctx['lot_size']} opts={len(ctx['option_meta'])}"
        )
    except Exception as e:
        failed_symbols[symbol] = str(e)
        print(f"SKIP  {symbol:<15} {e}")

if not contexts:
    raise RuntimeError("No selected money-flow symbols could build full engine contexts")

instrument_to_symbol = {}
names = {}
instrument_keys = []

for symbol, ctx in contexts.items():
    for key, label in [
        (ctx["spot_key"], f"{symbol} SPOT"),
        (ctx["future_key"], f"{symbol} FUT"),
    ]:
        instrument_to_symbol[key] = symbol
        names[key] = label
        instrument_keys.append(key)
    for key, meta in ctx["option_meta"].items():
        instrument_to_symbol[key] = symbol
        names[key] = f"{symbol} {meta['strike']:g} {meta['type']}"
        instrument_keys.append(key)

instrument_keys = list(dict.fromkeys(instrument_keys))

# Add NIFTY 50 to the same live stream for stock-vs-market relative strength.
if NIFTY_INSTRUMENT_KEY:
    instrument_keys.append(NIFTY_INSTRUMENT_KEY)
    names[NIFTY_INSTRUMENT_KEY] = "NIFTY 50 BENCHMARK"
    instrument_keys = list(dict.fromkeys(instrument_keys))

if len(instrument_keys) > MAX_FULL_FEED_INSTRUMENTS:
    raise RuntimeError(
        f"Full-feed subscription requires {len(instrument_keys)} instruments, "
        f"above configured limit {MAX_FULL_FEED_INSTRUMENTS}. Reduce "
        "OPTION_WINGS_EACH_SIDE or MONEY_FLOW_TOP_N before reconnecting."
    )

def lock_0920_option_oi_baselines():
    """Lock same-strike option OI baselines once, immediately after the 09:20 universe is built.

    On a same-day Railway restart, reuse stored baseline_oi from Neon instead of replacing it.
    """
    baseline_ts = freeze_ts.astimezone(IST)
    restored = {}
    if NEON_DATABASE_URL:
        try:
            with psycopg.connect(NEON_DATABASE_URL, connect_timeout=10) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT symbol, instrument_key, MAX(baseline_oi), MAX(baseline_ts)
                        FROM public.money_flow_option_snapshots
                        WHERE trading_date=%s AND baseline_oi IS NOT NULL
                        GROUP BY symbol, instrument_key
                    """, (baseline_ts.date(),))
                    for symbol, key, oi, ts in cur.fetchall():
                        restored[(symbol, key)] = (oi, ts)
        except Exception as e:
            print(f"09:20 baseline restore warning: {e}")
    keys_to_fetch=[]
    for symbol, ctx in contexts.items():
        for key in ctx["option_meta"]:
            if (symbol,key) in restored:
                oi,ts=restored[(symbol,key)]
                ctx["option_oi_baseline"][key]=safe_number(oi)
                ctx["option_oi_baseline_ts"]=ts or baseline_ts
            else:
                keys_to_fetch.append(key)
    if keys_to_fetch:
        quotes=get_full_quotes(list(dict.fromkeys(keys_to_fetch)))
        for symbol,ctx in contexts.items():
            for key in ctx["option_meta"]:
                if key in ctx["option_oi_baseline"]: continue
                oi=safe_number((quotes.get(key) or {}).get("oi"))
                if oi>0: ctx["option_oi_baseline"][key]=oi
            if ctx["option_oi_baseline"] and ctx.get("option_oi_baseline_ts") is None:
                ctx["option_oi_baseline_ts"]=baseline_ts
    print(f"09:20 OPTION-OI BASELINE LOCKED/RESTORED for {sum(bool(c['option_oi_baseline']) for c in contexts.values())} symbols")


lock_0920_option_oi_baselines()

# Freeze the exchange-reported NIFTY day open once for the session.
if NIFTY_INSTRUMENT_KEY:
    try:
        _benchmark_quote = get_full_quotes([NIFTY_INSTRUMENT_KEY]).get(NIFTY_INSTRUMENT_KEY, {})
        NIFTY_DAY_OPEN = safe_number((_benchmark_quote.get("ohlc") or {}).get("open")) or None
        if NIFTY_DAY_OPEN:
            print(f"NIFTY day open    : {NIFTY_DAY_OPEN:.2f}")
        else:
            print("NIFTY day open    : unavailable; RS will remain null until configured")
    except Exception as e:
        NIFTY_DAY_OPEN = None
        print(f"NIFTY benchmark warning: {e}")

print("\nMONEY-FLOW ENGINE READY")
print("Symbols           :", ", ".join(contexts))
print("Feed instruments  :", len(instrument_keys))
print("Freeze time       :", freeze_ts.strftime("%H:%M:%S IST"))
if failed_symbols:
    print("Skipped           :", failed_symbols)

# ============================================================
# UPSTOX STREAM + LIVE STORAGE
# ============================================================

config = upstox_client.Configuration()
config.access_token = TOKEN
api_client = upstox_client.ApiClient(config)

latest_data = {}
data_lock = threading.Lock()
session_complete_announced = False


def on_open():
    print("\n" + "=" * 100)
    print("CONNECTED TO UPSTOX | MONEY-FLOW MARKET ENGINE v3.7 TOP50 @ 09:20")
    print("=" * 100)
    print("IST Time          :", current_ist().strftime("%Y-%m-%d %H:%M:%S"))
    print("Market Window     : money-flow freeze -> 15:15 IST")
    print("Watchlist         :", ", ".join(contexts))
    print("Neon              :", "ENABLED" if NEON_DATABASE_URL else "DISABLED")
    print("CSV               :", CSV_FILE)
    print()


def on_message(message):
    feeds = message.get("feeds", {})
    for key, feed in feeds.items():
        try:
            full_feed = feed.get("fullFeed", {})
            market_ff = full_feed.get("marketFF") or full_feed.get("indexFF") or {}
            ltpc = market_ff.get("ltpc", {})
            greeks = market_ff.get("optionGreeks", {})
            row = {
                "name": names.get(key, key),
                "ltp": ltpc.get("ltp"),
                "ltt": ltpc.get("ltt"),
                "cp": ltpc.get("cp"),
                "oi": market_ff.get("oi"),
                "volume": market_ff.get("vtt"),
                "iv": market_ff.get("iv"),
                "delta": greeks.get("delta"),
                "gamma": greeks.get("gamma"),
                "theta": greeks.get("theta"),
                "vega": greeks.get("vega"),
            }
            with data_lock:
                latest_data[key] = row
        except Exception as e:
            print("PARSE ERROR:", names.get(key, key), e)


def on_error(*args):
    error = args[-1] if args else "Unknown Upstox error"
    print("\nUPSTOX ERROR:", error, "\n")


def on_close(*args):
    print("\nUPSTOX CONNECTION CLOSED\n")


def snapshot_scheduler():
    global session_complete_announced
    while True:
        now = current_ist()
        if after_market_window(now):
            if not session_complete_announced:
                print("MARKET WINDOW CLOSED (15:15 IST). No further v3.5 snapshots.")
                session_complete_announced = True

                if AUTO_RESTART_DAILY:
                    print(
                        "Railway daily reset enabled: exiting cleanly so Railway "
                        "can restart the service and wait for the next session."
                    )
                    # Railway Restart Policy must be set to Always.
                    # os._exit avoids waiting on the blocking websocket client.
                    os._exit(0)

            time.sleep(5)
            continue

        if not in_market_window(now):
            time.sleep(1)
            continue

        freeze_minute_of_day = MONEY_FLOW_FREEZE_HOUR * 60 + MONEY_FLOW_FREEZE_MINUTE
        elapsed_minutes = now.hour * 60 + now.minute - freeze_minute_of_day
        if elapsed_minutes >= 0 and elapsed_minutes % SNAPSHOT_MINUTES == 0 and 3 <= now.second <= 7:
            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
            snapshot_key = now.strftime("%Y-%m-%d %H:%M")
            with data_lock:
                all_data = deepcopy(latest_data)

            for symbol, ctx in contexts.items():
                if ctx["last_snapshot_key"] == snapshot_key:
                    continue
                symbol_keys = {ctx["spot_key"], ctx["future_key"], *ctx["option_meta"].keys()}
                symbol_snapshot = {k: v for k, v in all_data.items() if k in symbol_keys}
                try:
                    process_symbol_snapshot(ctx, timestamp, symbol_snapshot)
                except Exception as e:
                    print(f"{symbol:<18} ENGINE ERROR : {e}")
                finally:
                    ctx["last_snapshot_key"] = snapshot_key

        time.sleep(0.5)


scheduler_thread = threading.Thread(target=snapshot_scheduler, daemon=True)
scheduler_thread.start()

streamer = upstox_client.MarketDataStreamerV3(api_client, instrument_keys, "full")
streamer.on("open", on_open)
streamer.on("message", on_message)
streamer.on("error", on_error)
streamer.on("close", on_close)

print("Connecting to Upstox...")
streamer.connect()