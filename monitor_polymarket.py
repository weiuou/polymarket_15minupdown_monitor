import requests
import time
import re
import csv
import os
from datetime import datetime, timezone, timedelta
import sys
import argparse
import json
import threading
import bisect

# Configuration
SLUG = "btc-updown-15m-1767920400"
POLL_INTERVAL = 1  # seconds
CSV_FILE = "polymarket_15m.csv"

def parse_arguments():
    parser = argparse.ArgumentParser(description='Monitor Polymarket 15m Up/Down Events')
    parser.add_argument('--strike', type=float, help='Manually set the Strike Price (Price to Beat) from the website to calibrate Chainlink offset.')
    parser.add_argument('--slug', type=str, default="btc", help='Asset (btc/eth/sol/xrp) or full event slug/URL.')
    return parser.parse_args()

def get_polymarket_data(slug):
    url = "https://gamma-api.polymarket.com/events"
    params = {"slug": slug}
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data:
            return None
        return data[0] # Assuming first event is the one
    except Exception as e:
        print(f"Error fetching Polymarket data: {e}")
        return None

def get_clob_data(token_id):
    """
    Fetch Order Book from CLOB API for better accuracy.
    """
    url = "https://clob.polymarket.com/book"
    params = {"token_id": token_id}
    try:
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        response = requests.get(url, params=params, headers=headers, timeout=2) # Short timeout
        response.raise_for_status()
        return response.json()
    except Exception:
        return None

def get_best_prices_from_clob_book(clob_data):
    if not clob_data:
        return 0.0, 0.0

    bids = clob_data.get('bids') or []
    asks = clob_data.get('asks') or []

    best_bid = max((float(x.get('price', 0) or 0) for x in bids), default=0.0)
    best_ask = min((float(x.get('price', 0) or 0) for x in asks), default=0.0)

    return best_bid, best_ask

# Chainlink Data Streams Config
CL_STREAM_URL = "https://data.chain.link/api/query-timescale"
CL_STREAM_QUERY = "LIVE_STREAM_REPORTS_QUERY"
CL_STREAM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json"
}

ASSET_FEED_ID = {
    "btc": "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8",
    "eth": "0x000362205e10b3a147d02792eccee483dca6c7b44ecce7012cb8c6e0b68b3ae9",
    "sol": "0x0003b778d3f6b2ac4991302b89cb313f99a42467d6c9c5f96f57c29c0d2bc24f",
    "xrp": "0x0003c16c6aed42294f5cb4741f6e59ba2d728f0eae2eb9e6d3f555808c59fc45",
}

ACTIVE_ASSET = "btc"
ACTIVE_FEED_ID = ASSET_FEED_ID["btc"]
ACTIVE_FEED_LOCK = threading.Lock()

def _parse_asset_from_slug(slug):
    if not slug:
        return None
    s = slug.strip()
    if s.startswith("http://") or s.startswith("https://"):
        m_url = re.search(r"/event/([^/?#]+)", s)
        if m_url:
            s = m_url.group(1)

    s_low = s.lower()
    if s_low in ASSET_FEED_ID:
        return s_low

    m = re.match(r"^([a-z0-9]+)-updown-15m(?:-\d+)?$", s_low)
    if not m:
        return None
    return m.group(1).lower()

def _set_active_asset(asset):
    global ACTIVE_ASSET, ACTIVE_FEED_ID
    asset = (asset or "").lower()
    if asset not in ASSET_FEED_ID:
        asset = "btc"
    with ACTIVE_FEED_LOCK:
        if asset == ACTIVE_ASSET:
            return
        ACTIVE_ASSET = asset
        ACTIVE_FEED_ID = ASSET_FEED_ID[asset]
        with CHAINLINK_CACHE._lock:
            CHAINLINK_CACHE._times.clear()
            CHAINLINK_CACHE._prices.clear()

def _chainlink_ts_to_epoch(ts_iso):
    if not ts_iso:
        return None
    if ts_iso.endswith('Z'):
        ts_iso = ts_iso[:-1] + '+00:00'
    ts_dt = datetime.fromisoformat(ts_iso)
    if ts_dt.tzinfo is None:
        ts_dt = ts_dt.replace(tzinfo=timezone.utc)
    return ts_dt.timestamp()

def fetch_chainlink_stream_points(feed_id):
    params = {"query": CL_STREAM_QUERY, "variables": json.dumps({"feedId": feed_id})}
    response = requests.get(CL_STREAM_URL, params=params, headers=CL_STREAM_HEADERS, timeout=5)
    response.raise_for_status()
    data = response.json()

    reports = data.get("data", {}).get("liveStreamReports", {}).get("nodes", [])
    decimals = 18
    points = []
    for report in reports:
        ts_epoch = _chainlink_ts_to_epoch(report.get("validFromTimestamp"))
        if ts_epoch is None:
            continue
        try:
            price = float(report["price"]) / (10 ** decimals)
        except Exception:
            continue
        points.append((ts_epoch, price))

    points.sort(key=lambda x: x[0])
    return points

def fetch_chainlink_stream_latest():
    with ACTIVE_FEED_LOCK:
        feed_id = ACTIVE_FEED_ID
    points = fetch_chainlink_stream_points(feed_id)
    if not points:
        return None
    ts, price = points[-1]
    return {"price": price, "ts": ts}

# Deprecated: Binance functions removed as we use Chainlink Data Streams now
# def get_historical_btc_price(timestamp_ms): ... 


class ChainlinkPriceCache:
    def __init__(self, keep_seconds=3600):
        self.keep_seconds = keep_seconds
        self._lock = threading.Lock()
        self._times = []
        self._prices = []

    def add(self, ts, price):
        with self._lock:
            i = bisect.bisect_left(self._times, ts)
            if i < len(self._times) and self._times[i] == ts:
                self._prices[i] = price
            else:
                self._times.insert(i, ts)
                self._prices.insert(i, price)

            cutoff = ts - self.keep_seconds
            idx = bisect.bisect_left(self._times, cutoff)
            if idx > 0:
                del self._times[:idx]
                del self._prices[:idx]

    def latest(self):
        with self._lock:
            if not self._times:
                return None
            return self._times[-1], self._prices[-1]
    
    def snapshot_series(self):
        with self._lock:
            return list(self._times), list(self._prices)

    def get_around(self, target_ts):
        with self._lock:
            if not self._times:
                return None

            j = bisect.bisect_left(self._times, target_ts)
            before_i = j - 1
            after_i = j

            before = (self._times[before_i], self._prices[before_i]) if before_i >= 0 else None
            after = (self._times[after_i], self._prices[after_i]) if after_i < len(self._times) else None

            return {
                "before": {"ts": before[0], "price": before[1]} if before else None,
                "after": {"ts": after[0], "price": after[1]} if after else None
            }


CHAINLINK_CACHE = ChainlinkPriceCache(keep_seconds=7200)

def chainlink_price_collector(stop_event, poll_interval=1.0, csv_store=None, backfill_interval=2.0):
    last_backfill_wall = 0.0
    while not stop_event.is_set():
        added_any = False
        try:
            with ACTIVE_FEED_LOCK:
                feed_id = ACTIVE_FEED_ID
            points = fetch_chainlink_stream_points(feed_id)
            latest = CHAINLINK_CACHE.latest()
            last_ts = latest[0] if latest else None
            for ts, price in points:
                if last_ts is None or ts > last_ts:
                    CHAINLINK_CACHE.add(ts, price)
                    last_ts = ts
                    added_any = True
        except Exception as e:
            print(f"Error fetching Chainlink Stream price: {e}")

        if csv_store and added_any:
            now_wall = time.time()
            if now_wall - last_backfill_wall >= backfill_interval:
                try:
                    csv_store.backfill_from_chainlink_cache(max_rows=5000)
                except Exception as e:
                    print(f"[Backfill] Failed: {e}")
                last_backfill_wall = now_wall

        stop_event.wait(poll_interval)

def get_chainlink_price_at(target_ts):
    # Backward compatibility wrapper
    return CHAINLINK_CACHE.get_around(target_ts)

CSV_HEADER = [
    "Timestamp",
    "Time_Left_Sec",
    "Price_Stream",
    "Strike_Price",
    "Diff",
    "Up_Price",
    "Best_Bid",
    "Best_Ask",
    "Extra",
    "Slug"
]

CSV_I_TS = 0
CSV_I_LEFT_SEC = 1
CSV_I_BTC = 2
CSV_I_STRIKE = 3
CSV_I_DIFF = 4
CSV_I_YES = 5
CSV_I_BID = 6
CSV_I_ASK = 7
CSV_I_EXTRA = 8
CSV_I_SLUG = 9

def _csv_parse_ts(ts_str):
    ts_dt = datetime.fromisoformat(ts_str)
    if ts_dt.tzinfo is None:
        ts_dt = ts_dt.replace(tzinfo=timezone.utc)
    return ts_dt

def _try_parse_float(s):
    try:
        return float(s)
    except Exception:
        return None

def _chainlink_asof(times, prices, target_ts):
    if not times:
        return None
    i = bisect.bisect_right(times, target_ts) - 1
    if i >= 0:
        return prices[i]
    return None

STRIKE_MAX_DELTA_MIN = 1.0

def _chainlink_pick_strike_with_delta(times, prices, target_ts):
    if not times:
        return None, None

    i = bisect.bisect_left(times, target_ts)
    before = (times[i - 1], prices[i - 1]) if i - 1 >= 0 else None
    after = (times[i], prices[i]) if i < len(times) else None

    if before is None and after is None:
        return None, None
    if before is None:
        return after[1], abs(after[0] - target_ts)
    if after is None:
        return before[1], abs(target_ts - before[0])

    delta_before = abs(target_ts - before[0])
    delta_after = abs(after[0] - target_ts)
    if delta_after <= delta_before:
        return after[1], delta_after
    return before[1], delta_before

def _try_slug_ts(slug):
    if not slug:
        return None
    try:
        return int(slug.rsplit('-', 1)[-1])
    except Exception:
        return None

def get_strike_price_from_stream_for_slug(slug):
    slug_ts = _try_slug_ts(slug)
    if slug_ts is None:
        return None
    times, prices = CHAINLINK_CACHE.snapshot_series()
    strike, delta_sec = _chainlink_pick_strike_with_delta(times, prices, float(slug_ts))
    if strike is None:
        return None
    return {"price": strike, "delta_sec": delta_sec or 0.0}

class CsvStore:
    def __init__(self, path):
        self.path = path
        self._lock = threading.RLock()
        self._ensure_normalized()

    def _normalize_row(self, row):
        if not row:
            return None

        if len(row) == len(CSV_HEADER):
            return row

        if len(row) == 8:
            return row + ["", ""]

        if len(row) == 9:
            last = row[8]
            if isinstance(last, str) and "btc-updown-15m-" in last:
                return row[:8] + ["", last]
            return row[:8] + [last, ""]

        if len(row) >= 10:
            return row[:8] + [row[8], row[9]]

        padded = list(row) + [""] * (10 - len(row))
        return padded

    def _ensure_normalized(self):
        with self._lock:
            if not os.path.isfile(self.path):
                with open(self.path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(CSV_HEADER)
                return

            with open(self.path, 'r', newline='') as f:
                reader = csv.reader(f)
                rows = list(reader)

            if not rows:
                with open(self.path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(CSV_HEADER)
                return

            header = rows[0]
            data_rows = rows[1:]

            if header == ["Timestamp", "Time_Left_Sec", "BTC_Price_Stream", "Strike_Price", "Diff", "Up_Price", "Best_Bid", "Best_Ask", "Extra", "Slug"]:
                header = CSV_HEADER

            needs_rewrite = header != CSV_HEADER
            normalized = []
            for r in data_rows:
                nr = self._normalize_row(r)
                if nr is None:
                    continue
                if nr != r:
                    needs_rewrite = True
                normalized.append(nr)

            if needs_rewrite and data_rows and not normalized:
                return

            if not needs_rewrite:
                return

            tmp_path = self.path + ".tmp"
            with open(tmp_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(CSV_HEADER)
                writer.writerows(normalized)
            os.replace(tmp_path, self.path)

    def append_row(self, row):
        if len(row) != len(CSV_HEADER):
            raise ValueError("CSV row length mismatch")
        with self._lock:
            with open(self.path, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(row)

    def backfill_from_chainlink_cache(self, max_rows=3000):
        times, prices = CHAINLINK_CACHE.snapshot_series()
        has_cache = bool(times)

        with self._lock:
            with open(self.path, 'r', newline='') as f:
                reader = csv.reader(f)
                rows = list(reader)

            if not rows or rows[0] != CSV_HEADER:
                self._ensure_normalized()
                with open(self.path, 'r', newline='') as f:
                    reader = csv.reader(f)
                    rows = list(reader)
                if not rows or rows[0] != CSV_HEADER:
                    return 0

            header = rows[0]
            data_rows = rows[1:]
            if not data_rows:
                return 0

            start_idx = max(0, len(data_rows) - max_rows)
            strike_cache = {}
            updated = 0

            for i in range(start_idx, len(data_rows)):
                r = self._normalize_row(data_rows[i])
                if r is None:
                    continue

                try:
                    row_dt = _csv_parse_ts(r[CSV_I_TS])
                    row_ts = row_dt.timestamp()
                except Exception:
                    continue

                btc_2 = None
                if has_cache:
                    btc_price = _chainlink_asof(times, prices, row_ts)
                    if btc_price is not None:
                        btc_2 = round(btc_price, 2)
                        existing_btc = _try_parse_float(r[CSV_I_BTC])
                        if existing_btc is None or abs(existing_btc - btc_2) > 0.005:
                            r[CSV_I_BTC] = f"{btc_2:.2f}"
                            updated += 1
                    else:
                        existing_btc = _try_parse_float(r[CSV_I_BTC])
                        if existing_btc is not None:
                            btc_2 = round(existing_btc, 2)
                else:
                    existing_btc = _try_parse_float(r[CSV_I_BTC])
                    if existing_btc is not None:
                        btc_2 = round(existing_btc, 2)

                strike = None
                slug = r[CSV_I_SLUG]
                if has_cache and slug:
                    try:
                        slug_ts = int(slug.rsplit('-', 1)[-1])
                        if slug_ts in strike_cache:
                            strike, strike_invalid = strike_cache[slug_ts]
                        else:
                            strike, strike_delta_sec = _chainlink_pick_strike_with_delta(times, prices, float(slug_ts))
                            strike_invalid = bool(strike_delta_sec is not None and (strike_delta_sec / 60.0) > STRIKE_MAX_DELTA_MIN)
                            strike_cache[slug_ts] = (strike, strike_invalid)
                            if strike_invalid:
                                strike = None
                    except Exception:
                        strike = None
                        strike_invalid = False
                else:
                    strike_invalid = False

                if strike_invalid:
                    if r[CSV_I_STRIKE] != "no_price_data":
                        r[CSV_I_STRIKE] = "no_price_data"
                        updated += 1
                    if r[CSV_I_DIFF] != "-":
                        r[CSV_I_DIFF] = "-"
                        updated += 1
                    data_rows[i] = r
                    continue

                strike_for_diff = None
                if strike is not None:
                    strike_2 = round(strike, 2)
                    existing_strike = _try_parse_float(r[CSV_I_STRIKE])
                    if existing_strike is None or abs(existing_strike - strike_2) > 0.005:
                        r[CSV_I_STRIKE] = f"{strike_2:.2f}"
                        updated += 1
                    strike_for_diff = strike_2
                else:
                    existing_strike = _try_parse_float(r[CSV_I_STRIKE])
                    if existing_strike is not None:
                        strike_for_diff = round(existing_strike, 2)

                if btc_2 is not None and strike_for_diff is not None:
                    diff_2 = round(btc_2 - strike_for_diff, 2)
                    existing_diff = _try_parse_float(r[CSV_I_DIFF])
                    if existing_diff is None or abs(existing_diff - diff_2) > 0.005:
                        r[CSV_I_DIFF] = f"{diff_2:.2f}"
                        updated += 1

                data_rows[i] = r

            if updated == 0:
                return 0

            tmp_path = self.path + ".tmp"
            with open(tmp_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(data_rows)
            os.replace(tmp_path, self.path)
            return updated

def parse_market_info(data):
    if not data:
        return None
        
    # Handle list input (standard Gamma API response)
    if isinstance(data, list):
        if len(data) == 0:
            return None
        event = data[0]
    else:
        event = data

    if 'markets' not in event or not event['markets']:
        return None

    market = event['markets'][0]
    question = market.get('question', '')
    title = event.get('title', '')
    
    # Get basic data from Gamma API first
    # Prefer outcomePrices[0] (Yes) over lastTradePrice which can be ambiguous
    outcome_prices = market.get('outcomePrices', [])
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except json.JSONDecodeError:
            outcome_prices = []
            
    if outcome_prices and len(outcome_prices) > 0:
        yes_price = float(outcome_prices[0])
    else:
        yes_price = float(market.get('lastTradePrice', 0) or 0)

    best_bid = float(market.get('bestBid', 0) or 0)
    best_ask = float(market.get('bestAsk', 0) or 0)

    try:
        # Try to get CLOB data for higher accuracy
        clob_token_ids = json.loads(market.get('clobTokenIds', '[]'))
        if clob_token_ids and len(clob_token_ids) > 0:
            # Token ID for "Up" (Index 0)
            up_token_id = clob_token_ids[0]
            clob_data = get_clob_data(up_token_id)
            
            if clob_data:
                clob_best_bid, clob_best_ask = get_best_prices_from_clob_book(clob_data)
                if clob_best_bid > 0:
                    best_bid = clob_best_bid
                if clob_best_ask > 0:
                    best_ask = clob_best_ask

                if best_bid > 0 and best_ask > 0:
                    yes_price = (best_bid + best_ask) / 2
                elif best_bid > 0:
                    yes_price = best_bid
                elif best_ask > 0:
                    yes_price = best_ask
    except Exception as e:
        print(f"Error fetching CLOB data: {e}")

    strike_price = None
    expiry_date_str = None
    expiry_dt = None
    chainlink_data = None
    
    year = market.get('endDateIso', '')[:4]
    if not year:
        year = str(datetime.now(timezone.utc).year)
    match_q = re.search(r"BTC > ([\d,.]+) @ ([A-Za-z]+ \d+) (\d{2}:\d{2})\?", question)
    match_t = re.search(r"Above ([\d,.]+) on ([A-Za-z]+ \d+) \((\d{2}:\d{2}) UTC\)", title)
    match_ud = re.search(r"Up or Down - ([A-Za-z]+ \d+), (\d{1,2}:\d{2}[AP]M)-(\d{1,2}:\d{2}[AP]M) ET", title)

    if match_q:
        strike_str = match_q.group(1).replace(',', '')
        strike_price = float(strike_str)
        date_part = match_q.group(2)
        time_part = match_q.group(3)
        expiry_date_str = f"{year} {date_part} {time_part}"
        expiry_dt = datetime.strptime(expiry_date_str, "%Y %b %d %H:%M").replace(tzinfo=timezone.utc)
        
    elif match_t:
        strike_str = match_t.group(1).replace(',', '')
        strike_price = float(strike_str)
        date_part = match_t.group(2)
        time_part = match_t.group(3)
        expiry_date_str = f"{year} {date_part} {time_part}"
        expiry_dt = datetime.strptime(expiry_date_str, "%Y %b %d %H:%M").replace(tzinfo=timezone.utc)
        
    elif match_ud:
        date_part = match_ud.group(1) # January 8
        start_time_part = match_ud.group(2) # 8:00PM
        end_time_part = match_ud.group(3) # 8:15PM
        
        # Convert ET to UTC
        # Assuming ET is UTC-5 (EST).
        
        try:
            # Parse Start Time
            start_dt_str = f"{year} {date_part} {start_time_part}"
            start_dt_et = datetime.strptime(start_dt_str, "%Y %B %d %I:%M%p")
            start_dt_utc = (start_dt_et + timedelta(hours=5)).replace(tzinfo=timezone.utc)
            
            # Parse End Time
            end_dt_str = f"{year} {date_part} {end_time_part}"
            end_dt_et = datetime.strptime(end_dt_str, "%Y %B %d %I:%M%p")
            if end_dt_et <= start_dt_et:
                end_dt_et = end_dt_et + timedelta(days=1)
            end_dt_utc = (end_dt_et + timedelta(hours=5)).replace(tzinfo=timezone.utc)
            
            expiry_dt = end_dt_utc
            
            # Determine Strike Price
            now_utc = datetime.now(timezone.utc)
            if now_utc >= start_dt_utc:
                # Fetch Chainlink Strike Price (Official Source)
                ts_sec = int(start_dt_utc.timestamp())
                chainlink_data = get_chainlink_price_at(ts_sec)
        except ValueError as e:
            print(f"Date parsing error for Up/Down: {e}")
            return None
            
    else:
        return None

    return {
        "strike_price": strike_price,
        "expiry_dt": expiry_dt,
        "yes_price": yes_price,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "id": market.get('id'),
        "chainlink_data": chainlink_data,
        "title": title
    }



def print_table_header():
    price_label = f"{ACTIVE_ASSET.upper()} Price"
    header = f"{'Time':<10} | {'Left':<10} | {price_label:<10} | {'Strike':<10} | {'Diff':<10} | {'Up Price':<10} | {'Ask':<6} | {'Bid':<6} | {'Status'}"
    print("-" * len(header))
    print(header)
    print("-" * len(header))

def get_current_slug_ts():
    now = datetime.now(timezone.utc)
    current_ts = int(now.timestamp())
    # Round down to nearest 15m (900s)
    base_ts = (current_ts // 900) * 900
    return base_ts

def get_slug_from_ts(ts):
    return f"{ACTIVE_ASSET}-updown-15m-{ts}"

def get_slug_from_ts_for_asset(asset, ts):
    asset = (asset or "btc").lower()
    return f"{asset}-updown-15m-{ts}"

def main():
    args = parse_arguments()
    base_asset = _parse_asset_from_slug(args.slug) or "btc"
    _set_active_asset(base_asset)
    csv_path = f"polymarket_{base_asset}_15m.csv"
    csv_store = CsvStore(csv_path)
    
    print_table_header()
    
    calibrated = False
    current_market_ts = 0
    target_slug = get_slug_from_ts_for_asset(base_asset, get_current_slug_ts())
    last_strike_price = None
    stop_event = threading.Event()
    price_thread = threading.Thread(
        target=chainlink_price_collector,
        args=(stop_event,),
        kwargs={"poll_interval": 0.5, "csv_store": csv_store, "backfill_interval": 1.0},
        daemon=True
    )
    price_thread.start()

    try:
        point = fetch_chainlink_stream_latest()
        if point:
            CHAINLINK_CACHE.add(point["ts"], point["price"])
    except Exception:
        pass

    try:
        while True:
            # 1. Slug Rotation
            ts = get_current_slug_ts()
            if ts != current_market_ts:
                current_market_ts = ts
                target_slug = get_slug_from_ts_for_asset(base_asset, ts)
                print(f"\n[System] Switching to market: {target_slug}")
                calibrated = False
                last_strike_price = None

            # 2. Fetch Data
            poly_data = get_polymarket_data(target_slug)
            now = datetime.now(timezone.utc)
            cl_now = get_chainlink_price_at(now.timestamp())
            if cl_now:
                pick = cl_now.get("before") or cl_now.get("after")
                btc_price = pick["price"] if pick else None
            else:
                btc_price = None
            
            if poly_data and btc_price:
                info = parse_market_info(poly_data)
                
                if info:
                    strike_stream = get_strike_price_from_stream_for_slug(target_slug)
                    strike_delta_min = None
                    if strike_stream is not None:
                        strike_delta_min = (strike_stream.get("delta_sec") or 0.0) / 60.0
                        if strike_delta_min <= STRIKE_MAX_DELTA_MIN:
                            info['strike_price'] = strike_stream["price"]
                        else:
                            info['strike_price'] = None
                    elif args.strike is not None and not info.get('strike_price'):
                        info['strike_price'] = float(args.strike)

                    # Cache Strike
                    if info['strike_price']:
                        last_strike_price = info['strike_price']
                    
                    # 3. Calibration (Determine Strike Price)
                    if info['strike_price'] and not calibrated:
                        # Market Start Time is roughly Expiry - 15m.
                        start_ts = info['expiry_dt'] - timedelta(minutes=15)
                        
                        actual_strike = info['strike_price']
                        calibrated = True
                        
                        print(f"\n[Calibration] Market: {target_slug}")
                        print(f"[Calibration] Title:      {info.get('title', 'N/A')}")
                        print(f"[Calibration] Start Time:     {start_ts.strftime('%H:%M:%S')} UTC")
                        
                        cl_data = info.get('chainlink_data')
                        if cl_data:
                            # Show Before
                            before = cl_data.get("before")
                            if before:
                                before_ts_dt = datetime.fromtimestamp(before["ts"], timezone.utc)
                                print(f"[Calibration] Chainlink [Before]: {before['price']:.2f} @ {before_ts_dt.strftime('%H:%M:%S')} (Delta: {(before_ts_dt - start_ts).total_seconds():.0f}s)")
                            else:
                                print(f"[Calibration] Chainlink [Before]: N/A")
                            
                            # Show After
                            after = cl_data.get("after")
                            if after:
                                after_ts_dt = datetime.fromtimestamp(after["ts"], timezone.utc)
                                print(f"[Calibration] Chainlink [After]:  {after['price']:.2f} @ {after_ts_dt.strftime('%H:%M:%S')} (Delta: {(after_ts_dt - start_ts).total_seconds():.0f}s)")
                            else:
                                print(f"[Calibration] Chainlink [After]:  N/A (No update yet)")
                        else:
                            print(f"[Calibration] Chainlink Data: N/A")

                        print(f"[Calibration] Strike Price (Base): {actual_strike:.2f}")

                    # 4. Calculation
                    # Use Chainlink Stream Price directly
                    display_btc_price = btc_price 
                    
                    display_strike = info['strike_price']
                    
                    if display_strike:
                        diff = display_btc_price - display_strike
                        diff_str = f"{diff:.2f}"
                        status = "ITM" if diff > 0 else "OTM"
                        strike_str = f"{display_strike:.2f}"
                    else:
                        diff_str = "-"
                        status = "Wait"
                        strike_str = "no_price_data" if (strike_delta_min is not None and strike_delta_min > STRIKE_MAX_DELTA_MIN) else "TBD"

                    if info['expiry_dt']:
                        time_left = info['expiry_dt'] - now
                        time_left_sec = time_left.total_seconds()
                        
                        if time_left_sec < 0:
                            time_left_str = "Expired"
                        else:
                            mm, ss = divmod(int(time_left_sec), 60)
                            hh, mm = divmod(mm, 60)
                            time_left_str = f"{hh:02d}:{mm:02d}:{ss:02d}" if hh > 0 else f"{mm:02d}:{ss:02d}"
                    else:
                        time_left_str = "?"
                        time_left_sec = 0

                    # Log to CSV
                    csv_store.append_row([
                        now.isoformat(),
                        f"{time_left_sec:.0f}",
                        f"{display_btc_price:.2f}",
                        strike_str,
                        diff_str,
                        f"{info['yes_price']:.2f}",
                        f"{info['best_bid']:.2f}",
                        f"{info['best_ask']:.2f}",
                        "",
                        target_slug
                    ])
                    
                    # Print Row
                    row = f"{now.strftime('%H:%M:%S'):<10} | {time_left_str:<10} | {display_btc_price:<10.2f} | {strike_str:<10} | {diff_str:<10} | {info['yes_price']:<10.2f} | {info['best_ask']:<6.2f} | {info['best_bid']:<6.2f} | {status}"
                    print(row)
                    
                else:
                    print(f"Waiting for market data... ({target_slug})")
            else:
                print("Failed to fetch data.")
                
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
    finally:
        stop_event.set()
        try:
            csv_store.backfill_from_chainlink_cache(max_rows=10**9)
        except Exception as e:
            print(f"[Backfill] Failed: {e}")

if __name__ == "__main__":
    main()
