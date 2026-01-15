"""
Polymarket 1小时市场数据采集脚本
采集BTC Up/Down Hourly市场数据
"""

import requests
import time
import csv
import os
from datetime import datetime, timezone, timedelta
import threading

POLL_INTERVAL = 5  # 1小时市场变化较慢，5秒轮询即可
CSV_FILE = "polymarket_btc_1h.csv"

CSV_HEADER = [
    "Timestamp",
    "Time_Left_Sec",
    "Binance_Price",
    "Open_Price",
    "Diff",
    "Up_Price",
    "Best_Bid",
    "Best_Ask",
    "Volume",
    "Slug"
]


def get_binance_price():
    """获取Binance BTC/USDT当前价格"""
    try:
        url = "https://api.binance.com/api/v3/ticker/price"
        params = {"symbol": "BTCUSDT"}
        resp = requests.get(url, params=params, timeout=5)
        resp.raise_for_status()
        return float(resp.json()["price"])
    except Exception as e:
        print(f"Error fetching Binance price: {e}")
        return None


def get_binance_kline_open(interval="1h"):
    """获取当前1小时K线的开盘价"""
    try:
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": interval, "limit": 1}
        resp = requests.get(url, params=params, timeout=5)
        resp.raise_for_status()
        kline = resp.json()[0]
        return float(kline[1])  # Open price
    except Exception as e:
        print(f"Error fetching Binance kline: {e}")
        return None


def get_current_1h_slug():
    """生成当前1小时市场的slug"""
    now = datetime.now(timezone.utc)
    # 转换为ET时间 (UTC-5)
    et_time = now - timedelta(hours=5)
    month = et_time.strftime("%B").lower()
    day = et_time.day
    hour = et_time.hour
    am_pm = "am" if hour < 12 else "pm"
    hour_12 = hour % 12
    if hour_12 == 0:
        hour_12 = 12
    return f"bitcoin-up-or-down-{month}-{day}-{hour_12}{am_pm}-et"


def get_polymarket_1h_data(slug):
    """获取1小时市场数据"""
    url = "https://gamma-api.polymarket.com/events"
    params = {"slug": slug}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return None
        return data[0]
    except Exception as e:
        print(f"Error fetching Polymarket data: {e}")
        return None


def parse_market_info(data):
    """解析市场信息"""
    if not data or 'markets' not in data:
        return None
    market = data['markets'][0]
    import json as _json

    outcome_prices = market.get('outcomePrices', [])
    if isinstance(outcome_prices, str):
        outcome_prices = _json.loads(outcome_prices)

    up_price = float(outcome_prices[0]) if outcome_prices else 0
    best_bid = float(market.get('bestBid', 0) or 0)
    best_ask = float(market.get('bestAsk', 0) or 0)
    volume = float(market.get('volume', 0) or 0)

    return {
        'up_price': up_price,
        'best_bid': best_bid,
        'best_ask': best_ask,
        'volume': volume
    }


def ensure_csv():
    """确保CSV文件存在"""
    if not os.path.isfile(CSV_FILE):
        with open(CSV_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)


def append_row(row):
    """追加一行数据"""
    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(row)


def main():
    """主函数"""
    ensure_csv()
    print("Starting 1H market monitor...")

    current_slug = None

    while True:
        try:
            slug = get_current_1h_slug()
            if slug != current_slug:
                current_slug = slug
                print(f"\n[Switch] New market: {slug}")

            # 获取数据
            binance_price = get_binance_price()
            open_price = get_binance_kline_open()
            poly_data = get_polymarket_1h_data(slug)

            if binance_price and open_price and poly_data:
                info = parse_market_info(poly_data)
                if info:
                    now = datetime.now(timezone.utc)
                    # 计算剩余时间（到下一个整点）
                    next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                    time_left = (next_hour - now).total_seconds()

                    diff = binance_price - open_price

                    row = [
                        now.isoformat(),
                        f"{time_left:.0f}",
                        f"{binance_price:.2f}",
                        f"{open_price:.2f}",
                        f"{diff:.2f}",
                        f"{info['up_price']:.2f}",
                        f"{info['best_bid']:.2f}",
                        f"{info['best_ask']:.2f}",
                        f"{info['volume']:.0f}",
                        slug
                    ]
                    append_row(row)

                    status = "UP" if diff > 0 else "DOWN"
                    print(f"{now.strftime('%H:%M:%S')} | Left: {time_left:.0f}s | Price: {binance_price:.2f} | Open: {open_price:.2f} | Diff: {diff:.2f} | Up: {info['up_price']:.2f} | {status}")

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
