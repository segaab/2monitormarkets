import pandas as pd
from yahooquery import Ticker
from datetime import datetime, timedelta
from ta.volatility import AverageTrueRange
import pytz

# --- Ticker Map ---
TICKER_MAP = {
    "GOLD - COMMODITY EXCHANGE INC.": "GC=F",
    "EURO FX - CHICAGO MERCANTILE EXCHANGE": "6E=F",
    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "6A=F",
    "BITCOIN - CHICAGO MERCANTILE EXCHANGE": "BTC-USD",
    "MICRO BITCOIN - CHICAGO MERCANTILE EXCHANGE": "MBT=F",
    "MICRO ETHER - CHICAGO MERCANTILE EXCHANGE": "ETH-USD",
    "SILVER - COMMODITY EXCHANGE INC.": "SI=F",
    "WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE": "CL=F",
    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE": "6J=F",
    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "6C=F",
    "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE": "6B=F",
    "U.S. DOLLAR INDEX - ICE FUTURES U.S.": "DX-Y.NYB",
    "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE": "6N=F",
    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE": "6S=F",
    "DOW JONES U.S. REAL ESTATE IDX - CHICAGO BOARD OF TRADE": "^DJI",
    "E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE": "ES=F",
    "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE": "NQ=F",
    "NIKKEI STOCK AVERAGE - CHICAGO MERCANTILE EXCHANGE": "^N225",
    "SPDR S&P 500 ETF TRUST": "SPY"
}

TRADER_CATEGORIES = ["noncomm", "comm", "nonrept"]

# --- Net Ratio Change ---
def calculate_net_position_ratio(long, short):
    total = long + short
    return (long - short) / total if total else 0.0

def calculate_latest_net_ratio_changes(reports):
    if len(reports) < 2:
        return {}

    latest = reports[0]
    previous = reports[1]
    changes = {}

    for cat in TRADER_CATEGORIES:
        l_long = latest.get(f"{cat}_positions_long_all", 0)
        l_short = latest.get(f"{cat}_positions_short_all", 0)
        p_long = previous.get(f"{cat}_positions_long_all", 0)
        p_short = previous.get(f"{cat}_positions_short_all", 0)

        latest_ratio = calculate_net_position_ratio(l_long, l_short)
        previous_ratio = calculate_net_position_ratio(p_long, p_short)
        changes[f"{cat}_net_ratio_change"] = latest_ratio - previous_ratio

    return changes

# --- Fetch price data ---
def fetch_price_data(symbol):
    end = datetime.utcnow()
    start = end - timedelta(days=365)

    try:
        t = Ticker(symbol)
        df = t.history(start=start.strftime('%Y-%m-%d'), end=end.strftime('%Y-%m-%d'), interval='1d')

        if df.empty:
            return pd.DataFrame()

        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index()

        df["datetime"] = pd.to_datetime(df["date"], utc=True)
        df = df.sort_values("datetime")

        # Convert to GMT+3 (Etc/GMT-3 is inverted)
        df["datetime"] = df["datetime"].dt.tz_convert("Etc/GMT-3")

        df["avg_volume"] = df["volume"].rolling(window=5).mean()
        df["rvol"] = df["volume"] / df["avg_volume"]

        atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=14)
        df["atr"] = atr.average_true_range()

        return df

    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return pd.DataFrame()

# --- Dummy COT Fetch ---
def fetch_cot_reports_dummy(asset_name, limit=5):
    today = datetime.utcnow().date()
    data = []
    for i in range(limit):
        date = today - timedelta(weeks=i)
        data.append({
            "report_date": date,
            "asset": asset_name,
            "noncomm_positions_long_all": 200000 + i * 500,
            "noncomm_positions_short_all": 150000 + i * 300,
            "comm_positions_long_all": 180000 - i * 250,
            "comm_positions_short_all": 220000 - i * 150,
            "nonrept_positions_long_all": 50000 + i * 100,
            "nonrept_positions_short_all": 45000 - i * 100,
        })
    return data

# --- Apply COT Changes Forward ---
def forward_fill_cot_changes(price_df, cot_reports):
    cot_reports = sorted(cot_reports, key=lambda x: x['report_date'])
    ranges = []

    for i in range(len(cot_reports) - 1):
        start = cot_reports[i]['report_date']
        end = cot_reports[i + 1]['report_date'] - timedelta(days=1)
        changes = calculate_latest_net_ratio_changes([cot_reports[i + 1], cot_reports[i]])
        if changes:
            ranges.append((start, end, changes))

    last = calculate_latest_net_ratio_changes([cot_reports[-1], cot_reports[-2]])
    if last:
        ranges.append((cot_reports[-1]['report_date'], datetime.utcnow().date(), last))

    df = price_df.copy()
    df["date"] = df["datetime"].dt.date

    for col in [f"{cat}_net_ratio_change" for cat in TRADER_CATEGORIES]:
        df[col] = None

    for start, end, changes in ranges:
        mask = (df["date"] >= start) & (df["date"] <= end)
        for k, v in changes.items():
            df.loc[mask, k] = v

    return df

# --- Run All Assets ---
def run_multi_asset_analysis():
    all_data = []

    for asset_name, symbol in TICKER_MAP.items():
        print(f"\n📊 Processing: {asset_name} | Symbol: {symbol}")
        price_df = fetch_price_data(symbol)
        if price_df.empty:
            print("⚠️ Price data unavailable.")
            continue

        cot_reports = fetch_cot_reports_dummy(asset_name)
        if len(cot_reports) < 2:
            print("⚠️ COT data unavailable.")
            continue

        enriched_df = forward_fill_cot_changes(price_df, cot_reports)
        enriched_df["asset"] = asset_name
        enriched_df["symbol"] = symbol

        all_data.append(enriched_df)

    final_df = pd.concat(all_data, ignore_index=True)
    return final_df

# --- Execute ---
if __name__ == "__main__":
    result = run_multi_asset_analysis()

    display_cols = [
        "datetime", "asset", "symbol", "close", "volume", "rvol", "atr",
        "noncomm_net_ratio_change", "comm_net_ratio_change", "nonrept_net_ratio_change"
    ]

    print("\n✅ Final Combined Output Sample:")
    print(result[display_cols].tail(20))
