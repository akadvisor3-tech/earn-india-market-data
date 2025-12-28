# scripts/build_snapshots.py

import pandas as pd
import numpy as np
from pathlib import Path

# ------------------------
# Base paths
# ------------------------
BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DATA_DIR = BASE_DIR / "data"
BOT_SNAPSHOT_DIR = BASE_DIR / "precalc"

INDEX_LIST = ["NIFTY50", "BANKNIFTY", "FINNIFTY", "MIDCAP100", "SENSEX"]
STOCK_DIR = RAW_DATA_DIR / "stocks" / "NIFTY500"

# ------------------------
# Indicator functions
# ------------------------
def sma(series, n):
    return series.rolling(n).mean()

def ema(series, n):
    return series.ewm(span=n, adjust=False).mean()

def rsi(series, n=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(n).mean()
    avg_loss = loss.rolling(n).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def bollinger(series, n=20):
    mid = series.rolling(n).mean()
    std = series.rolling(n).std()
    upper = mid + (2 * std)
    lower = mid - (2 * std)
    return upper, mid, lower

def vwap(df):
    tp = (df["high"] + df["low"] + df["close"]) / 3
    return (tp * df["volume"]).cumsum() / df["volume"].cumsum()

# ------------------------
# Trend & flags
# ------------------------
def detect_trend(row):
    if row["close"] > row["sma20"] > row["sma50"]:
        return "Bullish"
    elif row["close"] < row["sma20"] < row["sma50"]:
        return "Bearish"
    return "Sideways"

def mean_reversion_flag(row):
    return abs(row["sma5_dist_pct"]) > 1.2

def volatility_flag(row):
    return abs(row["bb_position"]) > 1.0

# ------------------------
# Confidence score
# ------------------------
def confidence_score(row):
    """
    Direction-neutral confidence score.
    Measures strength, alignment & stability — NOT buy/sell direction.
    Output strictly between 0 and 100.
    """

    score = 50  # Neutral base
    penalties = 0

    # ------------------------
    # 1️⃣ Trend clarity (Bullish OR Bearish = opportunity)
    # ------------------------
    if row["trend"] in ["Bullish", "Bearish"]:
        score += 25

    # ------------------------
    # 2️⃣ Mean reversion risk (overextension)
    # ------------------------
    penalties += abs(row.get("sma5_dist_pct", 0)) * 2
    penalties += abs(row.get("ema20_dist_pct", 0)) * 1.5
    penalties += abs(row.get("vwap_dist_pct", 0)) * 1.2

    # ------------------------
    # 3️⃣ Volatility / fake breakout risk
    # ------------------------
    if row.get("volatility_flag"):
        penalties += 10

    # ------------------------
    # 4️⃣ Apply penalties
    # ------------------------
    score -= penalties

    # ------------------------
    # 5️⃣ Data quality scaling
    # ------------------------
    quality = row.get("data_quality_flag", "FULL")

    if quality == "PARTIAL":
        score *= 0.85
    elif quality == "LIMITED":
        score *= 0.70

    # ------------------------
    # 6️⃣ Clamp safely
    # ------------------------
    score = max(0, min(100, round(score)))

    return score

# ------------------------
# Data quality
# ------------------------
def data_quality_flag(df):
    rows = len(df)
    if rows >= 200:
        return "FULL"
    elif rows >= 60:
        return "PARTIAL"
    return "LIMITED"

# ------------------------
# Build snapshot row
# ------------------------
def build_snapshot(df, symbol):
    df = df.copy()

    df["sma5"] = sma(df["close"], 5)
    df["sma9"] = sma(df["close"], 9)
    df["sma20"] = sma(df["close"], 20)
    df["sma50"] = sma(df["close"], 50)
    df["sma120"] = sma(df["close"], 120)
    df["sma200"] = sma(df["close"], 200)

    df["ema20"] = ema(df["close"], 20)
    df["ema50"] = ema(df["close"], 50)

    df["vwap"] = vwap(df)
    df["vwap_dist_pct"] = (df["close"] - df["vwap"]) / df["vwap"] * 100

    df["bb_upper"], df["bb_middle"], df["bb_lower"] = bollinger(df["close"])
    df["rsi14"] = rsi(df["close"])

    last = df.iloc[-1]
    row = last.to_dict()

    row["symbol"] = symbol
    row["trend"] = detect_trend(last)
    row["trend_alignment"] = row["trend"]
    row["sma5_dist_pct"] = (last["close"] - last["sma5"]) / last["sma5"] * 100
    row["ema20_dist_pct"] = (last["close"] - last["ema20"]) / last["ema20"] * 100
    row["bb_position"] = (last["close"] - last["bb_middle"]) / (last["bb_upper"] - last["bb_middle"])
    row["mean_reversion_flag"] = mean_reversion_flag(row)
    row["volatility_flag"] = volatility_flag(row)
    row["confidence_score"] = confidence_score(row)
    row["data_quality_flag"] = data_quality_flag(df)

    return row

# ------------------------
# SNAPSHOT BUILDERS
# ------------------------

def build_indices_snapshot(timeframe):
    rows = []

    for symbol in INDEX_LIST:
        if timeframe == "daily":
            file_path = RAW_DATA_DIR / "indices" / f"{symbol}.csv"
        else:
            file_path = RAW_DATA_DIR / "indices" / timeframe / f"{symbol}.csv"

        if not file_path.exists():
            continue

        df = pd.read_csv(file_path)
        snapshot = build_snapshot(df, symbol)
        snapshot["timeframe"] = timeframe
        rows.append(snapshot)

    return pd.DataFrame(rows)


def build_stocks_snapshot(timeframe):
    rows = []

    for file in STOCK_DIR.glob("*.csv"):
        symbol = file.stem
        df = pd.read_csv(file)
        snapshot = build_snapshot(df, symbol)
        snapshot["timeframe"] = timeframe
        rows.append(snapshot)

    return pd.DataFrame(rows)

# ------------------------
# MAIN EXECUTION
# ------------------------

def main():
    BOT_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    # Indices
    build_indices_snapshot("daily").to_csv(
        BOT_SNAPSHOT_DIR / "indices_daily.csv", index=False
    )
    build_indices_snapshot("weekly").to_csv(
        BOT_SNAPSHOT_DIR / "indices_weekly.csv", index=False
    )
    build_indices_snapshot("monthly").to_csv(
        BOT_SNAPSHOT_DIR / "indices_monthly.csv", index=False
    )

    # Stocks
    build_stocks_snapshot("daily").to_csv(
        BOT_SNAPSHOT_DIR / "stocks_daily.csv", index=False
    )
    build_stocks_snapshot("weekly").to_csv(
        BOT_SNAPSHOT_DIR / "stocks_weekly.csv", index=False
    )
    build_stocks_snapshot("monthly").to_csv(
        BOT_SNAPSHOT_DIR / "stocks_monthly.csv", index=False
    )

    print("✅ Snapshot CSVs updated successfully")


if __name__ == "__main__":
    main()
