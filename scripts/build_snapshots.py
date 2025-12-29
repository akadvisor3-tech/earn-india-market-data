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

def pivot_levels(high, low, close):
    pp = (high + low + close) / 3
    r1 = (2 * pp) - low
    s1 = (2 * pp) - high
    r2 = pp + (high - low)
    s2 = pp - (high - low)

    return {
        "pivot_pp": round(pp, 2),
        "pivot_r1": round(r1, 2),
        "pivot_r2": round(r2, 2),
        "pivot_s1": round(s1, 2),
        "pivot_s2": round(s2, 2),
    }

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
def confidence_score(daily_row, weekly_row, monthly_row):
    """
    Direction-neutral confidence score.
    Measures strength, stability, and multi-timeframe alignment.
    Works equally well for BUY and SELL setups.
    Output: integer between 0 and 100.
    """

    # -------------------------
    # 0️⃣ Base score (neutral)
    # -------------------------
    score = 60

    # -------------------------
    # 1️⃣ Daily Trend Clarity
    # -------------------------
    if daily_row["trend"] in ("Bullish", "Bearish"):
        score += 20
    else:
        score -= 25  # Sideways markets are low quality

    # -------------------------
    # 2️⃣ Overextension penalties (Daily)
    # -------------------------
    score -= min(abs(daily_row.get("sma5_dist_pct", 0)) * 5, 20)
    score -= min(abs(daily_row.get("ema20_dist_pct", 0)) * 4, 15)
    score -= min(abs(daily_row.get("vwap_dist_pct", 0)) * 3, 15)

    # -------------------------
    # 3️⃣ Mean reversion & volatility risk
    # -------------------------
    if daily_row.get("mean_reversion_flag"):
        score -= 15

    if daily_row.get("volatility_flag"):
        score -= 10

    # -------------------------
    # 4️⃣ Multi-timeframe alignment (CORE LOGIC)
    # -------------------------
    daily_trend = daily_row["trend"]
    weekly_trend = weekly_row["trend"]
    monthly_trend = monthly_row["trend"]

    # ✅ Perfect alignment (Bull-Bull-Bull OR Bear-Bear-Bear)
    if (
        daily_trend == weekly_trend == monthly_trend
        and daily_trend != "Sideways"
    ):
        score += 10

    # ❌ Major conflict (Daily vs Monthly)
    elif daily_trend != monthly_trend and monthly_trend != "Sideways":
        score -= 30

    # ⚠️ Partial conflict (Daily vs Weekly)
    elif daily_trend != weekly_trend and weekly_trend != "Sideways":
        score -= 15

    # -------------------------
    # 5️⃣ Data quality adjustment (Daily data)
    # -------------------------
    quality = daily_row.get("data_quality_flag", "FULL")

    if quality == "PARTIAL":
        score -= 10
    elif quality == "LIMITED":
        score -= 25

    # -------------------------
    # 6️⃣ Clamp safely
    # -------------------------
    score = int(round(score))
    score = max(0, min(100, score))

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

# ------------------------
# Pivot levels (EOD)
# ------------------------
    pivots = pivot_levels(
        last["high"],
        last["low"],
        last["close"]
    )
    row.update(pivots)

# ------------------------
# Meta & indicators
# ------------------------
    row["symbol"] = symbol
    row["trend"] = detect_trend(last)
    row["trend_alignment"] = row["trend"]

    row["sma5_dist_pct"] = (last["close"] - last["sma5"]) / last["sma5"] * 100
    row["ema20_dist_pct"] = (last["close"] - last["ema20"]) / last["ema20"] * 100
    row["bb_position"] = (
        (last["close"] - last["bb_middle"])
        / (last["bb_upper"] - last["bb_middle"])
        if (last["bb_upper"] - last["bb_middle"]) != 0
        else 0
    )

    row["mean_reversion_flag"] = mean_reversion_flag(row)
    row["volatility_flag"] = volatility_flag(row)
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

# ------------------------
# Indices (build first)
# ------------------------
indices_daily = build_indices_snapshot("daily")
indices_weekly = build_indices_snapshot("weekly")
indices_monthly = build_indices_snapshot("monthly")

# ------------------------
# APPLY MULTI-TIMEFRAME CONFIDENCE
# ------------------------
weekly_map = indices_weekly.set_index("symbol")
monthly_map = indices_monthly.set_index("symbol")

confidence_scores = []

for _, daily_row in indices_daily.iterrows():
    symbol = daily_row["symbol"]

    if symbol in weekly_map.index and symbol in monthly_map.index:
        weekly_row = weekly_map.loc[symbol]
        monthly_row = monthly_map.loc[symbol]
        score = confidence_score(daily_row, weekly_row, monthly_row)
    else:
        score = 0  # safety fallback

    confidence_scores.append(score)

indices_daily["confidence_score"] = confidence_scores

# ------------------------
# SAVE INDICES CSVs
# ------------------------
indices_daily.to_csv(
    BOT_SNAPSHOT_DIR / "indices_daily.csv", index=False
)
indices_weekly.to_csv(
    BOT_SNAPSHOT_DIR / "indices_weekly.csv", index=False
)
indices_monthly.to_csv(
    BOT_SNAPSHOT_DIR / "indices_monthly.csv", index=False
)


# ------------------------
# Stocks (build first)
# ------------------------
stocks_daily = build_stocks_snapshot("daily")
stocks_weekly = build_stocks_snapshot("weekly")
stocks_monthly = build_stocks_snapshot("monthly")

# ------------------------
# APPLY MULTI-TIMEFRAME CONFIDENCE (STOCKS)
# ------------------------
weekly_map = stocks_weekly.set_index("symbol")
monthly_map = stocks_monthly.set_index("symbol")

confidence_scores = []

for _, daily_row in stocks_daily.iterrows():
    symbol = daily_row["symbol"]

    if symbol in weekly_map.index and symbol in monthly_map.index:
        weekly_row = weekly_map.loc[symbol]
        monthly_row = monthly_map.loc[symbol]
        score = confidence_score(daily_row, weekly_row, monthly_row)
    else:
        score = 0  # safety fallback

    confidence_scores.append(score)

stocks_daily["confidence_score"] = confidence_scores

# ------------------------
# SAVE STOCK CSVs
# ------------------------
stocks_daily.to_csv(
    BOT_SNAPSHOT_DIR / "stocks_daily.csv", index=False
)
stocks_weekly.to_csv(
    BOT_SNAPSHOT_DIR / "stocks_weekly.csv", index=False
)
stocks_monthly.to_csv(
    BOT_SNAPSHOT_DIR / "stocks_monthly.csv", index=False
)

print("✅ Snapshot CSVs updated successfully")


if __name__ == "__main__":
    main()
