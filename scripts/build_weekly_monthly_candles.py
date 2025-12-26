import os
import pandas as pd

BASE_PATHS = [
    ("data/indices/daily", "data/indices"),
    ("data/stocks/NIFTY500", "data/stocks/NIFTY500"),
]

def build_timeframes(daily_csv, weekly_out, monthly_out):
    df = pd.read_csv(daily_csv, parse_dates=["date"])
    if df.empty:
        return

    df = df.sort_values("date")
    df.set_index("date", inplace=True)

    ohlc = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    }

    weekly = df.resample("W-FRI").agg(ohlc).dropna()
    monthly = df.resample("M").agg(ohlc).dropna()

    weekly.reset_index().to_csv(weekly_out, index=False)
    monthly.reset_index().to_csv(monthly_out, index=False)

for daily_root, out_root in BASE_PATHS:
    weekly_dir = os.path.join(out_root, "weekly")
    monthly_dir = os.path.join(out_root, "monthly")

    os.makedirs(weekly_dir, exist_ok=True)
    os.makedirs(monthly_dir, exist_ok=True)

    for file in os.listdir(daily_root):
        if not file.endswith(".csv"):
            continue

        name = file.replace(".csv", "")
        daily_csv = os.path.join(daily_root, file)

        build_timeframes(
            daily_csv,
            os.path.join(weekly_dir, f"{name}.csv"),
            os.path.join(monthly_dir, f"{name}.csv"),
        )

print("✅ Weekly & Monthly candles generated")
