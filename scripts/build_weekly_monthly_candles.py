import os
import pandas as pd

BASE_PATHS = [
    "data/indices",
    "data/stocks/NIFTY500"
]

def build_candles(base_path):
    weekly_dir = os.path.join(base_path, "weekly")
    monthly_dir = os.path.join(base_path, "monthly")

    os.makedirs(weekly_dir, exist_ok=True)
    os.makedirs(monthly_dir, exist_ok=True)

    for file in os.listdir(base_path):
        if not file.endswith(".csv"):
            continue

        file_path = os.path.join(base_path, file)
        print(f"📊 Processing {file_path}")

        df = pd.read_csv(file_path)

        if "date" not in df.columns:
            print(f"⚠️ Skipped (no date column): {file}")
            continue

        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)

        # Weekly candles (Mon–Fri logic handled by pandas automatically)
        weekly = df.resample("W-FRI").agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }).dropna()

        weekly.to_csv(os.path.join(weekly_dir, file))

        # Monthly candles
        monthly = df.resample("M").agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum"
        }).dropna()

        monthly.to_csv(os.path.join(monthly_dir, file))

        print(f"✅ Done: {file}")

for path in BASE_PATHS:
    if os.path.exists(path):
        build_candles(path)

print("🎯 Weekly & Monthly candle build complete.")
