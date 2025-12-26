import json
import pandas as pd
import yfinance as yf
from pathlib import Path

# -----------------------------
# PATHS
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = BASE_DIR / "config" / "indices.json"
DATA_DIR = BASE_DIR / "data" / "indices"

DATA_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# LOAD INDICES
# -----------------------------
with open(CONFIG_FILE, "r") as f:
    indices = json.load(f)

# -----------------------------
# FETCH FUNCTION
# -----------------------------
def fetch_index(name, yahoo_symbol):
    print(f"📥 Fetching {name} ({yahoo_symbol})")

    csv_file = DATA_DIR / f"{name}.csv"

    df = yf.download(
        yahoo_symbol,
        period="max",
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df.empty:
        print(f"⚠️ No data for {name}")
        return

    df.reset_index(inplace=True)
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]
    df.columns = ["date", "open", "high", "low", "close", "volume"]

    df["date"] = pd.to_datetime(df["date"]).dt.date

    df.sort_values("date", inplace=True)
    df.to_csv(csv_file, index=False)

    print(f"✅ Saved {name} → {len(df)} rows")

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    for name, meta in indices.items():
        fetch_index(name, meta["yahoo"])

    print("\n🎯 Historical indices fetch complete.")
