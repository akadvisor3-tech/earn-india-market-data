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

# -----------------------------
# LOAD INDICES
# -----------------------------
with open(CONFIG_FILE, "r") as f:
    indices = json.load(f)

# -----------------------------
# UPDATE FUNCTION
# -----------------------------
def update_index(name, yahoo_symbol):
    csv_file = DATA_DIR / f"{name}.csv"

    if not csv_file.exists():
        print(f"⚠️ {name} CSV missing, skip")
        return

    df_old = pd.read_csv(csv_file)

    if df_old.empty or "date" not in df_old.columns:
        start_date = "2000-01-01"
    else:
        df_old["date"] = pd.to_datetime(df_old["date"]).dt.normalize()
        start_date = df_old["date"].max().strftime("%Y-%m-%d")

    df_new = yf.download(
        yahoo_symbol,
        start=start_date,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df_new.empty:
        print(f"ℹ️ No new data for {name}")
        return

    df_new.reset_index(inplace=True)
    df_new = df_new[["Date", "Open", "High", "Low", "Close", "Volume"]]
    df_new.columns = ["date", "open", "high", "low", "close", "volume"]
    df_new["date"] = pd.to_datetime(df_new["date"]).dt.normalize()

    df = pd.concat([df_old, df_new], ignore_index=True)
    df.drop_duplicates(subset="date", inplace=True)
    df.sort_values("date", inplace=True)

    df.to_csv(csv_file, index=False)
    print(f"✅ Updated {name} → {len(df)} rows")

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    for name, meta in indices.items():
        update_index(name, meta["yahoo"])

    print("🎯 Daily indices update complete.")
