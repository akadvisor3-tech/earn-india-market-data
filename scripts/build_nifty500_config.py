import pandas as pd
import json
from pathlib import Path

# Official NSE NIFTY 500 CSV
NSE_CSV_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

CONFIG_PATH = Path("config/stocks_nifty500.json")
CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)

print("📥 Downloading NIFTY 500 list from NSE...")
df = pd.read_csv(NSE_CSV_URL)

stocks = {}

for _, row in df.iterrows():
    symbol = row["Symbol"].strip()
    stocks[symbol] = {
        "yahoo": f"{symbol}.NS",
        "sector": row.get("Industry", "Unknown")
    }

with open(CONFIG_PATH, "w") as f:
    json.dump(stocks, f, indent=2)

print(f"✅ Generated {len(stocks)} NIFTY 500 symbols")
print(f"📁 Saved to {CONFIG_PATH}")
