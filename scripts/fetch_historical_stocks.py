import json
import os
import time
import yfinance as yf
import pandas as pd

CONFIG_PATH = "config/stocks_nifty500.json"
OUT_DIR = "data/stocks/NIFTY500"

os.makedirs(OUT_DIR, exist_ok=True)

with open(CONFIG_PATH, "r") as f:
    symbols = json.load(f)

print(f"📦 Total symbols: {len(symbols)}")

BATCH_SIZE = 25
SLEEP_BETWEEN = 2

def fetch_symbol(sym):
    try:
        ticker = yf.Ticker(sym)
        df = ticker.history(period="max", auto_adjust=False)

        if df.empty:
            print(f"⚠️ No data: {sym}")
            return

        df.reset_index(inplace=True)
        df.rename(columns=str.lower, inplace=True)

        out_file = os.path.join(OUT_DIR, f"{sym.replace('.', '_')}.csv")
        df.to_csv(out_file, index=False)

        print(f"✅ Saved {sym} ({len(df)} rows)")

    except Exception as e:
        print(f"❌ Error {sym}: {e}")

for i in range(0, len(symbols), BATCH_SIZE):
    batch = symbols[i:i + BATCH_SIZE]
    print(f"\n🚀 Batch {i+1} → {i+len(batch)}")

    for sym in batch:
        fetch_symbol(sym)

    time.sleep(SLEEP_BETWEEN)

print("🎯 NIFTY 500 historical fetch complete.")
