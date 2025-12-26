import json
import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

CONFIG_PATH = "config/stocks_nifty500.json"
DATA_DIR = "data/stocks/NIFTY500"

with open(CONFIG_PATH, "r") as f:
    symbols = json.load(f)

def update_symbol(sym):
    file_path = os.path.join(DATA_DIR, f"{sym.replace('.', '_')}.csv")

    if not os.path.exists(file_path):
        print(f"❌ Missing file for {sym}, skipping")
        return

    df_old = pd.read_csv(file_path)
    df_old["date"] = pd.to_datetime(df_old["date"])

    last_date = df_old["date"].max()
    fetch_from = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.utcnow().strftime("%Y-%m-%d")

    if fetch_from >= today:
        print(f"⏭️ {sym} already up-to-date")
        return

    try:
        df_new = yf.download(
            sym,
            start=fetch_from,
            end=today,
            progress=False,
            auto_adjust=False
        )

        if df_new.empty:
            print(f"⚠️ No new data for {sym}")
            return

        df_new.reset_index(inplace=True)
        df_new.rename(columns=str.lower, inplace=True)

        df_final = pd.concat([df_old, df_new], ignore_index=True)
        df_final.drop_duplicates(subset=["date"], inplace=True)
        df_final.sort_values("date", inplace=True)

        df_final.to_csv(file_path, index=False)
        print(f"✅ Updated {sym} (+{len(df_new)} rows)")

    except Exception as e:
        print(f"❌ Error {sym}: {e}")

print(f"📦 Updating {len(symbols)} stocks")

for sym in symbols:
    update_symbol(sym)

print("🎯 Daily NIFTY 500 update completed")
