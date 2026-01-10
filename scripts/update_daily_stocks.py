import json
import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

# ------------------------
# CONFIG
# ------------------------
CONFIG_PATH = "config/stocks_nifty500.json"
DATA_DIR = "data/stocks/NIFTY500"

with open(CONFIG_PATH, "r") as f:
    symbols = json.load(f)

# ------------------------
# UPDATE FUNCTION
# ------------------------
def update_symbol(sym):
    # Ensure Yahoo NSE symbol
    yahoo_symbol = sym if sym.endswith(".NS") else f"{sym}.NS"

    file_path = os.path.join(DATA_DIR, f"{sym.replace('.', '_')}.csv")

    if not os.path.exists(file_path):
        print(f"❌ Missing file for {sym}, skipping")
        return

    # ------------------------
    # LOAD EXISTING CSV
    # ------------------------
    df_old = pd.read_csv(file_path)

    if "date" not in df_old.columns:
        print(f"❌ {sym} CSV missing date column, skipping")
        return

    # Normalize old dates → date (not Timestamp)
    df_old["date"] = pd.to_datetime(df_old["date"], errors="coerce").dt.date
    df_old.dropna(subset=["date"], inplace=True)

    if df_old.empty:
        start_date = "2000-01-01"
    else:
        start_date = (max(df_old["date"]) + timedelta(days=1)).strftime("%Y-%m-%d")

    today = datetime.utcnow().strftime("%Y-%m-%d")

    if start_date >= today:
        print(f"⏭️ {sym} already up-to-date")
        return

    # ------------------------
    # FETCH FROM YAHOO
    # ------------------------
    try:
        df_new = yf.download(
            yahoo_symbol,
            start=start_date,
            end=today,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,   # REQUIRED for GitHub Actions stability
        )

        if df_new is None or df_new.empty:
            print(f"⚠️ No new data for {sym}")
            return

        df_new.reset_index(inplace=True)

        # ------------------------
        # HARD DATE NORMALIZATION (CRITICAL)
        # ------------------------
        if "Date" in df_new.columns:
            df_new.rename(columns={"Date": "date"}, inplace=True)
        elif "Datetime" in df_new.columns:
            df_new.rename(columns={"Datetime": "date"}, inplace=True)
        else:
            print(f"⚠️ {sym} no Date column returned, skipping")
            return

        df_new["date"] = pd.to_datetime(df_new["date"], errors="coerce").dt.date
        df_new.dropna(subset=["date"], inplace=True)

        if df_new.empty:
            print(f"⚠️ {sym} no valid rows after date normalization")
            return

        # Normalize column names
        df_new.rename(columns=str.lower, inplace=True)

        # ------------------------
        # MERGE & SAVE
        # ------------------------
        df_final = pd.concat([df_old, df_new], ignore_index=True)
        df_final.drop_duplicates(subset=["date"], inplace=True)
        df_final.sort_values("date", inplace=True)

        df_final.to_csv(file_path, index=False)
        print(f"✅ Updated {sym} (+{len(df_new)} rows)")

    except Exception as e:
        print(f"❌ Error {sym}: {e}")

# ------------------------
# RUN
# ------------------------
print(f"📦 Updating {len(symbols)} stocks")

for sym in symbols:
    update_symbol(sym)

print("🎯 Daily NIFTY 500 update completed")
