import os
import pandas as pd

DATA_DIR = "data/stocks/NIFTY500"

print("🔧 Normalizing stock CSV date format...")

files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]

for fname in files:
    path = os.path.join(DATA_DIR, fname)

    try:
        df = pd.read_csv(path)

        if "date" not in df.columns:
            print(f"❌ {fname}: missing date column")
            continue

        # Convert to datetime safely
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

        # Drop bad rows
        df.dropna(subset=["date"], inplace=True)

        # 🔥 CRITICAL STEP — normalize like indices
        df["date"] = df["date"].dt.date.astype(str)

        # Sort + dedupe
        df.drop_duplicates(subset=["date"], inplace=True)
        df.sort_values("date", inplace=True)

        df.to_csv(path, index=False)
        print(f"✅ Fixed {fname}")

    except Exception as e:
        print(f"❌ Error fixing {fname}: {e}")

print("🎯 Stock CSV normalization complete")
