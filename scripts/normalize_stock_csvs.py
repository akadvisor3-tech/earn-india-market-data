import os
import pandas as pd

DATA_DIR = "data/stocks/NIFTY500"

print("🔧 Normalizing NIFTY 500 stock CSVs")

for file in os.listdir(DATA_DIR):
    if not file.endswith(".csv"):
        continue

    path = os.path.join(DATA_DIR, file)

    try:
        df = pd.read_csv(path)

        # --- FIX COLUMN NAMES ---
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        if "date" not in df.columns:
            print(f"❌ Skipping {file} (no date column)")
            continue

        # --- FIX DATE ---
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["date"] = df["date"].dt.tz_localize(None)

        df.dropna(subset=["date"], inplace=True)
        df.sort_values("date", inplace=True)
        df.drop_duplicates(subset=["date"], inplace=True)

        df.to_csv(path, index=False)
        print(f"✅ Normalized {file}")

    except Exception as e:
        print(f"❌ Failed {file}: {e}")

print("🎯 Stock CSV normalization completed")
