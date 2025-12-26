import os
import pandas as pd

DATA_DIR = "data/indices"
REPORT_DIR = "reports"
OUT_FILE = os.path.join(REPORT_DIR, "indices_data_coverage.csv")

os.makedirs(REPORT_DIR, exist_ok=True)

rows = []

for file in sorted(os.listdir(DATA_DIR)):
    if not file.endswith(".csv"):
        continue

    path = os.path.join(DATA_DIR, file)
    symbol = file.replace(".csv", "")

    try:
        df = pd.read_csv(path)

        if df.empty or "date" not in df.columns:
            print(f"⚠️ Skipped {symbol}: empty or invalid")
            continue

        df["date"] = pd.to_datetime(df["date"])
        df = df.drop_duplicates(subset=["date"]).sort_values("date")

        rows.append({
            "index": symbol,
            "start_date": df["date"].min().date(),
            "end_date": df["date"].max().date(),
            "total_days": len(df)
        })

        print(f"✅ {symbol}: {len(df)} rows")

    except Exception as e:
        print(f"❌ Error processing {symbol}: {e}")

report_df = pd.DataFrame(rows).sort_values("index")
report_df.to_csv(OUT_FILE, index=False)

print(f"\n📊 Index coverage report saved → {OUT_FILE}")
