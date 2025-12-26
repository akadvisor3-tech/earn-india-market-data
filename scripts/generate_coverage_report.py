import os
import pandas as pd

BASE_DIR = "data/stocks/NIFTY500"
rows = []

for file in sorted(os.listdir(BASE_DIR)):
    if not file.endswith(".csv"):
        continue

    path = os.path.join(BASE_DIR, file)
    df = pd.read_csv(path)

    if df.empty or "date" not in df.columns:
        continue

    df["date"] = pd.to_datetime(df["date"])

    rows.append({
        "symbol": file.replace(".csv", ""),
        "start_date": df["date"].min().date(),
        "end_date": df["date"].max().date(),
        "total_days": len(df)
    })

report = pd.DataFrame(rows)
report = report.sort_values("symbol")

out = "reports/nifty500_data_coverage.csv"
os.makedirs("reports", exist_ok=True)
report.to_csv(out, index=False)

print(f"✅ Coverage report saved to {out}")
print(f"📊 Total stocks covered: {len(report)}")
