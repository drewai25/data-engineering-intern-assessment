from pathlib import Path
import pandas as pd

RAW_DIR = Path("raw")
PROCESSED_DIR = Path("processed")
ANALYTICS_DIR = Path("analytics")
REPORTS_DIR = Path("reports")

def main():
    # Create required folders
    for d in [RAW_DIR, PROCESSED_DIR, ANALYTICS_DIR, REPORTS_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    # Find a CSV in raw/
    csv_files = sorted(RAW_DIR.glob("*.csv"))
    if not csv_files:
        raise SystemExit("❌ No CSV found in raw/. Put your dataset CSV into raw/ and rerun.")

    csv_path = csv_files[0]
    print(f"Using CSV: {csv_path}")

    # Step 1: RAW -> Parquet (no modifications)
    # World Bank API CSVs often have metadata lines before the header row.
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    header_idx = None
    for i, line in enumerate(lines[:200]):
        s = line.strip().lstrip("\ufeff").replace('"', '').lower()
        if s.startswith("country name"):
            header_idx = i
            break

    if header_idx is None:
        raise ValueError("Could not locate header row.")
    df_raw = pd.read_csv(csv_path, skiprows=header_idx, engine="python")
    df_raw = df_raw.dropna(how="all")  # remove fully-empty rows if any
    df_raw.to_parquet(RAW_DIR / "dataset_raw.parquet", index=False)
    print("✅ Wrote raw/dataset_raw.parquet")

    # Step 2: Clean -> PROCESSED
    df = df_raw.copy()

    # Trim whitespace in text columns (do NOT convert NaN to "nan")
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype("string").str.strip()

    # Drop fully empty columns (e.g., 'Unnamed' artifacts)
    df = df.dropna(axis=1, how="all")

    # Drop exact duplicates
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)

    df.to_parquet(PROCESSED_DIR / "dataset_processed.parquet", index=False)
    print("✅ Wrote processed/dataset_processed.parquet")

    # Data quality report
    missing_pct = (df.isna().mean() * 100).sort_values(ascending=False)
    report_path = REPORTS_DIR / "data_quality_report.md"
    with report_path.open("w", encoding="utf-8") as f:
        f.write("# Data Quality Report\n\n")
        f.write(f"- Rows before dedupe: **{before}**\n")
        f.write(f"- Rows after dedupe: **{after}**\n\n")
        f.write("## Missingness (% per column)\n\n")
        f.write(missing_pct.to_string())
        f.write("\n")
    print("✅ Wrote reports/data_quality_report.md")

    # Step 3: Analytics outputs (2 tables)

    # Output 1: Column profile
    col_profile = pd.DataFrame({
        "column": df.columns,
        "dtype": [str(t) for t in df.dtypes],
        "missing_pct": (df.isna().mean() * 100).values,
        "unique_count": [df[c].nunique(dropna=True) for c in df.columns],
    })
    col_profile.to_parquet(ANALYTICS_DIR / "column_profile.parquet", index=False)
    print("✅ Wrote analytics/column_profile.parquet")

    # Output 2: Group summary (prefer World Bank standard columns if present)
    preferred = ["Country Name", "Country Code", "Indicator Name", "Indicator Code"]
    group_col = next((c for c in preferred if c in df.columns), None)

    if group_col is None:
        text_cols = list(df.select_dtypes(include=["object", "string"]).columns)
        group_col = text_cols[0] if text_cols else None

    if group_col:
        group_summary = df.groupby(group_col, dropna=False).size().reset_index(name="row_count")
    else:
        group_summary = pd.DataFrame({"row_count": [len(df)]})

    group_summary.to_parquet(ANALYTICS_DIR / "group_summary.parquet", index=False)
    print("✅ Wrote analytics/group_summary.parquet")

    print("\n🎉 Pipeline complete!")

if __name__ == "__main__":
    main()
