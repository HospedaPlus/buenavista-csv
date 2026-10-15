"""
US-11 / US-5 — F1 (Hotel Booking Demand, Kaggle) Data Profiler & Non-Destructive Auditor.

Profiles schema, data types, null/missing tokens (including literal "NULL" and "NA" strings),
zero-guest rows, and ADR statistical outliers using Tukey's 1.5x IQR fence without modifying
the source CSV file.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


DEFAULT_CSV_PATH = Path(__file__).resolve().parents[2] / "data" / "f1_historical" / "hotel_bookings.csv"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output" / "f1_audit"


def format_number(value: int) -> str:
    return f"{value:,}"


def write_section(file, title: str) -> None:
    file.write("\n" + "=" * 70 + "\n")
    file.write(f"{title}\n")
    file.write("=" * 70 + "\n")


def run_f1_audit(csv_path: Path = DEFAULT_CSV_PATH, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict:
    if not csv_path.exists():
        raise FileNotFoundError(f"F1 dataset not found at: {csv_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "audit_report.csv"
    summary_path = output_dir / "audit_summary.txt"

    df = pd.read_csv(csv_path)
    df_raw = pd.read_csv(csv_path, keep_default_na=False, low_memory=False)
    rows, columns = df.shape

    if "adr" not in df.columns:
        raise ValueError("Required column 'adr' is missing from F1 dataset.")

    audit_records = []
    for column in df.columns:
        null_count = int(df[column].isna().sum())
        non_null_count = int(df[column].notna().sum())
        unique_count = int(df[column].nunique(dropna=True))
        dtype_name = "str" if pd.api.types.is_string_dtype(df[column]) or df[column].dtype == object else str(df[column].dtype)

        audit_records.append({
            "category": "schema",
            "item": column,
            "data_type": dtype_name,
            "rows": rows,
            "non_null": non_null_count,
            "nulls": null_count,
            "null_percent": round((null_count / rows) * 100, 2),
            "unique_values": unique_count,
            "min": None,
            "max": None,
            "q1": None,
            "median": None,
            "q3": None,
            "iqr": None,
            "lower_bound": None,
            "upper_bound": None,
            "outliers": None,
            "outlier_percent": None,
            "status": "OK" if null_count == 0 else "HAS_NULLS",
        })

    adr = df["adr"]
    adr_nulls = int(adr.isna().sum())
    adr_negative = int((adr < 0).sum())
    adr_zero = int((adr == 0).sum())

    adr_clean = adr.dropna()
    adr_min = float(adr_clean.min())
    adr_max = float(adr_clean.max())
    q1 = float(adr_clean.quantile(0.25))
    median = float(adr_clean.median())
    q3 = float(adr_clean.quantile(0.75))
    iqr = q3 - q1
    lower_bound = q1 - (1.5 * iqr)
    upper_bound = q3 + (1.5 * iqr)

    adr_outlier_mask = (adr < lower_bound) | (adr > upper_bound)
    outlier_count = int(adr_outlier_mask.sum())
    outlier_percent = (outlier_count / rows) * 100

    # Zero-guest anomaly detection (adults + children + babies == 0)
    children_filled = pd.to_numeric(df["children"], errors="coerce").fillna(0)
    zero_guests_count = int(((df["adults"] == 0) & (children_filled == 0) & (df["babies"] == 0)).sum())

    # Literal string token counts in raw file
    literal_nulls = {
        col: int((df_raw[col] == "NULL").sum())
        for col in ["company", "agent", "country"]
        if col in df_raw.columns
    }
    literal_na_children = int((df_raw["children"] == "NA").sum()) if "children" in df_raw.columns else 0

    audit_records.append({
        "category": "adr",
        "item": "adr",
        "data_type": str(adr.dtype),
        "rows": rows,
        "non_null": int(adr.notna().sum()),
        "nulls": adr_nulls,
        "null_percent": round((adr_nulls / rows) * 100, 2),
        "unique_values": int(adr.nunique(dropna=True)),
        "min": round(adr_min, 2),
        "max": round(adr_max, 2),
        "q1": round(q1, 2),
        "median": round(median, 2),
        "q3": round(q3, 2),
        "iqr": round(iqr, 2),
        "lower_bound": round(lower_bound, 2),
        "upper_bound": round(upper_bound, 2),
        "outliers": outlier_count,
        "outlier_percent": round(outlier_percent, 2),
        "status": "CHECK" if adr_negative > 0 or outlier_count > 0 else "OK",
    })

    audit_report = pd.DataFrame(audit_records)
    audit_report.to_csv(report_path, index=False, encoding="utf-8-sig")

    with open(summary_path, "w", encoding="utf-8") as file:
        file.write("HOTEL BOOKING DEMAND - DATA AUDIT\n")
        file.write("Source discovery audit - B0-11 / US-5\n")

        write_section(file, "1. DATASET OVERVIEW")
        file.write(f"Dataset: {csv_path.name}\n")
        file.write(f"Rows: {format_number(rows)}\n")
        file.write(f"Columns: {format_number(columns)}\n")
        file.write(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB\n")

        write_section(file, "2. SCHEMA")
        file.write("Columns and data types:\n\n")
        for rec in audit_records[:-1]:
            file.write(f"- {rec['item']}: {rec['data_type']}\n")

        write_section(file, "3. NULL VALUES & TEXTUAL NULL TOKENS")
        total_nulls = int(df.isna().sum().sum())
        columns_with_nulls = int((df.isna().sum() > 0).sum())
        file.write(f"Total null values: {format_number(total_nulls)}\n")
        file.write(f"Columns containing nulls: {columns_with_nulls}\n\n")
        null_columns = df.isna().sum().sort_values(ascending=False)
        for col_name, count in null_columns.items():
            if count > 0:
                pct = (count / rows) * 100
                file.write(f"- {col_name}: {format_number(int(count))} ({pct:.2f}%)\n")

        write_section(file, "4. ADR ANALYSIS")
        file.write(f"Data type: {adr.dtype}\n")
        file.write(f"Null values: {format_number(adr_nulls)}\n")
        file.write(f"Negative values: {format_number(adr_negative)}\n")
        file.write(f"Zero values: {format_number(adr_zero)}\n")
        file.write(f"Minimum: {adr_min:.2f}\n")
        file.write(f"Maximum: {adr_max:.2f}\n")
        file.write(f"Mean: {adr_clean.mean():.2f}\n")
        file.write(f"Median: {median:.2f}\n")

        write_section(file, "5. ADR OUTLIER DETECTION")
        file.write("Method: IQR (Interquartile Range)\n\n")
        file.write(f"Q1: {q1:.2f}\n")
        file.write(f"Q3: {q3:.2f}\n")
        file.write(f"IQR: {iqr:.2f}\n")
        file.write(f"Lower bound: {lower_bound:.2f}\n")
        file.write(f"Upper bound: {upper_bound:.2f}\n")
        file.write(f"Detected outliers: {format_number(outlier_count)}\n")
        file.write(f"Outlier percentage: {outlier_percent:.2f}%\n")

        write_section(file, "6. EXTREME ADR VALUES")
        file.write("\nLowest ADR values:\n")
        for val in adr_clean.sort_values().head(10):
            file.write(f"- {val:.2f}\n")
        file.write("\nHighest ADR values:\n")
        for val in adr_clean.sort_values(ascending=False).head(10):
            file.write(f"- {val:.2f}\n")

        write_section(file, "7. AUDIT STATUS")
        if adr_negative > 0:
            file.write("WARNING: Negative ADR values were detected.\n")
        else:
            file.write("OK: No negative ADR values were detected.\n")
        if outlier_count > 0:
            file.write(f"REVIEW: {format_number(outlier_count)} ADR values were identified as statistical outliers using the IQR method.\n")
        if zero_guests_count > 0:
            file.write(f"REVIEW: {format_number(zero_guests_count)} bookings have 0 adults, 0 children, and 0 babies.\n")
        file.write("\nAudit completed successfully.\n")

    return {
        "rows": rows,
        "columns": columns,
        "total_nulls": total_nulls,
        "literal_null_tokens": literal_nulls,
        "literal_na_children": literal_na_children,
        "zero_guests_count": zero_guests_count,
        "adr": {
            "min": round(adr_min, 2),
            "max": round(adr_max, 2),
            "mean": round(float(adr_clean.mean()), 2),
            "median": round(median, 2),
            "q1": round(q1, 2),
            "q3": round(q3, 2),
            "iqr": round(iqr, 2),
            "lower_bound": round(lower_bound, 2),
            "upper_bound": round(upper_bound, 2),
            "negative_count": adr_negative,
            "zero_count": adr_zero,
            "outlier_count": outlier_count,
            "outlier_percent": round(outlier_percent, 2),
        },
        "report_path": str(report_path),
        "summary_path": str(summary_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit and profile F1 Hotel Booking Demand CSV.")
    parser.add_argument("--csv-path", type=Path, default=DEFAULT_CSV_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    res = run_f1_audit(args.csv_path, args.output_dir)
    print(f"F1 Audit completed: {res['rows']:,} rows, {res['columns']} cols | Outliers: {res['adr']['outlier_count']:,} ({res['adr']['outlier_percent']}%)")


if __name__ == "__main__":
    main()
