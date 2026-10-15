# buenavista-csv

Standalone repository for the file-based data sources (**F1** and **F4**) of the **HospedaPlus Revenue** platform (`US-5` & `US-6`). This repository is decoupled from the streaming (`buenavista-streaming`), database (`buenavista-simulated-db`), and API (`buenavista-api`) repositories.

## 1. Data Sources Scope

| ID | Source | Type | Frequency | Scope & Volume | Provenance / License |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F1** | Hotel Booking Demand | Historical CSV | One-time / Backfill | 119,390 rows × 32 cols (`City Hotel` & `Resort Hotel`) | [Kaggle Dataset](https://www.google.com/search?q=https%3A%2F%2Fwww.kaggle.com%2Fdatasets%2Fjessemostipak%2Fhotel-booking-demand) (`CC BY 4.0`) |
| **F4** | Traditional Agency Files | Simulated CSV | Weekly Batch | 15 agencies (~183 rows/week across Layouts A & B) | Synthetic generator (`seed=7`) |

## 2. Repository Structure

```text
buenavista-csv/
├── pyproject.toml                  # Package configuration & dependencies
├── .env.example                    # Environment variables template
├── schemas/
│   ├── f1_schema.json              # Canonical 32-column contract & IQR bounds for F1
│   └── f4_layouts_schema.json      # Dual-layout contract (Layout A & Layout B) for F4
├── data/
│   ├── f1_historical/
│   │   └── hotel_bookings.csv      # F1 raw Kaggle dataset
│   └── f2_reference/
│       └── events.jsonl            # Reference F2 events for cross-channel duplicate generation
├── src/buenavista_csv/
│   ├── defects.py                  # Unified DefectRates configuration (US-14 / US-26)
│   ├── f1_audit.py                 # F1 schema profiler & IQR outlier auditor (US-11)
│   ├── f4_generator.py             # F4 weekly CSV generator (US-25 / US-26)
│   └── validator.py                # Non-destructive batch quality validator (US-6)
├── output/                         # Generated audit, agency CSVs, and validation reports
├── tests/                          # Unit tests & malformed CSV fixtures (US-6 AC2)
└── docs/data-sources/              # Full Docusaurus technical documentation
```

## 3. Requirements & Setup

* **Python:** `>=3.11`
* **Package Manager:** `uv` (recommended) or `pip`

```bash
# Clone repository and install dependencies
git clone <repo-url>
cd buenavista-csv
uv sync --extra test
```

## 4. Usage & Validation Commands

Run all commands from the root of `buenavista-csv`:

```bash
# 1. Profile F1 schema, null tokens, and ADR outliers via IQR (US-11 / US-5)
uv run -m buenavista_csv.f1_audit

# 2. Generate 15 weekly F4 agency CSVs with configurable defects (US-25 / US-26)
uv run -m buenavista_csv.f4_generator \
  --seed 7 \
  --bookings-per-agency 12 \
  --duplicate-count 3 \
  --missing-currency-rate 0.10

# 3. Run non-destructive quality checks on F1 and F4 (US-6)
uv run -m buenavista_csv.validator

# 4. Run automated unit tests (including malformed fixture checks)
uv run python -m unittest discover -s tests -v
```

## 5. Key Outputs

* **F1 Audit Report:** `output/f1_audit/audit_report.csv` and `output/f1_audit/audit_summary.txt`
* **F4 Agency Files:** `output/f4_agency_files/*_week.csv` (15 files) and `_generation_summary.json`
* **Quality Validation Report:** `output/validation_reports/quality_validation_report.json`

## 6. Documentation

Detailed architectural role, Medallion (`Bronze` → `Silver` → `Gold`) consumption contracts, IQR outlier mathematics, and business rules (`RN-01`, `RN-02`, `RN-03`, `RN-05`, `RN-08`, `RN-11`) are documented in `docs/data-sources/index.md` for the shared Docusaurus site.
