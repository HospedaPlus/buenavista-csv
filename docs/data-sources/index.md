---
id: file-sources-f1-f4
title: "File-Based (CSV) Data Sources: F1 (Historical Demand) & F4 (Traditional Agencies)"
sidebar_label: "F1 & F4: File Sources (CSV)"
sidebar_position: 2
description: "Comprehensive technical specification, architectural role, data profiling, IQR outlier mathematics, defect injection contracts, and batch consumption strategy for HospedaPlus Revenue sources F1 and F4."
tags: [data-sources, f1, f4, batch-layer, medallion-bronze, data-quality, spark, airflow]
---

# File-Based (CSV) Data Sources: F1 (Historical Demand) & F4 (Traditional Agencies)

| Attribute | Specification |
| :--- | :--- |
| **Canonical Repository** | `buenavista-csv` |
| **Domain Owner** | Oscar Corzo (Data Engineering — File Sources Domain) |
| **Covered Sources** | **F1** (`hotel_bookings.csv` — Kaggle Historical CSV) and **F4** (`*_week.csv` — Simulated Traditional Agency Files) |
| **Architectural Layer** | **Batch Layer** (Daily / Weekly Scheduled Ingestion → Amazon S3 `Bronze` → PySpark `Silver` & `Gold`) |
| **Related User Stories** | `US-11` (F1 Exploration & IQR Audit), `US-14` (Defects Catalog), `US-24` (F2 Event Coupling), `US-25` (F4 Generator), `US-26` (Configurable Defect Injection), `US-5` (Repo Separation), `US-6` (Quality Controls & Batch Readiness) |
| **Business Rules Supported** | `RN-01` (Occupancy, Net ADR, Net RevPAR), `RN-02` (Channel Commissions), `RN-03` (90-Day Cancellation Baseline for Overbooking), `RN-05` (14-Day YoY Pickup Baseline), `RN-08` (Cross-Channel Deduplication ±1 Day), `RN-11` (Guest PII Pseudonymization) |
| **Runtime Status** | `Done` — Audited, Validated, Deterministically Seeded, and Batch-Ready |

:::info Repository Boundary Contract (US-5 & US-6)
Per the team's domain separation architecture, `buenavista-csv` exclusively houses standalone CSV/Excel data sources (**F1** and **F4**), their canonical JSON schemas, deterministic file generators, and non-destructive data-quality validators. It operates with **zero runtime coupling** to the FastAPI guest reviews service (**F5**, `buenavista-api`), the RabbitMQ streaming broker (**F2**, `buenavista-streaming`), or the operational PostgreSQL database (**F3**, `buenavista-simulated-db`).
:::

## 1. Architectural Role enables Batch Revenue Intelligence without Blocking Real-Time Inventory

HospédaPlus Revenue implements a hybrid **Lambda + Medallion Architecture** on AWS (`us-east-2`) to reconcile two competing operational demands: sub-250 ms real-time inventory protection (`RNF-01`, `RNF-02`) and deep historical recalculation of dynamic rates and overbooking ceilings before **06:00 local hotel time** (`RNF-03`).

Within this topology, **F1** and **F4** serve as the foundational file-based inputs of the **Batch Layer**:

1. **Source F1 (`Hotel Booking Demand`)** provides **119,390 historical bookings** (2015–2017) across two reference property profiles (`City Hotel` and `Resort Hotel`). Because cold-start operational databases (`F3`) lack multi-year seasonality and lead-time cancellation curves, F1 supplies the historical baseline required to compute **90-day cancellation probabilities by lead time (`RN-03`)** and **14-day year-over-year (YoY) pickup multipliers (`RN-05`)**.
2. **Source F4 (`Traditional Agency Files`)** simulates weekly batch drops from **15 brick-and-mortar travel agencies** (`~10%` commission per `RN-02`). Traditional agencies do not integrate with real-time REST APIs or RabbitMQ event streams; instead, they submit periodic CSV/Excel spreadsheets in heterogeneous column layouts containing both net-new reservations and **cross-channel duplicate bookings** that already arrived via OTAs (`F2`) or the direct booking engine (`F3`).

### End-to-End Data Flow across the Medallion Architecture

```
+---------------------------------------------------------------------------------------------------+
| 1. SOURCES (buenavista-csv)                                                                       |
|   * F1: data/f1_historical/hotel_bookings.csv (119,390 historical rows, one-time + backfill)      |
|   * F4: output/f4_agency_files/*_week.csv     (15 weekly agency CSVs, alternating Layouts A & B)  |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
| 2. BATCH ORCHESTRATION & QUALITY GATE (VPC Private Subnet - EC2 t3.large + Celery)                |
|   * Apache Airflow Scheduler dispatches tasks via RabbitMQ (vhost: "airflow") to Celery Workers.  |
|   * Pre-ingestion Quality Gate (`buenavista_csv.validator`) runs non-destructive schema & anomaly |
|     checks (US-6), emitting `quality_validation_report.json` without rewriting raw files.         |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
| 3. DATA LAKE LANDING — AMAZON S3 BRONZE (Raw Immutable Zone)                                      |
|   * s3://hospedaplus-datalake-bronze/f1_historical/load_date=YYYY-MM-DD/hotel_bookings.csv        |
|   * s3://hospedaplus-datalake-bronze/f4_agencies/year=YYYY/week=WW/<agency_slug>_week.csv         |
|   * Cost Optimization: S3 Lifecycle rule transitions Bronze objects to Standard-IA after 30 days. |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
| 4. DISTRIBUTED PROCESSING — APACHE SPARK ON AWS ECS FARGATE SPOT (Bronze -> Silver -> Gold)       |
|   * Bronze -> Silver (Cleaned & Conformed Parquet):                                               |
|       - Enforces explicit PySpark `StructType` schemas (Topic 13 — no schema inference).          |
|       - Converts F1 literal `"NULL"` / `"NA"` strings to typed nulls (`agent`, `company`).        |
|       - Filters/quarantines F1 negative ADR (-$6.38), Giant Outlier ($5,400.00), & 0-guest rows.  |
|       - Unifies F4 Layout A and Layout B into the canonical reservation structure.                |
|       - Resolves missing F4 currencies (Defect 3) via agency default lookup or quarantine flag.   |
|       - Deduplicates F4 vs. F2/F3 bookings within +/-1 day check-in window, keeping the channel   |
|         with the lowest commission rate (RN-08, CA-06: Direct 0% < Agency 10% < OTA 18%).         |
|       - Pseudonymizes `guest_name` / `guest_full_name` via SHA-256 salted hashing (RN-11, CA-10). |
|   * Silver -> Gold (Dimensional Star Schema in S3 Parquet):                                       |
|       - Populates `fact_reservations`, `fact_daily_revenue`, `dim_hotel`, `dim_channel`,          |
|         `dim_room_type`, and `dim_date` partitioned by `hotel_id` and `year_month`.               |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                         +------------------------+------------------------+
                         |                                                 |
                         v                                                 v
+------------------------------------------------+ +------------------------------------------------+
| 5A. ANALYTICS & BI (Serving Layer)             | | 5B. OPERATIONAL FEEDBACK LOOP (Before 06:00)   |
|   * AWS Glue Crawler updates Glue Data Catalog | |   * Daily suggested prices (RN-05, RN-06) and  |
|   * Amazon Athena runs serverless SQL over Gold| |     overbooking limits (RN-03) are written     |
|   * Power BI / QuickSight dashboards consume   | |     back to RDS PostgreSQL Multi-AZ so FastAPI |
|     KPIs for Revenue Managers & Hotel GMs      | |     serves updated rates in <250 ms (RNF-01).  |
+------------------------------------------------+ +------------------------------------------------+
```

## 2. Source F1: Hotel Booking Demand (`hotel_bookings.csv`) Profiles Historical Seasonality and Cancellation Risk

### Provenance, Licensing, and Operational Mapping

| Metadata Field | Value / Specification |
| :--- | :--- |
| **Source ID & Name** | **F1** — Hotel Booking Demand Dataset (`DS1` in Sprint 0 backlog) |
| **Upstream Origin** | [Kaggle — Hotel Booking Demand](https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand) (Originally curated by Nuno Antonio, Ana de Almeida, and Luis Nunes, *Data in Brief*, 2019) |
| **License** | Creative Commons Attribution 4.0 International (`CC BY 4.0`) |
| **File Path in Repo** | `data/f1_historical/hotel_bookings.csv` |
| **File Format & Encoding** | Comma-separated values (`.csv`), `UTF-8` encoding, header row on line 1 |
| **Dataset Volume** | **119,390 rows** × **32 columns** (**16.86 MB** on disk; **93.90 MB** in-memory DataFrame footprint) |
| **Temporal Coverage** | Arrival dates spanning **July 2015 through August 2017** (`2015`: 21,996 rows; `2016`: 56,707 rows; `2017`: 40,687 rows) |
| **Property Mapping** | Contains `City Hotel` (**79,330 rows**, 66.45%) and `Resort Hotel` (**40,060 rows**, 33.55%) from Portugal, mapped per Section 14 of the project brief as the historical baseline for two reference properties (`HTL-001` Urban and `HTL-002` Vacation) in the HospedaPlus chain |
| **Ingestion Frequency** | Historical one-time seed into S3 `Bronze` (`f1_historical/`), with idempotent partition overwrite support for backfills (`CT-08`) |

### Complete 32-Column Canonical Schema and Audit Profile (US-11)

The table below reflects the exact output produced by `buenavista_csv.f1_audit` (`output/f1_audit/audit_report.csv`) across all **119,390 records**:

| # | Column Name | Pandas Dtype | PySpark Target Type | Non-Null Count | Null / Missing Count | Null % | Unique Values | Sample Values & Domain Notes |
| :- | :--- | :--- | :--- | ---: | ---: | ---: | ---: | :--- |
| 1 | `hotel` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 2 | `Resort Hotel`, `City Hotel` |
| 2 | `is_canceled` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 2 | `0` (75,166 active), `1` (44,224 canceled/no-show; 37.04%) |
| 3 | `lead_time` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 479 | `0` to `737` days between booking and arrival |
| 4 | `arrival_date_year` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 3 | `2015`, `2016`, `2017` |
| 5 | `arrival_date_month` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 12 | `January` .. `December` |
| 6 | `arrival_date_week_number` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 53 | ISO week `1` .. `53` |
| 7 | `arrival_date_day_of_month` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 31 | Day `1` .. `31` |
| 8 | `stays_in_weekend_nights` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 17 | `0`, `1`, `2`, up to `19` weekend nights |
| 9 | `stays_in_week_nights` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 35 | `0`, `1`, `2`, up to `50` weekday nights |
| 10 | `adults` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 14 | `1`, `2`, `3`, `0` (includes zero-guest anomalies) |
| 11 | `children` | `float64` | `IntegerType` | 119,386 | **4** | **0.00%** | 5 | `0.0`, `1.0`, `2.0`, `10.0` (4 literal `"NA"` tokens in raw CSV) |
| 12 | `babies` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 5 | `0`, `1`, `2`, `9`, `10` |
| 13 | `meal` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 5 | `BB` (Bed & Breakfast), `HB`, `FB`, `SC`, `Undefined` |
| 14 | `country` | `str` (`object`) | `StringType` | 118,902 | **488** | **0.41%** | 177 | ISO-3 codes (`PRT`, `GBR`, `FRA`, `ESP`); 488 literal `"NULL"` |
| 15 | `market_segment` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 8 | `Online TA` (56,477), `Offline TA/TO` (24,219), `Groups` (19,811), `Direct` (12,606), `Corporate` (5,295), `Complementary` (743), `Aviation` (237), `Undefined` (2) |
| 16 | `distribution_channel` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 5 | `TA/TO` (97,870), `Direct` (14,645), `Corporate` (6,677), `GDS` (193), `Undefined` (5) |
| 17 | `is_repeated_guest` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 2 | `0` (first-time), `1` (repeated guest) |
| 18 | `previous_cancellations` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 15 | Historical cancellations prior to current booking |
| 19 | `previous_bookings_not_canceled` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 73 | Historical non-canceled bookings prior to current booking |
| 20 | `reserved_room_type` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 10 | Anonymized room codes `A`, `B`, `C`, `D`, `E`, `F`, `G`, `H`, `L`, `P` |
| 21 | `assigned_room_type` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 12 | Codes `A`..`L`, `I`, `K`, `P` (differs from reserved on upgrade/overbooking) |
| 22 | `booking_changes` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 21 | Number of modifications (`0` .. `21`) |
| 23 | `deposit_type` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 3 | `No Deposit` (104,641), `Non Refund` (14,587), `Refundable` (162) |
| 24 | `agent` | `float64` | `StringType` / `IntegerType` | 103,050 | **16,340** | **13.69%** | 333 | Travel agency ID (`304`, `240`); **16,340 literal `"NULL"` strings** |
| 25 | `company` | `float64` | `StringType` / `IntegerType` | 6,797 | **112,593** | **94.31%** | 352 | Corporate account ID (`110`, `270`); **112,593 literal `"NULL"` strings** |
| 26 | `days_in_waiting_list` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 128 | Days spent on waiting list before confirmation |
| 27 | `customer_type` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 4 | `Transient`, `Transient-Party`, `Contract`, `Group` |
| 28 | `adr` | `float64` | `DecimalType(12,2)` | 119,390 | 0 | 0.00% | 8,879 | Average Daily Rate (`Min: -6.38`, `Median: 94.58`, `Max: 5400.00`) |
| 29 | `required_car_parking_spaces` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 5 | `0`, `1`, `2`, `3`, `8` |
| 30 | `total_of_special_requests` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 6 | `0` .. `5` guest special requests |
| 31 | `reservation_status` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 3 | `Check-Out` (75,166), `Canceled` (43,017), `No-Show` (1,207) |
| 32 | `reservation_status_date` | `str` (`object`) | `DateType` | 119,390 | 0 | 0.00% | 926 | ISO date `YYYY-MM-DD` of last status transition |

### Statistical Outlier Analysis on `adr` via the Interquartile Range (IQR) Method

**ADR (Average Daily Rate)** represents the average revenue earned per occupied room night (`RN-01`). In statistical profiling, **outliers** are atypical or extreme observations that deviate significantly from the central distribution of the dataset, whereas the **Interquartile Range (IQR) method** (Tukey's fences) provides a non-parametric mathematical rule to detect them without being distorted by extreme skew.

#### Step-by-Step Mathematical Derivation on F1 (119,390 Bookings)

When all **119,390** `adr` values in `hotel_bookings.csv` are sorted in ascending order and partitioned into four equal quartiles (**25% of observations per quartile**):

* **Q1 (First Quartile = `$69.29`)**: Exactly 25% of historical bookings cost less than `$69.29` per night.
* **Median / Q2 (Second Quartile = `$94.58`)**: Exactly 50% of bookings cost less than `$94.58` per night (the robust center of the distribution, compared to the parametric mean of **`$101.83`**).
* **Q3 (Third Quartile = `$126.00`)**: Exactly 75% of bookings cost less than `$126.00` per night.
* **IQR (Interquartile Range = `$56.71`)**: Computed as `Q3 - Q1 = 126.00 - 69.29 = 56.71`. This measures the spread of the central 50% of "normal" room rates.

To establish the mathematical boundary between normal pricing and statistical outliers, the IQR method subtracts and adds `1.5 × IQR` (`1.5 × 56.71 = 85.065`) from the quartiles:

```
Lower Bound = Q1 - 1.5 * IQR = 69.29 - 85.065 = -15.77
Upper Bound = Q3 + 1.5 * IQR = 126.00 + 85.065 = 211.06

      Outlier Zone (< -$15.77)             Normal IQR Core (50% of data)            Outlier Zone (> $211.06)
  <------------------------------|       +-------------------------------+       |------------------------------>
  Min: -$6.38 (Negative error)   |       | Q1: $69.29 | Med: $94.58 | Q3: $126.00|  3,793 bookings (3.18%)
                                -15.77   +-------------------------------+     211.06  Max: $5,400.00 (Giant outlier)
```

Our automated audit (`buenavista_csv.f1_audit`) identified **3,793 bookings (`3.18%`)** above the upper Tukey fence of `$211.06` (and **5,753 bookings** when combining upper statistical outliers with non-positive rates `<= $0.00`).

#### Classification and Treatment of the Three ADR Anomaly Profiles

| Anomaly Profile | Observed Count | Extreme Value(s) | Root Cause & Hospitality Domain Interpretation | Bronze → Silver PySpark Treatment |
| :--- | ---: | :--- | :--- | :--- |
| **1. The Giant Outlier** | **1 row** (within 3,793 upper outliers) | **`$5,400.00`** (vs. 2nd highest `$510.00`, 3rd `$508.00`, 4th `$451.50`) | Single canceled City Hotel booking (`2016-03-25`). Because the second-highest rate in the entire dataset is `$510.00`, `$5,400.00` is a decimal/keystroke data-entry error (intended as `$54.00` or `$540.00`) that artificially inflates the parametric mean. | **Quarantine & Exclude from ADR/RevPAR**: Flag with `is_adr_giant_outlier = true` and exclude `adr > 1000.0` from Silver commercial aggregations (`RN-01`). |
| **2. The Negative Value** | **1 row** | **`-$6.38`** | Single Resort Hotel booking (`2017-03-05`). A hotel never pays a guest to occupy a room; this represents an accounting adjustment or PMS export glitch. | **Hard Filter / Quarantine**: Reject rows where `adr < 0` during Silver ingestion and log to the data-quality quarantine table. |
| **3. The Zero Values** | **1,959 rows** (`1.64%`) | **`$0.00`** | Legitimate hospitality occurrences: complimentary stays (`743` rows in `Complementary` segment), loyalty redemptions, staff usage, or package bookings billed on master folios. However, including `$0.00` stays in commercial rate averages distorts real room pricing. | **Retain for Occupancy, Flag for Commercial ADR**: Keep valid zero-rate stays for physical room occupancy (`RN-01` room nights sold), but tag `is_complimentary = (adr == 0)` so commercial pricing baselines (`RN-05`) filter on `adr > 0`. |

### Additional Structural Anomalies in F1 (Section 5.1 & 5.3)

1. **Literal `"NULL"` and `"NA"` Textual Tokens**:
   * In the raw CSV file, missing values are **not** encoded as empty strings `""` in three columns; instead, they appear as the literal 4-character ASCII string `"NULL"` in `company` (**112,593 rows**, 94.31%), `agent` (**16,340 rows**, 13.69%), and `country` (**488 rows**, 0.41%), and as `"NA"` in `children` (**4 rows**).
   * **Impact on Ingestion**: Standard pandas `read_csv()` converts `"NULL"` and `"NA"` to `NaN`, which forces integer ID columns (`agent`, `company`, `children`) into floating-point types (`float64`, e.g., `304.0`). In PySpark (`Topic 13`), the reader **must** specify `.option("nullValue", "NULL")` alongside an explicit `StructType` and handle `"NA"` in `children` (coalescing to `0` integer) before casting.
2. **Zero-Guest Reservations (Section 5.3 Defect 1 in Natural Historical Form)**:
   * Exactly **180 rows (`0.15%`)** in F1 have `adults = 0`, `children = 0`, and `babies = 0` simultaneously.
   * **Impact on Ingestion**: A booking with zero occupants is logically invalid for guest-count analytics and must be filtered out during the Bronze → Silver transformation (`adults + children + babies >= 1`).

---

## 3. Source F4: Traditional Agency Files (`*_week.csv`) Simulates Heterogeneous Weekly Drops and Cross-Channel Duplicates

### Provenance, Scope, and Multi-Format Design (US-25 & US-26)

Traditional travel agencies send weekly booking spreadsheets via batch file transfer. Per **Section 5** and **Section 5.2** of the project specification, F4 is a simulated source owned by `buenavista-csv` (`buenavista_csv.f4_generator`) that produces **15 weekly CSV files** (one per agency) with deterministic seeding (`--seed 7`).

| Parameter | Specification |
| :--- | :--- |
| **Source ID & Name** | **F4** — Traditional Agency Files (`Archivos de agencias tradicionales`) |
| **Generator Module** | `src/buenavista_csv/f4_generator.py` (CLI: `generate-f4`) |
| **Output Location** | `output/f4_agency_files/<agency_slug>_week.csv` + `_generation_summary.json` |
| **Agency Scope** | **15 Traditional Agencies**: `ViajaFacil`, `SolyMar Tours`, `EuroTrip Agency`, `Andes Travel`, `GlobalStay Agency`, `CaribeViajes`, `MontanaTours`, `CostaBella Travel`, `NorteSur Agencia`, `Horizonte Viajes`, `Rutas del Sur`, `BlueSky Agency`, `Destino Feliz`, `Puerta del Sol Travel`, `Viajes Amanecer` |
| **Ingestion Frequency** | **Weekly Batch** (`15 CSV files/week`; default `12` regular bookings/agency + `3` cloned cross-channel duplicates = **183 rows/week**) |
| **Commission Rule (`RN-02`)** | Traditional agency rate fixed at **`10%` (`0.10`)** (compared to `18%` for OTAs and `0%` for Direct/Corporate) |

### Alternating Schema Contracts: Layout A vs. Layout B

To faithfully simulate Section 5.2 (*"F4: agencias con formatos distintos"* / *"agencies with different formats"*) without requiring 15 bespoke parsers, the generator alternates between **two distinct CSV column schemas** based on agency index:

* **Layout A (Even-indexed agencies: 7 agencies — e.g., `solymar_tours_week.csv`, `andes_travel_week.csv`)**:
  ```csv
  agency_booking_id,guest_name,hotel_id,check_in,check_out,room_type,currency,total_amount,commission_rate
  AG02-00014,Sofia Rossi,hotel-005,2026-12-02,2026-12-06,SINGLE,USD,1134.92,0.1
  ```
* **Layout B (Odd-indexed agencies: 8 agencies — e.g., `viajafacil_week.csv`, `eurotrip_agency_week.csv`)**:
  ```csv
  booking_ref,hotel_id,guest_full_name,checkin_date,checkout_date,room_category,amount,currency,commission_pct
  AG01-00001,hotel-021,Rafael Fernandez,2026-11-07,2026-11-08,SINGLE,1826.63,EUR,0.1
  ```

#### Canonical Schema Reconciliation Table (Layout A ↔ Layout B → Silver)

| Canonical Silver Field | Layout A Column (Pos) | Layout B Column (Pos) | Data Type | Validation & Business Rules |
| :--- | :--- | :--- | :--- | :--- |
| `reservation_source_id` | `agency_booking_id` (Col 1) | `booking_ref` (Col 1) | `STRING` | Format `AG<01..15>-<00001..99999>`; non-null, unique per file |
| `guest_name_raw` | `guest_name` (Col 2) | `guest_full_name` (Col 3) | `STRING` | Used in Bronze/Silver staging for fuzzy deduplication (`RN-08`), then replaced by `guest_id_hash` (`RN-11`, `CA-10`) |
| `hotel_id` | `hotel_id` (Col 3) | `hotel_id` (Col 2) | `STRING` | Format `hotel-001` .. `hotel-028` (maps to `HTL-001` .. `HTL-028` in F3) |
| `check_in` | `check_in` (Col 4) | `checkin_date` (Col 4) | `DATE` | ISO-8601 `YYYY-MM-DD` |
| `check_out` | `check_out` (Col 5) | `checkout_date` (Col 5) | `DATE` | ISO-8601 `YYYY-MM-DD`; must satisfy `check_out > check_in` |
| `room_type` | `room_type` (Col 6) | `room_category` (Col 6) | `STRING` | Enum: `SINGLE`, `DOUBLE`, `TWIN`, `SUITE` |
| `currency` | `currency` (Col 7) | `currency` (Col 8) | `STRING` | Enum: `EUR`, `USD` (subject to **Defect 3**: empty string `""` at configurable `--missing-currency-rate`) |
| `total_amount` | `total_amount` (Col 8) | `amount` (Col 7) | `DECIMAL(12,2)` | Gross reservation amount (`> 0`); converted via F7 Frankfurter exchange rates |
| `commission_rate` | `commission_rate` (Col 9) | `commission_pct` (Col 9) | `DECIMAL(5,4)` | `0.10` (`10%` traditional agency commission per `RN-02`) |

### Deliberate Data-Quality Defect Injection (Section 5.3, US-14, US-24, US-25, US-26)

Source F4 is responsible for injecting **two of the five mandatory data-quality defects** catalogued in `US-14` (`data_quality_defects_catalog.md`) and standardized in `US-26`:

#### 1. Defect 3 — Missing Currency in Agency Files (`--missing-currency-rate`)
* **Business Impact**: Without an explicit ISO currency code (`EUR` or `USD`), `total_amount` cannot be converted via the **F7 (Frankfurter Exchange Rate API)** or aggregated into net-of-commission ADR and RevPAR (`RN-01`, `RN-02`).
* **Implementation Evolution (`US-25` → `US-26`)**: In the initial `US-25` prototype, missing currency was hardcoded as `rng.choice(["EUR", "USD", None])` (~33% missing rate). Under `US-26`, this was refactored to read from the unified `DefectRates` configuration (`--missing-currency-rate`, default **`0.10`** = 10%).
* **Observed Verification**: With default seed `7` across 183 generated rows (`15 agencies × 12 bookings + 3 duplicates`), exactly **14 rows (`7.7%`)** contain an empty currency field (`""`), such as `AG01-00007` in `viajafacil_week.csv` and `AG02-00016` in `solymar_tours_week.csv`. At `--missing-currency-rate 0.40` (`n = 603`), the observed rate is **41.3%**.

#### 2. Defect 4 — Cross-Channel Duplicate Bookings (`--duplicate-count`, F4 ↔ F2 Coupling)
* **Business Rule (`RN-08` & `CA-06`)**: *"A duplicate reservation across channels (same guest, hotel, and dates within ±1 day) is flagged and the booking from the channel with the lower commission prevails."*
* **Cross-Story Architectural Coupling (`US-12` → `US-24` → `US-25`)**:
  1. Instead of generating synthetic duplicates in isolation, `f4_generator.py` reads actual `reservation.created` events emitted by the **F2 Channel Manager Simulator (`US-24`, `events.jsonl`)**.
  2. Because the F2 JSON event schema defined in `US-12` omits guest PII (`guest_name`), `f4_generator.py` uses `_derive_guest_name(reservation_id)` (seeded deterministically by `reservation_id`) to assign a canonical guest identity to the F2 booking.
  3. For each cloned reservation (`--duplicate-count 3`), the generator injects a corresponding row into an agency CSV with:
     * A distinct agency booking reference (`AG01-00013`, `AG02-00026`, `AG03-00039`).
     * The exact same `hotel_id`, `room_type`, `check_out`, `currency`, and `total_amount`.
     * A `check_in` date shifted by `rng.choice([-1, 0, +1])` days to test the **±1 day tolerance window** of `RN-08`.
     * A realistic guest name transcription discrepancy generated by `_vary_name()` using four mutation modes:
       - `swap_order`: Swaps first and last name (`"Ferreira Diego"` vs `"Diego Ferreira"`).
       - `middle_initial`: Abbreviates surname to initial (`"Diego F."`).
       - `typo`: Transposes two adjacent inner characters in the surname (`"Ferriera"`).
       - `drop_accent_like`: Lowercases the surname (`"Diego ferreira"` vs `"Diego Ferreira"`).

#### Verified Cross-Channel Duplicate Lineage (`_generation_summary.json`, Seed = 7)

| F4 Agency File | F4 Agency Booking ID | Cloned F2 `reservation_id` | Original F2 Channel (Commission) | F4 Agency Commission | Canonical F2 Guest Name | Mutated F4 Guest Name | Hotel ID | Check-In Shift | RN-08 Winning Channel in Silver |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `viajafacil_week.csv` | `AG01-00013` | `res-10024` | `booking_channel_a` (OTA `18%`) | Traditional Agency (`10%`) | `Diego Ferreira` | `Diego ferreira` | `hotel-022` | `0 days` (`2026-11-11`) | **F4 Agency (`10%` < `18%`)** |
| `solymar_tours_week.csv` | `AG02-00026` | `res-10012` | `expedia` (OTA `18%`) | Traditional Agency (`10%`) | `Patricia Rossi` | `Patricia Rossi` | `hotel-001` | `-1 day` (`2026-11-07` vs `11-08`) | **F4 Agency (`10%` < `18%`)** |
| `eurotrip_agency_week.csv` | `AG03-00039` | `res-10029` | `direct_ota` (OTA `18%`) | Traditional Agency (`10%`) | `Elena Santos` | `Elena santos` | `hotel-006` | `0 days` (`2026-12-09`) | **F4 Agency (`10%` < `18%`)** |

:::tip How Spark Resolves RN-08 Across F2, F3, and F4
During the daily **Bronze → Silver** PySpark job, reservations from **F3** (Direct `0%` / Corporate `0%`), **F4** (Traditional Agency `10%`), and **F2** (OTA `18%`) are unioned into a staging DataFrame. Spark normalizes guest names (lowercase, punctuation stripped, sorted tokens + Soundex/Levenshtein distance `<= 1`), joins on `hotel_id` and `ABS(DATEDIFF(a.check_in, b.check_in)) <= 1`, and applies `ROW_NUMBER() OVER (PARTITION BY dedup_cluster_id ORDER BY commission_rate ASC, created_at ASC)`. The row with `row_num = 1` prevails as `is_canonical = true`, while higher-commission duplicates are marked `is_duplicate_channel = true` so their room nights and revenue are never double-counted (`CA-06`).
:::

---

## 4. Consumption Strategy, S3 Partitioning, and PySpark Transformation Contract

### Step 1: Pre-Ingestion Validation and S3 Bronze Landing (Airflow + Celery)

Every daily batch run (scheduled to finish before **06:00 local time**, `RNF-03`) executes via **Apache Airflow (`CeleryExecutor`)** with task messages brokered through **RabbitMQ (`vhost: airflow`)**:

1. **Task 1 — `validate_file_sources` (`US-6`)**: Invokes `python3 -m buenavista_csv.validator` against the incoming F1 and F4 files.
   * Verifies structural schema compliance against `schemas/f1_schema.json` and `schemas/f4_layouts_schema.json`.
   * Emits `output/validation_reports/quality_validation_report.json` logging every detected anomaly with its exact `file`, `record`, and `field` without altering a single byte of the source CSVs (verified via SHA-256 checksum assertions).
2. **Task 2 — `upload_csv_sources_to_s3_bronze`**: Uploads the raw files to Amazon S3 `Bronze` using deterministic, idempotent keys (`CT-08`):
   * **F1 Target URI**: `s3://hospedaplus-datalake-bronze/f1_historical/load_date={{ ds }}/hotel_bookings.csv`
   * **F4 Target URI**: `s3://hospedaplus-datalake-bronze/f4_agencies/year={{ execution_date.year }}/week={{ execution_date.isocalendar()[1] }}/<agency_slug>_week.csv`

### Step 2: Explicit PySpark Schema Enforcement and Silver Standardization (ECS Fargate Spot)

To satisfy **Topic 13 (`Spark I — Explicit Schemas`)**, **Topic 14 (`Spark II — Data Processing`)**, and **Topic 15 (`Spark III — Partitioning & Parquet`)**, the PySpark job running on **ECS Fargate Spot** never uses `inferSchema=True`. Instead, it enforces explicit `StructType` definitions:

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# 1. Explicit schema for F4 Layout A and Layout B
LAYOUT_A_SCHEMA = StructType([
    StructField("agency_booking_id", StringType(), nullable=False),
    StructField("guest_name", StringType(), nullable=False),
    StructField("hotel_id", StringType(), nullable=False),
    StructField("check_in", StringType(), nullable=False),
    StructField("check_out", StringType(), nullable=False),
    StructField("room_type", StringType(), nullable=False),
    StructField("currency", StringType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=False),
    StructField("commission_rate", DoubleType(), nullable=False),
])

LAYOUT_B_SCHEMA = StructType([
    StructField("booking_ref", StringType(), nullable=False),
    StructField("hotel_id", StringType(), nullable=False),
    StructField("guest_full_name", StringType(), nullable=False),
    StructField("checkin_date", StringType(), nullable=False),
    StructField("checkout_date", StringType(), nullable=False),
    StructField("room_category", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("currency", StringType(), nullable=True),
    StructField("commission_pct", DoubleType(), nullable=False),
])

def standardize_f4_layout_b(df_b):
    """Normalize Layout B column names and ordering to match canonical Layout A."""
    return df_b.select(
        F.col("booking_ref").alias("agency_booking_id"),
        F.col("guest_full_name").alias("guest_name"),
        F.col("hotel_id"),
        F.col("checkin_date").alias("check_in"),
        F.col("checkout_date").alias("check_out"),
        F.col("room_category").alias("room_type"),
        F.col("currency"),
        F.col("amount").alias("total_amount"),
        F.col("commission_pct").alias("commission_rate"),
    )
```

### Step 3: Business Rule Enforcement Matrix in Silver & Gold

| Rule / Requirement | Source(s) Affected | Transformation Logic Applied in PySpark (`Bronze` → `Silver` → `Gold`) |
| :--- | :--- | :--- |
| **`RN-01` & `RN-02`** (Net ADR & RevPAR) | `F1`, `F4` (plus `F2`, `F3`) | `net_revenue = total_amount * (1.0 - commission_rate)` where `commission_rate` is `0.10` for `F4` traditional agencies, `0.18` for `F2` OTAs, and `0.00` for `F3` direct/corporate. `ADR = SUM(net_revenue) / SUM(room_nights_sold)`. |
| **`RN-03`** (Overbooking Limit) | `F1` (90-day baseline) + `F3` | Computes rolling 90-day cancellation rate by `lead_time` bucket from `F1` (`is_canceled = 1`) and `F2`/`F3` to calculate `MIN(0.05 * inventory, (hist_cancel_rate * current_bookings) - 1)`. |
| **`RN-05`** (14-Day Pickup Multiplier) | `F1` (YoY baseline) + `F2`, `F3`, `F4` | Compares projected 14-day occupancy (`F2 + F3 + F4` deduplicated) against the same calendar window in the prior year (`F1` baseline): applies `1.10x` if `+15%` above YoY, or `0.92x` if `-15%` below YoY. |
| **`RN-08` & `CA-06`** (Cross-Channel Deduplication) | `F4` vs. `F2` / `F3` | Matches normalized guest name + `hotel_id` + `check_in` within `±1 day`; retains the record with the lowest `commission_rate` (`Direct 0% < F4 Agency 10% < F2 OTA 18%`). |
| **`RN-11`, `RNF-06` & `CA-10`** (PII Pseudonymization) | `F4` (`guest_name`) | Computes `F.sha2(F.concat(F.lit(kms_salt), F.lower(F.trim(F.col("guest_name")))), 256).alias("guest_id_hash")` and drops raw `guest_name` before writing Silver Parquet files. (`F1` natively contains no guest names/emails/phones). |

---

## 5. Repository Structure, Quality Controls, and Execution Runbook (`buenavista-csv`)

### Repository Layout (`US-5` & `US-6`)

```
buenavista-csv/
├── pyproject.toml                           # Standalone package config & CLI entrypoints
├── Dockerfile                               # Containerized batch validator/generator image
├── .env.example                             # Environment variables template (no secrets committed, CT-02)
├── README.md                                # Quickstart and operational instructions
├── schemas/
│   ├── f1_schema.json                       # Canonical 32-column contract & IQR bounds for F1
│   └── f4_layouts_schema.json               # Dual-layout (Layout A & Layout B) contract for F4
├── data/
│   ├── f1_historical/
│   │   └── hotel_bookings.csv               # Kaggle Hotel Booking Demand dataset (119,390 rows)
│   └── f2_reference/
│       └── events.jsonl                     # Bundled F2 reference events for standalone AC2 testing
├── src/
│   └── buenavista_csv/
│       ├── __init__.py
│       ├── defects.py                       # Unified DefectRates dataclass (US-14 / US-26)
│       ├── f1_audit.py                      # F1 schema profiler & IQR outlier auditor (US-11)
│       ├── f4_generator.py                  # 15-agency weekly CSV generator (US-25 / US-26)
│       └── validator.py                     # Non-destructive quality gate for F1 & F4 (US-6)
├── output/
│   ├── f1_audit/
│   │   ├── audit_report.csv                 # Machine-readable column & IQR audit metrics
│   │   └── audit_summary.txt                # Executive text report of F1 anomalies
│   ├── f4_agency_files/
│   │   ├── *_week.csv                       # 15 generated weekly agency CSV files
│   │   └── _generation_summary.json         # Traceability log of injected F2 duplicates & missing currencies
│   └── validation_reports/
│       └── quality_validation_report.json   # Structured US-6 validation output
├── tests/
│   ├── fixtures/
│   │   └── malformed_f4_week.csv            # Known malformed test file for US-6 AC2 verification
│   └── test_buenavista_csv.py               # Automated test suite (unittest / pytest compatible)
└── docs/
    └── data-sources/                        # Modular Docusaurus pages ready to copy into doc-website/
        ├── _category_.json
        ├── index.md
        ├── f1-hotel-booking-demand.md
        ├── f4-traditional-agency-files.md
        ├── quality-controls-and-batch-ingestion.md
        └── img/
            └── iqr_outlier_detection.png
```

### Local CLI Runbook (Zero External Service Dependencies)

All commands run deterministically in a clean environment without requiring a running PostgreSQL instance, FastAPI server, or RabbitMQ broker (`US-5 AC4`):

```bash
# 1. Install package in editable mode (via uv or pip)
pip install -e .

# 2. Run F1 Historical Dataset Audit & IQR Outlier Profiler (US-11 / US-5)
PYTHONPATH=src python3 -m buenavista_csv.f1_audit   --csv-path data/f1_historical/hotel_bookings.csv   --output-dir output/f1_audit

# 3. Generate F4 Weekly Traditional Agency CSVs with Configurable Defects (US-25 / US-26)
PYTHONPATH=src python3 -m buenavista_csv.f4_generator   --f2-events data/f2_reference/events.jsonl   --output-dir output/f4_agency_files   --seed 7   --bookings-per-agency 12   --duplicate-count 3   --missing-currency-rate 0.10

# 4. Execute Non-Destructive Batch Quality Controls across F1 and F4 (US-6)
PYTHONPATH=src python3 -m buenavista_csv.validator   --f1-path data/f1_historical/hotel_bookings.csv   --f4-dir output/f4_agency_files   --f2-events data/f2_reference/events.jsonl   --report-dir output/validation_reports

# 5. Run Automated Verification Suite (US-11, US-25, US-26, US-5, US-6)
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

### Non-Destructive Quality Gate Verification (`US-6 AC1 & AC2`)

When `buenavista_csv.validator` runs against a known malformed file (`tests/fixtures/malformed_f4_week.csv`), it detects and reports every structural and business defect by exact file name, record identifier, and field without altering the input file:

```json
{
  "file": "malformed_f4_week.csv",
  "valid_schema": true,
  "layout": "A",
  "rows": 3,
  "defect_count": 8,
  "defects": [
    {
      "type": "MISSING_REQUIRED_VALUE",
      "file": "malformed_f4_week.csv",
      "field": "guest_name",
      "record": "AG99-00002",
      "detail": "Missing guest name"
    },
    {
      "type": "INVALID_ROOM_TYPE",
      "file": "malformed_f4_week.csv",
      "field": "room_type",
      "record": "AG99-00002",
      "detail": "Invalid room_type 'PENTHOUSE'"
    },
    {
      "type": "INVALID_CURRENCY",
      "file": "malformed_f4_week.csv",
      "field": "currency",
      "record": "AG99-00002",
      "detail": "Unsupported currency 'GBP'"
    },
    {
      "type": "INVALID_DATE_RANGE",
      "file": "malformed_f4_week.csv",
      "field": "check_in,check_out",
      "record": "AG99-00002",
      "detail": "check_out (2026-11-12) <= check_in (2026-11-15) (Section 5.3 Defect 2)"
    },
    {
      "type": "INVALID_TYPE",
      "file": "malformed_f4_week.csv",
      "field": "total_amount",
      "record": "AG99-00002",
      "detail": "Non-numeric amount 'not_a_number'"
    },
    {
      "type": "INVALID_COMMISSION_RATE",
      "file": "malformed_f4_week.csv",
      "field": "commission_rate",
      "record": "AG99-00002",
      "detail": "Commission rate out of [0,1]: 1.5"
    },
    {
      "type": "INTRA_FILE_DUPLICATE",
      "file": "malformed_f4_week.csv",
      "field": "agency_booking_id",
      "record": "AG99-00001",
      "detail": "Duplicate booking ID AG99-00001"
    },
    {
      "type": "MISSING_CURRENCY",
      "file": "malformed_f4_week.csv",
      "field": "currency",
      "record": "AG99-00001",
      "detail": "Currency code is empty (Section 5.3 Defect 3)"
    }
  ]
}
```

### Known Limitations and Downstream Handover Notes

1. **Static Historical Window in F1 (`2015–2017`)**: Because Kaggle's `hotel_bookings.csv` spans July 2015 to August 2017 while simulated operational sources (`F2`, `F3`, `F4`) operate in `2026–2027`, the PySpark Silver transformation aligns F1 historical seasonality by `(arrival_date_month, arrival_date_day_of_month, lead_time_bucket)` rather than literal calendar year when computing `RN-03` cancellation curves and `RN-05` YoY pickup baselines.
2. **Guest Name Derivation on F2 Duplicates**: Because the validated `US-12` JSON schema for `F2` channel-manager events carries `reservation_id` and guest counts (`adults`, `children`) without a `guest_name` string, `f4_generator.py` derives a deterministic canonical guest name from `reservation_id` (`_derive_guest_name()`). The Spark deduplication job (`RN-08`) uses this same deterministic function (or joins on the enriched F2 Silver table) before comparing against F4's mutated `guest_name`.
3. **Currency Imputation Policy for Defect 3**: In `buenavista-csv`, rows with missing currency (`--missing-currency-rate 0.10`) are intentionally preserved in raw `Bronze` CSVs and flagged in `quality_validation_report.json`. Imputation (via agency-country default currency mapping or quarantine) is delegated exclusively to the **Bronze → Silver** Spark job (`US-6 AC1`: validator never silently rewrites raw source files).
