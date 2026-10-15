"""
Automated test suite for buenavista-csv covering US-11, US-25 (F4), US-26, US-5, and US-6.
Compatible with both `pytest` and standard library `python3 -m unittest`.
"""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from src.buenavista_csv.defects import DefectRates
from src.buenavista_csv.f1_audit import run_f1_audit
from src.buenavista_csv.f4_generator import generate_agency_files
from src.buenavista_csv.validator import validate_f1_file, validate_f4_directory, validate_f4_file

REPO_ROOT = Path(__file__).resolve().parents[1]
F1_CSV = REPO_ROOT / "data" / "f1_historical" / "hotel_bookings.csv"
F2_EVENTS = REPO_ROOT / "data" / "f2_reference" / "events.jsonl"
MALFORMED_F4 = REPO_ROOT / "tests" / "fixtures" / "malformed_f4_week.csv"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class TestBuenavistaCSV(unittest.TestCase):
    def test_f1_audit_and_iqr_metrics(self) -> None:
        """US-11 & US-5: Verify F1 profile, IQR bounds, textual NULLs, and non-destructive read."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            before_hash = _sha256(F1_CSV)
            res = run_f1_audit(F1_CSV, tmp_path)
            after_hash = _sha256(F1_CSV)

            self.assertEqual(before_hash, after_hash, "F1 audit must never modify the original source CSV")
            self.assertEqual(res["rows"], 119390)
            self.assertEqual(res["columns"], 32)
            self.assertEqual(res["total_nulls"], 129425)
            self.assertEqual(res["literal_null_tokens"]["company"], 112593)
            self.assertEqual(res["literal_null_tokens"]["agent"], 16340)
            self.assertEqual(res["literal_null_tokens"]["country"], 488)
            self.assertEqual(res["literal_na_children"], 4)
            self.assertEqual(res["zero_guests_count"], 180)

            adr = res["adr"]
            self.assertEqual(adr["q1"], 69.29)
            self.assertEqual(adr["median"], 94.58)
            self.assertEqual(adr["q3"], 126.00)
            self.assertEqual(adr["iqr"], 56.71)
            self.assertEqual(adr["lower_bound"], -15.77)
            self.assertEqual(adr["upper_bound"], 211.06)
            self.assertEqual(adr["outlier_count"], 3793)
            self.assertEqual(adr["outlier_percent"], 3.18)
            self.assertEqual(adr["min"], -6.38)
            self.assertEqual(adr["max"], 5400.00)
            self.assertEqual(adr["negative_count"], 1)
            self.assertEqual(adr["zero_count"], 1959)

    def test_f4_generator_and_cross_channel_duplicates(self) -> None:
        """US-25 AC2, US-26 & US-5: Verify 15 agencies, dual layouts, missing currency, and F2 duplicates."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            summary = generate_agency_files(
                output_dir=tmp_path,
                f2_events_path=F2_EVENTS,
                seed=7,
                bookings_per_agency=12,
                duplicate_count=3,
                missing_currency_rate=0.10,
            )
            self.assertEqual(len(summary["agencies"]), 15)
            self.assertEqual(summary["total_rows"], 183)
            self.assertEqual(summary["rows_missing_currency"], 14)
            self.assertEqual(len(summary["duplicates_created"]), 3)

            layouts = [info["layout"] for info in summary["agencies"].values()]
            self.assertEqual(layouts.count("A"), 7)
            self.assertEqual(layouts.count("B"), 8)

    def test_us6_validator_detects_malformed_records(self) -> None:
        """US-6 AC1 & AC2: Malformed records are caught with exact file, field, and record ID without rewriting source."""
        before_hash = _sha256(MALFORMED_F4)
        report = validate_f4_file(MALFORMED_F4)
        after_hash = _sha256(MALFORMED_F4)

        self.assertEqual(before_hash, after_hash, "Validator must never rewrite the input CSV")
        self.assertTrue(report["valid_schema"])
        defect_types = {d["type"] for d in report["defects"]}
        self.assertIn("MISSING_REQUIRED_VALUE", defect_types)
        self.assertIn("INVALID_ROOM_TYPE", defect_types)
        self.assertIn("INVALID_CURRENCY", defect_types)
        self.assertIn("INVALID_DATE_RANGE", defect_types)
        self.assertIn("INVALID_TYPE", defect_types)
        self.assertIn("INVALID_COMMISSION_RATE", defect_types)
        self.assertIn("INTRA_FILE_DUPLICATE", defect_types)
        self.assertIn("MISSING_CURRENCY", defect_types)

    def test_us6_directory_validation_identifies_cross_channel_duplicates(self) -> None:
        """US-6: Validate generated F4 directory against F2 reference events."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generate_agency_files(
                output_dir=tmp_path,
                f2_events_path=F2_EVENTS,
                seed=7,
                bookings_per_agency=12,
                duplicate_count=3,
                missing_currency_rate=0.10,
            )
            dir_report = validate_f4_directory(tmp_path, F2_EVENTS)
            self.assertEqual(dir_report["status"], "PASSED_WITH_CATALOGUED_DEFECTS")
            self.assertEqual(dir_report["agency_files_found"], 15)
            self.assertEqual(dir_report["defect_counts_by_type"]["MISSING_CURRENCY"], 14)
            self.assertEqual(dir_report["defect_counts_by_type"]["CROSS_CHANNEL_DUPLICATE"], 3)

    def test_defect_rates_bounds(self) -> None:
        """US-26: DefectRates validates bounds [0.0, 1.0]."""
        rates = DefectRates(missing_currency_rate=0.0)
        self.assertEqual(rates.missing_currency_rate, 0.0)
        with self.assertRaises(ValueError):
            DefectRates(missing_currency_rate=1.5)


if __name__ == "__main__":
    unittest.main()
