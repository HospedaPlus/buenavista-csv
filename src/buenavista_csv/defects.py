"""
US-14 / US-26 — Unified, configurable data-quality defect rates for file-based sources.

Maintains contract parity with the platform-wide DefectRates specification while keeping
buenavista-csv completely decoupled from database (F3) and API (F5) packages.
"""

from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class DefectRates:
    zero_guests_rate: float = 0.02          # Defect 1 (F3 / detected in F1)
    invalid_date_range_rate: float = 0.02   # Defect 2 (F3 / F2 / validated in F4)
    missing_currency_rate: float = 0.10     # Defect 3 (F4 agency files)
    duplicate_rate: float = 0.10            # Defect 4 (F4 vs F2 cross-channel duplicates)
    out_of_order_rate: float = 0.10         # Defect 5 (F2 channel manager events)

    def __post_init__(self) -> None:
        for name in (
            "zero_guests_rate",
            "invalid_date_range_rate",
            "missing_currency_rate",
            "duplicate_rate",
            "out_of_order_rate",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0.0 and 1.0, got {value}")


DEFAULT_DEFECT_RATES = DefectRates()
