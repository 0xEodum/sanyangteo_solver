from enum import Enum


class MatchStatus(str, Enum):
    """Match status classification."""

    OK = "ok"
    LOW_CONFIDENCE = "low_confidence"
    NO_MATCH = "no_match"
    MANUAL_OVERRIDE = "manual_override"


class OrderStatus(str, Enum):
    """Overall order fulfilment status."""

    FULLY_CLOSED = "fully_closed"  # All items matched and sufficient qty
    PARTIALLY_CLOSED = "partially_closed"  # Some items with insufficient qty
    CANNOT_CLOSE = "cannot_close"  # Items below threshold or no matches
