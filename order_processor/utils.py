from datetime import datetime
from decimal import Decimal
from typing import Any


def normalize(obj: Any) -> Any:
    """Normalize Decimal, datetime and other types for JSON serialization."""
    if isinstance(obj, dict):
        return {k: normalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize(v) for v in obj]
    if isinstance(obj, Decimal):
        value = float(obj)
        return int(value) if value.is_integer() else value
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj
