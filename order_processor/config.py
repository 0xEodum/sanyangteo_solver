from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class ProcessorConfig:
    """
    Configuration for the order processor business rules.

    Centralises all tunable thresholds so they are not embedded in SQL.
    """

    # Matching thresholds
    sim_threshold_ok: float = 0.42
    sim_threshold_low: float = 0.30

    # Availability rules
    allow_insufficient: bool = True
    insufficient_threshold: float = 0.20  # Max 20% shortage allowed

    # Price filtering (optional)
    price_margin: Optional[float] = None  # None = no filtering, 0.10 = +10% from min

    # Solver settings
    solver_timeout: int = 60  # seconds

    def to_dict(self) -> Dict[str, Optional[float]]:
        """Convert the configuration to a serialisable dictionary."""
        return {
            "sim_threshold_ok": self.sim_threshold_ok,
            "sim_threshold_low": self.sim_threshold_low,
            "allow_insufficient": self.allow_insufficient,
            "insufficient_threshold": self.insufficient_threshold,
            "price_margin": self.price_margin,
            "solver_timeout": self.solver_timeout,
        }
