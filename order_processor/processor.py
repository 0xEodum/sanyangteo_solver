from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, Optional

import psycopg2
import traceback

from .candidate import CandidateMixin
from .config import ProcessorConfig
from .context import ContextMixin
from .enums import OrderStatus
from .repository import RepositoryMixin
from .solver import SolverMixin
from .status import StatusMixin


class OrderProcessor(
    RepositoryMixin,
    CandidateMixin,
    StatusMixin,
    SolverMixin,
    ContextMixin,
):
    """Process orders from JSON input to optimized supplier assignment."""

    def __init__(
        self,
        db_config: Dict[str, str],
        config: Optional[ProcessorConfig] = None,
        verbose: bool = True,
    ):
        self.conn = psycopg2.connect(**db_config)
        self.config = config or ProcessorConfig()
        self.verbose = verbose
        self.last_solver_info: Dict[str, Any] = {}
        self.last_solution: Optional[Dict[str, Any]] = None
        self.last_enriched_payload: Optional[Dict[str, Any]] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()

    def log(self, message: str) -> None:
        """Print log message if verbose."""
        if self.verbose:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] {message}")

    def process_order(self, message_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main pipeline: Process order from JSON to solution.
        """
        self.log("=" * 70)
        self.log("ORDER PROCESSING PIPELINE V3")
        self.log("=" * 70)
        self.log("Configuration:")
        self.log(f"  Similarity threshold (OK): {self.config.sim_threshold_ok}")
        self.log(f"  Similarity threshold (Low): {self.config.sim_threshold_low}")
        self.log(f"  Allow insufficient: {self.config.allow_insufficient}")
        self.log(f"  Insufficient threshold: {self.config.insufficient_threshold}")
        self.log(f"  Price margin: {self.config.price_margin}")
        self.log("=" * 70)

        try:
            self.last_solution = None
            self.last_enriched_payload = None
            self.last_solver_info = {}
            input_context = self.extract_input_context(message_json)

            msg_id = self.ingest_message(message_json)
            order_id = self.create_order(msg_id, message_json)
            raw_matches = self.match_plants_raw(order_id)
            match_stats = self.classify_and_update_matches(order_id, raw_matches)
            raw_candidates = self.get_candidates_raw(order_id)

            enriched_payload = self.filter_and_classify_candidates(raw_candidates)
            self.last_enriched_payload = enriched_payload

            order_status, status_details = self.determine_status(enriched_payload)
            line_diagnostics = self._build_line_diagnostics(enriched_payload)
            pipeline_context = self._sanitize_for_context(
                {
                    "input": input_context,
                    "matching": {"stats": match_stats},
                    "candidates": {
                        "stats": enriched_payload.get("stats"),
                        "diagnostics": line_diagnostics,
                    },
                    "order_status": order_status.value,
                    "status_details": deepcopy(status_details),
                }
            )

            if enriched_payload["stats"]["items_for_solver"] == 0:
                self.log("=" * 70)
                self.log(f"ORDER STATUS: {order_status.value}")
                self.log("No items ready for solver - cannot find optimal solution")
                self.log("=" * 70)
                pipeline_context["solver"] = {
                    "status": "SKIPPED",
                    "reason": "no_items_for_solver",
                }
                return {
                    "success": False,
                    "order_id": order_id,
                    "order_status": order_status.value,
                    "status_details": status_details,
                    "error": "no_items_for_solver",
                    "summary": self.get_order_summary(order_id),
                    "diagnostics": {"lines": line_diagnostics},
                    "pipeline_context": pipeline_context,
                    "solver_details": pipeline_context["solver"],
                }

            solution = self.solve_assignment(order_id, enriched_payload)
            pipeline_context["solver"] = self.last_solver_info

            if not solution:
                self.log("=" * 70)
                self.log("SOLVER FAILED: No feasible solution found")
                self.log("=" * 70)
                min_order_shortfalls = self._calculate_min_order_shortfalls(
                    enriched_payload
                )
                if min_order_shortfalls:
                    impacted_lines = {entry["line_no"] for entry in min_order_shortfalls}
                    breakdown = status_details["breakdown"]
                    breakdown["fully_closed"] = [
                        line
                        for line in breakdown.get("fully_closed", [])
                        if line not in impacted_lines
                    ]
                    breakdown["partially_closed"] = [
                        line
                        for line in breakdown.get("partially_closed", [])
                        if line not in impacted_lines
                    ]
                    status_details["breakdown"]["cannot_close"].extend(
                        min_order_shortfalls
                    )
                    status_details["order_status"] = OrderStatus.CANNOT_CLOSE.value
                    order_status = OrderStatus.CANNOT_CLOSE
                    self._refresh_status_counts(status_details)
                    self.last_solver_info.setdefault("diagnostics", {})[
                        "min_order_amount"
                    ] = min_order_shortfalls
                    pipeline_context["solver"] = self.last_solver_info
                    pipeline_context.setdefault("order_status_updates", []).append(
                        {
                            "order_status": order_status.value,
                            "details": min_order_shortfalls,
                        }
                    )
                    pipeline_context["status_details"] = self._sanitize_for_context(
                        deepcopy(status_details)
                    )
                if self.last_enriched_payload:
                    pipeline_context["enriched_payload"] = self._sanitize_for_context(
                        deepcopy(self.last_enriched_payload)
                    )
                return {
                    "success": False,
                    "error": "solver_infeasible",
                    "order_id": order_id,
                    "order_status": order_status.value,
                    "status_details": status_details,
                    "diagnostics": {"lines": line_diagnostics},
                    "summary": self.get_order_summary(order_id),
                    "pipeline_context": pipeline_context,
                    "solver_details": self.last_solver_info,
                }

            self.last_solution = solution
            run_id = self.record_assignment(order_id, solution, order_status, status_details)

            output = self.generate_output(order_id, run_id, order_status, status_details)
            output["pipeline_context"] = pipeline_context
            output["solver_details"] = self.last_solver_info

            self.log("=" * 70)
            self.log("SUCCESS")
            self.log("=" * 70)

            return output

        except Exception as exc:
            self.log(f"ERROR: {exc}")
            traceback.print_exc()
            raise
