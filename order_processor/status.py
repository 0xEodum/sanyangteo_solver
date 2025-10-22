from typing import Any, Dict, List, Optional, Set, Tuple

from .config import ProcessorConfig
from .enums import MatchStatus, OrderStatus


class StatusMixin:
    """
    Order status evaluation and reasoning helpers.

    Expects:
        - self.config: ProcessorConfig
        - self.log(message: str)
    """

    config: ProcessorConfig

    def _derive_line_reasons(self, item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build structured reason entries for a line that cannot be solved."""
        line_no = item.get("line_no")
        reason_map: Dict[str, List[Dict[str, Any]]] = {}
        for candidate in item.get("candidates_all", []):
            rejection_reasons = candidate.get("rejection_reasons") or []
            reason_details = candidate.get("reason_details") or []
            for reason_code in rejection_reasons:
                detail_entry = next(
                    (
                        d.get("details")
                        for d in reason_details
                        if d.get("code") == reason_code
                    ),
                    {},
                )
                detail_entry = detail_entry or {}
                detail_entry = {k: v for k, v in detail_entry.items() if v is not None}
                detail_entry.setdefault("supplier_id", candidate.get("supplier_id"))
                reason_map.setdefault(reason_code, []).append(detail_entry)

        if not reason_map:
            return [
                {
                    "line_no": line_no,
                    "reason": "no_available_candidates",
                }
            ]

        priority = [
            "insufficient_quantity",
            "below_min_line_qty",
            "price_above_margin",
            "supplier_blacklisted",
            "filtered_out_by_policy",
            "min_order_amount_risk",
            "no_available_candidates",
        ]

        entries: List[Dict[str, Any]] = []
        for reason_code in priority:
            details_list = reason_map.get(reason_code)
            if not details_list:
                continue
            clean_details = [
                {k: v for k, v in detail.items() if v is not None}
                for detail in details_list
            ]
            if len(clean_details) == 1:
                details_payload: Optional[Dict[str, Any]] = clean_details[0]
            else:
                details_payload = {"candidates": clean_details}
            entries.append(
                {
                    "line_no": line_no,
                    "reason": reason_code,
                    **({"details": details_payload} if details_payload else {}),
                }
            )

        if not entries:
            entries.append(
                {
                    "line_no": line_no,
                    "reason": "no_available_candidates",
                }
            )
        return entries

    def _refresh_status_counts(self, status_details: Dict[str, Any]) -> Dict[str, Any]:
        """Recompute status counts after mutating breakdown entries."""
        breakdown = status_details.get("breakdown", {})
        fully_closed_lines: Set[int] = set(breakdown.get("fully_closed", []))
        partially_closed_lines: Set[int] = set(breakdown.get("partially_closed", []))
        cannot_close_lines: Set[int] = {
            entry["line_no"] for entry in breakdown.get("cannot_close", [])
        }
        status_details["counts"] = {
            "fully_closed": len(fully_closed_lines),
            "partially_closed": len(partially_closed_lines),
            "cannot_close": len(cannot_close_lines),
        }
        return status_details

    def _calculate_min_order_shortfalls(self, enriched_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyse candidates to detect min_order_amount shortfalls per line."""
        shortfalls: Dict[Tuple[int, int], Dict[str, Any]] = {}
        if not enriched_payload:
            return []
        for item in enriched_payload.get("items", []):
            line_no = item.get("line_no")
            for candidate in item.get("candidates_all", []):
                details = candidate.get("min_order_amount_details")
                if not details:
                    continue
                supplier_id = details.get("supplier_id") or candidate.get(
                    "supplier_id"
                )
                key = (line_no, supplier_id)
                delta = details.get("delta_amount")
                if key not in shortfalls or (
                    delta is not None
                    and delta
                    < shortfalls[key].get("details", {}).get("delta_amount", float("inf"))
                ):
                    shortfalls[key] = {
                        "line_no": line_no,
                        "reason": "min_order_amount_not_met",
                        "details": {
                            "supplier_id": supplier_id,
                            "required_amount": details.get("required_amount"),
                            "actual_amount": details.get("actual_amount"),
                            "delta_amount": details.get("delta_amount"),
                            "suggested_qty_increase": details.get(
                                "suggested_qty_increase"
                            ),
                        },
                    }
        return list(shortfalls.values())

    def determine_order_status(self, items: List[Dict[str, Any]]) -> OrderStatus:
        """
        Determine overall order fulfilment status.
        """
        fully_closed_items = 0
        partially_closed_items = 0
        cannot_close_items = 0

        for item in items:
            match_status = item["match"]["status"]

            if match_status in [MatchStatus.NO_MATCH, MatchStatus.LOW_CONFIDENCE]:
                cannot_close_items += 1
                continue

            candidates = item.get("candidates", [])
            available_candidates = [c for c in candidates if c.get("is_available")]

            if not available_candidates:
                cannot_close_items += 1
                continue

            has_sufficient = any(
                c.get("sufficient_qty") for c in available_candidates
            )

            if has_sufficient:
                fully_closed_items += 1
            else:
                partially_closed_items += 1

        if cannot_close_items > 0:
            return OrderStatus.CANNOT_CLOSE
        if partially_closed_items > 0:
            return OrderStatus.PARTIALLY_CLOSED
        return OrderStatus.FULLY_CLOSED

    def determine_status(self, enriched_payload: Dict[str, Any]) -> Tuple[OrderStatus, Dict[str, Any]]:
        """
        Determine overall order status and provide detailed breakdown.
        """
        self.log("Step 7: Determining order status...")

        items = enriched_payload["items"]

        fully_closed_lines: Set[int] = set()
        partially_closed_lines: Set[int] = set()
        cannot_close_entries: List[Dict[str, Any]] = []

        for item in items:
            line_no = item["line_no"]
            match_info = item.get("match", {}) or {}
            match_status = match_info.get("status")
            raw_candidates_count = item.get("candidates_raw_count", 0)
            solver_candidates = item.get("candidates", [])

            if match_status == MatchStatus.NO_MATCH.value:
                cannot_close_entries.append(
                    {"line_no": line_no, "reason": "unknown_plant"}
                )
                continue

            if match_status == MatchStatus.LOW_CONFIDENCE.value:
                cannot_close_entries.append(
                    {
                        "line_no": line_no,
                        "reason": "low_confidence_match",
                        "details": {
                            "score": match_info.get("score"),
                            "threshold_ok": self.config.sim_threshold_ok,
                        },
                    }
                )
                continue

            if raw_candidates_count == 0:
                cannot_close_entries.append(
                    {"line_no": line_no, "reason": "no_candidates_raw"}
                )
                continue

            if not item.get("goes_to_solver"):
                cannot_close_entries.extend(self._derive_line_reasons(item))
                continue

            has_sufficient = any(
                candidate.get("sufficient_qty") for candidate in solver_candidates
            )

            if has_sufficient:
                fully_closed_lines.add(line_no)
            else:
                partially_closed_lines.add(line_no)

        order_status = self.determine_order_status(items)

        status_breakdown = {
            "fully_closed": sorted(fully_closed_lines),
            "partially_closed": sorted(partially_closed_lines),
            "cannot_close": cannot_close_entries,
        }

        status_details = {
            "order_status": order_status.value,
            "breakdown": status_breakdown,
        }
        self._refresh_status_counts(status_details)

        self.log(f"  -> Order status: {order_status.value}")
        self.log(
            f"    Fully closed: {status_details['counts']['fully_closed']}"
        )
        self.log(
            f"    Partially closed: {status_details['counts']['partially_closed']}"
        )
        self.log(
            f"    Cannot close: {status_details['counts']['cannot_close']}"
        )

        return order_status, status_details
