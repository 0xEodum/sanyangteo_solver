import math
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from .config import ProcessorConfig
from .enums import MatchStatus


class CandidateMixin:
    """
    Provides candidate classification and filtering business rules.

    Expects the consuming class to define:
        - self.config: ProcessorConfig
        - self.log(message: str): logging helper
    """

    config: ProcessorConfig  # Protocol hint

    def classify_match_status(self, score: Optional[float]) -> MatchStatus:
        """
        Classify match status based on similarity score.

        Args:
            score: Similarity score from database (0.0 - 1.0)

        Returns:
            MatchStatus classification
        """
        if score is None:
            return MatchStatus.NO_MATCH

        if score >= self.config.sim_threshold_ok:
            return MatchStatus.OK
        if score >= self.config.sim_threshold_low:
            return MatchStatus.LOW_CONFIDENCE
        return MatchStatus.NO_MATCH

    def is_candidate_available(
        self,
        qty_requested: int,
        qty_available: Optional[int],
    ) -> Tuple[bool, Optional[float]]:
        """
        Check if candidate has sufficient availability.

        Args:
            qty_requested: Quantity requested in order
            qty_available: Available quantity from supplier (None = unlimited)

        Returns:
            (is_available, shortage_pct)
        """
        if qty_available is None:
            return True, None

        if qty_available >= qty_requested:
            return True, None

        shortage_pct = (qty_requested - qty_available) / qty_requested

        if (
            self.config.allow_insufficient
            and shortage_pct <= self.config.insufficient_threshold
        ):
            return True, shortage_pct * 100

        return False, shortage_pct * 100

    def filter_candidates_by_price(
        self,
        candidates: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Filter candidates by price margin (if configured).
        """
        if not candidates or self.config.price_margin is None:
            return candidates

        min_price = min(c["price"] for c in candidates)
        max_allowed = min_price * (1 + self.config.price_margin)

        return [c for c in candidates if c["price"] <= max_allowed]

    def classify_candidates(
        self,
        candidates: List[Dict[str, Any]],
        qty_requested: int,
    ) -> List[Dict[str, Any]]:
        """
        Classify and enrich candidates with availability status.
        """
        enriched: List[Dict[str, Any]] = []

        for candidate in candidates:
            qty_available = candidate.get("availability_qty")
            is_available, shortage_pct = self.is_candidate_available(
                qty_requested, qty_available
            )

            enriched.append(
                {
                    **candidate,
                    "is_available": is_available,
                    "sufficient_qty": qty_available is None
                    or qty_available >= qty_requested,
                    "shortage_pct": shortage_pct,
                }
            )

        return enriched

    def filter_and_classify_candidates(self, candidates_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter and classify candidates using business rules.
        """
        self.log("Step 6: Filtering and classifying candidates...")

        items = candidates_payload.get("items", [])
        suppliers = {
            s["supplier_id"]: s for s in candidates_payload.get("suppliers", [])
        }

        enriched_items: List[Dict[str, Any]] = []
        items_with_candidates = 0
        items_for_solver = 0

        for item in items:
            match_status = item["match"]["status"]
            qty = item["qty"]
            raw_candidates = item.get("candidates", [])
            raw_candidates_count = len(raw_candidates)

            price_values = [
                float(c["price"]) for c in raw_candidates if c.get("price") is not None
            ]
            max_allowed_price: Optional[float] = None
            if price_values and self.config.price_margin is not None:
                min_price = min(price_values)
                max_allowed_price = round(min_price * (1 + self.config.price_margin), 2)

            classified_candidates: List[Dict[str, Any]] = []
            rejection_summary: Dict[str, int] = {}
            min_order_amount_risks: List[Dict[str, Any]] = []

            for candidate in raw_candidates:
                candidate_info = {
                    k: v for k, v in candidate.items() if k != "supplier_rules"
                }
                supplier_id = candidate_info.get("supplier_id")
                supplier_meta = suppliers.get(supplier_id, {})
                supplier_rules = (
                    candidate.get("supplier_rules")
                    or supplier_meta.get("rules")
                    or {}
                )
                constraints = (
                    supplier_rules.get("constraints", {}) if supplier_rules else {}
                )
                policies = supplier_rules.get("policies", {}) if supplier_rules else {}

                price = candidate_info.get("price")
                if isinstance(price, Decimal):
                    price = float(price)
                candidate_info["price"] = price

                availability_qty = candidate.get("availability_qty")
                if isinstance(availability_qty, Decimal):
                    availability_qty = float(availability_qty)
                candidate_info["availability_qty"] = availability_qty

                reason_details: List[Dict[str, Any]] = []
                rejection_reasons: List[str] = []

                is_available, shortage_pct = self.is_candidate_available(
                    qty, availability_qty
                )
                candidate_info["is_available"] = is_available
                candidate_info["sufficient_qty"] = availability_qty is None or (
                    availability_qty >= qty
                )
                candidate_info["shortage_pct"] = (
                    round(float(shortage_pct), 2)
                    if shortage_pct is not None
                    else None
                )

                if not is_available:
                    details = {
                        "supplier_id": supplier_id,
                        "requested": qty,
                        "available": availability_qty,
                        "shortage_pct": candidate_info["shortage_pct"],
                    }
                    rejection_reasons.append("insufficient_quantity")
                    reason_details.append(
                        {"code": "insufficient_quantity", "details": details}
                    )
                elif shortage_pct is not None:
                    candidate_info["availability_details"] = {
                        "supplier_id": supplier_id,
                        "requested": qty,
                        "available": availability_qty,
                        "shortage_pct": candidate_info["shortage_pct"],
                    }

                min_line_qty = constraints.get("min_line_qty")
                if min_line_qty and qty < min_line_qty:
                    details = {
                        "supplier_id": supplier_id,
                        "qty": qty,
                        "min_line_qty": min_line_qty,
                    }
                    rejection_reasons.append("below_min_line_qty")
                    reason_details.append(
                        {"code": "below_min_line_qty", "details": details}
                    )

                if max_allowed_price is not None and price is not None:
                    if price > max_allowed_price:
                        details = {
                            "supplier_id": supplier_id,
                            "price": price,
                            "max_allowed_price": max_allowed_price,
                        }
                        rejection_reasons.append("price_above_margin")
                        reason_details.append(
                            {"code": "price_above_margin", "details": details}
                        )

                if policies.get("blacklisted"):
                    details = {"supplier_id": supplier_id}
                    rejection_reasons.append("supplier_blacklisted")
                    reason_details.append(
                        {"code": "supplier_blacklisted", "details": details}
                    )

                policy_filters = candidate.get("policy_filters") or policies.get(
                    "filters"
                )
                if policy_filters:
                    details = {"supplier_id": supplier_id, "policies": policy_filters}
                    rejection_reasons.append("filtered_out_by_policy")
                    reason_details.append(
                        {"code": "filtered_out_by_policy", "details": details}
                    )

                min_order_amount = constraints.get("min_order_amount")
                line_total = None
                if price is not None:
                    line_total = float(price) * qty
                    candidate_info["line_total"] = line_total

                if min_order_amount and line_total is not None:
                    delta_amount = float(min_order_amount) - line_total
                    if delta_amount > 0:
                        suggested_qty_increase = (
                            math.ceil(delta_amount / price) if price else None
                        )
                        moa_details = {
                            "supplier_id": supplier_id,
                            "required_amount": float(min_order_amount),
                            "actual_amount": round(line_total, 2),
                            "delta_amount": round(delta_amount, 2),
                            "suggested_qty_increase": suggested_qty_increase,
                        }
                        candidate_info["min_order_amount_risk"] = True
                        candidate_info["min_order_amount_details"] = moa_details
                        reason_details.append(
                            {"code": "min_order_amount_risk", "details": moa_details}
                        )
                        min_order_amount_risks.append(
                            {
                                "line_no": item["line_no"],
                                **moa_details,
                            }
                        )
                    else:
                        candidate_info["min_order_amount_risk"] = False
                else:
                    candidate_info["min_order_amount_risk"] = False

                candidate_info["eligible_for_solver"] = len(rejection_reasons) == 0
                candidate_info["rejected_reason"] = (
                    rejection_reasons[0] if rejection_reasons else None
                )
                candidate_info["rejection_reasons"] = rejection_reasons
                candidate_info["rejection_details"] = (
                    reason_details[0]["details"] if rejection_reasons else None
                )
                candidate_info["reason_details"] = reason_details

                if rejection_reasons:
                    for reason_code in rejection_reasons:
                        rejection_summary[reason_code] = (
                            rejection_summary.get(reason_code, 0) + 1
                        )

                classified_candidates.append(candidate_info)

            available_candidates = [
                c for c in classified_candidates if c.get("eligible_for_solver")
            ]

            goes_to_solver = (
                match_status in ["ok", "manual_override"] and len(available_candidates) > 0
            )

            item_snapshot = {k: v for k, v in item.items() if k != "candidates"}
            enriched_item = {
                **item_snapshot,
                "candidates_raw_count": raw_candidates_count,
                "candidates_filtered_count": len(available_candidates),
                "candidates_total_count": len(classified_candidates),
                "candidates_available_count": len(available_candidates),
                "candidates": available_candidates,
                "candidates_all": classified_candidates,
                "rejection_summary": rejection_summary,
                "max_allowed_price": max_allowed_price,
                "min_order_amount_risks": min_order_amount_risks,
                "goes_to_solver": goes_to_solver,
            }

            enriched_items.append(enriched_item)

            if raw_candidates_count > 0:
                items_with_candidates += 1

            if goes_to_solver:
                items_for_solver += 1

        result = {
            "order_id": candidates_payload["order_id"],
            "items": enriched_items,
            "suppliers": suppliers,
            "stats": {
                "total_items": len(items),
                "items_with_candidates": items_with_candidates,
                "items_for_solver": items_for_solver,
                "items_cannot_solve": len(items) - items_for_solver,
            },
            "config_used": self.config.to_dict(),
        }

        self.log(
            f"  -> Ready for solver: {items_for_solver}/{len(items)} items"
        )

        return result
