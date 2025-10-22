from copy import deepcopy
from typing import Any, Dict, List, Optional


class ContextMixin:
    """
    Utilities for building and sanitising pipeline context payloads.
    """

    def extract_input_context(self, message_json: Dict[str, Any]) -> Dict[str, Any]:
        """Extract key input metadata for downstream services."""
        data = message_json.get("data", {}) or {}
        parsed_data = data.get("parsed_data", {}) or {}
        original_message = data.get("original_message", {}) or {}
        message_block = original_message.get("message", {}) or {}

        return {
            "key": message_json.get("key"),
            "headers": message_json.get("headers", {}),
            "timestamp": message_json.get("timestamp"),
            "parsed_order": {
                "city": parsed_data.get("city"),
                "items": parsed_data.get("items", []),
            },
            "order_text": message_block.get("text"),
            "source_chat": original_message.get("chat"),
            "sender": original_message.get("sender"),
            "processing_metadata": data.get("processing_metadata"),
        }

    def _strip_supplier_rules(self, payload: Any) -> Any:
        """Remove supplier rules blobs from payload for safe logging/output."""
        if isinstance(payload, dict):
            has_supplier_id = "supplier_id" in payload
            result: Dict[str, Any] = {}
            for key, value in payload.items():
                if key == "supplier_rules":
                    continue
                if key == "rules" and has_supplier_id:
                    continue
                result[key] = self._strip_supplier_rules(value)
            return result
        if isinstance(payload, list):
            return [self._strip_supplier_rules(item) for item in payload]
        return payload

    def _sanitize_for_context(self, payload: Any) -> Any:
        """Deep copy payload stripping supplier rules for pipeline context."""
        if payload is None:
            return None
        return self._strip_supplier_rules(deepcopy(payload))

    def _build_line_diagnostics(self, enriched_payload: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Produce compact diagnostics per line for downstream debugging."""
        diagnostics: List[Dict[str, Any]] = []
        if not enriched_payload:
            return diagnostics
        for item in enriched_payload.get("items", []):
            diagnostics.append(
                {
                    "line_no": item.get("line_no"),
                    "match_status": item.get("match", {}).get("status"),
                    "raw_candidates": item.get("candidates_raw_count"),
                    "solver_candidates": len(item.get("candidates", [])),
                    "rejection_summary": item.get("rejection_summary", {}),
                    "min_order_amount_risks": item.get("min_order_amount_risks", []),
                }
            )
        return diagnostics
