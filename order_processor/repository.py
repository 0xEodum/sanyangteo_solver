from decimal import Decimal
from typing import Any, Dict, List

from psycopg2.extras import Json, RealDictCursor

from .config import ProcessorConfig
from .enums import MatchStatus, OrderStatus


class RepositoryMixin:
    """
    Database access layer for the order processor.

    Expects:
        - self.conn: psycopg2 connection
        - self.config: ProcessorConfig
        - self.log(message: str)
        - self.classify_match_status: callable returning MatchStatus
    """

    config: ProcessorConfig

    def ingest_message(self, message_json: Dict[str, Any]) -> int:
        """
        Ingest message into ingest_messages table.

        Returns:
            msg_id
        """
        self.log("Step 1: Ingesting message...")

        cursor = self.conn.cursor()

        idempotency_key = message_json.get("key")
        headers = message_json.get("headers", {})
        data = message_json.get("data", {})

        classification = headers.get("classification", "ORDER")

        provider = headers.get("provider")
        service = headers.get("service")
        parsing_schema = headers.get("parsing_schema_version")

        validation_passed = headers.get("validation_passed") == "True"

        cursor.execute(
            """
            INSERT INTO ingest_messages (
                idempotency_key, provider, service, parsing_schema,
                classification, headers, payload, original_msg, validation_passed
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (idempotency_key)
            DO UPDATE SET
                headers = EXCLUDED.headers,
                payload = EXCLUDED.payload,
                validation_passed = EXCLUDED.validation_passed
            RETURNING msg_id
        """,
            (
                idempotency_key,
                provider,
                service,
                parsing_schema,
                classification,
                Json(headers),
                Json(data.get("parsed_data", {})),
                Json(data.get("original_message", {})),
                validation_passed,
            ),
        )

        msg_id = cursor.fetchone()[0]
        self.log(f"  -> Message ingested: msg_id={msg_id}")

        cursor.close()
        return msg_id

    def create_order(self, msg_id: int, message_json: Dict[str, Any]) -> int:
        """
        Create order and order items from parsed data.

        Returns:
            order_id
        """
        self.log("Step 2: Creating order...")

        cursor = self.conn.cursor()

        data = message_json.get("data", {})
        parsed = data.get("parsed_data", {})
        original = data.get("original_message", {})

        sender = original.get("sender", {})
        buyer_contact = {
            "telegram_id": sender.get("id"),
            "username": sender.get("username"),
            "display_name": sender.get("display_name"),
            "city": parsed.get("city"),
        }

        cursor.execute(
            """
            INSERT INTO orders (source_msg_id, buyer_contact)
            VALUES (%s, %s)
            RETURNING order_id
        """,
            (msg_id, Json(buyer_contact)),
        )

        order_id = cursor.fetchone()[0]

        items = parsed.get("items", [])
        for line_no, item in enumerate(items, start=1):
            height_text = None
            height_min = None
            height_max = None
            height_unit = None

            height = item.get("height")
            if height:
                height_unit = item.get("height_unit", "см")
                if isinstance(height, str) and "-" in height:
                    height_text = height
                    parts = height.split("-")
                    try:
                        height_min = Decimal(parts[0])
                        height_max = Decimal(parts[1])
                    except Exception:
                        pass
                else:
                    height_text = str(height)
                    try:
                        height_min = Decimal(str(height))
                        height_max = height_min
                    except Exception:
                        pass

            cursor.execute(
                """
                INSERT INTO order_items (
                    order_id, line_no, raw_name, lang, qty, qty_unit,
                    height_text, height_min, height_max, height_unit
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    order_id,
                    line_no,
                    item.get("plant_name"),
                    "ru",
                    item.get("quantity"),
                    item.get("quantity_unit", "шт"),
                    height_text,
                    height_min,
                    height_max,
                    height_unit,
                ),
            )

        cursor.close()
        self.log(f"  -> Order created: order_id={order_id}, items={len(items)}")
        return order_id

    def match_plants_raw(self, order_id: int) -> List[Dict[str, Any]]:
        """
        Get raw matching results from database.
        """
        self.log("Step 3: Getting raw plant matches...")

        cursor = self.conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(
            """
            SELECT * FROM fn_match_order_items_raw(%s)
        """,
            (order_id,),
        )

        matches = [dict(row) for row in cursor.fetchall()]

        self.log(f"  -> Got {len(matches)} raw matches")

        cursor.close()
        return matches

    def classify_and_update_matches(self, order_id: int, matches: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Classify matches using configured thresholds and update database.
        """
        self.log("Step 4: Classifying matches...")

        cursor = self.conn.cursor()

        stats = {"ok": 0, "low_confidence": 0, "no_match": 0}

        for match in matches:
            score = match.get("score")
            status = self.classify_match_status(score)

            cursor.execute(
                """
                UPDATE order_items
                SET plant_id = %s,
                    match_state = %s,
                    match_score = %s,
                    match_meta = %s,
                    modified_by = 'system',
                    modified_at = now()
                WHERE order_id = %s AND line_no = %s
            """,
                (
                    match["plant_id"],
                    status.value,
                    score,
                    Json(
                        {
                            "canonical_name": match["canonical_name"],
                            "matched_synonym": match["matched_synonym"],
                            "classification_threshold_ok": self.config.sim_threshold_ok,
                            "classification_threshold_low": self.config.sim_threshold_low,
                        }
                    ),
                    order_id,
                    match["line_no"],
                ),
            )

            stats[status.value] += 1

        cursor.execute(
            """
            SELECT line_no FROM order_items
            WHERE order_id = %s AND line_no NOT IN %s
        """,
            (order_id, tuple(m["line_no"] for m in matches) if matches else (-1,)),
        )

        no_match_items = cursor.fetchall()
        stats["no_match"] += len(no_match_items)

        self.log(
            "  -> Classified: "
            f"{stats['ok']} ok, "
            f"{stats['low_confidence']} low_confidence, "
            f"{stats['no_match']} no_match"
        )

        cursor.close()
        return stats

    def get_candidates_raw(self, order_id: int) -> Dict[str, Any]:
        """
        Get ALL candidates from database (no filtering).
        """
        self.log("Step 5: Getting all candidates (raw)...")

        cursor = self.conn.cursor()

        cursor.execute(
            """
            SELECT fn_get_order_candidates(%s)
        """,
            (order_id,),
        )

        candidates = cursor.fetchone()[0]

        items = candidates.get("items", [])
        total_candidates = sum(len(item.get("candidates", [])) for item in items)

        self.log(
            f"  -> Got {len(items)} items, {total_candidates} total candidates"
        )

        cursor.close()
        return candidates

    def get_order_summary(self, order_id: int) -> Dict[str, Any]:
        """Fetch order summary with aggregated stats."""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT fn_get_order_summary(%s) AS summary", (order_id,))
        row = cursor.fetchone() or {}
        cursor.close()
        return row.get("summary") or {}

    def _calculate_discount(self, subtotal: Decimal, rules: Dict[str, Any]) -> Decimal:
        """Calculate total discount based on supplier rules."""
        discounts = rules.get("discounts", {})
        order_discounts = discounts.get("order_amount", [])
        discount_pct = Decimal("0")

        for tier in sorted(
            order_discounts, key=lambda x: x["threshold"], reverse=True
        ):
            if subtotal >= tier["threshold"]:
                discount_pct = Decimal(str(tier["percent"]))
                break

        return subtotal * (discount_pct / 100)

    def _get_discount_breakdown(self, subtotal: Decimal, rules: Dict[str, Any]) -> Dict[str, Any]:
        """Get discount breakdown for details."""
        discounts = rules.get("discounts", {})
        order_discounts = discounts.get("order_amount", [])

        for tier in sorted(
            order_discounts, key=lambda x: x["threshold"], reverse=True
        ):
            if subtotal >= tier["threshold"]:
                return {
                    "type": "order_amount",
                    "threshold": tier["threshold"],
                    "percent": tier["percent"],
                    "amount": float(subtotal * Decimal(str(tier["percent"])) / 100),
                }

        return {}

    def record_assignment(
        self,
        order_id: int,
        solution: Dict[str, Any],
        order_status: OrderStatus,
        status_details: Dict[str, Any],
    ) -> int:
        """
        Record assignment in database.
        """
        self.log("Step 9: Recording assignment...")

        cursor = self.conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(
            """
            INSERT INTO assignment_runs (
                order_id, solver, objective, config, status, meta
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING run_id
        """,
            (
                order_id,
                "ortools_cp_sat",
                "minimize_suppliers",
                Json(self.config.to_dict()),
                solution["status"],
                Json(
                    {
                        "num_suppliers": solution["num_suppliers"],
                        "objective_value": solution["objective_value"],
                        "suppliers_used": solution["suppliers_used"],
                        "order_status": order_status.value,
                        "status_details": status_details,
                    }
                ),
            ),
        )

        run_id = cursor.fetchone()["run_id"]

        for assignment in solution["assignments"]:
            cursor.execute(
                """
                INSERT INTO item_assignments (
                    run_id, order_id, line_no, supplier_id, pack_code, unit_price, currency
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    run_id,
                    order_id,
                    assignment["line_no"],
                    assignment["supplier_id"],
                    assignment["pack_code"],
                    assignment["price"],
                    "RUB",
                ),
            )

        supplier_totals: Dict[int, Decimal] = {}
        for assignment in solution["assignments"]:
            supplier_id = assignment["supplier_id"]
            supplier_totals.setdefault(supplier_id, Decimal("0"))
            supplier_totals[supplier_id] += Decimal(str(assignment["price"])) * assignment[
                "qty"
            ]

        for supplier_id, subtotal in supplier_totals.items():
            cursor.execute(
                """
                SELECT rules FROM suppliers WHERE supplier_id = %s
            """,
                (supplier_id,),
            )

            supplier = cursor.fetchone()
            rules = supplier["rules"] if supplier else {}

            discount_amt = self._calculate_discount(subtotal, rules)

            extra_fees = Decimal("0")
            extra = rules.get("extra", {})
            delivery_fee = extra.get("delivery_fee", 0)
            free_threshold = extra.get("free_delivery_threshold")

            if delivery_fee and (not free_threshold or subtotal < free_threshold):
                extra_fees = Decimal(str(delivery_fee))

            total = subtotal - discount_amt + extra_fees

            cursor.execute(
                """
                INSERT INTO supplier_baskets (
                    run_id, supplier_id, subtotal, discount_amt, extra_fees, total, details
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    run_id,
                    supplier_id,
                    float(subtotal),
                    float(discount_amt),
                    float(extra_fees),
                    float(total),
                    Json(
                        {
                            "rules_applied": rules,
                            "discount_breakdown": self._get_discount_breakdown(
                                subtotal, rules
                            ),
                        }
                    ),
                ),
            )

        cursor.execute(
            """
            UPDATE assignment_runs SET finished_at = now() WHERE run_id = %s
        """,
            (run_id,),
        )

        self.log(f"  -> Assignment recorded: run_id={run_id}")

        cursor.close()
        return run_id

    def generate_output(
        self,
        order_id: int,
        run_id: int,
        order_status: OrderStatus,
        status_details: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Generate final output JSON.
        """
        self.log("Step 10: Generating output...")

        order_summary = self.get_order_summary(order_id)

        cursor = self.conn.cursor(cursor_factory=RealDictCursor)

        # Get assignment run
        cursor.execute(
            """
            SELECT * FROM assignment_runs WHERE run_id = %s
        """,
            (run_id,),
        )

        run = cursor.fetchone()

        # Get item assignments
        cursor.execute(
            """
            SELECT
                ia.*,
                oi.raw_name,
                oi.qty,
                oi.qty_unit,
                p.canonical_name,
                s.name as supplier_name
            FROM item_assignments ia
            JOIN order_items oi ON oi.order_id = ia.order_id AND oi.line_no = ia.line_no
            LEFT JOIN plants p ON p.plant_id = oi.plant_id
            JOIN suppliers s ON s.supplier_id = ia.supplier_id
            WHERE ia.run_id = %s
            ORDER BY ia.line_no
        """,
            (run_id,),
        )

        assignments = cursor.fetchall()
        shortage_lookup: Dict[tuple, Any] = {}
        if getattr(self, "last_solution", None):
            for assignment in self.last_solution.get("assignments", []):
                key = (assignment.get("line_no"), assignment.get("supplier_id"))
                shortage_lookup[key] = assignment.get("shortage_pct")

        # Get supplier baskets with contact info
        cursor.execute(
            """
            SELECT
                sb.*,
                s.name as supplier_name,
                s.phone,
                s.email,
                s.telegram
            FROM supplier_baskets sb
            JOIN suppliers s ON s.supplier_id = sb.supplier_id
            WHERE sb.run_id = %s
            ORDER BY sb.total DESC
        """,
            (run_id,),
        )

        baskets = cursor.fetchall()

        cursor.close()

        # Build assignment entries with nested structure
        assignment_entries: List[Dict[str, Any]] = []
        for a in assignments:
            key = (a["line_no"], a["supplier_id"])
            unit_price = float(a["unit_price"])
            entry = {
                "line_no": a["line_no"],
                "raw_name": a["raw_name"],
                "canonical_name": a["canonical_name"],
                "qty": a["qty"],
                "qty_unit": a["qty_unit"],
                "supplier": {"id": a["supplier_id"], "name": a["supplier_name"]},
                "pack_code": a["pack_code"],
                "unit_price": unit_price,
                "line_total": unit_price * a["qty"],
                "currency": a["currency"],
            }
            shortage_pct = shortage_lookup.get(key)
            if shortage_pct is not None:
                entry["shortage_pct"] = shortage_pct
            assignment_entries.append(entry)

        # Build output
        total_cost = float(sum(b["total"] for b in baskets))

        output = {
            "success": True,
            "order_id": order_id,
            "run_id": run_id,
            "order_status": order_status.value,
            "status_details": status_details,
            "summary": order_summary,
            "solution": {
                "status": run["status"],
                "solver": run["solver"],
                "objective": run["objective"],
                "num_suppliers": len(baskets),
                "total_cost": total_cost,
                "solved_at": (
                    run["started_at"].isoformat() if run["started_at"] else None
                ),
                "solve_time_seconds": (
                    (run["finished_at"] - run["started_at"]).total_seconds()
                    if run["finished_at"] and run["started_at"]
                    else None
                ),
            },
            "config_used": run["config"],
            "assignments": assignment_entries,
            "baskets": [
                {
                    "supplier": {
                        "id": b["supplier_id"],
                        "name": b["supplier_name"],
                        "phone": b["phone"],
                        "email": b["email"],
                        "telegram": b["telegram"],
                    },
                    "subtotal": float(b["subtotal"]),
                    "discount_amount": float(b["discount_amt"]),
                    "discount_percent": (
                        round(float(b["discount_amt"]) / float(b["subtotal"]) * 100, 2)
                        if b["subtotal"] > 0
                        else 0
                    ),
                    "extra_fees": float(b["extra_fees"]),
                    "total": float(b["total"]),
                    "details": b["details"],
                }
                for b in baskets
            ],
        }

        self.log(f"  -> Output generated")
        self.log(f"     Order status: {order_status.value}")
        self.log(f"     Total cost: {total_cost:.2f} RUB")
        self.log(f"     Suppliers: {len(baskets)}")

        return output
