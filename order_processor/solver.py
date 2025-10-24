from typing import Any, Dict, List, Optional, Tuple

from ortools.sat.python import cp_model

from .config import ProcessorConfig


class SolverMixin:
    """
    CP-SAT assignment solver wrapper.

    Expects:
        - self.config: ProcessorConfig
        - self.log(message: str)
        - self.last_solver_info: Dict[str, Any]
        - self.last_solution: Optional[Dict[str, Any]]
    """

    config: ProcessorConfig
    last_solver_info: Dict[str, Any]
    last_solution: Optional[Dict[str, Any]]

    def solve_assignment(
        self,
        order_id: int,
        enriched_payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Solve supplier assignment using CP-SAT with candidate-level variables.

        NEW MODEL:
        - x[candidate_idx] - choose specific candidate (item, supplier, pack_code, price)
        - y[supplier_id] - supplier is used

        Only processes items that go_to_solver=True.
        """
        self.log("Step 8: Solving assignment (ILP)...")

        items = enriched_payload["items"]
        suppliers_info = enriched_payload["suppliers"]

        self.last_solver_info = {
            "solver": "ortools_cp_sat",
            "status": "NOT_STARTED",
            "parameters": {
                "timeout_seconds": self.config.solver_timeout,
                "optimization_priority": self.config.optimization_priority,
            },
            "model_stats": {"total_items_received": len(items)},
            "constraint_audit": {"min_order_amount": []},
        }

        solver_items = [item for item in items if item["goes_to_solver"]]

        self.last_solver_info["model_stats"].update(
            {
                "items_in_model": len(solver_items),
                "items_skipped": len(items) - len(solver_items),
            }
        )

        if not solver_items:
            self.log("  -> No items ready for solver")
            self.last_solver_info.update(
                {"status": "SKIPPED", "reason": "no_items_for_solver"}
            )
            return None

        # Build flat list of candidates with indices
        all_candidates: List[Dict[str, Any]] = []
        item_candidate_map: Dict[int, List[int]] = {}  # item_idx -> [candidate_indices]

        for item_idx, item in enumerate(solver_items):
            candidate_indices = []
            for candidate in item["candidates"]:
                candidate_idx = len(all_candidates)
                candidate_indices.append(candidate_idx)
                all_candidates.append({
                    "candidate_idx": candidate_idx,
                    "item_idx": item_idx,
                    "line_no": item["line_no"],
                    "qty": item["qty"],
                    "supplier_id": candidate["supplier_id"],
                    "pack_code": candidate.get("pack_code"),
                    "pack_match_status": candidate.get("pack_match_status"),
                    "price": candidate["price"],
                    "shortage_pct": candidate.get("shortage_pct"),
                })
            item_candidate_map[item_idx] = candidate_indices

        supplier_ids = sorted(set(c["supplier_id"] for c in all_candidates))

        self.last_solver_info["model_stats"].update({
            "suppliers_considered": len(supplier_ids),
            "total_candidates": len(all_candidates),
        })

        self.log(
            f"  -> Problem: {len(solver_items)} items, "
            f"{len(all_candidates)} candidates, {len(supplier_ids)} suppliers"
        )

        model = cp_model.CpModel()

        # x[candidate_idx] - binary variable for each candidate
        x: Dict[int, cp_model.IntVar] = {}
        for cand in all_candidates:
            cand_idx = cand["candidate_idx"]
            x[cand_idx] = model.NewBoolVar(
                f"x_c{cand_idx}_i{cand['line_no']}_s{cand['supplier_id']}"
            )

        # y[supplier_id] - binary variable for each supplier
        y = {s_id: model.NewBoolVar(f"y_s{s_id}") for s_id in supplier_ids}

        # Constraint: Each item must select exactly 1 candidate
        for item_idx, candidate_indices in item_candidate_map.items():
            model.Add(sum(x[c_idx] for c_idx in candidate_indices) == 1)

        # Constraint: x[candidate] <= y[supplier] (link candidates to suppliers)
        for cand in all_candidates:
            model.Add(x[cand["candidate_idx"]] <= y[cand["supplier_id"]])

        # Constraint: Min order amount per supplier
        for supplier_id in supplier_ids:
            rules = suppliers_info[supplier_id].get("rules", {})
            constraints = rules.get("constraints", {})
            min_order_amount = constraints.get("min_order_amount")

            if min_order_amount:
                supplier_candidates = [c for c in all_candidates if c["supplier_id"] == supplier_id]
                audit_entry = {
                    "supplier_id": supplier_id,
                    "min_order_amount": float(min_order_amount),
                    "lines": [],
                }

                terms = []
                for cand in supplier_candidates:
                    price = cand["price"]
                    if price is None:
                        continue
                    qty = cand["qty"]
                    line_total = float(price) * qty
                    amount_cents = int(line_total * 100)
                    terms.append(amount_cents * x[cand["candidate_idx"]])

                    audit_entry["lines"].append({
                        "line_no": cand["line_no"],
                        "qty": qty,
                        "unit_price": float(price),
                        "line_total": round(line_total, 2),
                        "pack_code": cand["pack_code"],
                    })

                if terms:
                    min_amount_cents = int(float(min_order_amount) * 100)
                    model.Add(sum(terms) >= min_amount_cents * y[supplier_id])
                    audit_entry["line_total_sum"] = round(
                        sum(line["line_total"] for line in audit_entry["lines"]), 2
                    )
                    self.last_solver_info["constraint_audit"]["min_order_amount"].append(audit_entry)

        # Objective function based on optimization_priority
        if self.config.optimization_priority == "container_match":
            # Minimize: suppliers * 1000 + container_penalties
            # penalty = 1 for 'alike', 0 for 'exactly'
            container_penalties = []
            for cand in all_candidates:
                penalty = 1 if cand.get("pack_match_status") == "alike" else 0
                container_penalties.append(penalty * x[cand["candidate_idx"]])

            model.Minimize(
                sum(y[s_id] for s_id in supplier_ids) * 1000 + sum(container_penalties)
            )
            self.log("  -> Optimization: prioritize exact container matches, then minimize suppliers")
        else:
            # Default: minimize number of suppliers only
            model.Minimize(sum(y[s_id] for s_id in supplier_ids))
            self.log("  -> Optimization: minimize number of suppliers")

        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = float(self.config.solver_timeout)

        self.log("  -> Solving...")
        status = solver.Solve(model)

        self.last_solver_info.update(
            {
                "status": solver.StatusName(status),
                "solver_status_code": status,
                "solve_wall_time_seconds": solver.WallTime(),
            }
        )
        try:
            self.last_solver_info["best_objective_bound"] = solver.BestObjectiveBound()
        except Exception:
            self.last_solver_info["best_objective_bound"] = None

        if status == cp_model.OPTIMAL:
            self.log("  -> OPTIMAL solution found")
        elif status == cp_model.FEASIBLE:
            self.log("  -> FEASIBLE solution found (not proven optimal)")
        else:
            self.log(
                f"  -> No solution found (status={solver.StatusName(status)})"
            )
            self.last_solver_info["reason"] = "solver_returned_no_solution"
            return None

        # Extract solution
        selected_suppliers = [s_id for s_id in supplier_ids if solver.Value(y[s_id]) == 1]

        assignments = []
        for cand in all_candidates:
            if solver.Value(x[cand["candidate_idx"]]) == 1:
                assignments.append({
                    "line_no": cand["line_no"],
                    "supplier_id": cand["supplier_id"],
                    "pack_code": cand["pack_code"],
                    "pack_match_status": cand.get("pack_match_status"),
                    "price": float(cand["price"]),
                    "qty": cand["qty"],
                    "shortage_pct": cand.get("shortage_pct"),
                })

        solution = {
            "status": solver.StatusName(status),
            "objective_value": solver.ObjectiveValue(),
            "num_suppliers": len(selected_suppliers),
            "assignments": assignments,
            "suppliers_used": selected_suppliers,
        }
        self.last_solver_info["objective_value"] = solver.ObjectiveValue()
        self.last_solver_info["suppliers_selected"] = selected_suppliers

        self.log(f"  -> Solution: {solution['num_suppliers']} suppliers used")
        self.log(f"     Suppliers: {solution['suppliers_used']}")

        return solution
