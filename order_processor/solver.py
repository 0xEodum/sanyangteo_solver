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
        Solve supplier assignment using CP-SAT.

        Only processes items that go_to_solver=True.
        """
        self.log("Step 8: Solving assignment (ILP)...")

        items = enriched_payload["items"]
        suppliers_info = enriched_payload["suppliers"]

        self.last_solver_info = {
            "solver": "ortools_cp_sat",
            "status": "NOT_STARTED",
            "parameters": {"timeout_seconds": self.config.solver_timeout},
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

        supplier_ids = set()
        for item in solver_items:
            for candidate in item["candidates"]:
                supplier_ids.add(candidate["supplier_id"])

        supplier_ids = sorted(supplier_ids)

        self.last_solver_info["model_stats"]["suppliers_considered"] = len(supplier_ids)

        self.log(
            f"  -> Problem: {len(solver_items)} items, {len(supplier_ids)} suppliers"
        )

        model = cp_model.CpModel()

        x: Dict[Tuple[int, int], cp_model.IntVar] = {}
        for i, item in enumerate(solver_items):
            line_no = item["line_no"]
            for candidate in item["candidates"]:
                supplier = candidate["supplier_id"]
                x[i, supplier] = model.NewBoolVar(f"x_i{line_no}_s{supplier}")

        y = {supplier: model.NewBoolVar(f"y_s{supplier}") for supplier in supplier_ids}

        for i, item in enumerate(solver_items):
            available_suppliers = [cand["supplier_id"] for cand in item["candidates"]]
            model.Add(sum(x[i, supplier] for supplier in available_suppliers) == 1)

        for i, item in enumerate(solver_items):
            for candidate in item["candidates"]:
                supplier = candidate["supplier_id"]
                model.Add(x[i, supplier] <= y[supplier])

        for supplier in supplier_ids:
            rules = suppliers_info[supplier].get("rules", {})
            constraints = rules.get("constraints", {})
            min_order_amount = constraints.get("min_order_amount")

            if min_order_amount:
                supplier_items: List[Tuple[int, int]] = []
                audit_entry = {
                    "supplier_id": supplier,
                    "min_order_amount": float(min_order_amount),
                    "lines": [],
                }
                for i, item in enumerate(solver_items):
                    qty = item["qty"]
                    for candidate in item["candidates"]:
                        if candidate["supplier_id"] == supplier:
                            price = candidate["price"]
                            if price is None:
                                continue
                            line_total = float(price) * qty
                            amount_cents = int(line_total * 100)
                            supplier_items.append((i, amount_cents))
                            audit_entry["lines"].append(
                                {
                                    "line_no": item["line_no"],
                                    "qty": qty,
                                    "unit_price": float(price),
                                    "line_total": round(line_total, 2),
                                }
                            )
                            break

                if supplier_items:
                    min_amount_cents = int(float(min_order_amount) * 100)
                    total_expr = sum(
                        amount * x[index, supplier] for index, amount in supplier_items
                    )
                    model.Add(total_expr >= min_amount_cents * y[supplier])
                    audit_entry["line_total_sum"] = round(
                        sum(line["line_total"] for line in audit_entry["lines"]), 2
                    )
                    self.last_solver_info["constraint_audit"][
                        "min_order_amount"
                    ].append(audit_entry)

        for i, item in enumerate(solver_items):
            qty = item["qty"]
            for candidate in item["candidates"]:
                supplier = candidate["supplier_id"]
                rules = suppliers_info[supplier].get("rules", {})
                constraints = rules.get("constraints", {})
                min_line_qty = constraints.get("min_line_qty")

                if min_line_qty and qty < min_line_qty:
                    model.Add(x[i, supplier] == 0)

        model.Minimize(sum(y[supplier] for supplier in supplier_ids))

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

        solution = {
            "status": solver.StatusName(status),
            "objective_value": solver.ObjectiveValue(),
            "num_suppliers": int(solver.ObjectiveValue()),
            "assignments": [],
            "suppliers_used": [],
        }
        self.last_solver_info["objective_value"] = solver.ObjectiveValue()

        for supplier in supplier_ids:
            if solver.Value(y[supplier]) == 1:
                solution["suppliers_used"].append(supplier)

        for i, item in enumerate(solver_items):
            line_no = item["line_no"]
            qty = item["qty"]

            for candidate in item["candidates"]:
                supplier = candidate["supplier_id"]
                if (i, supplier) in x and solver.Value(x[i, supplier]) == 1:
                    solution["assignments"].append(
                        {
                            "line_no": line_no,
                            "supplier_id": supplier,
                            "pack_code": candidate.get("pack_code"),
                            "price": float(candidate["price"]),
                            "qty": qty,
                            "shortage_pct": candidate.get("shortage_pct"),
                        }
                    )
                    break

        self.log(f"  -> Solution: {solution['num_suppliers']} suppliers used")
        self.log(f"     Suppliers: {solution['suppliers_used']}")
        self.last_solver_info["suppliers_selected"] = solution["suppliers_used"]

        return solution
