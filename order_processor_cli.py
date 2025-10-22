"""
Order Processor CLI entry point.

Refactored to delegate business logic to the `order_processor` package.
"""

import argparse
import json
from pathlib import Path
from typing import Any, Dict

from order_processor import OrderProcessor, ProcessorConfig, normalize


def load_message(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def build_config(args: argparse.Namespace) -> ProcessorConfig:
    return ProcessorConfig(
        sim_threshold_ok=args.sim_threshold_ok,
        sim_threshold_low=args.sim_threshold_low,
        allow_insufficient=args.allow_insufficient,
        insufficient_threshold=args.insufficient_threshold,
        price_margin=args.price_margin,
        solver_timeout=args.solver_timeout,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process order from JSON"
    )
    parser.add_argument("input_json", type=Path, help="Input JSON file")
    parser.add_argument(
        "--output", type=Path, help="Output JSON file (default: stdout)"
    )

    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-port", default="5432")
    parser.add_argument("--db-name", default="nursery_beta")
    parser.add_argument("--db-user", default="postgres")
    parser.add_argument("--db-password", default="postpass")

    parser.add_argument(
        "--sim-threshold-ok",
        type=float,
        default=0.42,
        help="Similarity threshold for OK status (default: 0.42)",
    )
    parser.add_argument(
        "--sim-threshold-low",
        type=float,
        default=0.30,
        help="Similarity threshold for LOW_CONFIDENCE (default: 0.30)",
    )
    parser.add_argument(
        "--allow-insufficient",
        action="store_true",
        default=True,
        help="Allow insufficient quantity (default: True)",
    )
    parser.add_argument(
        "--no-allow-insufficient",
        dest="allow_insufficient",
        action="store_false",
        help="Disallow insufficient quantity",
    )
    parser.add_argument(
        "--insufficient-threshold",
        type=float,
        default=0.20,
        help="Max shortage allowed as fraction (default: 0.20 = 20%)",
    )
    parser.add_argument(
        "--price-margin",
        type=float,
        default=None,
        help="Price margin filter (default: None = no filter)",
    )
    parser.add_argument(
        "--solver-timeout",
        type=int,
        default=60,
        help="Solver timeout in seconds (default: 60)",
    )

    parser.add_argument(
        "--quiet", action="store_true", help="Suppress progress messages"
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    message_json = load_message(args.input_json)

    db_config = {
        "host": args.db_host,
        "port": args.db_port,
        "database": args.db_name,
        "user": args.db_user,
        "password": args.db_password,
    }

    config = build_config(args)

    with OrderProcessor(db_config, config=config, verbose=not args.quiet) as processor:
        output = processor.process_order(message_json)

    output = normalize(output)
    output_json = json.dumps(output, indent=2, ensure_ascii=False)

    if args.output:
        args.output.write_text(output_json, encoding="utf-8")
        print(f"\n-> Output written to: {args.output}")
    else:
        print("\n" + output_json)


if __name__ == "__main__":
    main()
