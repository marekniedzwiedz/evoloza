from __future__ import annotations

import json
import sys
from time import perf_counter_ns

from solver import solve


def main() -> int:
    payload = json.load(sys.stdin)
    grid = tuple(payload["grid"])
    budget = int(payload["budget"])

    started_at_ns = perf_counter_ns()
    result = solve(grid, budget=budget)
    elapsed_ns = perf_counter_ns() - started_at_ns

    json.dump(
        {
            "solved": bool(result.solved),
            "path": [[row, col] for row, col in result.path],
            "cost": result.cost,
            "expanded": result.expanded,
            "status": str(result.status),
            "elapsed_ns": elapsed_ns,
        },
        sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
