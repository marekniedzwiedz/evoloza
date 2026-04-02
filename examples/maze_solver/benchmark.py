from __future__ import annotations

import json
import math
import random
import subprocess
import sys
from dataclasses import dataclass
from heapq import heappop, heappush
from pathlib import Path

from solver import SearchResult


Coordinate = tuple[int, int]
ORTHOGONAL_STEPS = ((1, 0), (-1, 0), (0, 1), (0, -1))
DEFAULT_SIZE = 224
DEFAULT_BUDGET = 960
DEFAULT_TIMEOUT_MS = 16
SOLVE_BONUS = 10_000
QUALITY_BONUS = 2_500
QUALITY_PENALTY = 12
LATENCY_BONUS = 400
LATENCY_BUCKET_NS = 2_000_000
RUNNER_TIMEOUT_SLACK_SECONDS = 0.5
RUNNER_PATH = Path(__file__).with_name("solver_runner.py")


@dataclass(frozen=True)
class CaseSpec:
    kind: str
    seed: int
    size: int
    budget: int
    timeout_ms: int
    wall_rate: float
    roughness: int
    scenic_bias: float
    turn_chance: float
    belt_stride: int
    toll_weight: int
    cluster_rounds: int


CASE_SPECS = (
    CaseSpec("switchbacks", 7, 224, 940, 16, 0.14, 0, 0.58, 0.26, 23, 8, 8),
    CaseSpec("switchbacks", 11, 256, 1_090, 18, 0.14, 0, 0.58, 0.26, 23, 8, 10),
    CaseSpec("switchbacks", 19, 320, 1_360, 22, 0.14, 0, 0.58, 0.26, 29, 8, 12),
    CaseSpec("switchbacks", 23, 384, 1_700, 26, 0.14, 0, 0.58, 0.26, 31, 8, 14),
    CaseSpec("tollways", 31, 224, 980, 16, 0.10, 2, 0.50, 0.20, 17, 9, 10),
    CaseSpec("tollways", 43, 256, 1_140, 18, 0.10, 2, 0.50, 0.20, 19, 9, 12),
    CaseSpec("tollways", 59, 320, 1_450, 22, 0.10, 2, 0.50, 0.20, 23, 9, 14),
    CaseSpec("tollways", 71, 384, 1_820, 26, 0.10, 2, 0.50, 0.20, 29, 9, 16),
    CaseSpec("dense", 83, 224, 1_040, 18, 0.21, 1, 0.66, 0.34, 27, 8, 14),
    CaseSpec("dense", 97, 256, 1_220, 20, 0.21, 1, 0.66, 0.34, 29, 8, 16),
    CaseSpec("dense", 101, 320, 1_520, 24, 0.21, 1, 0.66, 0.34, 31, 8, 18),
    CaseSpec("dense", 107, 384, 1_920, 28, 0.21, 1, 0.66, 0.34, 37, 8, 20),
)


def build_case(case: CaseSpec | int, size: int = DEFAULT_SIZE) -> tuple[str, ...]:
    spec = normalize_case_spec(case, size)
    rng = random.Random(spec.seed)
    grid = [[str(weight_for(rng, spec.roughness)) for _ in range(spec.size)] for _ in range(spec.size)]

    mandatory_path = monotone_path(rng, spec.size, down_bias=0.52, turn_chance=0.18)
    scenic_path = monotone_path(
        rng,
        spec.size,
        down_bias=spec.scenic_bias,
        turn_chance=spec.turn_chance,
    )
    bypass_path = monotone_path(
        rng,
        spec.size,
        down_bias=1.0 - spec.scenic_bias,
        turn_chance=max(0.15, spec.turn_chance - 0.08),
    )
    protected = mandatory_path | scenic_path | bypass_path | {(0, 0), (spec.size - 1, spec.size - 1)}

    for row_index in range(spec.size):
        for col_index in range(spec.size):
            node = (row_index, col_index)
            if node in scenic_path:
                grid[row_index][col_index] = str(rng.randint(1, 2))
            elif node in bypass_path:
                grid[row_index][col_index] = str(rng.randint(2, 4))
            elif node in mandatory_path:
                grid[row_index][col_index] = str(rng.randint(5, 9))
            elif rng.random() < spec.wall_rate:
                grid[row_index][col_index] = "#"
            else:
                grid[row_index][col_index] = str(weight_for(rng, spec.roughness))

    apply_toll_belts(grid, spec, scenic_path, protected)
    apply_wall_clusters(grid, spec, rng, protected)
    soften_scenic_corridor(grid, scenic_path)

    grid[0][0] = "S"
    grid[spec.size - 1][spec.size - 1] = "G"
    return tuple("".join(row_cells) for row_cells in grid)


def normalize_case_spec(case: CaseSpec | int, size: int) -> CaseSpec:
    if isinstance(case, CaseSpec):
        return case
    return CaseSpec(
        kind="classic",
        seed=case,
        size=size,
        budget=DEFAULT_BUDGET,
        timeout_ms=DEFAULT_TIMEOUT_MS,
        wall_rate=0.18,
        roughness=1,
        scenic_bias=0.60,
        turn_chance=0.24,
        belt_stride=23,
        toll_weight=8,
        cluster_rounds=10,
    )


def monotone_path(
    rng: random.Random,
    size: int,
    down_bias: float,
    turn_chance: float,
) -> set[Coordinate]:
    path = {(0, 0)}
    row = 0
    col = 0
    prefer_down = rng.random() < down_bias
    streak = 0

    while row < size - 1 or col < size - 1:
        if row == size - 1:
            move_down = False
        elif col == size - 1:
            move_down = True
        else:
            if streak >= 5 and rng.random() < turn_chance:
                prefer_down = not prefer_down
                streak = 0
            move_down = prefer_down if rng.random() < 0.72 else not prefer_down
        if move_down:
            row += 1
        else:
            col += 1
        path.add((row, col))
        streak += 1
        if rng.random() < turn_chance:
            prefer_down = not prefer_down
            streak = 0

    return path


def apply_toll_belts(
    grid: list[list[str]],
    spec: CaseSpec,
    scenic_path: set[Coordinate],
    protected: set[Coordinate],
) -> None:
    size = spec.size
    row_belts = range(spec.belt_stride // 2, size - 1, spec.belt_stride)
    col_belts = range((spec.belt_stride * 2) // 3, size - 1, spec.belt_stride)

    for belt_row in row_belts:
        gate_col = path_gate_for_row(scenic_path, belt_row)
        for col_index in range(size):
            node = (belt_row, col_index)
            if node in protected:
                continue
            if abs(col_index - gate_col) <= 1:
                grid[belt_row][col_index] = min_digit(grid[belt_row][col_index], 2)
            elif grid[belt_row][col_index] != "#":
                grid[belt_row][col_index] = str(spec.toll_weight)

    for belt_col in col_belts:
        gate_row = path_gate_for_col(scenic_path, belt_col)
        for row_index in range(size):
            node = (row_index, belt_col)
            if node in protected:
                continue
            if abs(row_index - gate_row) <= 1:
                grid[row_index][belt_col] = min_digit(grid[row_index][belt_col], 2)
            elif grid[row_index][belt_col] != "#":
                grid[row_index][belt_col] = str(spec.toll_weight)


def path_gate_for_row(path: set[Coordinate], row_index: int) -> int:
    matches = sorted(col for row, col in path if row == row_index)
    if not matches:
        raise ValueError("Scenic path must cross every row in a monotone grid")
    return matches[len(matches) // 2]


def path_gate_for_col(path: set[Coordinate], col_index: int) -> int:
    matches = sorted(row for row, col in path if col == col_index)
    if not matches:
        raise ValueError("Scenic path must cross every column in a monotone grid")
    return matches[len(matches) // 2]


def apply_wall_clusters(
    grid: list[list[str]],
    spec: CaseSpec,
    rng: random.Random,
    protected: set[Coordinate],
) -> None:
    size = spec.size
    for _ in range(spec.cluster_rounds):
        height = rng.randint(2, 6)
        width = rng.randint(2, 7)
        top = rng.randint(1, size - height - 2)
        left = rng.randint(1, size - width - 2)
        rectangle = {
            (row_index, col_index)
            for row_index in range(top, top + height)
            for col_index in range(left, left + width)
        }
        if rectangle & protected:
            continue
        for row_index, col_index in rectangle:
            if rng.random() < 0.88:
                grid[row_index][col_index] = "#"
            else:
                grid[row_index][col_index] = str(spec.toll_weight)


def soften_scenic_corridor(grid: list[list[str]], scenic_path: set[Coordinate]) -> None:
    for row_index, col_index in scenic_path:
        for row_delta, col_delta in ORTHOGONAL_STEPS:
            neighbor = (row_index + row_delta, col_index + col_delta)
            neighbor_row, neighbor_col = neighbor
            if not (0 <= neighbor_row < len(grid) and 0 <= neighbor_col < len(grid[0])):
                continue
            if grid[neighbor_row][neighbor_col] == "#":
                continue
            grid[neighbor_row][neighbor_col] = min_digit(grid[neighbor_row][neighbor_col], 3)


def min_digit(cell: str, limit: int) -> str:
    if cell in {"S", "G", ".", "#"}:
        return cell
    return str(min(int(cell), limit))


def weight_for(rng: random.Random, roughness: int = 0) -> int:
    roll = rng.random()
    if roughness <= 0:
        thresholds = (0.48, 0.74, 0.88, 0.96)
    elif roughness == 1:
        thresholds = (0.34, 0.60, 0.80, 0.92)
    else:
        thresholds = (0.22, 0.46, 0.68, 0.86)
    if roll < thresholds[0]:
        return 1
    if roll < thresholds[1]:
        return 2
    if roll < thresholds[2]:
        return 3
    if roll < thresholds[3]:
        return 5
    return 8


def find_terminals(grid: tuple[str, ...]) -> tuple[Coordinate, Coordinate]:
    start: Coordinate | None = None
    goal: Coordinate | None = None
    for row_index, row in enumerate(grid):
        for col_index, cell in enumerate(row):
            if cell == "S":
                start = (row_index, col_index)
            elif cell == "G":
                goal = (row_index, col_index)
    if start is None or goal is None:
        raise ValueError("Grid must contain S and G")
    return start, goal


def is_open(grid: tuple[str, ...], node: Coordinate) -> bool:
    row, col = node
    return 0 <= row < len(grid) and 0 <= col < len(grid[0]) and grid[row][col] != "#"


def cell_cost(grid: tuple[str, ...], node: Coordinate) -> int:
    row, col = node
    cell = grid[row][col]
    if cell in {"S", "G", "."}:
        return 1
    return int(cell)


def dijkstra_cost(grid: tuple[str, ...]) -> int:
    start, goal = find_terminals(grid)
    frontier: list[tuple[int, Coordinate]] = [(0, start)]
    best_cost = {start: 0}

    while frontier:
        current_cost, current = heappop(frontier)
        if current == goal:
            return current_cost
        if current_cost != best_cost[current]:
            continue
        row, col = current
        for row_delta, col_delta in ORTHOGONAL_STEPS:
            neighbor = (row + row_delta, col + col_delta)
            if not is_open(grid, neighbor):
                continue
            new_cost = current_cost + cell_cost(grid, neighbor)
            if new_cost >= best_cost.get(neighbor, 10**9):
                continue
            best_cost[neighbor] = new_cost
            heappush(frontier, (new_cost, neighbor))
    raise ValueError("Benchmark case is unsolved for exact search")


def validate_result(grid: tuple[str, ...], result: SearchResult) -> tuple[bool, int]:
    if not result.solved:
        return True, -1
    start, goal = find_terminals(grid)
    if not result.path or result.path[0] != start or result.path[-1] != goal:
        return False, -1

    total_cost = 0
    for previous, current in zip(result.path, result.path[1:]):
        if abs(previous[0] - current[0]) + abs(previous[1] - current[1]) != 1:
            return False, -1
        if not is_open(grid, current):
            return False, -1
        total_cost += cell_cost(grid, current)

    if total_cost != result.cost:
        return False, -1
    return True, total_cost


def require_plain_int(name: str, value: object) -> int:
    if type(value) is not int:
        raise TypeError("SearchResult.{0} must be a plain int".format(name))
    return value


def normalize_result(result: SearchResult) -> SearchResult:
    return SearchResult(
        solved=bool(result.solved),
        path=tuple(result.path),
        cost=require_plain_int("cost", result.cost),
        expanded=require_plain_int("expanded", result.expanded),
        status=str(result.status),
    )


def run_solver_case(grid: tuple[str, ...], budget: int) -> tuple[SearchResult, int]:
    payload = {"grid": grid, "budget": budget}
    completed = subprocess.run(
        [sys.executable, str(RUNNER_PATH)],
        capture_output=True,
        check=False,
        cwd=str(RUNNER_PATH.parent),
        input=json.dumps(payload),
        text=True,
        timeout=RUNNER_TIMEOUT_SLACK_SECONDS + 1.0,
    )
    if completed.returncode != 0:
        return SearchResult(False, tuple(), -1, budget, "runner_error"), 10**9

    data = json.loads(completed.stdout)
    result = SearchResult(
        solved=bool(data.get("solved")),
        path=tuple((int(row), int(col)) for row, col in data.get("path", [])),
        cost=data.get("cost"),
        expanded=data.get("expanded"),
        status=str(data.get("status", "invalid_result")),
    )
    elapsed_ns = require_plain_int("elapsed_ns", data.get("elapsed_ns"))
    return normalize_result(result), elapsed_ns


def case_score(optimal_cost: int, result: SearchResult, elapsed_ns: int) -> tuple[int, str]:
    if not result.solved:
        return 0, result.status

    gap = max(0, result.cost - optimal_cost)
    quality_score = max(0, QUALITY_BONUS - gap * QUALITY_PENALTY)
    latency_score = max(0, LATENCY_BONUS - math.ceil(elapsed_ns / LATENCY_BUCKET_NS))
    total = SOLVE_BONUS + quality_score + latency_score
    label = "solved gap={0} expanded={1} elapsed_ms={2:.1f}".format(
        gap,
        result.expanded,
        elapsed_ns / 1_000_000,
    )
    return total, label


def main() -> int:
    total_score = 0
    solved_count = 0
    for index, case in enumerate(CASE_SPECS, start=1):
        grid = build_case(case)
        optimal_cost = dijkstra_cost(grid)
        try:
            result, elapsed_ns = run_solver_case(grid, case.budget)
        except (TypeError, ValueError, subprocess.TimeoutExpired, json.JSONDecodeError):
            result = SearchResult(False, tuple(), -1, case.budget, "invalid_result")
            elapsed_ns = case.timeout_ms * 1_000_000

        if elapsed_ns > case.timeout_ms * 1_000_000:
            result = SearchResult(False, tuple(), -1, result.expanded, "time_limit_exceeded")

        valid, _ = validate_result(grid, result)
        if result.solved and not valid:
            result = SearchResult(False, tuple(), -1, result.expanded, "invalid_path")

        score, label = case_score(optimal_cost, result, elapsed_ns)
        if result.solved:
            solved_count += 1
        total_score += score
        print(
            (
                "case={0}/{1} kind={2} seed={3} size={4} budget={5} timeout_ms={6} "
                "optimal={7} result={8} case_score={9}"
            ).format(
                index,
                len(CASE_SPECS),
                case.kind,
                case.seed,
                case.size,
                case.budget,
                case.timeout_ms,
                optimal_cost,
                label,
                score,
            )
        )

    print("solved={0}/{1}".format(solved_count, len(CASE_SPECS)))
    print("EVOLOZA_SCORE={0}".format(total_score))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
