# Mission
Improve `solver.py` so it solves more very large weighted mazes under tight
search and time limits while staying close to the exact optimal path cost.

## Goal
- Increase the benchmark score on the mixed maze suite in `benchmark.py`.
- Preserve correctness of returned paths.

## Constraints
- Only edit `solver.py`.
- Keep the public `solve(grid_lines, budget=DEFAULT_BUDGET)` entrypoint.
- Keep the `SearchResult` dataclass fields intact.
- Do not add new dependencies.
- Keep the solver deterministic.
- Treat `python3 benchmark.py` as expensive relative to `python3 -m unittest -q`.
- Expect the benchmark to enforce per-case time limits independently of the
  `expanded` count reported by candidate code.

## Strategy
- Favor heuristics and data structures that help under a tight expansion budget
  and a benchmark-measured wall-time limit.
- Solve rate matters more than perfect path quality.
- After solve rate, focus on reducing cost gap to the exact optimum.
- After quality, shave latency where you can because the benchmark includes a
  small trusted speed tie-breaker.
- Use node expansions carefully; they still affect solve rate, but the official
  benchmark only trusts independently verified outcomes and timing.
- Run `python3 -m unittest -q` freely while iterating.
- Run `python3 benchmark.py` only after a meaningful solver change that is ready for evaluation.
- Avoid rerunning the full benchmark repeatedly for tiny tweaks inside one round.
