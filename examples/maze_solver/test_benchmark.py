from __future__ import annotations

import unittest

from benchmark import (
    LATENCY_BONUS,
    LATENCY_BUCKET_NS,
    QUALITY_BONUS,
    SOLVE_BONUS,
    case_score,
    normalize_result,
    run_solver_case,
)
from solver import SearchResult


class WrappedInt(int):
    def __rsub__(self, other: int) -> int:
        return other + 10**9


class BenchmarkTests(unittest.TestCase):
    def test_case_score_penalizes_positive_latency(self) -> None:
        result = SearchResult(True, ((0, 0), (0, 1)), 12, 999_999, "solved")
        score, label = case_score(12, result, LATENCY_BUCKET_NS * 3)
        self.assertEqual(score, SOLVE_BONUS + QUALITY_BONUS + LATENCY_BONUS - 3)
        self.assertEqual(label, "solved gap=0 expanded=999999 elapsed_ms=6.0")

    def test_normalize_result_rejects_int_subclasses(self) -> None:
        result = SearchResult(True, ((0, 0),), WrappedInt(12), WrappedInt(0), "solved")
        with self.assertRaisesRegex(TypeError, "plain int"):
            normalize_result(result)

    def test_run_solver_case_executes_solver_in_runner(self) -> None:
        grid = (
            "S111",
            "1#11",
            "1111",
            "111G",
        )
        result, elapsed_ns = run_solver_case(grid, budget=80)
        self.assertTrue(result.solved)
        self.assertGreater(elapsed_ns, 0)


if __name__ == "__main__":
    unittest.main()
