from __future__ import annotations

import unittest
from pathlib import Path

from benchmark import allowed_changed_files, repo_relative_path


class BenchmarkTests(unittest.TestCase):
    def test_repo_relative_path_uses_repo_root_when_nested(self) -> None:
        root = Path(__file__).resolve().parent
        relative = repo_relative_path(root / "engine.py", root=root)
        self.assertEqual(relative, "examples/chess_engine/engine.py")

    def test_allowed_changed_files_accepts_repo_and_local_paths(self) -> None:
        root = Path(__file__).resolve().parent
        self.assertEqual(
            allowed_changed_files(root=root),
            {"engine.py", "examples/chess_engine/engine.py"},
        )


if __name__ == "__main__":
    unittest.main()
