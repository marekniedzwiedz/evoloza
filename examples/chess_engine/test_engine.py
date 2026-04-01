from __future__ import annotations

import sys
import unittest
from pathlib import Path

import chess
import chess.engine

from engine import choose_move


class EngineTests(unittest.TestCase):
    def test_choose_move_is_legal(self) -> None:
        board = chess.Board()
        move = choose_move(board)
        self.assertIn(move, board.legal_moves)

    def test_choose_move_does_not_mutate_board(self) -> None:
        board = chess.Board("r1bqkbnr/pppp1ppp/2n5/4p3/2B1P3/2N2N2/PPPP1PPP/R1BQK2R w KQkq - 0 4")
        before = board.fen()
        choose_move(board)
        self.assertEqual(board.fen(), before)

    def test_uci_engine_returns_legal_move(self) -> None:
        path = Path(__file__).resolve().parent / "engine.py"
        engine = chess.engine.SimpleEngine.popen_uci([sys.executable, str(path)])
        try:
            board = chess.Board()
            result = engine.play(board, chess.engine.Limit(time=0.1), info=chess.engine.INFO_NONE)
            self.assertIsNotNone(result.move)
            self.assertIn(result.move, board.legal_moves)
        finally:
            engine.quit()


if __name__ == "__main__":
    unittest.main()
