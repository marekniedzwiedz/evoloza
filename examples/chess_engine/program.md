# Mission
Improve `engine.py` so the current working tree engine wins short self-play
matches against the previous committed version.

## Goal
- Increase the benchmark score by beating the engine in `HEAD:engine.py`.
- Preserve legal move generation.

## Constraints
- Only edit `engine.py`.
- Keep the public entrypoint `choose_move(board, depth=DEFAULT_DEPTH)`.
- Keep `engine.py` speaking basic UCI so `benchmark.py` can launch it.
- Do not add new dependencies beyond `requirements.txt`.
- Keep the benchmark fast enough for many rounds.

## Strategy
- Prefer small search and evaluation improvements.
- Favor legal, deterministic play over risky complexity.
- Do not overfit to one starting position.
- If an idea draws too often, try sharper move ordering or a better evaluation.
- Use internet to find out ideas - explore open source engines like stockfish, look up reddit and other sources.
