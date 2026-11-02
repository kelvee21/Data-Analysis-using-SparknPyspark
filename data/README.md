# data/ (gitignored except this note)

made by the code, never committed:
- `raw_movies.jsonl` - written by `python -m src.tmdb_client`. one api answer
  per line, credits included, nothing changed. every later step reads this,
  so the api is only needed once.
- `movies_clean.parquet/` - written by `python -m src.main` and the last
  cell of the notebook. a folder, not a file: spark writes a part file per
  partition plus a `_SUCCESS` marker.

the tests do not need either of them. they build fake rows in `tests/helpers.py`.
