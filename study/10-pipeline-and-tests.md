# 10 — The Pipeline Script and the Tests

This page walks through `src/main.py`, the one command that runs the whole
lab, then the 32 pytest tests in `tests/` that protect the fetch, cleaning,
KPI and chart rules. It ends with a commit plan for the git deliverable.

> **Help:** the functions `main.py` calls are explained in [06 · Cleaning line by line](06-cleaning-line-by-line.md) and [08 · KPIs line by line](08-kpis-line-by-line.md). The charts are in [09 · Charts with Matplotlib](09-visualisation.md).

## Part 1 — `src/main.py`

### What it does, in one table

| Stage | Code | Output |
|---|---|---|
| 0. Arguments, logging, backend | `argparse` (`--fetch`), `logging.basicConfig`, `matplotlib.use("Agg")` | — |
| 1. Fetch if needed | `save_jsonl(fetch_all())` | `data/raw_movies.jsonl` |
| 2. Start Spark | `get_spark()` | one local SparkSession |
| 3. Read raw, count bad lines | `cleaning.load_raw(...).cache()`, two `count()`s | log line |
| 4. Clean + metrics + checks | `cleaning.clean`, `kpis.add_profit_roi`, `.cache()`, `cleaning.validate` | the `df_movies` DataFrame |
| 5. Save clean data | `df_movies.coalesce(1).write.mode("overwrite").parquet(...)` | `data/movies_clean.parquet/` |
| 6. Tables | `md_table(...)` on value counts, nulls, rankings, searches, franchises, directors | sections of `results.md` |
| 7. Charts | eight `save_chart(visualize.plot_...(df_movies), "chartN.png")` | `reports/chart1.png` … `chart8.png` |
| 8. Stop and write | `spark.stop()` in `finally`, then `write_text` | `reports/results.md` |

### How to run it, and why `-m`

```powershell
python -m src.main            # reuse data/raw_movies.jsonl if it is there
python -m src.main --fetch    # call the api again first
```

`src/` is a **package** (it has an `__init__.py`), and `main.py` imports its
neighbours as `from src import cleaning, kpis, visualize`. `python -m
src.main` runs the module *as part of the package*, with the current folder
(the lab root) on Python's import path, so `src` can be found. Running
`python src/main.py` would put `src/` itself on the path, and `from src
import ...` would fail. So always run from the lab folder, with `-m`.

### `main()`: flags, the backend, and `finally`

```python
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fetch", action="store_true", help="call the tmdb api even if the raw file exists")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    matplotlib.use("Agg")

    if args.fetch or not RAW_PATH.exists():
        save_jsonl(fetch_all())

    REPORTS.mkdir(exist_ok=True)
    spark = get_spark()
    out: list[str] = []
    try:
        charts = run(spark, out)
    except Exception:
        log.exception("pipeline failed, nothing more written")
        sys.exit(1)
    finally:
        # always give the jvm back, even when a step failed
        spark.stop()

    (REPORTS / "results.md").write_text("\n".join(out), encoding="utf-8")
```

| Part | Why |
|---|---|
| `argparse`, `action="store_true"` | `--fetch` takes no value: `args.fetch` is `True` if typed, else `False` |
| `args.fetch or not RAW_PATH.exists()` | fetch when asked, **or** when there is no raw file yet. With the file on disk, no network is needed |
| `matplotlib.use("Agg")` inside `main()` | file-only drawing for the script; the notebook, which imports the same modules, keeps inline charts |
| `try / except / finally` | any failure is logged with its stack trace (`log.exception`) and the script exits with code 1. `finally` runs in both cases, so the Spark JVM is always shut down |
| `results.md` written **after** the `try` | if `run()` failed, `sys.exit(1)` already left, so a half-built `results.md` is never written |
| `out = []` passed into `run` | `run` appends Markdown sections to the list; `main` joins and writes them at the end |

### `run()`: read, count bad lines, cache

```python
df_raw = cleaning.load_raw(spark, RAW_PATH).cache()
# counting only _corrupt_record straight off the file is not allowed in
# spark, the cache above is what makes this count work
bad = df_raw.filter(F.col("_corrupt_record").isNotNull()).count()
raw_rows = df_raw.count()
log.info("raw rows: %d, unreadable lines: %d", raw_rows, bad)
```

- `load_raw` reads the file with the explicit `RAW_SCHEMA` in **PERMISSIVE**
  mode: a line that is not valid JSON does not crash the read; its text goes
  into a `_corrupt_record` column and the other columns are null. See
  [11 · Hard concepts §8](11-hard-concepts-explained.md#h-8-schema-on-read-raw-schema-permissive-and-corrupt-record).
- Spark refuses a query on a raw JSON file whose only referenced column is
  `_corrupt_record`; caching the DataFrame first makes it allowed. That is
  what the comment means.
- On this data: `raw: 18 movies (0 unreadable lines)` (first line of
  `results.md`).

### `run()`: clean, cache, validate, unpersist

```python
df_movies = kpis.add_profit_roi(cleaning.clean(df_raw)).cache()
checks = cleaning.validate(df_movies)
df_raw.unpersist()
```

- Spark is **lazy**: `clean(...)` only builds a plan. Each **action** (an
  operation that needs a real result: `count`, `collect`, `first`, `write`,
  `toPandas`) runs the plan from the start. `df_movies` feeds the Parquet
  write, 5 value-count tables, the null table, 10 rankings, 2 searches, 3
  group tables and 8 charts. Without `cache()` each would redo the read and
  every cleaning step.
- `cache()` is itself lazy. `validate()` runs the first action on
  `df_movies` (one `agg(...).first()`), so it is what fills the cache.
- After that, nothing reads `df_raw` again, so `unpersist()` frees its
  memory.
- `validate()` checks rows > 0, unique ids, no null titles, no budget or
  revenue ≤ 0, ratings within 0–10, all in **one** aggregation, and raises
  `ValueError` if any fails. Because it raises, the `except` in `main()`
  catches it and nothing is written. Here: "data checks passed" on 18 rows.

### Saving to Parquet

```python
df_movies.coalesce(1).write.mode("overwrite").parquet(str(CLEAN_PATH))
```

- **Parquet** is a column-based file format. It stores the schema with the
  data, compresses well, and lets a reader load only the columns it needs.
  A CSV stores text and loses types: dates come back as strings.
- `coalesce(1)` merges the partitions into one before writing, so the folder
  holds one part file instead of several tiny ones. Unlike `repartition(1)`,
  it does this without a shuffle.
- `mode("overwrite")` replaces the folder; the default mode (`"error"`)
  would fail on the second run.
- Spark writes a **folder**: `data/movies_clean.parquet/` with a `_SUCCESS`
  marker and a `part-00000-...snappy.parquet` file.
- On Windows this line needs **winutils** and `HADOOP_HOME`. Without them it
  fails with `UnsatisfiedLinkError ... NativeIO$Windows`. See
  [01 · Setting up Spark on Windows](01-setup-windows.md).

### `md_table`: a DataFrame as a Markdown table

```python
def md_table(df: DataFrame, max_rows: int = 50) -> str:
    rows = df.limit(max_rows).collect()
    if not rows:
        return "_no rows_\n"

    def fmt(v: object) -> str:
        if v is None:
            return ""
        if isinstance(v, float):
            return f"{v:,.2f}"
        return str(v).replace("|", "\\|")

    head = "| " + " | ".join(df.columns) + " |\n|" + "---|" * len(df.columns) + "\n"
    return head + "".join("| " + " | ".join(fmt(r[c]) for c in df.columns) + " |\n" for r in rows)
```

| Line | Why |
|---|---|
| `df.limit(max_rows).collect()` | `limit` first, so at most 50 rows reach the driver. `collect()` returns a list of `Row` objects |
| `"_no rows_\n"` | an empty search shows a readable line, not a header with no body |
| `v is None → ""` | a null shows as an empty cell |
| `f"{v:,.2f}"` | floats get a thousands separator and 2 decimals: `2,942.19` |
| `.replace("\|", "\\\|")` | a `\|` inside a value would break the Markdown table, so it is escaped |
| `"---\|" * n` | the separator row Markdown needs under the header |

Integers are not floats in Python, so `vote_count` prints as `40259`, with no
comma, while money prints as `2,942.19`.

### Two more details in `run()`

```python
nulls = df_movies.select([F.count_if(F.col(c).isNull()).alias(c) for c in df_movies.columns])
```

One `count_if(col IS NULL)` per column, computed in a single `select`.
Result: 0 everywhere except `belongs_to_collection` (2: the standalone
films).

```python
found = frame.cache()
out.append(f"### {name}\n\n{found.count()} matches.\n\n" + md_table(found))
found.unpersist()
```

Each search result is used twice (count, then table), so it is cached for
those two actions and released straight after.

### How to read `reports/results.md`

| Section | What it holds | Key number |
|---|---|---|
| data | raw lines, bad lines, rows and columns after cleaning | 18 raw, 0 unreadable, 18 clean, 25 columns |
| cleaned movies | 11 key columns, ordered by `idx` (which follows `id`) | `idx 0` = Titanic (id 597) |
| value counts (step 2.4) | one table per extracted column, top 15 | Adventure 15, Action 10, Science Fiction 10 |
| nulls per column | one row of counts | only `belongs_to_collection` = 2 |
| best and worst (step 3.1) | 10 top-5 tables | Endgame top revenue 2,942.19 |
| searches (step 3.2) | match count + table | **0 matches** for both |
| franchise vs standalone (step 3.3) | 2-row comparison | 16 vs 2 films |
| most successful franchises (step 3.4) | top 10 by `total_revenue` | The Avengers Collection 7,918.80 |
| most successful directors (step 3.5) | top 10 by `total_revenue` | James Cameron 5,187.90 |
| charts (step 4) | image links to the 8 PNGs | — |

Money is in **million USD** everywhere. "0 matches" for both searches is the
right answer for these 18 IDs; the tests prove the searches find a match
when one exists.

## Part 2 — pytest from zero

### What a test is

**pytest** is the standard Python test runner. A **test** is a function
whose name starts with `test_`, in a file whose name starts with `test_`.
pytest finds them, runs them, and reports which passed. Inside a test you
use **`assert`**: it does nothing when the condition is true and raises
`AssertionError` when false; pytest shows both sides.

```python
def test_profit_and_roi(spark: SparkSession) -> None:
    row = first(enriched(spark, movie(budget=100_000_000, revenue=450_000_000)))
    assert row.profit_musd == 350.0
    assert row.roi == 4.5
```

Run all tests from the lab folder with `pytest` (or `python -m pytest -q`;
`-q` is quiet, one dot per passing test). Result on this laptop:
`32 passed in 590.11s (0:09:50)`, almost ten minutes (why, and how to speed
it up, is at the end of this part).

### Fixtures and `conftest.py`

A **fixture** is a function that prepares something a test needs. A test
asks for it by naming it as a parameter; pytest runs the fixture and passes
the result in.

```python
@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    # one spark for the whole test run. starting the jvm takes a few seconds,
    # doing it per test would make the suite crawl.
    s = get_spark("tmdb-tests")
    yield s
    s.stop()
```

- `yield s` hands `s` to the tests; code after `yield` runs at the end as
  cleanup.
- `scope="session"`: **one** SparkSession for the whole run. The default
  scope, `"function"`, would start and stop Spark for every test.
- It calls the same `get_spark()` as `main.py` and the notebook, so tests run
  with the same settings.
- `tests/conftest.py` is a special file name: pytest loads it first, and its
  fixtures are available to every test file in the folder with no import.

### `tests/helpers.py`: the test data

`helpers.py` is a plain module (imported as `from tests.helpers import ...`;
`tests/__init__.py` makes `tests` a package). It holds four functions.

**`movie(**changes)`, a factory.** A **factory** is a function that builds
test data. `movie()` returns one complete, valid, fake TMDB record shaped
like a line of `raw_movies.jsonl`. `**changes` collects keyword arguments
into a dict, and `m.update(changes)` replaces only those keys:

```python
movie(budget=0, revenue=2_500_000)        # everything normal except the money
movie(id=3, title=None)                   # a film with no title
```

Each test states only what is special about its case.

**`raw_df(spark, records, extra_lines=())`.** Builds a raw DataFrame the
same way `main.py` reads the file:

```python
lines = [json.dumps(r) for r in records] + list(extra_lines)
rdd = spark.sparkContext.parallelize(lines)
return (spark.read.schema(cleaning.RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(rdd))
```

`json.dumps` turns each dict into a JSON string; `parallelize` turns the
Python list into an **RDD** (Spark's older, lower-level distributed list);
`spark.read...json(rdd)` reads those strings with the real schema and
options. `extra_lines` lets a test add raw text, such as a broken JSON line.

**`cleaned(spark, *records)`** = `cleaning.clean(raw_df(...))`, and
**`enriched(spark, *records)`** = `kpis.add_profit_roi(cleaned(...))`. Short
names for the two most common set-ups.

### Every test and the rule it protects

**`tests/test_cleaning.py`** (15 tests, Step 2)

| Test | Rule it protects |
|---|---|
| `test_final_columns` | `FINAL_COLUMNS` has 22 names and starts with `id` |
| `test_columns_come_out_in_brief_order` | output is `idx` + the 22 columns, in spec order (2.10) |
| `test_nested_columns_become_pipe_text` | genres → `"Action\|Science Fiction"`, collection → its name, languages → `english_name`, cast joined, sizes = 2, director from crew (2.3) |
| `test_two_directors_both_kept` | two directors give `"A\|B"`, not just the first |
| `test_empty_lists_become_null` | empty cast, crew, genres → null; `cast_size` → 0 |
| `test_zero_money_is_null_and_rest_is_millions` | budget 0 → null; revenue 2,500,000 → 2.5 (2.6) |
| `test_no_votes_means_no_rating` | `vote_count = 0` → `vote_average` null (2.6) |
| `test_placeholders_become_null` | `"No Data"` and a blank tagline → null (2.6) |
| `test_bad_values_become_null_not_a_crash` | `"not a date"` and budget `"lots"` → null, no crash (2.5) |
| `test_broken_json_line_is_dropped` | a truncated line `{"id": 2, "title": ` is dropped; 1 row remains |
| `test_dupes_unreleased_and_no_title_removed` | from ids 1, 1, 2 (Post Production), 3 (no title), 4 only 1 and 4 survive (2.7, 2.9) |
| `test_idx_starts_at_zero` | ids 30, 10, 20 get `idx` 0, 1, 2 in id order (2.11) |
| `test_inspect_anomalies_splits_cells` | `"Action\|Science Fiction"` counts once per genre (2.4) |
| `test_validate_passes_on_good_data` | `validate` returns `rows = 2` on two good films |
| `test_validate_stops_on_empty_table` | a "Rumored" film is filtered out, leaving 0 rows; `validate` raises "no rows" |

**`tests/test_kpis.py`** (10 tests, Step 3)

| Test | Rule it protects |
|---|---|
| `test_profit_and_roi` | 450 − 100 = 350; 450 / 100 = 4.5 |
| `test_rank_skips_nulls_and_uses_threshold` | a 5 M-budget film is excluded by `ROI_MIN_BUDGET_MUSD`; a film with no budget never appears in "low roi" |
| `test_rankings_has_all_ten_lists` | `rankings` returns 10 tables |
| `test_has_matches_whole_names_only` | `"Bruce Willis Jr"` does not match `"Bruce Willis"` |
| `test_willis_search` | the Sci-Fi Action film is found, the Drama one is not |
| `test_tarantino_search_shortest_first` | runtime 110 comes before 150 |
| `test_codirectors_both_get_the_revenue` | co-directors A and B each get the film's 200 M |
| `test_franchise_split` | collection → franchise; null → standalone |
| `test_top_franchises_sums_revenue` | two films of 100 M and 300 M give `total_revenue` 400 |
| `test_udf_gives_same_roi_as_builtin` | `roi_udf` and the built-in division agree, including a null budget |

**`tests/test_tmdb_client.py`** (5 tests, Step 1, no network)

| Test | Rule it protects |
|---|---|
| `test_movie_ids_match_lab_spec` | 19 IDs, the first is 0 |
| `test_404_is_skipped` | a 404 returns `None` |
| `test_429_then_ok_retries` | a 429 then a 200 gives the film, after 2 calls |
| `test_gives_up_after_three_tries` | three 503s give `None` after exactly 3 calls |
| `test_save_jsonl_one_movie_per_line` | one JSON object per line, and no `.tmp` file left behind |

These use a `FakeSession` whose `get()` returns a `FakeResponse` with the
next status from a list, so no real HTTP call is made. `monkeypatch` (a
built-in pytest fixture that swaps something out for one test) replaces
`time.sleep` with a function that does nothing, so the back-off waits take
no time. `tmp_path` (another built-in fixture) gives an empty temporary
folder.

**`tests/test_visualize.py`** (2 tests, Step 4)

| Test | Rule it protects |
|---|---|
| `test_histogram_counts_every_movie` | 5 films in 4 buckets: bar heights add up to 5 |
| `test_boxplot_draws` | the Spark-side boxplot draws, with title "runtime boxplot" |

The search tests matter because the real data gives **0 matches** for both
searches. Without them a reviewer could ask: "How do you know the search
works at all?"

### The schema-inference story

An earlier version read the raw file with Spark guessing the types
(**schema inference**). A test where every record had `"cast": []` then
failed with `AnalysisException ... requires INTEGRAL type`:

1. An empty list gives no clue what is inside it. Spark guessed
   `array<string>`.
2. `credits.cast.name` on an array of structs means "the `name` field of each
   element". On an array of strings there are no fields, so Spark tried to
   read `name` as an array position, which must be an integer.

The code now reads with the explicit `RAW_SCHEMA`, where `cast` is always an
array of structs with `name`. The comment in `test_empty_lists_become_null`
records it: "with inferSchema this broke ... the fixed schema means one movie
on its own is enough now."

### Why Spark tests are slow, and how to speed them up

32 tests on a handful of rows each should take seconds. They take minutes
because every Spark **action** has a fixed cost (planning, starting tasks,
moving data between the JVM and Python), and on Windows that cost is high.
Each test runs at least one action, and several steps **shuffle** (move rows
between partitions): `dropDuplicates`, `groupBy`, `orderBy`, and the
`row_number()` window.

| Change (none applied) | Why it helps |
|---|---|
| Fewer actions per test (collect once, assert on the list) | each action pays the fixed cost |
| `SPARK_MASTER=local[1]` for tests | one thread: less scheduling overhead on tiny data |
| `SPARK_SHUFFLE_PARTITIONS=1` for tests | each shuffle makes 1 task instead of 4 |
| `spark.ui.enabled=false` | skips starting the Spark web UI |
| Share one cleaned DataFrame per module (`scope="module"` fixture) | clean once, assert many times |

Because `get_spark()` reads the master and partition count from environment
variables, the first two need no code change:

```powershell
$env:SPARK_MASTER="local[1]"; $env:SPARK_SHUFFLE_PARTITIONS="1"; pytest
```

The trade-off: tests then run with different settings from the pipeline.

## Part 3 — Git commit plan

The deliverable asks for "clear commits and organized structure". The plan is
one commit per lab step, so the history reads like the spec.

| # | Commit message | Files |
|---|---|---|
| 1 | `project skeleton: spark session, requirements, gitignore` | `requirements.txt`, `.gitignore`, `.env.example`, `src/__init__.py`, `src/spark_session.py`, `data/README.md` |
| 2 | `step 1: fetch 19 tmdb ids with credits, save json lines` | `src/tmdb_client.py`, `tests/__init__.py`, `tests/test_tmdb_client.py` |
| 3 | `step 2: raw schema, cleaning rules, data checks, tests` | `src/cleaning.py`, `tests/conftest.py`, `tests/helpers.py`, `tests/test_cleaning.py` |
| 4 | `step 3: kpis, searches, franchises, directors, tests` | `src/kpis.py`, `tests/test_kpis.py` |
| 5 | `step 4: charts and main pipeline` | `src/visualize.py`, `src/main.py`, `tests/test_visualize.py`, `reports/results.md`, `reports/chart*.png` |
| 6 | `notebook, final report and readme` | `analysis.ipynb`, `reports/final_report.md`, `README.md` |
| 7 | `study notes` | `study/` |

Rules behind the plan:

- **Never commit `.env`.** It holds the API key. `.env.example` shows the
  variable names with empty values.
- **`data/` is not committed** except `data/README.md` (`data/*` then
  `!data/README.md` in `.gitignore`). Both the raw file and the Parquet are
  rebuilt by the code; the README in `data/` says how.
- `__pycache__/`, `.pytest_cache/`, `.ipynb_checkpoints/`, `spark-warehouse/`,
  `metastore_db/`, `derby.log`, `*.crc` are build or Spark side files, all
  ignored.
- Each commit should pass the tests that exist at that point.

Check before committing:

```powershell
git status            # nothing unexpected, no .env, nothing under data/ except README.md
git diff --staged     # read what you are about to commit
```

## Self-check

1. When does `python -m src.main` call the TMDB API?
2. `cache()` is lazy. Which call in `run()` fills the cache of `df_movies`?
3. Why is `spark.stop()` in a `finally` block?
4. How do the `tmdb_client` tests avoid both the network and the back-off waits?
5. Why is `raw_df` in `helpers.py` written with `RAW_SCHEMA` and PERMISSIVE instead of `spark.createDataFrame(records)`?

Answers: (1) With `--fetch`, or when `data/raw_movies.jsonl` does not exist.
(2) `cleaning.validate(df_movies)`, the first action on it. (3) So the JVM
is released even when a step raises. (4) A `FakeSession` returns preset
statuses, and `monkeypatch` replaces `time.sleep` with a no-op. (5) So tests
read data exactly as `main.py` does, including the schema and the
corrupt-line handling.

Next: [11 · Hard concepts explained](11-hard-concepts-explained.md)
