# 03 — The Python You Need for This Lab

Only the Python features that this project's code actually uses, each
shown with the real line it comes from. If you can read every example on
this page, you can read every file in `src/` and `tests/`.

> **Help:** the PySpark side of these lines (`F.col`, `withColumn`, ...)
> is explained in [05 · DataFrame fundamentals](05-dataframe-fundamentals.md).
> This page is plain Python only.

## 1. Lists, sets and dicts

A **list** is an ordered sequence in square brackets. A **set** is an
unordered group with no duplicates, in curly braces; checking `x in s` is
fast. A **dict** (dictionary) maps keys to values, in curly braces with
`key: value` pairs.

```python
# tmdb_client.py: a list of ints and a set of status codes
MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397,
    ...
]
RETRYABLE = {429, 500, 502, 503, 504}
if r.status_code not in RETRYABLE: ...

# cleaning.py: a list of strings
DROP_COLUMNS = ["adult", "imdb_id", "original_title", "video", "homepage"]
```

A TMDB movie arrives as a dict whose values can be lists of dicts. This is
the test helper's version, from `tests/helpers.py`:

```python
def movie(**changes: object) -> Movie:
    m = {
        "id": 1, "title": "Test Movie", "budget": 100_000_000, ...
        "genres": [{"id": 28, "name": "Action"}, {"id": 878, "name": "Science Fiction"}],
        "credits": {"cast": [{"name": "Actor One"}, {"name": "Actor Two"}], ...},
    }
    m.update(changes)      # replace or add keys from another dict
    return m
```

`**changes` collects any keyword arguments into a dict, so
`movie(id=2, budget=0)` gives the default movie with those two keys
changed. Every test builds its rows this way.

`100_000_000` is just `100000000`. Python ignores underscores in numbers;
they are there for your eyes.

`kpis.rankings()` returns a dict whose values are DataFrames, and
`main.py` loops over it:

```python
for name, frame in kpis.rankings(df_movies).items():   # .items() gives (key, value) pairs
    out.append(f"### {name}\n\n" + md_table(frame))
```

`dict.get(key, default)` returns the default when the key is missing.
`visualize.plot_distribution` uses it so empty histogram buckets count as 0:

```python
heights = [counts.get(i, 0) for i in range(bins)]
```

## 2. JSON

**JSON** (JavaScript Object Notation) is the text format the TMDB API
sends. It maps almost one-to-one onto Python: object → dict, array →
list, string → str, number → int/float, `true/false` → `True/False`,
`null` → `None`.

```python
import json
json.dumps({"id": 597, "adult": False})   # dict -> text: '{"id": 597, "adult": false}'
json.loads('{"id": 597}')                 # text -> dict
```

`tmdb_client.save_jsonl()` writes one movie per line with
`json.dumps(rec, ensure_ascii=False)`. `ensure_ascii=False` keeps
characters like 日本語 as they are, instead of `日...` escapes. The test
helper `raw_df` uses `json.dumps` to turn test dicts into JSON text for
Spark.

## 3. Functions: defaults and keyword arguments

```python
def fetch_movie(session: requests.Session, base_url: str, movie_id: int, retries: int = 3) -> Movie | None:
def rank(df: DataFrame, col: str, n: int = 5, ascending: bool = False, where: Column | None = None) -> DataFrame:
```

`retries=3` is a **default**: if the caller does not pass `retries`, it is
3. Callers can pass arguments by name (**keyword arguments**), in any
order:

```python
rank(df, "roi", n, ascending=True, where=rich)
```

`rank` has three defaults. That is what lets one function produce all ten
rankings.

A default of `None` followed by a check is the safe way to default to a
list:

```python
def fetch_all(movie_ids: list[int] | None = None) -> list[Movie]:
    ...
    if movie_ids is None:
        movie_ids = MOVIE_IDS
```

`*extra` in a definition collects any number of extra positional
arguments into a tuple. `cleaning._named("origin_country")` passes one
extra field name:

```python
def _named(*extra: str) -> T.ArrayType:
    fields = [T.StructField("id", T.LongType()), T.StructField("name", T.StringType())]
    fields += [T.StructField(f, T.StringType()) for f in extra]
    return T.ArrayType(T.StructType(fields))
```

A name starting with `_` (like `_named`) is a convention for "helper,
used only inside this file".

This codebase writes no **type hints** (annotations like
`def f(x: int) -> str`). You will see them in other projects; Python does
not enforce them either way.

## 4. Docstrings and comments

A **docstring** is a string right under a `def` or at the top of a file.
It becomes the function's built-in help (`help(cleaning.validate)`).
Every module in `src/` starts with one saying which lab step it covers:

```python
"""Step 2: clean the raw movies with spark.
...
"""
```

`#` comments in this code explain *why*, not *what*, for example in
`cleaning.py`: `# concat_ws gives "" for an empty list. when() with no otherwise() is null.`

## 5. Modules, packages and imports

A **module** is one `.py` file. A **package** is a folder of modules with
an `__init__.py` file in it. This project has two packages, `src/` and
`tests/`:

```
src/
    __init__.py          # marks the folder as a package (empty)
    spark_session.py  tmdb_client.py  cleaning.py  kpis.py  visualize.py  main.py
tests/
    __init__.py  conftest.py  helpers.py  test_cleaning.py  test_kpis.py  ...
```

Imports name the package explicitly (**absolute imports**):

```python
# src/main.py
from src import cleaning, kpis, visualize
from src.spark_session import get_spark
from src.tmdb_client import RAW_PATH, fetch_all, save_jsonl

# tests/test_kpis.py
from src import kpis
from tests.helpers import enriched, movie
```

For `import src` to work, Python must find the folder that *contains*
`src/`, which is the project root. That is why every command is run from
the root with `-m`:

```bash
python -m src.main           # -m runs a module by its dotted package name
python -m src.tmdb_client
```

`python -m` puts the current folder (the root) on Python's search list,
`sys.path`. `python src/main.py` would put `src/` itself there instead, and
`from src import ...` would fail with `ModuleNotFoundError: No module named 'src'`.
pytest finds the root on its own because `tests/` is a package with an
`__init__.py`.

`import src.tmdb_client as tmdb_client` (in `test_tmdb_client.py`) gives
the module a short local name.

## 6. f-strings

An **f-string** has an `f` before the quotes and puts values inside `{}`:

```python
url = f"{base_url}/movie/{movie_id}"               # "https://api.themoviedb.org/3/movie/597"
joined = F.concat_ws("|", F.col(f"{column}.{name_key}"))    # "genres.name"
problems.append(f"{checks[name]} rows with {name}")
return f"{v:,.2f}"                                  # 2264.16 -> "2,264.16"
ax.set_title(f"{column} distribution (median={median:g})")
```

After a `:` comes a **format spec**: `,` adds thousands separators and
`.2f` means two decimals. That is why `results.md` shows `2,264.16`.
`:g` prints a number in its shortest natural form.

## 7. Comprehensions and generator expressions

A **list comprehension** builds a list from a loop in one line:

```python
# main.py: one null-count expression per column
nulls = df_movies.select([F.count_if(F.col(c).isNull()).alias(c) for c in df_movies.columns])
```

Read it as "for each column name `c`, make `F.count_if(...)`". It is the
same as:

```python
exprs = []
for c in df_movies.columns:
    exprs.append(F.count_if(F.col(c).isNull()).alias(c))
```

With `zip`, which walks two lists side by side:

```python
# visualize.py: "Action\n(n=10)" style labels
labels = [f"{g}\n(n={n})" for g, n in zip(pdf["genre"], pdf["n"])]
```

Without square brackets it is a **generator expression**: the same idea,
but values are made one at a time, often passed straight into a function:

```python
assert sorted(r.id for r in df.collect()) == [1, 4]
"".join("| " + " | ".join(fmt(r[c]) for c in df.columns) + " |\n" for r in rows)
```

A **dict comprehension** builds a dict:

```python
assert {r.director: r.total_revenue for r in kpis.top_directors(df).collect()} == {"A": 200.0, "B": 200.0}
```

## 8. `*` unpacking and tuple unpacking

A `*` in a call spreads a list into separate arguments:

```python
df.drop(*DROP_COLUMNS)                 # same as df.drop("adult", "imdb_id", ...)
df.select(idx.alias("idx"), *FINAL_COLUMNS)
assert cleaned(spark, movie()).columns == ["idx", *cleaning.FINAL_COLUMNS]
```

`def cleaned(spark, *records)` in `tests/helpers.py` is the reverse: any
number of movies are collected into one tuple.

**Tuple unpacking** assigns several names at once:

```python
quartiles = df.agg(F.percentile_approx(column, [0.25, 0.5, 0.75]).alias("q")).first()
assert quartiles is not None
q1, med, q3 = quartiles["q"]
low_fence, high_fence = q1 - 1.5 * iqr, q3 + 1.5 * iqr
```

## 9. lambda

A **lambda** is a one-line function without a name. `cleaning.py` uses one
to tell Spark which crew members to keep:

```python
F.filter("credits.crew", lambda c: c["job"] == "Director")
```

`lambda c: c["job"] == "Director"` means "a function that takes `c` and
returns whether its job is Director". Important: this lambda is **not** run
by Python once per crew member. PySpark calls it once, with a Column
placeholder, to build a Spark expression. The filtering then happens
inside the JVM. That is why it is fast, unlike a UDF (§10).

`test_tmdb_client.py` replaces `time.sleep` with `no_wait`, a named
function that does nothing. A lambda would work too, but a lambda cannot carry
type hints, and the strict type check (§18) flags its parameter as unknown.

## 10. Decorators

A **decorator** is a line starting with `@` above a function. It passes the
function to another function that wraps it. In `kpis.py`:

```python
@F.udf(returnType=T.DoubleType())
def roi_udf(revenue: float | None, budget: float | None) -> float | None:
    if revenue is None or budget is None or budget == 0:
        return None
    return revenue / budget
```

`@F.udf(...)` turns the plain Python function into a Spark **UDF**
(user-defined function) that can be used on columns:
`kpis.roi_udf("revenue_musd", "budget_musd")`. Without the decorator you
would write `roi_udf = F.udf(roi_udf, T.DoubleType())`, which does the same
thing. See [08 · KPIs line by line](08-kpis-line-by-line.md#h-4-a-real-spark-udf-roi-udf).

`@pytest.fixture(scope="session")` in `tests/conftest.py` is another
decorator. It registers `spark()` as a pytest **fixture**, a piece of setup
that a test receives by naming it as an argument (`def test_x(spark: SparkSession) -> None:`).
`scope="session"` means one SparkSession for the whole test run.

## 11. Classes, briefly

The only classes in the project are test fakes in `test_tmdb_client.py`:

```python
class FakeSession(requests.Session):
    """Answers with the statuses in the list, one per call."""

    def __init__(self, statuses: list[int], data: object = None) -> None:
        super().__init__()
        self.statuses = list(statuses)
        self.data = data
        self.calls = 0

    def get(self, url: str | bytes, **kwargs: Any) -> requests.Response:
        self.calls += 1
        return FakeResponse(self.data, self.statuses.pop(0))
```

A **class** is a template for objects. `__init__` runs when you create one
(`FakeSession([429, 200])`); `self` is the object itself. `FakeSession` has
a `get` method with the same shape as `requests.Session.get`, so
`fetch_movie` cannot tell the difference. This is called **duck typing**:
if it has the methods you call, it works. It also *inherits* from
`requests.Session` (the `(requests.Session)` after the class name), so the
type checker agrees it is a valid `session` argument; `super().__init__()`
runs the parent's setup first.

## 12. `pathlib`

`pathlib.Path` treats file paths as objects instead of strings:

```python
RAW_PATH = Path(__file__).resolve().parents[1] / "data" / "raw_movies.jsonl"
```

| Expression | Meaning |
|---|---|
| `__file__` | The path of the current file (`.../src/tmdb_client.py`) |
| `.resolve()` | Make it absolute |
| `.parents[1]` | Go up 2 levels: `[0]` = `src`, `[1]` = project root |
| `/ "data"` | Join path parts, with the right separator on Windows or Linux |
| `path.parent.mkdir(parents=True, exist_ok=True)` | Create the folder and any missing parents; no error if it exists |
| `path.with_suffix(".tmp")` | Same path, other extension: `raw_movies.tmp` |
| `RAW_PATH.exists()` | Does the file exist? |
| `str(path)` | Back to a string; Spark wants strings: `.json(str(path))` |

`os.replace(tmp, path)` renames a file, replacing the target if it exists.

## 13. Environment variables and `python-dotenv`

```python
from dotenv import load_dotenv
load_dotenv()                                                   # copy .env lines into os.environ
api_key = os.getenv("TMDB_API_KEY", "")                         # "" if not set
master = os.getenv("SPARK_MASTER", "local[*]")                  # with a default
```

`load_dotenv()` reads `.env` and sets each `NAME=value` as an environment
variable for this process. It does not overwrite variables that already
exist, so a real environment variable wins over `.env`.

`spark_session.py` also *sets* variables, with `setdefault`, which only
sets a value if none exists:

```python
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
```

`sys.executable` is the full path of the Python running this script. This
forces Spark's Python workers to use the same Python. On Windows,
otherwise, Spark may pick another Python that has no PySpark installed.

`warnings.filterwarnings("ignore", message="PySpark does not yet fully support pandas")`
hides one specific warning by the start of its text.

## 14. `if __name__ == "__main__":`

Every module has a variable `__name__`. When you run a file directly (or
with `-m`) it is `"__main__"`; when another file imports it, it is the
module name. So:

```python
if __name__ == "__main__":
    main()
```

means "run `main()` only when this file is run directly, not when it is
imported". `main.py` imports `src.tmdb_client`, and thanks to this guard
the import alone does not call the API. `tmdb_client.py` and `main.py`
both end with such a block.

## 15. `argparse`

`argparse` reads command-line options:

```python
ap = argparse.ArgumentParser()
ap.add_argument("--fetch", action="store_true", help="call the tmdb api even if the raw file exists")
args = ap.parse_args()
if args.fetch or not RAW_PATH.exists():
    save_jsonl(fetch_all())
```

`action="store_true"` makes `--fetch` a switch: `args.fetch` is `True` if
you typed it, `False` if not. `python -m src.main --help` prints the help
text for free.

## 16. try / except / else / finally, and raising

From `fetch_movie`:

```python
try:
    r = session.get(url, params={"append_to_response": "credits"}, timeout=TIMEOUT)
except requests.RequestException as e:
    log.warning("movie %s: network error %s (try %d)", movie_id, e, attempt)
else:
    if r.status_code == 404:
        ...
```

From `main()`:

```python
try:
    charts = run(spark, out)
except Exception:
    log.exception("pipeline failed, nothing more written")
    sys.exit(1)
finally:
    # always give the jvm back, even when a step failed
    spark.stop()
```

| Block | Runs when |
|---|---|
| `try:` | Always; the risky code goes here |
| `except X as e:` | The `try` raised an error of type `X` |
| `else:` | The `try` did **not** raise |
| `finally:` | Always, last, whether it raised or not. Even `sys.exit(1)` in the `except` runs `finally` first |

Putting the status checks in `else` rather than inside `try` means a bug
in those checks is not mistaken for a network error. Putting
`spark.stop()` in `finally` means the JVM is always released.

Ways the code stops on purpose:

- `r.raise_for_status()` raises an `HTTPError` for a 4xx/5xx status.
- `raise SystemExit("no TMDB_API_KEY in .env. ...")` ends the program with
  that message.
- `raise ValueError("data checks failed: " + "; ".join(problems))` in
  `cleaning.validate`. A test checks it with
  `with pytest.raises(ValueError, match="no rows"):`.

## 17. Logging

```python
log = logging.getLogger(__name__)
log.warning("movie %s: not found, skip it", movie_id)
log.info("data checks passed on %d rows", checks["rows"])
```

The `%s` and `%d` are filled from the extra arguments.
`logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")`
in the `__main__` blocks decides what gets printed and how. `log.exception`
logs an error plus the full traceback. Logging is preferred over `print`
because you can switch levels without editing every line.

## 18. Type hints

Every function in `src/` and `tests/` says what it takes and what it gives
back:

```python
def rank(df: DataFrame, col: str, n: int = 5, ascending: bool = False, where: Column | None = None) -> DataFrame:
```

- `df: DataFrame` means "df should be a Spark DataFrame". `-> DataFrame`
  is the return type.
- `Column | None` means "a Column or `None`". `None` is how `where`
  says "no filter".
- `list[str]`, `dict[str, DataFrame]`: a list of strings, a dict from
  string keys to DataFrames.
- `Any` (from `typing`) means "anything". It is used for values that
  really can be anything, like one movie from the API:
  `Movie = dict[str, Any]` in `tmdb_client.py`. That line is a **type
  alias**: a short name for a longer type.

Python does **not** check hints when it runs; `rank(df, 42)` still runs and
then fails inside Spark. The hints are for your editor and for a **type
checker**. With them, VS Code (Pylance) knows that `rank(...)` returns a
DataFrame, so typing `.` after it lists `filter`, `orderBy`, `show`, ...,
and hovering shows the real types instead of `Unknown`.

The project has a `pyrightconfig.json` that turns on **strict** mode, so
any value whose type the checker cannot work out is flagged. To run the
same check from a terminal:

```bash
pip install pyright pandas-stubs types-requests
pyright
```

It should print `0 errors`. Two patterns you will see because of it:

```python
checks = df.agg(...).first()
assert checks is not None
```

`.first()` is typed `Row | None` (an empty table has no first row). An
`agg` with no `groupBy` always gives one row, so the `assert` tells the
checker so. After that line it knows `checks` is a `Row`. Tests use the
`first(df)` helper in `tests/helpers.py` for the same reason.

```python
out: list[str] = []
```

An empty `[]` gives the checker nothing to go on, so the type is written
on the variable.

## Self-check

1. In `rank(df, col, n=5, ascending=False, where=None)`, what does `rank(df, "roi")` give? [Top 5 by ROI, highest first, no extra filter.]
2. Why must you run `python -m src.main` from the project root? [`from src import ...` needs the folder containing `src/` on `sys.path`; `-m` from the root puts it there.]
3. Does the lambda in `F.filter("credits.crew", lambda c: ...)` run once per crew member in Python? [No. It runs once to build a Spark expression; the JVM does the filtering.]
4. What does `Path(__file__).resolve().parents[1]` point to from `src/tmdb_client.py`? [The project root folder.]
5. Why is `spark.stop()` in `finally:`? [It runs whether the pipeline succeeded or failed, so the JVM is always released.]
6. Does Python stop `rank(df, 42)` because `col` is hinted as `str`? [No. Hints are not checked at run time; pyright or VS Code flags it before you run.]
