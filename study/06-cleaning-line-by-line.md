# 06 — Cleaning, Line by Line (Step 2)

A walk through `src/cleaning.py`, mapped to spec Steps 2.1–2.11, plus the
data checks that guard the result. For each function: what it does, why
each line is there, and which decision it encodes. Real numbers come from
`reports/results.md`.

> **Help:** `withColumn`, `when`, null rules, schemas and array functions
> are taught in [05 · DataFrame fundamentals](05-dataframe-fundamentals.md).
> What the raw JSON looks like is in
> [04 · The TMDB API](04-tmdb-api-and-fetch.md#h-9-what-one-raw-record-looks-like).

## 1. The map: spec step to function

Every function takes a DataFrame and returns a new one. The module
docstring: "same steps and names as my pandas version (src/cleaning.py in
DEM02), but every function takes a spark DataFrame and gives a new one
back." `clean()` chains them:

```python
def clean(df_raw: DataFrame) -> DataFrame:
    """All of step 2 in the order the brief lists it."""
    return (
        df_raw.transform(drop_corrupt_rows)
        .transform(drop_irrelevant_columns)
        .transform(extract_collection_name)
        .transform(pipe_join_names, "genres", "name")
        .transform(pipe_join_names, "spoken_languages", "english_name")
        .transform(pipe_join_names, "production_countries", "name")
        .transform(pipe_join_names, "production_companies", "name")
        .transform(extract_credits)
        .transform(convert_dtypes)
        .transform(clean_unrealistic_values)
        .transform(filter_released_and_dedup)
        .transform(reorder_and_reset)
    )
```

| Spec step | Function | Columns after |
|---|---|---|
| — | `load_raw` with `RAW_SCHEMA` | 26 (25 + `_corrupt_record`) |
| — | `drop_corrupt_rows` | 25 |
| 2.1 Drop irrelevant columns | `drop_irrelevant_columns` | 20 |
| 2.2–2.3 JSON-like columns → names | `extract_collection_name`, `pipe_join_names` ×4 | 20 |
| 2.3 (credits) | `extract_credits` | 23 |
| 2.5 Convert types | `convert_dtypes` | 23 |
| 2.6 Unrealistic values, million USD, placeholders | `clean_unrealistic_values` | 23 |
| 2.7–2.9 Released only, duplicates, missing keys, ≥ 10 values | `filter_released_and_dedup` | 22 |
| 2.10–2.11 Order, reset index | `reorder_and_reset` | 23 (22 + `idx`) |
| (Step 3) Profit, ROI | `kpis.add_profit_roi` | **25** |
| — Checks | `validate` | (no change; raises on failure) |
| 2.4 Inspect with value counts | `inspect_anomalies` (called from `main.py`) | — |

On the real data, `results.md` reports: "raw: 18 movies (0 unreadable
lines). after cleaning: 18 movies, 25 columns. data checks passed." No row
was dropped by any rule. The rules still matter: the 15 tests in
`tests/test_cleaning.py` prove each one works on data that does trigger it.

## 2. RAW_SCHEMA and load_raw

```python
def load_raw(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read the json lines file with my schema. bad lines go to _corrupt_record."""
    return (
        spark.read
        .schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(str(path))
    )
```

Spec Step 2.2 says "evaluate JSON-like columns". In pandas you would parse
strings. In Spark there is nothing to parse: the schema already declares
`genres` as an array of `{id, name}` structs, and the reader builds them.
So 2.2 is done **by reading**.

Three decisions in `RAW_SCHEMA` (full code and reasons in
[05](05-dataframe-fundamentals.md#h-3-schemas-inference-versus-an-explicit-schema)):

- **Written out, not inferred**: no extra pass over the file, and an
  all-empty `cast` list can no longer be mis-typed.
- **Plain fields as strings**: typing happens later with `try_cast`, so a
  bad value becomes a null instead of a read error.
- **Only needed fields listed**: TMDB sends 28 keys; fields like
  `backdrop_path`, `origin_country` and `softcore` are not in the schema,
  so Spark never reads them.

## 3. drop_corrupt_rows and counting bad lines

```python
def drop_corrupt_rows(df: DataFrame) -> DataFrame:
    # i count these in main.py and log them. here they just get dropped.
    return df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
```

A good line has `_corrupt_record = null`. A broken line has its raw text
there and nulls everywhere else. Keep the good ones, then drop the helper
column. `test_broken_json_line_is_dropped` adds the line
`'{"id": 2, "title": '` (cut off mid-object) next to one good movie and
expects `count() == 1`.

**Counting** happens in `main.run()`, and needs a `cache()`:

```python
df_raw = cleaning.load_raw(spark, RAW_PATH).cache()
# counting only _corrupt_record straight off the file is not allowed in
# spark, the cache above is what makes this count work
bad = df_raw.filter(F.col("_corrupt_record").isNotNull()).count()
raw_rows = df_raw.count()
log.info("raw rows: %d, unreadable lines: %d", raw_rows, bad)
```

Without the cache, Spark refuses:

```
AnalysisException [UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN] ...
Queries from raw JSON/CSV/XML files are disallowed when the referenced columns
only include the internal corrupt record column
```

The reason: with column pruning (see
[02](02-what-is-spark.md#h-8-jobs-stages-tasks-shuffles-and-reading-a-plan)),
a query that needs only `_corrupt_record` would read *no real columns*, so
nothing could ever fail to parse, and the count would be wrong. Caching
first makes Spark parse full rows once. On this run the count is **0
unreadable lines**.

## 4. drop_irrelevant_columns (Step 2.1)

```python
DROP_COLUMNS = ["adult", "imdb_id", "original_title", "video", "homepage"]

def drop_irrelevant_columns(df: DataFrame) -> DataFrame:
    # drop() skips names that are not there, so no "if col in columns" check
    return df.drop(*DROP_COLUMNS)
```

`*DROP_COLUMNS` spreads the list into five arguments. `drop` ignores
unknown names, so the function is safe on any input. 25 − 5 = **20**
columns.

These five are in `RAW_SCHEMA` only so that this step has something to
drop, mirroring the spec. Catalyst notices they are dropped and does not
read them from the file anyway.

## 5. extract_collection_name

```python
def extract_collection_name(df: DataFrame) -> DataFrame:
    # belongs_to_collection is {id, name} or null. .name on a null struct is
    # just null, so no special case for standalone films.
    return df.withColumn("belongs_to_collection", F.col("belongs_to_collection.name"))
```

A movie belongs to at most one collection, so this is a single struct,
not an array. Take its name. Titanic and Beauty and the Beast have no
collection: the struct is null, so the name is null, with no `if` needed.
That null becomes important in Step 3.3 (franchise vs standalone).

## 6. pipe_join_names

```python
def pipe_join_names(df: DataFrame, column: str, name_key: str) -> DataFrame:
    """[{name: "Action"}, {name: "Drama"}] -> "Action|Drama", empty list -> null.

    genres.name on an array of structs gives an array of only the names,
    so concat_ws can join it. no python loop, this all stays in the jvm.
    """
    joined = F.concat_ws("|", F.col(f"{column}.{name_key}"))
    # concat_ws gives "" for an empty list. when() with no otherwise() is null.
    return df.withColumn(column, F.when(joined != "", joined))
```

Line by line, for `genres` on Avatar:

| Step | Value |
|---|---|
| `F.col("genres.name")` | `["Science Fiction", "Action", "Adventure"]` (the dot takes `name` from every struct) |
| `F.concat_ws("\|", ...)` | `"Science Fiction\|Action\|Adventure"` |
| `F.when(joined != "", joined)` | same string; for an empty list `concat_ws` gives `""`, which becomes **null** |

Why turn `""` into null? An empty string looks like a value but means
"nothing". `dropna`, `isNull()` and the null counts only see real nulls.
`test_empty_lists_become_null` checks `row.genres is None`.

It is called four times from `clean()`, with different arguments:

| Call | Field used | Why |
|---|---|---|
| `pipe_join_names, "genres", "name"` | `name` | e.g. `Drama\|Romance` for Titanic |
| `pipe_join_names, "spoken_languages", "english_name"` | **`english_name`** | The `name` field is the language's own name: `日本語` for Japanese and `""` for Xhosa in Endgame's raw record. `english_name` is always readable |
| `pipe_join_names, "production_countries", "name"` | `name` | `United States of America` |
| `pipe_join_names, "production_companies", "name"` | `name` | `Marvel Studios` |

`test_nested_columns_become_pipe_text` checks `row.genres == "Action|Science Fiction"`
and `row.spoken_languages == "English"`.

## 7. extract_credits

```python
def extract_credits(df: DataFrame) -> DataFrame:
    """cast, cast_size, director and crew_size out of the credits struct.

    the pandas version did this in the api client. here the raw file keeps
    the credits as they came and spark does it, so i can redo it without
    calling the api again.
    """
    cast_names = F.concat_ws("|", F.col("credits.cast.name"))
    # F.filter keeps only the crew entries whose job is Director. a film can
    # have two directors, so i keep all of them, not just the first one.
    directors = F.concat_ws("|", F.filter("credits.crew", lambda c: c["job"] == "Director")["name"])
    return (
        df.withColumn("cast", F.when(cast_names != "", cast_names))
        .withColumn("cast_size", F.coalesce(F.size("credits.cast"), F.lit(0)))
        .withColumn("director", F.when(directors != "", directors))
        .withColumn("crew_size", F.coalesce(F.size("credits.crew"), F.lit(0)))
        .drop("credits")
    )
```

| Line | Decision and reason |
|---|---|
| `cast_names` | All cast names, in TMDB's billing order, joined with `"\|"` |
| `F.filter(... c["job"] == "Director")` | The crew list holds every job; Endgame's first crew entry is a "Second Assistant Director". `==` is an exact match, so assistants are not counted |
| `[...]["name"]` then `concat_ws` | **All directors kept.** Endgame is `Joe Russo\|Anthony Russo` in `results.md`; Frozen is `Jennifer Lee\|Chris Buck`. Taking only the first would wrongly credit one person. `test_two_directors_both_kept` expects `"A\|B"` |
| `F.when(... != "", ...)` | `""` → null, as in `pipe_join_names`. A film with no listed director gets a null `director` |
| `F.coalesce(F.size(...), F.lit(0))` | Number of entries. `size` of a missing array is null; `coalesce` makes it 0, because "no credits" means 0 people. `test_empty_lists_become_null` checks `row.cast_size == 0` |
| `.drop("credits")` | Everything needed has been pulled out; the big nested column goes |

The docstring's point is worth repeating in a review: because the raw file
keeps credits untouched, this rule can be changed and rerun without
calling the API again.

Column count: 20 + 4 new − 1 (`credits`) = **23**.

## 8. convert_dtypes (Step 2.5)

```python
def convert_dtypes(df: DataFrame) -> DataFrame:
    # spark 4 has ansi mode on, so a plain .cast("double") on "abc" throws and
    # the whole job dies. try_cast gives null instead, which is what
    # pd.to_numeric(errors="coerce") did in the pandas version.
    return (
        df.withColumn("id", F.expr("try_cast(id AS BIGINT)"))
        .withColumn("budget", F.expr("try_cast(budget AS DOUBLE)"))
        .withColumn("revenue", F.expr("try_cast(revenue AS DOUBLE)"))
        .withColumn("popularity", F.expr("try_cast(popularity AS DOUBLE)"))
        .withColumn("vote_count", F.expr("try_cast(vote_count AS BIGINT)"))
        .withColumn("vote_average", F.expr("try_cast(vote_average AS DOUBLE)"))
        .withColumn("runtime", F.expr("try_cast(runtime AS DOUBLE)"))
        .withColumn("release_date", F.expr("CAST(try_to_timestamp(release_date, 'yyyy-MM-dd') AS DATE)"))
    )
```

The spec says "set invalid values to NaN". In Spark the marker for a
missing value is **null** (NaN exists for doubles but means "not a number"
from bad maths, not "missing"). So invalid → null.

These casts are needed, not just a safety net: `RAW_SCHEMA` reads every
plain field as a **string**, so this is where `"356000000"` becomes the
number `356000000.0`.

**Why `try_cast` and not `.cast()`?** PySpark 4 has **ANSI mode** on by
default (ANSI is the SQL standard). Under it a bad cast is an error, not a
silent null:

```python
spark.createDataFrame([("abc",)], "s string").select(F.col("s").cast("int")).show()
# SparkNumberFormatException: [CAST_INVALID_INPUT] The value 'abc' of the type "STRING"
# cannot be cast to "INT" because it is malformed. ... Use `try_cast` to tolerate
# malformed input and return NULL instead.

spark.createDataFrame([("abc",)], "s string").select(F.expr("try_cast(s AS INT)")).show()
# NULL
```

One bad value would kill the whole job. `try_cast` turns it into null,
which is exactly what the spec wants. `F.expr` runs a piece of SQL text as
a Column.

**Dates.** `release_date` arrives as the string `"2019-04-24"`.
`try_to_timestamp(..., 'yyyy-MM-dd')` parses it, giving null for anything
that does not match the pattern; `CAST(... AS DATE)` drops the time part.
The session time zone is fixed to UTC in `get_spark()`, so the date is the
same on every machine.

`test_bad_values_become_null_not_a_crash` feeds `release_date="not a date"`
and `budget="lots"` and expects both to come out null.

## 9. clean_unrealistic_values (Step 2.6)

```python
def clean_unrealistic_values(df: DataFrame) -> DataFrame:
    # 0 budget / revenue / runtime means "tmdb does not know", not zero
    for col in ["budget", "revenue", "runtime"]:
        df = df.withColumn(col, F.when(F.col(col) > 0, F.col(col)))
    df = (
        df.withColumn("budget_musd", F.col("budget") / 1_000_000)
        .withColumn("revenue_musd", F.col("revenue") / 1_000_000)
        .drop("budget", "revenue")
    )
    # a movie nobody voted on shows 0.0, that is not a rating
    df = df.withColumn("vote_average", F.when(F.col("vote_count") > 0, F.col("vote_average")))
    for col in ["overview", "tagline"]:
        df = df.withColumn(col, F.when(~F.trim(F.col(col)).isin(PLACEHOLDERS), F.col(col)))
    return df
```

| Decision | Reason |
|---|---|
| **Zero → null** for budget, revenue, runtime | TMDB stores 0 when it does not know. A film did not cost $0 or run 0 minutes. If 0 stayed, averages would drop and ROI would divide by zero. The spec offers "NaN or infer from similar movies"; inferring would invent data, so null was chosen. `> 0` also catches negative junk |
| **Million USD**: `/ 1_000_000` | Spec 2.6. Endgame's `356000000` becomes `356.00`, as in `results.md`. Null stays null. `test_zero_money_is_null_and_rest_is_millions`: budget 0 → null, revenue 2,500,000 → 2.5 |
| Drop `budget`, `revenue` | Only the `_musd` versions are in the spec's final list. Columns stay at 23 (+2, −2) |
| **`vote_count = 0` → rating null** | If nobody voted, TMDB shows `vote_average = 0.0`. That is not a terrible film; it is no rating. Keeping 0.0 would put it at the bottom of "low rated". `test_no_votes_means_no_rating` |
| **Placeholders → null** for `overview`, `tagline` | `PLACEHOLDERS = ["", "No Data", "No overview found.", "N/A", "n/a"]`. `trim` removes spaces first, so `"  "` becomes `""` and matches. `test_placeholders_become_null` |
| `~` before `isin` | "Keep the value when it is **not** a placeholder"; otherwise no `when` matches → null |

**On the real data, none of these rules fired.** All 18 films are big
releases with known budgets, revenues and runtimes, the lowest
`vote_count` is 10,557 (Frozen II), and every tagline and overview is real
text. The null counts in §13 prove it.

## 10. filter_released_and_dedup (Steps 2.7 to 2.9)

```python
def filter_released_and_dedup(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("status") == "Released")
        .drop("status")
        # same id twice -> keep one. which one is not fixed in spark, fine here
        # because a repeat is the same api answer twice.
        .dropDuplicates(["id"])
        .dropna(subset=["id", "title"])
        # keep rows that have at least 10 real values
        .dropna(thresh=10)
    )
```

| Line | Spec | Notes |
|---|---|---|
| `filter(status == "Released")` | 2.9 | Removes "Post Production", "Rumored", etc. A null status is also removed (null compares as not true). Done first, so less data reaches the shuffle below; Catalyst pushes it into the file read anyway |
| `drop("status")` | 2.9 | Every remaining row says "Released"; the column carries no information |
| `dropDuplicates(["id"])` | 2.7 | Duplicates by **id**, not by whole row. Spark keeps one copy without guaranteeing which; the comment says why that is fine. Causes a shuffle |
| `dropna(subset=["id", "title"])` | 2.7 | A row with no id or no title cannot be identified. A non-numeric id became null in `convert_dtypes` and is removed here |
| `dropna(thresh=10)` | 2.8 | Keep rows with at least 10 non-null values out of the 22 columns. An empty list would count as a value, but by now lists are strings or null |

`test_dupes_unreleased_and_no_title_removed` feeds ids 1, 1, 2 (Post
Production), 3 (no title), 4 and expects `[1, 4]`.

23 − 1 = **22** columns: exactly the spec's list, in a different order.

## 11. reorder_and_reset (Steps 2.10 and 2.11)

```python
def reorder_and_reset(df: DataFrame) -> DataFrame:
    """Final column order plus a 0-based idx, like reset_index in pandas.

    spark rows have no index. row_number over a window with no partitionBy
    pulls every row into one partition to number them. ok for 18 movies;
    on a big table i would drop idx and just use id as the key.
    """
    idx = F.row_number().over(Window.orderBy("id")) - 1
    return df.select(idx.alias("idx"), *FINAL_COLUMNS)
```

**Reorder (2.10).** `select(*FINAL_COLUMNS)` returns the 22 columns of
`FINAL_COLUMNS` (defined at the top of `cleaning.py`) in spec order.
`test_columns_come_out_in_brief_order` checks
`columns == ["idx", *cleaning.FINAL_COLUMNS]`.

**Reset index (2.11).** pandas rows have an index `0, 1, 2...`. Spark rows
have **no index and no fixed order**. So the lab makes one:

- `Window.orderBy("id")` defines a **window**: all rows, sorted by `id`.
- `F.row_number().over(...)` numbers rows 1, 2, 3... in that order.
- `- 1` makes it 0-based like pandas.

Ordering by `id` makes the index **deterministic**: same data, same index,
every run. In `results.md`, `idx 0` is Titanic (id 597) and `idx 17` is
The Lion King (id 420818). `test_idx_starts_at_zero` feeds ids 30, 10, 20
and expects `(0, 10), (1, 20), (2, 30)`.

The cost is stated honestly in the docstring and the README ("one thing i
left on purpose"): a window with no `partitionBy` moves every row to one
partition, and Spark logs a warning about it. Fine for 18 rows, a
bottleneck for millions. Windows are covered in
[07 · Aggregations, windows, sorting](07-aggregations-windows-sorting.md#h-6-window-functions).

## 12. validate: data checks that stop the run

```python
def validate(df: DataFrame) -> dict[str, Any]:
    """Checks that must hold before anyone reads a number from this table.

    one pass over the data with a single agg, not one count() per rule.
    raises instead of printing, so a bad run stops here and does not write
    a wrong parquet or wrong charts.
    """
    checks = df.agg(
        F.count("*").alias("rows"),
        F.count_distinct("id").alias("distinct_ids"),
        F.count_if(F.col("title").isNull()).alias("null_titles"),
        F.count_if(F.col("budget_musd") <= 0).alias("bad_budget"),
        F.count_if(F.col("revenue_musd") <= 0).alias("bad_revenue"),
        F.count_if((F.col("vote_average") < 0) | (F.col("vote_average") > 10)).alias("bad_rating"),
    ).first()
    # an agg with no groupBy always gives exactly one row, this is for the type checker
    assert checks is not None
    problems: list[str] = []
    if checks["rows"] == 0:
        problems.append("no rows left after cleaning")
    if checks["distinct_ids"] != checks["rows"]:
        problems.append("duplicate ids")
    for name in ["null_titles", "bad_budget", "bad_revenue", "bad_rating"]:
        if checks[name] > 0:
            problems.append(f"{checks[name]} rows with {name}")
    if problems:
        raise ValueError("data checks failed: " + "; ".join(problems))
    log.info("data checks passed on %d rows", checks["rows"])
    return checks.asDict()
```

Cleaning rules *change* data. Checks *assert* what must be true afterwards.

| Check | Catches |
|---|---|
| `rows == 0` | Everything was filtered out (wrong status filter, empty file) |
| `distinct_ids != rows` | `dropDuplicates` did not run or was broken |
| `null_titles > 0` | `dropna(subset=...)` did not run |
| `bad_budget`, `bad_revenue` (≤ 0) | The zero → null rule did not run |
| `bad_rating` (outside 0–10) | Corrupt ratings |

Design points:

- **One pass.** Six results from one `agg` and one `.first()` action,
  instead of six `count()` calls that would each re-scan the data.
- **Raise, do not print.** In `main.run()` it is called right after the
  clean table is built and before anything is written. A `ValueError`
  goes to `main()`'s `except`, which logs it and exits with code 1, so a
  bad run leaves no wrong Parquet or charts behind.
- Null budgets are allowed: `null <= 0` is null, which `count_if` does not
  count.

On the real data it returns `rows = 18` and passes; `results.md` says
"data checks passed". Tests: `test_validate_passes_on_good_data` (2 rows),
and `test_validate_stops_on_empty_table`, where the only movie is
"Rumored", so `pytest.raises(ValueError, match="no rows")`.

## 13. inspect_anomalies and the null check (Step 2.4)

pandas has `value_counts()`. Spark does not, so the lab builds it:

```python
def inspect_anomalies(df: DataFrame, columns: list[str], n: int = 10) -> dict[str, DataFrame]:
    out: dict[str, DataFrame] = {}
    for col in columns:
        counts = (
            df.select(F.explode(F.split(F.col(col), r"\|")).alias(col))
            .groupBy(col).count()
            .orderBy(F.desc("count"), col)
            .limit(n)
        )
        out[col] = counts
    return out
```

1. `F.split(..., r"\|")` turns `"Action|Adventure"` into an array (the
   backslash is needed because `split` takes a regex; see
   [05](05-dataframe-fundamentals.md#h-9-array-functions)).
2. `F.explode` gives one row per item. As the docstring puts it, a cell
   `"A|B"` "counts once for A and once for B".
3. `groupBy(col).count()` counts rows per value; sort by count, then by
   name for a fixed order of ties; keep the top `n`.
4. It returns a dict of small DataFrames, not printed output, "so a test
   can check this ran".

`main.py` runs it on the **cleaned** table for five columns, with `n=15`.
From `results.md`:

| genres | count | | spoken_languages | count |
|---|---|---|---|---|
| Adventure | 15 | | English | 18 |
| Action | 10 | | Russian | 3 |
| Science Fiction | 10 | | Xhosa | 3 |
| Family | 5 | | French | 2 |
| Animation | 4 | | Spanish | 2 |
| Fantasy | 4 | | Arabic, German, Hindi, Italian, | 1 each |
| Thriller | 3 | | Japanese, Korean, Swahili, | |
| Drama | 2 | | Swedish, Thai | |
| Romance | 2 | | | |
| Comedy, Crime | 1 each | | | |

| original_language | count | | production_countries | count |
|---|---|---|---|---|
| en | 18 | | United States of America | 18 |
| | | | United Kingdom | 2 |

| belongs_to_collection | count |
|---|---|
| The Avengers Collection | 4 |
| Frozen, Jurassic Park, Star Wars Collections | 2 each |
| Avatar, Black Panther, Harry Potter, The Fast and the Furious, The Incredibles, The Lion King (Reboot) Collections | 1 each |

What to read from them ("identify anomalies", as the spec says):

- **No spelling variants or junk values.** No "Sci-Fi" next to "Science
  Fiction", no empty strings. The extraction is clean.
- **A very narrow sample.** All 18 are English-language, US-produced
  films. Any conclusion is about Hollywood blockbusters, not "movies".
- The genre counts add up to 57 tags for 18 films (about 3 each), which
  is why per-genre totals can never be added back up to a total.
- The 2 films with no collection do not appear in the collection counts:
  `split` of null is null, and `explode` drops it.

### Nulls after cleaning

`main.py` counts nulls in every column with
`F.count_if(F.col(c).isNull())`:

| idx | id | title | tagline | release_date | genres | belongs_to_collection | original_language | budget_musd | revenue_musd | production_companies | production_countries | vote_count | vote_average | popularity | runtime | overview | spoken_languages | poster_path | cast | cast_size | director | crew_size | profit_musd | roi |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 | 0 | 0 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

Only `belongs_to_collection` has nulls: **2**, the two standalone films,
Titanic and Beauty and the Beast. That null is real information ("not part
of a franchise"), not missing data, and Step 3.3 uses it to split
franchise from standalone.

## 14. Why it is shaped like this

- **One function per spec rule**, named like the pandas version, so each
  rule is readable, testable on its own, and comparable with DEM02.
- **Constants at the top of `cleaning.py`** (`DROP_COLUMNS`,
  `FINAL_COLUMNS`, `PLACEHOLDERS`, `RAW_SCHEMA`), so rules change without
  touching logic.
- **Built-in functions only.** No Python UDF anywhere in the cleaning, so
  everything stays in the JVM and Catalyst can optimise it (README:
  "built in functions, no python udf in the pipeline").
- **No action inside `clean()`.** It never calls `count()` or `show()`. It
  only builds the plan; `validate` is the first action.

## Self-check

1. Why does `main.py` cache `df_raw` before counting `_corrupt_record`? [Spark refuses a query on raw JSON that references only the corrupt-record column; caching parses full rows first.]
2. Why `english_name` instead of `name` for spoken languages? [`name` is the native name; it is `""` for Xhosa and `日本語` for Japanese.]
3. Why are the casts in `convert_dtypes` needed, not just a safety net? [`RAW_SCHEMA` reads plain fields as strings; this is where they become numbers and dates.]
4. Why does `validate` use one `agg` instead of six `count()` calls? [One pass over the data instead of six.]
5. How many nulls are left after cleaning, and where? [2, in `belongs_to_collection`: Titanic and Beauty and the Beast.]
