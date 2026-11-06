# 11 — The Hard Parts, Explained Simply

These are the Spark ideas in this lab that are easy to name and hard to
explain when a reviewer asks "why?". Many of them are the choices listed
under "spark choices" in the [README](../README.md). Each section has:

> **Help:** if a function name below is new, look it up in [14 · PySpark function reference](14-pyspark-function-reference.md). DataFrame basics are in [05 · DataFrame fundamentals](05-dataframe-fundamentals.md) and what Spark is in [02 · What Spark is](02-what-is-spark.md).

- **Plain idea**: the concept without jargon.
- **In this project**: where it shows up in the code, with real numbers.
- **Line to say in review**: a short answer to practise out loud.
- **Follow-up trap**: the next question a reviewer is likely to ask.

Two words used everywhere below:

- **Driver**: the one Python process that runs your script and plans the work.
- **Executors**: the worker processes that do the work on chunks of data
  called **partitions**. With `master("local[*]")` the executors are threads
  on this laptop, one per CPU core.

---

## 1. Lazy evaluation

**Plain idea.** When you write `df.filter(...)` or `df.withColumn(...)`,
Spark does not touch the data. It only adds a step to a **plan** (a recipe).
The data is read and processed only when you ask for a real answer. Seeing
the whole recipe first lets Spark optimise it, for example by reading only
the columns you use.

**In this project.** `cleaning.clean(df_raw)` chains twelve steps with
`.transform(...)` and returns at once, whatever the data size: nothing has
run. The module docstring says it: "nothing runs until something asks for a
result (count, show, write), so clean() is just building a plan." The
notebook cell `df_cleaned.explain()` prints that plan; its comment says to
read it bottom up: scan the JSON with the schema, the filters and projects
from `clean()`, then the window for `idx`.

**Line to say in review.** "Transformations build a plan; nothing runs until
an action. That is why `clean()` returns instantly, and why Spark's optimiser
(Catalyst) can prune columns and push filters into the file reader."

**Follow-up trap.** "Then where would an error in `clean()` show up?" → A
wrong column name fails early, when the plan is checked. A bad *value* (say,
a failed cast in ANSI mode) only fails when the data is processed, so the
stack trace points at the action, not the line that caused it.

---

## 2. Transformations vs actions

**Plain idea.**
- A **transformation** returns a new DataFrame and is lazy: `select`,
  `filter`, `withColumn`, `drop`, `groupBy().agg()`, `orderBy`,
  `dropDuplicates`, `limit`, `coalesce`.
- An **action** returns a result to Python or writes data, and triggers a
  **job** (a unit of work Spark schedules): `count`, `collect`, `first`,
  `show`, `toPandas`, `write...parquet`.

**In this project.**

| Code | Kind |
|---|---|
| `cleaning.clean(...)`, `kpis.add_profit_roi(...)`, `kpis.rank(...)` | transformations (return DataFrames) |
| `df_raw.count()`, `found.count()` | actions |
| `cleaning.validate(df)`: `df.agg(...).first()` | action |
| `df_movies.coalesce(1).write.mode("overwrite").parquet(...)` | action |
| `df.limit(max_rows).collect()` in `md_table` | `limit` is a transformation, `collect` the action |
| `.toPandas()` in the chart functions | action |

**Line to say in review.** "If it gives back a DataFrame, it is a
transformation. If it gives back Python data or writes a file, it is an
action, and that is when Spark actually runs."

**Follow-up trap.** "Is `cache()` an action?" → No. It only marks the
DataFrame to be kept; the next action fills the cache (§3).

---

## 3. `cache()` and `unpersist()`

**Plain idea.** Every action re-runs the whole plan from the source. If you
use the same DataFrame thirty times, Spark rebuilds it thirty times.
`cache()` says: "the first time you compute this, keep the result in
memory." Later actions start from the stored result. `unpersist()` frees
that memory when you are done.

**In this project.** `run()` in `main.py`:

```python
df_raw = cleaning.load_raw(spark, RAW_PATH).cache()
...
df_movies = kpis.add_profit_roi(cleaning.clean(df_raw)).cache()
checks = cleaning.validate(df_movies)
df_raw.unpersist()
...
found = frame.cache()          # each search result: count, then table
...
found.unpersist()
...
df_movies.unpersist()
```

- `df_raw` is cached so the two counts (bad lines, all lines) and the clean
  step read the file once. It also makes the `_corrupt_record` count legal
  (§8).
- `df_movies` feeds the Parquet write, 5 value-count tables, the null table,
  10 rankings, 2 searches, 3 group tables and 8 charts. `validate()` is its
  first action, so it fills the cache.
- Once `df_movies` is cached, `df_raw` is no longer read, so it is released.
- Each search result is used twice, so it is cached for two actions only.

**Line to say in review.** "I cache what is reused and unpersist it when it
is not needed any more. `cache()` is lazy: `validate()` is the first action
on the clean table, so it is what fills the cache."

**Follow-up trap.** "Why not cache everything?" → Caching uses executor
memory, and caching something used once only adds cost. Cache a DataFrame
that is expensive to build and used several times.

---

## 4. Shuffles, `spark.sql.shuffle.partitions` and AQE

**Plain idea.** Some operations need rows with the same key in the same
place: `groupBy`, `orderBy`, `dropDuplicates`, joins, windows. Spark moves
rows between partitions to get there. That move is a **shuffle**: data is
written out, sent across, and read back. It is the most expensive thing Spark
does. After a shuffle the data is split into `spark.sql.shuffle.partitions`
pieces; the default is **200**. **AQE** (Adaptive Query Execution) looks at
the real size of the shuffled data at run time and merges small partitions.

**In this project.** `spark_session.py`:

```python
# default is 200 shuffle partitions. for 18 rows that is 200 near
# empty tasks after every groupBy. AQE would merge them anyway, but
# starting small keeps the spark ui readable.
.config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

Every `groupBy` in `kpis.py`, every `orderBy` in `rank`, and
`dropDuplicates` in `filter_released_and_dedup` shuffle.

**Line to say in review.** "Each shuffle splits its output into
`shuffle.partitions` pieces. 200 is sized for big clusters; for 18 rows that
is 200 near-empty tasks. I start at 4 and leave AQE on, which can merge
further. The number comes from an env var, so a cluster run can raise it."

**Follow-up trap.** "If AQE merges them anyway, why set 4?" → AQE is on by
default since Spark 3.2 and would fix most of it. Setting 4 makes the intent
explicit and keeps the Spark UI readable. On a real cluster you size it to
the data, roughly 100–200 MB per partition.

---

## 5. Python UDFs vs built-in functions

**Plain idea.** A **UDF** (user-defined function) is your own Python
function applied to each row. Spark's engine runs in the **JVM** (Java
Virtual Machine), not in Python. For a Python UDF, every row is converted,
sent to a separate Python worker process, computed, and sent back, and the
optimiser cannot see inside it. Built-in functions (`F.col(a) / F.col(b)`,
`F.when`, `F.split`) stay in the JVM.

**In this project.** The spec asks for a "UDF to streamline ranking". The
code answers in two ways:

- `kpis.rank(df, col, n=5, ascending=False, where=None)` is a reusable
  Python **function** that builds each ranking from built-ins. It is the
  "UDF" in the everyday sense: one function for all 10 lists.
- `kpis.roi_udf` is a real Spark UDF, kept for comparison only:

```python
@F.udf(returnType=T.DoubleType())
def roi_udf(revenue: float | None, budget: float | None) -> float | None:
    if revenue is None or budget is None or budget == 0:
        return None
    return revenue / budget
```

The pipeline uses the built-in division in `add_profit_roi`.
`test_udf_gives_same_roi_as_builtin` proves the two agree. The notebook's
"udf vs built in" section shows the plan for the UDF contains
`ArrowEvalPython`: the step where rows leave the JVM for Python.

**Line to say in review.** "`rank` is my ranking function, built from Spark
column code so it runs at full speed. I also wrote a true Spark UDF for ROI,
with a test showing it matches the built-in, but the pipeline does not use
it: a Python UDF ships every row to Python and back and blocks the
optimiser."

**Follow-up trap.** "When would you use a UDF then?" → Only when no built-in
can do it, for example calling a Python library per row. Even then prefer a
**pandas UDF** (`@F.pandas_udf`), which works on Arrow batches instead of
single rows.

---

## 6. ANSI mode and `try_cast`

**Plain idea.** **ANSI mode** makes Spark follow the SQL standard strictly:
bad input raises an error instead of quietly becoming null. Spark 4 turns it
on by default. Under ANSI, `CAST('abc' AS DOUBLE)` fails with
`CAST_INVALID_INPUT`, and dividing by zero fails with `DIVIDE_BY_ZERO`.
`try_cast` and `try_to_timestamp` return null instead.

**In this project.** `RAW_SCHEMA` reads plain fields as **strings** on
purpose ("this is the raw layer"), and `convert_dtypes` types them:

```python
# spark 4 has ansi mode on, so a plain .cast("double") on "abc" throws and
# the whole job dies. try_cast gives null instead, which is what
# pd.to_numeric(errors="coerce") did in the pandas version.
.withColumn("budget", F.expr("try_cast(budget AS DOUBLE)"))
.withColumn("release_date", F.expr("CAST(try_to_timestamp(release_date, 'yyyy-MM-dd') AS DATE)"))
```

`test_bad_values_become_null_not_a_crash` feeds `release_date="not a date"`
and `budget="lots"` and checks both come out null.

A second guard: `clean_unrealistic_values` turns a 0 budget into null
**before** `add_profit_roi` divides by it. Null / anything is null, so ROI is
null instead of a `DIVIDE_BY_ZERO` error. The comment in `add_profit_roi`
says so: "dividing by a null budget is null too, so no divide by zero."

**Line to say in review.** "Spark 4 runs in ANSI mode, so a plain cast fails
the whole job on one bad value. I read raw fields as strings and type them
with `try_cast`, so a bad value becomes null, like `errors="coerce"` in
pandas."

**Follow-up trap.** "Isn't silently nulling bad data dangerous?" → It can
hide problems, so two checks follow: `results.md` prints the nulls per
column (all 0 except `belongs_to_collection`: 2, the standalone films), and
`validate()` stops the run on null titles or out-of-range values.

---

## 7. Nulls: no fake zeros

**Plain idea.** **Null** means "unknown". Zero means "we know it is zero". A
TMDB budget of 0 means nobody entered it, not that the film was free. Keep
the 0 and the average budget drops and the film's ROI is infinite. Rules:

| Situation | Result |
|---|---|
| `null > 5`, `null = null` | null (not true, not false) |
| `filter(...)` on a null condition | row dropped |
| `avg`, `sum`, `count(col)` | skip nulls |
| `count("*")` | counts every row |
| `null - x`, `null / x` | null |
| `orderBy(col)` ascending | nulls **first** |
| `orderBy(F.desc(col))` | nulls **last** |

**In this project.**
- `clean_unrealistic_values`: budget, revenue, runtime ≤ 0 → null;
  `vote_average` → null when `vote_count = 0`; placeholders like `"No Data"`
  → null.
- `pipe_join_names` and `extract_credits` use `F.when(x != "", x)` with no
  `otherwise()`, so an empty list gives null, not `""`.
- `rank` drops null rows first: "otherwise a film with no budget would sit
  at the top of 'lowest roi'", because an ascending sort puts nulls first.

**Line to say in review.** "Zero means 'unknown' in TMDB, so I store null.
Averages skip those rows instead of being dragged down, and `rank` drops
nulls so a film with no budget can never top 'lowest ROI'."

**Follow-up trap.** "How many nulls did that create in your 18 films?" →
None: the null table in `results.md` is 0 for every column except
`belongs_to_collection`. The rules are proven by tests
(`test_zero_money_is_null_and_rest_is_millions`,
`test_no_votes_means_no_rating`, `test_rank_skips_nulls_and_uses_threshold`),
not by this data.

---

## 8. Schema on read: `RAW_SCHEMA`, PERMISSIVE and `_corrupt_record`

**Plain idea.** A **schema** is the list of column names and types. Spark can
**infer** it (read the data and guess) or you can **give** it. Guessing costs
an extra pass over the file, and the guess depends on what the data happens
to contain. A given schema is faster and the same on every run.

**Read modes** decide what happens to a line that is not valid JSON:
**FAILFAST** stops the job; **DROPMALFORMED** silently drops it;
**PERMISSIVE** (the default) keeps the row, sets its fields to null, and puts
the raw text in a column you name, here `_corrupt_record`.

**In this project.** `cleaning.py`:

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

The comment above `RAW_SCHEMA` gives three reasons: inference reads the whole
file once just to work out types; a guess can change between files ("a file
where every cast list is empty gets guessed as array<string> and then
cast.name breaks", which really happened in an earlier version's tests); and
fields not listed are skipped, so new API fields cannot break the code.

`_corrupt_record` must be listed in the schema to be filled. `main.py` counts
the bad lines and `drop_corrupt_rows` removes them. `results.md`: "raw: 18
movies (0 unreadable lines)". `test_broken_json_line_is_dropped` adds a
truncated line and checks one row remains. Spark refuses a query that reads
**only** `_corrupt_record` straight from a JSON file; the `.cache()` on
`df_raw` is what makes the count allowed.

**Line to say in review.** "I give Spark the schema instead of letting it
guess: no extra pass, stable types, and a broken line goes to
`_corrupt_record` and is counted instead of crashing the run."

**Follow-up trap.** "Why are budget and id strings in the schema?" → It is
the raw layer. If a field ever arrives as `"lots"`, a numeric schema would
null it silently at read time. As a string it survives, and `convert_dtypes`
handles it with `try_cast` in one visible place.

---

## 9. Nested JSON to flat columns

**Plain idea.** TMDB returns nested data: a film has a **list** of genres,
each genre an **object** with `id` and `name`. Spark reads an object as a
**struct** (a column with named sub-fields) and a list as an **array**.
Analysis is easier with flat text, so we pull out the names and join them
with `|`.

**In this project.** `pipe_join_names`:

```python
joined = F.concat_ws("|", F.col(f"{column}.{name_key}"))
# concat_ws gives "" for an empty list. when() with no otherwise() is null.
return df.withColumn(column, F.when(joined != "", joined))
```

| Step | Value for Avatar |
|---|---|
| `genres` (array of structs) | `[{id:878, name:"Science Fiction"}, {id:28, name:"Action"}, {id:12, name:"Adventure"}]` |
| `F.col("genres.name")` | `["Science Fiction", "Action", "Adventure"]` |
| `F.concat_ws("\|", ...)` | `"Science Fiction\|Action\|Adventure"` |

`extract_collection_name` uses `F.col("belongs_to_collection.name")`; on a
null struct that is just null, so standalone films need no special case.
`extract_credits` finds directors with a **higher-order function**, one that
takes a small function as input:

```python
directors = F.concat_ws("|", F.filter("credits.crew", lambda c: c["job"] == "Director")["name"])
```

The `lambda` is not run in Python: PySpark turns it into a Spark expression,
so it runs in the JVM like any built-in.

**Line to say in review.** "Spark reads the JSON objects as structs and lists
as arrays. `col("genres.name")` pulls one field out of every element and
`concat_ws` joins them with a pipe, as the spec asks, all without a Python
loop."

**Follow-up trap.** "Why a pipe string and not keep the array?" → The spec
asks for `|`-separated text, and a string column is easy to read in pandas or
a CSV. The cost: every query for one genre must `split` again (see `has()` in
`kpis.py`).

---

## 10. `explode`, fan-out and double counting

**Plain idea.** `F.explode(array)` makes one output row per element. A film
with 3 genres becomes 3 rows: **fan-out**. That is what you want when
counting "films per genre", but any sum over the exploded rows counts that
film more than once.

**In this project.** Three places explode:

| Where | Effect |
|---|---|
| `inspect_anomalies(df, ["genres"])` | each film counts once per genre: Adventure 15, Action 10, Science Fiction 10 ... the counts add up to more than 18 |
| `plot_roi_by_genre` | Avatar's ROI is in the Action, Adventure and Science Fiction means |
| `top_directors` | Joe Russo and Anthony Russo each get Endgame + Infinity War: 4,994.60 each |

`test_codirectors_both_get_the_revenue` makes this a deliberate rule:
directors A and B of a 200 M film each show 200.

**Is that right?** For "how much did films *by this director* make?", yes:
each co-director directed the whole film. Splitting it 50/50 would say each
directed half a film. It is wrong to **add up** the director column: the
top-10 table counts the two Russo films twice and the two Frozen films
twice (Jennifer Lee and Chris Buck, 2,727.90 each). Rank directors this way;
never sum the table.

**Line to say in review.** "Explode gives each co-director full credit,
which is right for a per-director ranking. The column is then not additive:
summing it double-counts co-directed films."

**Follow-up trap.** "What if you grouped without exploding?" → The raw
strings differ by order: TMDB lists `Joe Russo|Anthony Russo` on Endgame and
`Anthony Russo|Joe Russo` on Infinity War (see the cleaned-movies table in
`results.md`), so they would become two "directors" with one film each.

---

## 11. A window with no `partitionBy`

**Plain idea.** A **window function** computes a value for each row from
other rows, without collapsing them (unlike `groupBy`). `row_number()` gives
1, 2, 3 ... in some order. `Window.partitionBy(x)` restarts the numbering
for each value of `x`, so groups can be done in parallel. With **no**
`partitionBy`, one counter must run over every row, so Spark moves all rows
into **one** partition, on one executor.

**In this project.** `reorder_and_reset` builds the spec's "reset index":

```python
idx = F.row_number().over(Window.orderBy("id")) - 1
return df.select(idx.alias("idx"), *FINAL_COLUMNS)
```

`- 1` makes it 0-based like pandas. The docstring and the README both admit
the cost: "pulls every row into one partition to number them. ok for 18
movies; on a big table i would drop idx and just use id as the key." Spark
logs a warning about it ("No Partition Defined for Window operation!").
`test_idx_starts_at_zero` checks ids 30, 10, 20 get idx 0, 1, 2.

**Line to say in review.** "Spark rows have no index, so 'reset index' is a
`row_number` over a fixed order. Without `partitionBy` it runs on one
partition: fine for 18 rows, a bottleneck at scale, where I would key on
`id` instead."

**Follow-up trap.** "Why order by id and not the fetch order?" → Spark does
not keep a guaranteed row order, so "original order" is not stable. `id` is
unique and always there, so `idx` is the same on every run. The cost: `idx
0` is Titanic (id 597), not Endgame, the first real ID in the spec list.

---

## 12. `toPandas()` and `collect()`: the driver-memory risk

**Plain idea.** Both copy rows from the executors to the driver: `collect()`
gives a list of `Row` objects, `toPandas()` a pandas DataFrame. The driver is
one process with limited memory. On a big table this crashes it with an
out-of-memory error, and you lose Spark's parallelism. **Arrow** (a columnar
in-memory format) makes `toPandas()` faster, but not smaller.

**In this project.** Every transfer is small, and the README lists it as a
choice: "only small results go to the driver."

| Call | Rows to the driver |
|---|---|
| `md_table`: `df.limit(max_rows).collect()` | at most 50 |
| `plot_revenue_vs_budget`, `plot_popularity_vs_rating` | 18 (the only whole-column `toPandas()`) |
| `plot_roi_by_genre` after `groupBy` | 11 |
| `plot_yearly_box_office` after `groupBy` | 9 |
| `plot_distribution`: bucket counts | at most 20 |
| `plot_boxplot`: quartiles, whisker ends, outliers | a handful |

`spark_session.py` enables Arrow with a fallback:
`spark.sql.execution.arrow.pyspark.enabled` and
`...arrow.pyspark.fallback.enabled` both `true`.

**Line to say in review.** "I reduce first, then bring data to the driver:
`limit` before `collect`, `groupBy` before `toPandas`, histogram buckets and
quartiles computed in Spark. Only the two scatter plots send one row per
film."

**Follow-up trap.** "And with a million films?" → Sample the scatter plots
(`df.sample(fraction=0.01)`), as the `visualize.py` docstring says. Everything
else already scales: the histogram sends 20 numbers whatever the table size.

---

## 13. `percentile_approx` vs an exact median

**Plain idea.** An exact median needs all values sorted, which on big data
means a full shuffle. `percentile_approx(col, p)` uses a compact summary
and returns a value close to the true one. It always returns **one of the
actual values**: it never averages or interpolates between two.

**In this project.** Two places:

1. `franchise_vs_standalone`: `F.percentile_approx("roi", 0.5)` as
   `median_roi`. The comment: "roi is a median: one huge film moves the mean
   and not the median. percentile_approx because an exact median needs a full
   sort."
2. `plot_distribution` (the median line) and `plot_boxplot` (Q1, median, Q3).

The effect on a tiny group: the standalone group is 2 films, Titanic (ROI
11.32, in the top-ROI table) and Beauty and the Beast. `results.md` shows
`median_roi` 7.91 for the group. The textbook median of two values is their
average; `percentile_approx` returns the lower one, so 7.91 is Beauty and the
Beast's ROI, and the exact median would be (7.91 + 11.32) / 2 ≈ 9.62.

The same difference moves boxplot fences: pandas interpolates quartiles,
Spark picks real values, so on 18 rows the fences differ. The notebook notes
that Avatar and Endgame are inside the Spark high fence (3,129) while the
pandas version flagged them as outliers.

**Line to say in review.** "`percentile_approx` scales and returns a real
value. On tiny groups that shows: the standalone 'median' is the lower of two
films. On a small group I would use `F.median`; at scale the approximation is
the right tool."

**Follow-up trap.** "Does it change your conclusion?" → No, because there is
no conclusion to draw from 2 films either way.

---

## 14. Data checks that stop the run: `validate()`

**Plain idea.** A pipeline that writes wrong numbers quietly is worse than
one that stops. A **data check** is a rule that must hold before anyone
reads a number (no duplicate ids, values in range). Run it after cleaning and
**raise** if it fails, so nothing downstream is written.

**In this project.** `cleaning.validate`:

```python
checks = df.agg(
    F.count("*").alias("rows"),
    F.count_distinct("id").alias("distinct_ids"),
    F.count_if(F.col("title").isNull()).alias("null_titles"),
    F.count_if(F.col("budget_musd") <= 0).alias("bad_budget"),
    F.count_if(F.col("revenue_musd") <= 0).alias("bad_revenue"),
    F.count_if((F.col("vote_average") < 0) | (F.col("vote_average") > 10)).alias("bad_rating"),
).first()
```

All six checks in **one** aggregation, so one pass over the data instead of
one `count()` per rule. It raises `ValueError("data checks failed: ...")`;
`main()` catches it, logs it and exits with code 1 before any Parquet,
results or chart is written. Tests: `test_validate_passes_on_good_data`,
`test_validate_stops_on_empty_table`. On this data: "data checks passed".

**Line to say in review.** "`validate` runs six rules in a single
aggregation and raises if any fails, so a bad run never writes a Parquet or
charts."

**Follow-up trap.** "Why `count_if(budget <= 0)` if zeros are already null?"
→ It is a guard on the cleaning itself: if a later change broke
`clean_unrealistic_values`, the check would catch it. Null budgets are not
counted, because `null <= 0` is null.

---

## 15. When Spark is overkill, and why we still use it

**Plain idea.** Spark is built for data too big for one machine. It pays a
fixed cost per job: starting a JVM, planning, scheduling tasks, moving data
between Java and Python. For small data that cost is most of the run time.

**In this project.**
- The data is **18 rows**.
- `python -m src.main` takes 39 to 83 seconds on this laptop, nearly all
  Spark start-up and per-action overhead, and the 32 tests take almost ten
  minutes (timings in
  [15 · Review-day runbook](15-review-day-runbook.md)).
- `tmdb_client.py` already makes the call: "19 http calls do not need a
  cluster", so Step 1 is plain Python.
- The final report says it plainly: "pandas would be faster here."

**Why still Spark.** The lab is about learning PySpark, and the code is
written as it would have to be for a real table: schema on read, built-ins
instead of UDFs, cache and unpersist, small results to the driver, data
checks, config from environment variables. The one place that would not
scale (the window with no `partitionBy`) is named in the code and README.

**Line to say in review.** "For 18 rows pandas would be faster; Spark's
start-up dominates. I used Spark because that is the lab, and wrote it the
way I would for big data."

**Follow-up trap.** "At what size would you switch to Spark for real?" →
When the data no longer fits comfortably in one machine's memory, roughly
tens of GB, or when it already lives on a Spark platform. Below that,
pandas, Polars or DuckDB are simpler and faster.

---

## Self-check

1. Which call fills the cache of `df_movies`, and why is `df_raw` unpersisted right after?
2. What does PERMISSIVE mode do with a line that is not valid JSON?
3. Why does `results.md` show 7.91 as the standalone median ROI?
4. Joe Russo shows 4,994.60 total revenue. Is it wrong that Anthony Russo shows the same?
5. Why does `validate` use one `agg` instead of one `count()` per rule?

Answers: (1) `cleaning.validate(df_movies)`, its first action; after that
nothing reads `df_raw`, so its memory is freed. (2) Keeps the row with null
fields and puts the raw text in `_corrupt_record`; the code then counts and
drops it. (3) `percentile_approx` returns an actual value, the lower of the
two films, instead of averaging them. (4) No, both directed both films; it is
only wrong to add the two together. (5) One pass over the data instead of
six.

Next: [12 · Code-review defense](12-code-review-defense.md)
