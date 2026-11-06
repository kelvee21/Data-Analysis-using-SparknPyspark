# 02 — What Spark Is and How It Runs Your Code

The ideas you need before any PySpark line makes sense: why Spark exists,
who does the work, and why nothing happens until you ask for a result.
Every idea is tied to something you can see when this lab runs.

> **Help:** the setup that makes Spark start is in
> [01 · Setting up Spark on Windows](01-setup-windows.md). The DataFrame
> methods themselves are in [05 · DataFrame fundamentals](05-dataframe-fundamentals.md).

## 1. The problem Spark solves

pandas loads a whole table into the memory of **one machine** and works on
it with **one CPU core** for most operations. That is perfect up to a few
gigabytes. It breaks when:

- the data is bigger than one machine's memory (say 500 GB of logs), or
- one core is too slow, even though the machine (or a room of machines)
  has hundreds of cores sitting idle.

Spark's answer: cut the data into pieces, send the pieces to many
**workers** (cores on many machines), let each worker process its piece,
and combine the results. A group of machines working together like this
is a **cluster**.

**Be honest about this lab.** It has 18 movies. pandas would do every step
in a fraction of a second; Spark spends about 20 seconds just starting.
The lab does not use Spark because the data needs it. It uses Spark to
teach the API that *does* scale: the same `groupBy`, `explode` and
`filter` code would run unchanged on 18 million movies on a cluster. The
README says it directly: "the pandas version could get away with anything
at 18 rows. here i wrote it the way it would have to run on a real table."
When a reviewer asks "why Spark for 18 rows?", that is the answer.

This lab is the same lab as the earlier pandas one (DEM02), with the same
file layout and function names, so you can compare the two line by line.

## 2. Driver and executors

A Spark program has two kinds of process:

| Role | What it does | In this lab |
|---|---|---|
| **Driver** | Runs your Python code, builds the plan, splits it into tasks, collects results | The `python -m src.main` process (or the notebook kernel) plus the JVM it starts |
| **Executors** | Run the tasks on their piece of the data and send results back | Threads inside that same JVM on your laptop |

On a real cluster the driver is on one machine and executors are on many
others. Here everything is on one laptop. The code does not change; only
the `master` setting does.

The **SparkSession** is your handle on all of this. You create it once and
every DataFrame comes from it. The lab builds it in one place,
`src/spark_session.py`, so the notebook, `main.py` and the tests never
disagree:

```python
load_dotenv()  # so the SPARK_ settings in .env count, not only real env vars
master = os.getenv("SPARK_MASTER", "local[*]")
spark = (
    SparkSession.builder
    .appName(app_name)
    .master(master)
    # default is 200 shuffle partitions. for 18 rows that is 200 near
    # empty tasks after every groupBy. AQE would merge them anyway, but
    # starting small keeps the spark ui readable.
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # dates: same answer on every machine, no matter the laptop timezone
    .config("spark.sql.session.timeZone", "UTC")
    # arrow makes toPandas() a columnar copy instead of row by row.
    # fallback means it still works if pyarrow is missing.
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
```

Each setting is explained in the sections below: master (§3), shuffle
partitions and AQE (§4, §9), Arrow (§5). `getOrCreate()` returns the
running session if one exists, so calling `get_spark()` twice is safe.

## 3. The master: `local[*]` or a cluster

The **master** setting says where the executors are.

| Master value | Meaning |
|---|---|
| `local` | One thread. Everything runs one task at a time |
| `local[4]` | Four threads, so up to four tasks at once |
| `local[*]` | One thread per CPU core of this machine |
| `spark://host:7077`, `yarn`, `k8s://...` | A real cluster |

The lab reads it from the `SPARK_MASTER` environment variable and defaults
to `local[*]`. The docstring explains why: "the master comes from
SPARK_MASTER so the same code can be pointed at a cluster later; on my
laptop it stays local[*]." Changing where the code runs is a config
change, not a code change.

## 4. Partitions

A **partition** is one piece of a DataFrame. One task processes one
partition. More partitions means more possible parallelism, but each
partition has a fixed overhead.

In this lab:

- `cleaning.load_raw` reads `data/raw_movies.jsonl` into **1 partition**,
  because the file is small. Spark only splits a file when it is bigger
  than its split size (at least 4 MB on this laptop, at most 128 MB by
  default).
- After a **shuffle** (see §8), Spark uses `spark.sql.shuffle.partitions`
  partitions. The default is 200. With 18 rows that means 200 tasks, most
  of them empty, each with start-up cost. The lab sets it to **4**
  (overridable with `SPARK_SHUFFLE_PARTITIONS`).
- Before writing Parquet, `main.py` calls `coalesce(1)`: "18 rows do not
  need 4 tiny files". `coalesce(n)` merges partitions down to `n` without
  a shuffle.

You can ask a DataFrame how many partitions it has:

```python
df.rdd.getNumPartitions()
```

## 5. DataFrames versus pandas

A Spark **DataFrame** is a table with named, typed columns, like a pandas
DataFrame. The differences matter:

| | pandas DataFrame | Spark DataFrame |
|---|---|---|
| Where the data lives | In your Python process's memory | Split into partitions across executors |
| When work happens | Immediately, every line | Only when an action asks (lazy, §6) |
| Can you change it? | Yes, `df["x"] = ...` changes it in place | No. Every method returns a **new** DataFrame |
| Row index | Yes (`0, 1, 2...`) | No. Rows have no built-in order or number |
| Column syntax | `df["budget"] / 1e6` | `F.col("budget") / 1_000_000` (a Column expression) |
| Size limit | One machine's RAM | Disk and RAM of the whole cluster |

Where this shows up in the code:

- **No index.** Spec Step 2.11 says "reset index". Spark has no index, so
  `cleaning.reorder_and_reset()` builds one with `row_number()` (see
  [06 · Cleaning line by line](06-cleaning-line-by-line.md#h-11-reorder-and-reset-steps-2-10-and-2-11)).
- **Immutable.** That is why every cleaning function looks like
  `return df.withColumn(...)`. It never changes `df`; it returns a new one.
- **Going back to pandas.** `df.toPandas()` copies every row into the
  driver as a pandas DataFrame. With **Arrow** turned on (a columnar
  memory format, via the `pyarrow` package), the copy is done column by
  column instead of row by row, which is much faster. `visualize.py` only
  calls `toPandas()` on small, already-grouped results, plus the two
  scatter plots, because on a big table it would run the driver out of
  memory.

## 6. Lazy evaluation

**Lazy** means Spark does not compute anything when you call a
transformation. It only writes down what you asked for. The whole chain
runs when you call an **action**.

From `main.run()`:

```python
df_movies = kpis.add_profit_roi(cleaning.clean(df_raw)).cache()   # nothing computed yet
checks = cleaning.validate(df_movies)                              # action: now it runs
```

`cleaning.clean()` returns instantly, even though it describes about 30
column operations. The module docstring says it: "nothing runs until
something asks for a result (count, show, write), so clean() is just
building a plan."

Why lazy?

1. **Optimisation.** Spark's optimiser (called **Catalyst**) sees the
   whole plan before running it, so it can reorder and merge steps. §8
   shows it pushing a filter into the file read and skipping unused
   columns.
2. **No wasted work.** If you never ask for a column, it is never
   computed or even read.

The trap: an error in a transformation (say, a bad cast) does not appear
on the line that wrote it. It appears later, at the action. When a
traceback points at `.first()` or `.count()`, look back up the chain.

## 7. Transformations, actions and cache

| Transformations (lazy, return a DataFrame) | Actions (run the plan, return a result) |
|---|---|
| `select`, `withColumn`, `drop`, `filter` | `show()`, `count()` |
| `groupBy().agg()`, `orderBy`, `limit` | `collect()`, `first()`, `take(n)` |
| `dropDuplicates`, `dropna`, `explode` | `toPandas()` |
| `transform(fn)`, `coalesce(n)` | `write.parquet(...)` |

A rule of thumb: if it gives you back a DataFrame, it is a transformation.
If it gives you Python values, prints, or writes a file, it is an action.

**`cache()`** keeps a computed DataFrame in memory so later actions reuse
it. From `main.py`:

```python
# cache because every kpi, table and chart below reads this same table.
# without it spark would redo the read + clean for each one.
df_movies = kpis.add_profit_roi(cleaning.clean(df_raw)).cache()
```

The cleaned table feeds ten rankings, two searches, three groupings and
eight charts. Without `cache()`, each of those actions would re-read the
JSON file and re-run every cleaning step. `cache()` is lazy too: the first
action (`validate`) fills it.

**`unpersist()`** frees the memory when the cache is no longer needed.
`main.py` unpersists `df_raw` right after the clean table is cached, and
`df_movies` at the end of `run()`. On a laptop with 18 rows this hardly
matters; on a cluster, forgotten caches fill executor memory.

`main.py` also caches the raw table for a Spark-specific reason, covered
in [06](06-cleaning-line-by-line.md#h-3-drop-corrupt-rows-and-counting-bad-lines).

## 8. Jobs, stages, tasks, shuffles, and reading a plan

When an action runs, Spark breaks the work down three levels:

| Level | What it is | How many |
|---|---|---|
| **Job** | All the work for one action | One per action (`count`, `collect`, ...) |
| **Stage** | A run of steps that need no data from other partitions | A new stage starts at every shuffle |
| **Task** | One stage applied to one partition | = number of partitions in that stage |

A **shuffle** is when rows have to move between partitions. It happens
when rows with the same key must end up together:

- `groupBy("director")`: all rows for "James Cameron" must meet.
- `orderBy(...)`: a global sort needs to compare rows from every partition.
- `dropDuplicates(["id"])`: two copies of the same `id` must meet.
- A window with no `partitionBy` (the `idx` column): every row must go to
  **one** partition.

Shuffles are the expensive part of Spark: rows are written out, sent over
the network (on a real cluster) and read back. In a plan, a shuffle is an
`Exchange` node. `df.explain()` prints the plan. Here is the cleaning part
of the plan for the cached `df_movies`, trimmed, from this lab:

```
Window [row_number() ... ORDER BY id]                               <- idx
+- Sort [id ASC]
   +- Exchange SinglePartition                                       <- shuffle: all rows to 1 partition
      +- Filter (atleastnnonnulls(2, id, title) AND atleastnnonnulls(10, ...))
         +- SortAggregate(key=[id], functions=[first(title), ...])   <- dropDuplicates(["id"])
            +- AQEShuffleRead coalesced
               +- Exchange hashpartitioning(id, 4)                   <- shuffle by id, 4 partitions
                  +- Project [CASE WHEN (budget > 0.0) THEN (budget / 1000000.0) END AS budget_musd, ...]
                     +- Project [try_cast(id as bigint) AS id, ..., concat_ws(|, genres.name) ...]
                        +- Filter (isnotnull(status) AND isnull(_corrupt_record) AND status = Released)
                           +- FileScan json [id, title, ..., credits, _corrupt_record]
                                PushedFilters: [IsNotNull(status), IsNull(_corrupt_record), EqualTo(status,Released)]
```

Read it **bottom up**:

1. `FileScan json` reads the file. The list of columns does **not**
   include `adult`, `imdb_id`, `video` or `homepage`: Spark saw they are
   dropped later and never reads them. This is **column pruning**.
2. The `status = Released` filter, written near the *end* of `clean()`,
   has moved to the very bottom, next to the read. This is **predicate
   pushdown**: filter as early as possible so less data flows up.
3. The dozen `withColumn` calls were merged into two `Project` nodes.
4. Two `Exchange` nodes: one for `dropDuplicates(["id"])` into 4
   partitions, one `SinglePartition` for the `idx` window.

All of this is Catalyst at work, possible only because evaluation is lazy.

Then, the plan for `kpis.top_directors(df_movies)` on top of the cache:

```
TakeOrderedAndProject(limit=10, orderBy=[total_revenue DESC, mean_rating DESC])
+- HashAggregate(keys=[director], functions=[count(1), sum(revenue_musd), avg(vote_average)])
   +- Generate explode(split(director, \|, -1))
      +- Filter isnotnull(director)
         +- InMemoryTableScan [revenue_musd, vote_average, director]      <- reads the cache
```

There is **no `Exchange`** before the `groupBy` here. The cached table is
already in one partition (because of the `idx` window), so all rows for
each director are already together and no shuffle is needed. Spark noticed
that by itself.

## 9. Adaptive Query Execution (AQE)

The plan above says `AdaptiveSparkPlan` at the top and `AQEShuffleRead
coalesced` in the middle. **AQE** (Adaptive Query Execution) lets Spark
change the plan *while it runs*, using real sizes measured after each
shuffle. The two settings in `get_spark()`:

| Setting | What it does here |
|---|---|
| `spark.sql.adaptive.enabled` | Turns AQE on (it is on by default in Spark 4; the code makes it explicit) |
| `spark.sql.adaptive.coalescePartitions.enabled` | After a shuffle, merge tiny partitions into fewer tasks. That is the `coalesced` in the plan |

So even if `SPARK_SHUFFLE_PARTITIONS` were left at 200, AQE would merge the
empty ones. The comment in the code says this and gives the reason for
still setting 4: "starting small keeps the spark ui readable."

## 10. The Spark UI

While a Spark program runs, the driver serves a web page, usually at
**http://localhost:4040**. (If 4040 is taken, it uses 4041, then 4042.)
`get_spark()` logs the exact address: `spark 4.3.0 on local[*], ui at http://...`.

| Tab | What you see |
|---|---|
| **Jobs** | One row per action, with its stages and duration |
| **Stages** | Tasks per stage, time per task, shuffle read/write sizes |
| **Storage** | Cached DataFrames (the `df_movies` cache appears here) |
| **SQL / DataFrame** | A diagram of each query plan, the same as `explain()`, with AQE's final version |
| **Executors** | Each executor's memory and task counts |

The UI disappears when `spark.stop()` runs, and `main.py` always stops
Spark at the end. To look around, use the notebook: `analysis.ipynb` keeps
the session alive between cells, so open the UI while it is running.

On this data, most jobs take well under a second each, and almost all of
that is overhead, not computing. That is the honest picture of Spark on
tiny data.

## 11. Where the time goes on this laptop

| What | Typical time here | Why |
|---|---|---|
| Starting the SparkSession | about 20 s | Starting a JVM, loading Spark's jars |
| Each action on 18 rows | a fraction of a second to a few seconds | Planning, scheduling, starting tasks |
| The test suite | minutes | Dozens of actions, each with that fixed cost |
| Actual computing on 18 rows | close to zero | 18 rows is nothing |

This is the reverse of big-data work, where the fixed cost is small next to
hours of real computing. Keep both pictures in mind.

## Self-check

1. What is the difference between the driver and an executor? [The driver runs your code and plans; executors run tasks on partitions.]
2. `cleaning.clean(df_raw)` returns in milliseconds. Has the data been cleaned? [No. It returned a plan; work runs at the next action.]
3. In the plan, why does the `status = Released` filter appear next to the file read? [Predicate pushdown: Catalyst moves filters as early as possible.]
4. Why does the lab set shuffle partitions to 4 when AQE could coalesce them anyway? [AQE would merge them, but starting small keeps the Spark UI readable.]
5. Why `.cache()` the cleaned movies, and why `unpersist()` later? [Every KPI and chart reuses them; unpersist frees the memory when done.]
