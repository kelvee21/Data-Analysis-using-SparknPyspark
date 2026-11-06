# 13 — Glossary, Practice Questions and Hands-on Drills

Use this page last, as a rehearsal. Cover the answers, say yours out loud,
then compare. Part C has drills to type into a PySpark shell on the Parquet
file the pipeline writes, with the real output.

> **Help:** this glossary covers concepts. Every PySpark function used in the code is in [14 · PySpark function reference](14-pyspark-function-reference.md). Longer explanations are in [11 · Hard concepts](11-hard-concepts-explained.md).

## A. Glossary (one line each)

| Term | Meaning |
|---|---|
| **Spark** | An engine that processes data in parallel, on one machine or a cluster |
| **PySpark** | The Python interface to Spark; your Python builds plans that the JVM runs |
| **JVM** | Java Virtual Machine; Spark's engine runs in it, which is why Java 17 is needed |
| **SparkSession** | The entry point to Spark; `get_spark()` builds the one everything shares |
| **Driver** | The process that runs your script, plans the work, and receives `collect()` results |
| **Executor** | A worker process (here: threads on this laptop) that runs tasks on partitions |
| **`local[*]`** | Run Spark on this machine, one worker thread per CPU core; set by `SPARK_MASTER` |
| **Partition** | One chunk of a DataFrame; one task processes one partition |
| **Task / stage / job** | Work on one partition / tasks with no shuffle between them / everything one action triggers |
| **Spark UI** | Web page showing jobs and stages, at port 4040 (or 4041 if 4040 is taken) |
| **DataFrame** | A table with named, typed columns, split into partitions; never changed in place |
| **Row** | One record from `collect()` or `first()`; read fields as `row.title` or `row["count"]` |
| **Column expression** | A description of a computation, e.g. `F.col("revenue_musd") / F.col("budget_musd")` |
| **Schema** | Column names and types of a DataFrame |
| **Schema inference** | Spark guessing the schema by reading the data; not used here |
| **Schema on read / `RAW_SCHEMA`** | Giving Spark the schema when reading; `cleaning.RAW_SCHEMA` |
| **StructType / StructField** | A schema written out by hand / one column in it |
| **Struct** | A column with named sub-fields, e.g. `belongs_to_collection.name` |
| **Array** | A column holding a list, e.g. `genres` before flattening |
| **PERMISSIVE mode** | Read mode that keeps a malformed line as a row of nulls instead of failing |
| **`_corrupt_record`** | The column that receives the raw text of a malformed line |
| **Transformation** | Lazy operation returning a new DataFrame (`filter`, `select`, `withColumn`) |
| **Action** | Operation that returns data or writes, and triggers a job (`count`, `collect`, `write`) |
| **Lazy evaluation** | Nothing runs until an action; Spark builds a plan first |
| **Plan / `explain()`** | The steps Spark will run / the method that prints them |
| **Catalyst** | Spark's optimiser: rewrites the plan (prunes columns, pushes filters) |
| **Shuffle** | Moving rows between partitions so equal keys meet; the most expensive operation |
| **`spark.sql.shuffle.partitions`** | Partitions after a shuffle; default 200, 4 here (`SPARK_SHUFFLE_PARTITIONS`) |
| **AQE** | Adaptive Query Execution: merges small shuffle partitions at run time; on here |
| **`cache()` / `unpersist()`** | Keep a computed DataFrame in memory for reuse / release it |
| **`coalesce(n)`** | Merge partitions down to n without a shuffle; `coalesce(1)` before the Parquet write |
| **Narrow / wide transformation** | Needs no shuffle (`filter`) / needs one (`groupBy`, `orderBy`) |
| **UDF** | User-defined function; a Python UDF runs row by row in a Python worker |
| **pandas UDF** | A UDF that works on Arrow batches; faster than a plain Python UDF |
| **`ArrowEvalPython`** | The plan step where rows go from the JVM to Python for a UDF |
| **Built-in function** | A function in `pyspark.sql.functions`; runs in the JVM |
| **Higher-order function** | Takes a function as input, e.g. `F.filter(array, lambda c: ...)` |
| **ANSI mode** | Strict SQL rules: bad casts and divide-by-zero raise errors; on by default in Spark 4 |
| **`try_cast`** | A cast that returns null instead of failing |
| **Null** | "Unknown"; skipped by `avg`/`sum`; comparisons with it give null |
| **`explode`** | One output row per array element |
| **Fan-out** | Row multiplication from `explode` or a join; risks double counting |
| **Additive** | Can be summed across rows without double counting |
| **Window function** | Computes per row from other rows without collapsing them (`row_number`) |
| **`partitionBy` (window)** | Restarts the window per group; without it all rows go to one partition |
| **Aggregation** | Many rows to one per group: `groupBy(...).agg(...)` |
| **`percentile_approx`** | Approximate percentile; returns an actual value, never interpolates |
| **Quartiles / IQR / fences** | 25 % and 75 % points / their distance / Q1 − 1.5 IQR and Q3 + 1.5 IQR, beyond which a value is an outlier |
| **Histogram bucket** | One equal-width range of values; `plot_distribution` counts per bucket in Spark |
| **`ax.bxp`** | Matplotlib boxplot drawn from pre-computed numbers |
| **Data check / `validate()`** | Rules that must hold after cleaning; raises to stop the run |
| **Parquet** | Column-based file format that stores the schema and compresses well |
| **JSON Lines** | One JSON object per line; Spark can split and read it in parallel |
| **`toPandas()`** | Copies all rows to the driver as a pandas DataFrame |
| **Arrow / PyArrow** | Columnar memory format / its Python library; speeds up `toPandas()` |
| **Driver OOM** | The driver running out of memory, usually from `collect`/`toPandas` on big data |
| **winutils / `HADOOP_HOME`** | Windows helper binaries Spark needs to write files; missing → `NativeIO$Windows` error |
| **Agg backend** | Matplotlib renderer that writes files and opens no window |
| **Survivorship bias** | Conclusions drawn only from cases that passed a filter (here: blockbusters) |
| **ROI** | Return on investment: `revenue / budget` in this lab |
| **`python -m src.main`** | Run `main.py` as a module of the `src` package, from the lab folder |
| **Fixture (pytest)** | A function that prepares something a test needs; requested by parameter name |
| **`conftest.py`** | pytest file whose fixtures every test in the folder can use |
| **`monkeypatch`** | Built-in pytest fixture that swaps something out for one test (here `time.sleep`) |
| **Factory** | A function that builds test data, e.g. `movie(**changes)` in `tests/helpers.py` |
| **Exponential back-off** | Waiting longer after each failed retry: 2, 4, 8 s in `fetch_movie` |
| **`append_to_response`** | TMDB parameter that adds credits to the movie answer in one call |

## B. Practice questions (with model answers)

**1. How many films are in the clean table, and why not 19?**
18. ID 0 does not exist on TMDB; the fetch got a 404 and skipped it. No
cleaning rule removed any of the other 18.

**2. Which file reads the raw data, and with what options?**
`cleaning.load_raw`: `spark.read.schema(RAW_SCHEMA)`, `mode` PERMISSIVE,
`columnNameOfCorruptRecord` `_corrupt_record`, then `.json(path)`.

**3. Why is `revenue_musd` 2,942.19 for Endgame and not 2942186933?**
`clean_unrealistic_values` divides by 1,000,000 to get million USD.

**4. What does `F.when(F.col("budget") > 0, F.col("budget"))` return for budget 0?**
Null. There is no `otherwise()`, so non-matching rows get null.

**5. Which action fills the cache of `df_movies` in `main.py`?**
`cleaning.validate(df_movies)`, the first action on it.

**6. Why `orderBy(order, "title")` in `rank`?**
To break ties the same way every run (Endgame and Infinity War both 8.25).

**7. Why `r"\|"` in `F.split`?**
`split` takes a regular expression; a bare `|` means "or".

**8. What is the most successful franchise and by what measure?**
The Avengers Collection: highest `total_revenue`, 7,918.80 from 4 films.

**9. What does `percentile_approx` return as the median of 7.91 and 11.32?**
7.91, one of the actual values. The exact median is their average, about
9.62.

**10. What error would a plain `CAST('abc' AS DOUBLE)` give in Spark 4?**
`CAST_INVALID_INPUT`, because ANSI mode is on.

**11. What is `idx` for Titanic and why?**
0. The index is `row_number()` ordered by `id`, and Titanic's id (597) is the
smallest.

**12. What would `validate()` do if every film had status "Rumored"?**
The released filter would leave 0 rows, and `validate` would raise
`ValueError("data checks failed: no rows left after cleaning")`.

**13. Where would writing Parquet fail on Windows, and with what?**
`df_movies.coalesce(1).write...parquet(...)` without winutils/`HADOOP_HOME`
fails with `UnsatisfiedLinkError ... NativeIO$Windows`.

**14. Why does the runtime boxplot have exactly one outlier circle?**
Titanic (194 min) is the only runtime beyond the high fence.

**15. How many films are standalone, and which?**
2: Titanic and Beauty and the Beast.

## C. Hands-on drills

These run on the Parquet file `python -m src.main` writes. Open PowerShell in
the lab folder:

```powershell
$env:HADOOP_HOME="C:\Users\User\hadoop"; $env:PATH="C:\Users\User\hadoop\bin;"+$env:PATH
python
```

Then, in Python (reusing the project's own session builder):

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.spark_session import get_spark
spark = get_spark("drills")
spark.sparkContext.setLogLevel("ERROR")      # hide the WARN lines while practising
df = spark.read.parquet("data/movies_clean.parquet")
df.count(), len(df.columns)                  # (18, 25)
df.printSchema()
```

`printSchema()` shows the types Parquet kept: `idx: integer`, `id: long`,
`release_date: date`, `budget_musd: double`, `cast_size: integer`,
`vote_count: long`, `profit_musd: double`, `roi: double`, and so on. Try each
drill before reading the solution. All outputs below were produced on this
laptop.

### Drill 1 — Top 3 films by runtime

```python
df.orderBy(F.desc("runtime")).select("title", "runtime").show(3, truncate=False)
```

```text
+-----------------+-------+
|title            |runtime|
+-----------------+-------+
|Titanic          |194.0  |
|Avengers: Endgame|181.0  |
|Avatar           |162.0  |
+-----------------+-------+
only showing top 3 rows
```

`show(n)` is an action that prints the first n rows. `truncate=False` stops it
cutting long text at 20 characters. Titanic is the outlier in chart 7.

### Drill 2 — Count films per release year

```python
df.groupBy(F.year("release_date").alias("year")).count().orderBy("year").show()
```

```text
+----+-----+
|year|count|
+----+-----+
|1997|    1|
|2009|    1|
|2011|    1|
|2012|    1|
|2013|    1|
|2015|    4|
|2017|    2|
|2018|    4|
|2019|    3|
+----+-----+
```

This is why chart 4 is not a trend: most years hold one film.

### Drill 3 — Average rating per original language

```python
(df.groupBy("original_language")
   .agg(F.round(F.avg("vote_average"), 2).alias("avg_rating"),
        F.count("*").alias("movies"))
   .show())
```

```text
+-----------------+----------+------+
|original_language|avg_rating|movies|
+-----------------+----------+------+
|               en|       7.4|    18|
+-----------------+----------+------+
```

One group: every film is English-language. A one-row answer is still a
finding about the sample.

### Drill 4 — Add a `profit_margin` column

Profit margin = profit / revenue: the share of revenue that is profit.

```python
(df.withColumn("profit_margin", F.round(F.col("profit_musd") / F.col("revenue_musd"), 3))
   .orderBy(F.desc("profit_margin"))
   .select("title", "profit_musd", "revenue_musd", "profit_margin")
   .show(5, truncate=False))
```

```text
+--------------------------------------------+-----------+------------+-------------+
|title                                       |profit_musd|revenue_musd|profit_margin|
+--------------------------------------------+-----------+------------+-------------+
|Avatar                                      |2686.706026|2923.706026 |0.919        |
|Titanic                                     |2064.162353|2264.162353 |0.912        |
|Jurassic World                              |1521.537444|1671.537444 |0.91         |
|Harry Potter and the Deathly Hallows: Part 2|1216.511219|1341.511219 |0.907        |
|Frozen II                                   |1303.683476|1453.683476 |0.897        |
+--------------------------------------------+-----------+------------+-------------+
only showing top 5 rows
```

Same top 5 as "top roi" in `results.md`, because margin = 1 − 1/ROI: the two
always sort the same way.

### Drill 5 — Films per genre (explode)

```python
(df.withColumn("genre", F.explode(F.split("genres", r"\|")))
   .groupBy("genre").count()
   .orderBy(F.desc("count"), "genre")
   .show(5))
```

```text
+---------------+-----+
|          genre|count|
+---------------+-----+
|      Adventure|   15|
|         Action|   10|
|Science Fiction|   10|
|         Family|    5|
|      Animation|    4|
+---------------+-----+
only showing top 5 rows
```

Matches the genres value counts in `results.md`.

### Drill 6 — Films with a budget of 250 M or more

```python
(df.filter(F.col("budget_musd") >= 250)
   .select("title", "budget_musd")
   .orderBy(F.desc("budget_musd"))
   .show(truncate=False))
```

```text
+------------------------+-----------+
|title                   |budget_musd|
+------------------------+-----------+
|Avengers: Endgame       |356.0      |
|Star Wars: The Last Jedi|300.0      |
|Avengers: Infinity War  |300.0      |
|The Lion King           |260.0      |
+------------------------+-----------+
```

The two 300 M films are tied and their order is not guaranteed (the "top
budget" table in `results.md` has them the other way round, because `rank`
sorts ties by title). Add `"title"` as a second sort key to fix it.

### Drill 7 — The standalone films

```python
(df.filter(F.col("belongs_to_collection").isNull())
   .select("title", "budget_musd", "revenue_musd", "roi")
   .show(truncate=False))
```

```text
+--------------------+-----------+------------+-----------------+
|title               |budget_musd|revenue_musd|roi              |
+--------------------+-----------+------------+-----------------+
|Titanic             |200.0      |2264.162353 |11.320811765     |
|Beauty and the Beast|160.0      |1266.115964 |7.913224775000001|
+--------------------+-----------+------------+-----------------+
```

Test for null with `isNull()`, never `== None`: a comparison with null gives
null, so the filter would keep nothing. Note Beauty and the Beast's ROI, 7.91:
it is the standalone `median_roi` in `results.md`.

### Drill 8 — Top film per year (window with `partitionBy`)

```python
w = Window.partitionBy(F.year("release_date")).orderBy(F.desc("revenue_musd"))
(df.withColumn("rank_in_year", F.row_number().over(w))
   .filter("rank_in_year = 1")
   .select(F.year("release_date").alias("year"), "title", "revenue_musd")
   .orderBy("year")
   .show(truncate=False))
```

```text
+----+--------------------------------------------+------------+
|year|title                                       |revenue_musd|
+----+--------------------------------------------+------------+
|1997|Titanic                                     |2264.162353 |
|2009|Avatar                                      |2923.706026 |
|2011|Harry Potter and the Deathly Hallows: Part 2|1341.511219 |
|2012|The Avengers                                |1518.815515 |
|2013|Frozen                                      |1274.219009 |
|2015|Star Wars: The Force Awakens                |2068.223624 |
|2017|Star Wars: The Last Jedi                    |1334.407706 |
|2018|Avengers: Infinity War                      |2052.415039 |
|2019|Avengers: Endgame                           |2942.186933 |
+----+--------------------------------------------+------------+
```

Unlike `reorder_and_reset`, this window has a `partitionBy`, so each year
can be numbered in parallel and no single-partition warning appears.

### Drill 9 — Exact vs approximate median ROI

```python
df.select(F.median("roi").alias("exact_median"),
          F.percentile_approx("roi", 0.5).alias("approx")).show()
```

```text
+-----------------+-----------------+
|     exact_median|           approx|
+-----------------+-----------------+
|7.944507124342106|7.913224775000001|
+-----------------+-----------------+
```

With 18 values (an even count), the exact median averages the 9th and 10th;
`percentile_approx` returns the 9th. Repeat it grouped by franchise and
standalone to see the gap grow on the 2-film group.

### Drill 10 — Spark SQL: directors with more than one film (and a trap)

```python
df.createOrReplaceTempView("movies")
spark.sql("""
    SELECT director, COUNT(*) AS n FROM movies
    GROUP BY director HAVING n > 1 ORDER BY n DESC, director
""").show(truncate=False)
```

```text
+-----------------------+---+
|director               |n  |
+-----------------------+---+
|James Cameron          |2  |
|Jennifer Lee|Chris Buck|2  |
|Joss Whedon            |2  |
+-----------------------+---+
```

The Russo brothers are missing. TMDB lists them as `Joe Russo|Anthony Russo`
on Endgame and `Anthony Russo|Joe Russo` on Infinity War, so the raw strings
differ and each group has one film. That is why `top_directors` explodes the
string first. Exercise: rewrite the query with `explode(split(director,
'\\|'))` and check both Russos show 2 films.

### Drill 11 — A broken line and PERMISSIVE mode

```python
from src.cleaning import RAW_SCHEMA
lines = ['{"id": "1", "title": "Good"}', '{"id": 2, "title": ']
bad = (spark.read.schema(RAW_SCHEMA)
       .option("mode", "PERMISSIVE")
       .option("columnNameOfCorruptRecord", "_corrupt_record")
       .json(spark.sparkContext.parallelize(lines)))
bad.select("id", "title", "_corrupt_record").show(truncate=False)
```

```text
+----+-----+-------------------+
|id  |title|_corrupt_record    |
+----+-----+-------------------+
|1   |Good |NULL               |
|NULL|NULL |{"id": 2, "title": |
+----+-----+-------------------+
```

The first line reads normally with `_corrupt_record` null. The second has
null `id` and `title`, and its raw text in `_corrupt_record`. This is exactly
what `drop_corrupt_rows` removes. Change the mode to `"FAILFAST"` and run
`.count()`: the read now fails with a `Py4JJavaError` on the bad line.

When finished: `spark.stop()`.

## Self-check

1. In Drill 7, why `isNull()` and not `== None`?
2. Why do Drill 4 (margin) and the ROI ranking give the same top 5?
3. What is wrong with grouping on the raw `director` string?
4. Drill 6 has a tie. How do you make the order stable?

Answers: (1) Any comparison with null gives null, and `filter` drops null
conditions, so `== None` keeps no rows. (2) Margin = 1 − 1/ROI rises whenever
ROI rises. (3) Co-directors listed in a different order become different
groups; explode first. (4) Add a second sort key, e.g.
`orderBy(F.desc("budget_musd"), "title")`.

Next: [14 · PySpark function reference](14-pyspark-function-reference.md)
