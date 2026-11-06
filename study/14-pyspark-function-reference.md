# 14 — PySpark Function Reference (A–Z)

> Every PySpark function, method and class used in `src/`, `tests/` and
> `analysis.ipynb`, each with the form used here, what it does, the line from
> this project, and a gotcha. Use your browser's find (Ctrl+F) to jump to a
> name. A pandas → PySpark table is at the end.
>
> Imports used in the code: `from pyspark.sql import functions as F`,
> `from pyspark.sql import types as T`, `from pyspark.sql.window import Window`.
> So `F.col` is `pyspark.sql.functions.col`, and so on.
>
> Pages: [05 DataFrames](05-dataframe-fundamentals.md) ·
> [06 Cleaning](06-cleaning-line-by-line.md) ·
> [07 Aggregations](07-aggregations-windows-sorting.md) ·
> [08 KPIs](08-kpis-line-by-line.md) · [09 Charts](09-visualisation.md) ·
> [11 Hard concepts](11-hard-concepts-explained.md)

**Kinds:** *T* = transformation (lazy, returns a DataFrame); *A* = action
(runs a job); *C* = builds a column expression; *S* = session/reader/schema.

---

## Operators on columns

| Operator | Meaning | Line from this project | Gotcha |
|---|---|---|---|
| `&` | AND of two conditions | `has("genres", "Science Fiction") & has("genres", "Action") & has("cast", "Bruce Willis")` | Use `&`, not `and`; wrap each comparison in brackets: `(a > 1) & (b < 2)` |
| `\|` | OR | `(F.col("vote_average") < 0) \| (F.col("vote_average") > 10)` | Same: not `or`, and brackets matter because `\|` binds tighter than `<` |
| `~` | NOT | `~F.trim(F.col(col)).isin(PLACEHOLDERS)` | `~` of null is null, so a null text stays null (which is the goal here) |
| `>`, `<=`, `==`, `!=` | comparisons, give a boolean column | `F.col(col) > 0`, `joined != ""` | Comparing with null gives null, not False |
| `-`, `/` | arithmetic on columns | `F.col("revenue_musd") - F.col("budget_musd")` | Under ANSI, `/ 0` raises `DIVIDE_BY_ZERO`; dividing by null gives null |
| `col[...]` | field of a struct, or field of every struct in an array | `F.filter(...)["name"]`, `c["job"]` | Works on `array<struct>`; on `array<string>` Spark wants an integer index |

## A

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `DataFrame.agg` | T | `df.agg(expr, ...)` / `groupBy(...).agg(...)` | computes aggregates over the whole table or per group | `df.agg(F.count("*").alias("rows"), F.count_distinct("id")...)` in `validate` | Without `groupBy` it returns exactly one row |
| `Column.alias` | C | `expr.alias("name")` | names the result column | `F.round(F.sum("revenue_musd"), 1).alias("total_revenue")` | Without it the column gets a long auto name like `round(sum(revenue_musd), 1)` |
| `F.array_contains` | C | `F.array_contains(array_col, value)` | true if the array holds `value` as a whole element | `F.array_contains(F.split(F.col(col), r"\|"), value)` in `has` | Exact match: "Bruce Willis Jr" ≠ "Bruce Willis" |
| `T.ArrayType` | S | `T.ArrayType(element_type)` | schema type for a list | `T.ArrayType(T.StructType(fields))` in `_named` | The element type must be given; that is the point of an explicit schema |
| `Column.asc` | C | `F.col(c).asc()` | ascending sort order | `order = F.col(col).asc() if ascending else F.col(col).desc()` | Nulls come **first** in ascending order; `rank` drops them before sorting |
| `Row.asDict` | — | `row.asDict()` | a `Row` as a Python dict | `return checks.asDict()` in `validate` | Nested `Row`s stay `Row`s unless `recursive=True` |
| `F.avg` | C | `F.avg(col)` | mean, skipping nulls | `F.round(F.avg("revenue_musd"), 1).alias("mean_revenue")` | Nulls are skipped, so the divisor is the count of non-null values |

## B

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `Column.between` | C | `col.between(lo, hi)` | `lo <= col <= hi` | `F.col(column).between(low_fence, high_fence)` in `plot_boxplot` | Both ends inclusive |
| `SparkSession.builder` | S | `SparkSession.builder.appName(...).master(...).config(...).getOrCreate()` | configures and creates the session | `spark_session.get_spark` | `getOrCreate` reuses a running session, and some settings (master) cannot change once it exists |

## C

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `DataFrame.cache` | T | `df.cache()` | keep the result in memory after the first action | `df_movies = kpis.add_profit_roi(cleaning.clean(df_raw)).cache()` | Lazy: nothing is stored until an action runs (`validate` here) |
| `Column.cast` | C | `col.cast("int")` | change type | `F.least(F.floor(...), F.lit(bins - 1)).cast("int")` | Under ANSI a bad cast raises; safe here because the value is already a number |
| `F.coalesce` | C | `F.coalesce(a, b)` | first non-null of its arguments | `F.coalesce(F.size("credits.cast"), F.lit(0))` | Different from `DataFrame.coalesce` (partitions) |
| `DataFrame.coalesce` | T | `df.coalesce(1)` | reduce the number of partitions without a shuffle | `df_movies.coalesce(1).write.mode("overwrite").parquet(...)` | One partition means one task: fine for 18 rows, slow for big writes |
| `F.col` | C | `F.col("name")`, `F.col("struct.field")` | refers to a column; dot reaches into structs | `F.col("belongs_to_collection.name")` | A name with a dot inside it would need backticks |
| `DataFrame.collect` | A | `df.collect()` | all rows to the driver as a list of `Row` | `rows = df.limit(max_rows).collect()` in `md_table` | Risky on big data; `limit` first |
| `DataFrame.columns` | — | `df.columns` | list of column names (no job) | `len(df_movies.columns)` → 25 | A property, no brackets |
| `F.concat_ws` | C | `F.concat_ws(sep, array_or_cols)` | joins strings with a separator, skipping nulls | `F.concat_ws("\|", F.col(f"{column}.{name_key}"))` | Empty or null array gives `""`, not null; hence the `F.when(joined != "", joined)` |
| `T.StructType` / `T.StructField` | S | `T.StructType([T.StructField("id", T.StringType()), ...])` | a hand-written schema / one field of it | `RAW_SCHEMA` in `cleaning.py` | Fields not listed are skipped when reading JSON |
| `DataFrame.count` | A | `df.count()` | number of rows | `bad = df_raw.filter(F.col("_corrupt_record").isNotNull()).count()` | Every call is a job; cache before counting repeatedly |
| `F.count` | C | `F.count("*")`, `F.count(col)` | rows per group / non-null values | `F.count("*").alias("movies")` | `count("*")` counts nulls; `count(col)` does not |
| `F.count_if` | C | `F.count_if(condition)` | number of rows where the condition is true | `F.count_if(F.col("title").isNull()).alias("null_titles")` | Null conditions are not counted |
| `F.count_distinct` | C | `F.count_distinct(col)` | number of distinct non-null values | `F.count_distinct("id").alias("distinct_ids")` | Exact, so it shuffles; `approx_count_distinct` is the cheap version |
| `DataFrame.createOrReplaceTempView` | S | `df.createOrReplaceTempView("movies")` | lets `spark.sql` query the DataFrame by name | Drill 10 in [13](13-glossary-and-practice.md) (not in `src/`) | The view lives only in this session |

## D

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `Column.desc` / `F.desc` | C | `F.col(c).desc()` / `F.desc("count")` | descending sort order | `.orderBy(F.desc("count"), col)` in `inspect_anomalies` | Nulls come **last** in descending order |
| `T.DoubleType` / `LongType` / `StringType` | S | `T.DoubleType()` | schema types | `@F.udf(returnType=T.DoubleType())` | Note the brackets: the type is an instance |
| `DataFrame.drop` | T | `df.drop(*names)` | remove columns | `df.drop(*DROP_COLUMNS)` | Silently ignores names that are not there |
| `DataFrame.dropDuplicates` | T | `df.dropDuplicates(["id"])` | one row per id | `filter_released_and_dedup` | Which duplicate survives is not defined; shuffles |
| `DataFrame.dropna` | T | `df.dropna(subset=[...])`, `df.dropna(thresh=10)` | drop rows with nulls in given columns / with fewer than `thresh` non-nulls | `.dropna(subset=["id", "title"]).dropna(thresh=10)` | `thresh` counts non-null values across **all** columns |

## E

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `Column.eqNullSafe` | C | `a.eqNullSafe(b)` | equality where null == null is true | notebook: `~F.col("roi").eqNullSafe(F.col("roi_from_udf"))` | Plain `==` would say null rows "differ" by giving null |
| `DataFrame.explain` | — | `df.explain()` | prints the physical plan | notebook: `df_cleaned.explain()`, `both.explain()` | Read bottom up; `InMemoryTableScan` means the data came from the cache |
| `F.explode` | C | `F.explode(array_col)` | one row per array element | `F.explode(F.split("director", r"\|"))` in `top_directors` | Rows with a null or empty array disappear |
| `F.expr` | C | `F.expr("sql text")` | a SQL expression as a column | `F.expr("try_cast(budget AS DOUBLE)")` | Errors in the SQL text show up only when the plan is analysed |

## F

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `DataFrame.filter` | T | `df.filter(condition)` or `df.filter("sql")` | keep rows where the condition is true | `df.filter(F.col("status") == "Released")` | Rows where the condition is null are dropped too |
| `F.filter` | C | `F.filter(array_col, lambda x: ...)` | keep array elements matching a condition (higher-order function) | `F.filter("credits.crew", lambda c: c["job"] == "Director")` | The lambda is turned into a Spark expression; ordinary Python code inside it will not work |
| `DataFrame.first` | A | `df.first()` | the first row as a `Row` (or `None`) | `df.agg(...).first()` | "First" is only meaningful after `orderBy` |
| `F.floor` | C | `F.floor(x)` | round down | `F.floor((F.col(column) - lo) / width)` | Returns a long; cast if you need int |

## G

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `SparkSession.builder.getOrCreate` | S | `.getOrCreate()` | create the session or return the running one | `get_spark` | The notebook, tests and `main` each call `get_spark`, but within one process they share one session |
| `DataFrame.groupBy` | T | `df.groupBy(col_or_expr).agg(...)` / `.count()` | groups rows; follow with an aggregate | `df.withColumn("group", group).groupBy("group").agg(...)` | Shuffles; output has `shuffle.partitions` partitions (AQE may merge) |

## I

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `Column.isin` | C | `col.isin(list)` | true if the value is in the list | `F.trim(F.col(col)).isin(PLACEHOLDERS)` | Null is not "in" anything: gives null |
| `Column.isNull` / `isNotNull` | C | `F.col(c).isNull()` | null test | `df.filter(F.col(col).isNotNull())` in `rank` | Never test with `== None` |

## L

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `F.least` | C | `F.least(a, b)` | the smaller value | `F.least(F.floor(...), F.lit(bins - 1))` | Skips nulls; returns null only if all are null |
| `DataFrame.limit` | T | `df.limit(n)` | at most n rows | `.limit(n)` in `rank`; `df.limit(max_rows)` in `md_table` | Without `orderBy` first, *which* n rows is not defined |
| `F.lit` | C | `F.lit(value)` | a constant as a column | `F.lit(0)`, `F.lit(bins - 1)` | Needed where a function wants a column, not a Python value |

## M

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `F.max` / `F.min` | C | `F.max(col)` | largest / smallest value, skipping nulls | `F.min(column).alias("lo")` in `plot_distribution` | Both null if the column is all null; the code checks `if lo is None` |
| `F.median` | C | `F.median(col)` | exact median (Spark 3.4+) | Drill 9 in [13](13-glossary-and-practice.md) (not in `src/`) | Exact, so it sorts; averages the two middle values |
| `DataFrame.write.mode` | A | `df.write.mode("overwrite").parquet(path)` | write mode then format | `df_movies.coalesce(1).write.mode("overwrite").parquet(str(CLEAN_PATH))` | Default mode `"error"` fails if the folder exists |

## O

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `DataFrame.orderBy` | T | `df.orderBy(col1, col2)` | sort | `.orderBy(order, "title")` in `rank` | Global sort shuffles; add a tie-breaker for stable output |
| `Window.orderBy` | S | `Window.orderBy("id")` | window ordered by `id`, no partition | `F.row_number().over(Window.orderBy("id")) - 1` | No `partitionBy` → all rows in one partition (warning in the log) |
| `F.when(...).otherwise(...)` | C | `F.when(cond, a).otherwise(b)` | if / else | `F.when(F.col("belongs_to_collection").isNotNull(), "franchise").otherwise("standalone")` | Without `otherwise`, unmatched rows are null (used on purpose in cleaning) |
| `DataFrameReader.option` | S | `.option("mode", "PERMISSIVE")` | sets a reader option | `.option("columnNameOfCorruptRecord", "_corrupt_record")` | The corrupt column must also be in the schema |
| `Column.over` | C | `F.row_number().over(window)` | applies a window function | `reorder_and_reset` | Window functions need `over` |

## P

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `SparkContext.parallelize` | S | `spark.sparkContext.parallelize(list)` | turns a Python list into an RDD | `tests/helpers.raw_df` | Used only so tests can feed JSON strings to `spark.read.json` |
| `DataFrameWriter.parquet` | A | `.parquet(path)` | writes Parquet (a folder) | `main.run` | On Windows needs winutils + `HADOOP_HOME` |
| `DataFrameReader.parquet` | S | `spark.read.parquet(path)` | reads Parquet with its stored schema | notebook: `spark.read.parquet(CLEAN_PATH).count()` | No schema to give: Parquet carries it |
| `F.percentile_approx` | C | `F.percentile_approx(col, 0.5)` or `(col, [0.25, 0.5, 0.75])` | approximate percentile(s) | `F.round(F.percentile_approx("roi", 0.5), 2).alias("median_roi")` | Returns an actual value, never interpolates: standalone median ROI 7.91 is one film's ROI |
| `DataFrame.printSchema` | — | `df.printSchema()` | prints column names and types as a tree | notebook: `df_raw.printSchema()` | No job; reads the plan only |

## R

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `spark.read.json` | S | `spark.read.schema(S).option(...).json(path_or_rdd)` | reads JSON Lines | `cleaning.load_raw` | Without `.schema(...)` it infers, which costs a pass over the data |
| `F.round` | C | `F.round(col, n)` | round to n decimals | `F.round(F.avg("vote_average"), 2)` | Half-up rounding; display only, the cached data keeps full precision |
| `F.row_number` | C | `F.row_number().over(w)` | 1, 2, 3 ... in window order | `reorder_and_reset` | Starts at 1; the code subtracts 1 |

## S

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `DataFrameReader.schema` | S | `spark.read.schema(RAW_SCHEMA)` | gives the schema, no inference | `cleaning.load_raw` | A field typed wrong in the schema reads as null in PERMISSIVE mode |
| `DataFrame.select` | T | `df.select(col, ...)`, `df.select([list])` | pick or compute columns | `df.select(idx.alias("idx"), *FINAL_COLUMNS)` | Columns not selected are gone |
| `SparkContext.setLogLevel` | S | `spark.sparkContext.setLogLevel("WARN")` | how much Spark logs | `get_spark`, from `SPARK_LOG_LEVEL` | Only affects messages after the call |
| `DataFrame.show` | A | `df.show(n, truncate=...)` | prints rows | notebook cells | Prints, returns `None`; truncates text at 20 characters by default |
| `F.size` | C | `F.size(array_col)` | number of elements | `F.size("credits.cast")` | Null array gives -1 or null depending on settings; the code wraps it in `F.coalesce(..., F.lit(0))` |
| `F.split` | C | `F.split(col, regex)` | string → array | `F.split(F.col(col), r"\|")` | The pattern is a regex: escape `\|` |
| `SparkSession.stop` | S | `spark.stop()` | shuts down Spark and the JVM | `finally: spark.stop()` in `main` | After it, any DataFrame from this session is unusable |
| `F.sum` | C | `F.sum(col)` | total, skipping nulls | `F.round(F.sum("revenue_musd"), 1).alias("total_revenue")` | Summing exploded rows double counts |
| `DataFrame.summary` | A | `df.summary("count", "mean", ...)` | count, mean, min, percentiles, max per column | notebook EDA cell | Percentiles here are approximate too |

## T

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `DataFrame.toPandas` | A | `df.toPandas()` | all rows to the driver as pandas | `.orderBy("year").toPandas()` in `plot_yearly_box_office` | Driver memory risk; reduce first. Uses Arrow when enabled |
| `DataFrame.transform` | T | `df.transform(func, *args)` | calls `func(df, *args)`; keeps chains readable | `.transform(pipe_join_names, "genres", "name")` | Extra arguments need Spark 3.3+ |
| `F.trim` | C | `F.trim(col)` | removes spaces at both ends | `F.trim(F.col(col)).isin(PLACEHOLDERS)` | Why a tagline of `"  "` counts as the placeholder `""` |
| `try_cast` (SQL) | C | `F.expr("try_cast(x AS DOUBLE)")` | cast that gives null on bad input | `convert_dtypes` | Plain `CAST` raises under ANSI |
| `try_to_timestamp` (SQL) | C | `F.expr("CAST(try_to_timestamp(d, 'yyyy-MM-dd') AS DATE)")` | parse a date, null if it does not match | `convert_dtypes` | Pattern letters are case-sensitive: `MM` month, `mm` minutes |

## U

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `F.udf` | C | `@F.udf(returnType=T.DoubleType())` | wraps a Python function as a column function | `kpis.roi_udf` | Runs row by row in Python; not used in the pipeline |
| `SparkContext.uiWebUrl` | — | `spark.sparkContext.uiWebUrl` | address of the Spark UI | logged by `get_spark` | Port 4040, or 4041+ if another Spark is running |
| `DataFrame.unpersist` | — | `df.unpersist()` | drop it from the cache | `df_raw.unpersist()`, `found.unpersist()` | Immediate, not lazy |

## W, Y

| Name | Kind | Form used | What it does | Line from this project | Gotcha |
|---|---|---|---|---|---|
| `DataFrame.withColumn` | T | `df.withColumn(name, expr)` | add or replace one column | `.withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))` | Many calls in a loop make a long plan; fine at this size |
| `F.year` | C | `F.year(date_col)` | year as an integer | `df.groupBy(F.year("release_date").alias("year"))` | Null date → null year, which forms its own group |

## pandas → PySpark for what this lab does

| Task | pandas (DEM02 version) | PySpark (this lab) |
|---|---|---|
| Read JSON Lines | `pd.read_json(path, lines=True)` | `spark.read.schema(RAW_SCHEMA).json(path)` |
| Drop columns | `df.drop(columns=[...])` | `df.drop(*cols)` |
| New column | `df["roi"] = df.revenue / df.budget` | `df.withColumn("roi", F.col("revenue_musd") / F.col("budget_musd"))` |
| Coerce to number | `pd.to_numeric(s, errors="coerce")` | `F.expr("try_cast(x AS DOUBLE)")` |
| Parse dates | `pd.to_datetime(s, errors="coerce")` | `F.expr("CAST(try_to_timestamp(d, 'yyyy-MM-dd') AS DATE)")` |
| Replace with NaN | `s.replace(0, np.nan)` | `F.when(F.col(c) > 0, F.col(c))` |
| Join list of dicts | `s.apply(lambda l: "\|".join(d["name"] for d in l))` | `F.concat_ws("\|", F.col("genres.name"))` |
| value_counts | `s.str.split("\|").explode().value_counts()` | `explode(split(...))` + `groupBy().count().orderBy(desc)` |
| Drop duplicates | `df.drop_duplicates("id")` | `df.dropDuplicates(["id"])` |
| Drop NA | `df.dropna(subset=[...])`, `df.dropna(thresh=10)` | same names: `df.dropna(subset=[...])`, `df.dropna(thresh=10)` |
| Filter rows | `df[df.status == "Released"]` | `df.filter(F.col("status") == "Released")` |
| Reorder columns | `df[cols]` | `df.select(*cols)` |
| Reset index | `df.reset_index(drop=True)` | `F.row_number().over(Window.orderBy("id")) - 1` |
| Sort, top n | `df.nlargest(5, "roi")` | `df.orderBy(F.desc("roi")).limit(5)` |
| Group and aggregate | `df.groupby("g").agg(...)` | `df.groupBy("g").agg(...)` |
| Median | `s.median()` (exact) | `F.percentile_approx(c, 0.5)` (approximate) |
| Quantiles | `s.quantile([.25, .5, .75])` (interpolated) | `F.percentile_approx(c, [0.25, 0.5, 0.75])` (real values) |
| Histogram | `ax.hist(s, bins=20)` | bucket with `F.floor`, `groupBy().count()`, then `ax.bar` |
| Boxplot | `ax.boxplot(s)` | quartiles in Spark, then `ax.bxp([stats])` |
| Contains whole item | `"X" in s.split("\|")` | `F.array_contains(F.split(c, r"\|"), "X")` |
| Save | `df.to_parquet(path)` | `df.write.mode("overwrite").parquet(path)` |
| Look at rows | `df.head()` | `df.show(5)` / `df.first()` |
| Describe | `df.describe()` | `df.summary()` |

Next: [15 · Review-day runbook](15-review-day-runbook.md)
