# 05 — DataFrame Fundamentals

The PySpark building blocks this lab uses, taught from zero with small
examples you can run. Every function here appears in `src/cleaning.py`,
`src/kpis.py`, `src/visualize.py` or `src/main.py`.

> **Help:** why nothing runs until an action, and what a partition is, are
> in [02 · What Spark is](02-what-is-spark.md). The A–Z list of every
> function is in [14 · PySpark function reference](14-pyspark-function-reference.md).

All examples assume this start (it takes about 20 seconds), run from the
project root:

```python
import json
from pyspark.sql import functions as F
from pyspark.sql import types as T
from src.spark_session import get_spark
spark = get_spark()
```

`F` and `T` are the standard short names: `F` for
`pyspark.sql.functions` (column functions) and `T` for
`pyspark.sql.types` (data types).

## 1. SparkSession

The **SparkSession** is the entry point: it starts Spark and creates every
DataFrame. You make one per program. `get_spark()` builds it with
`.getOrCreate()`, which returns the existing session if one is already
running. The tests share one through a session-scoped fixture in
`tests/conftest.py`:

```python
@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    # one spark for the whole test run. starting the jvm takes a few seconds,
    # doing it per test would make the suite crawl.
    s = get_spark("tmdb-tests")
    yield s
    s.stop()
```

`spark.stop()` shuts Spark down and frees the JVM.

## 2. Reading JSON

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

`spark.read` returns a **reader**. Each method sets one thing, then
`.json(path)` reads. It expects JSON Lines (one object per line).

| Call | Meaning |
|---|---|
| `.schema(RAW_SCHEMA)` | Use these columns and types; do not guess |
| `.option("mode", "PERMISSIVE")` | A line that is not valid JSON does not fail the read |
| `.option("columnNameOfCorruptRecord", "_corrupt_record")` | ...instead, its raw text goes into this column, and the other columns are null |
| `.json(str(path))` | Read the file (Spark wants a string, not a `Path`) |

The other modes are `DROPMALFORMED` (silently drop bad lines) and
`FAILFAST` (stop on the first bad line). PERMISSIVE was chosen so bad
lines can be **counted and reported**, then dropped; see
[06](06-cleaning-line-by-line.md#h-3-drop-corrupt-rows-and-counting-bad-lines).

The tests read fake records the same way, from JSON text, with the same
schema (`tests/helpers.py`):

```python
def raw_df(spark: SparkSession, records: list[Movie], extra_lines: Sequence[str] = ()) -> DataFrame:
    """Read fake records the same way main.py reads the file: json text + my schema."""
    lines = [json.dumps(r) for r in records] + list(extra_lines)
    rdd = spark.sparkContext.parallelize(lines)
    return (spark.read.schema(cleaning.RAW_SCHEMA)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(rdd))
```

An **RDD** (Resilient Distributed Dataset) is Spark's older, lower-level
collection type. Here it is just a way to hand Spark a list of JSON strings
instead of a file. `extra_lines` lets a test add a broken line.

## 3. Schemas: inference versus an explicit schema

A **schema** is the list of columns with their types. Spark types you will
meet:

| Spark type (`T.`) | Python equivalent | Example column |
|---|---|---|
| `StringType` | `str` | `title` |
| `LongType` (= `bigint`) / `IntegerType` | `int` | `id`, `vote_count` / `cast_size` |
| `DoubleType` | `float` | `popularity`, `budget_musd` |
| `DateType` | `datetime.date` | `release_date` after cleaning |
| `StructType([StructField(...), ...])` | a dict with fixed keys | `belongs_to_collection` |
| `ArrayType(element_type)` | a list | `genres` |

### Two ways to get a schema

**Inference**: leave out `.schema(...)` and Spark reads the data once,
looks at every value, and guesses a type for each key. An earlier version
of this lab did that. It has three problems, all listed in the comment
above `RAW_SCHEMA`:

```python
# i write the schema out instead of letting spark guess it.
# - guessing (inferSchema) reads the whole file once just to work out types.
#   fine for 18 movies, slow for 18 million.
# - a guess can change from one file to the next. a file where every cast
#   list is empty gets guessed as array<string> and then cast.name breaks.
# - fields i do not list are just skipped, so new api fields can not break me.
```

The second problem really happened while writing tests. Every fake record
had `"cast": []`, so there was no element to look at and Spark guessed
`array<string>`. Then `F.col("credits.cast.name")` failed, because a string
has no `name` field. Spark read `cast[name]` as "index the array with
`name`", and array indexes must be integers:

```
AnalysisException [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "cast[name]" ...
The second parameter requires the "INTEGRAL" type, however "name" has the type "STRING".
```

**An explicit schema** (schema on read) fixes all three: no extra pass, the
same types every run, and unknown fields ignored. `test_empty_lists_become_null`
now passes with a single movie whose lists are all empty: "the fixed schema
means one movie on its own is enough now."

### Building a schema in code

```python
def _named(*extra: str) -> T.ArrayType:
    # array of {id, name, ...} structs, the shape tmdb uses for genres,
    # companies and friends
    fields = [T.StructField("id", T.LongType()), T.StructField("name", T.StringType())]
    fields += [T.StructField(f, T.StringType()) for f in extra]
    return T.ArrayType(T.StructType(fields))


RAW_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("title", T.StringType()),
    ...
    T.StructField("budget", T.StringType()),
    ...
    T.StructField("belongs_to_collection", T.StructType([
        T.StructField("id", T.LongType()), T.StructField("name", T.StringType())])),
    T.StructField("genres", _named()),
    T.StructField("production_companies", _named("origin_country")),
    ...
    T.StructField("credits", T.StructType([
        T.StructField("cast", T.ArrayType(T.StructType([
            T.StructField("name", T.StringType()),
            T.StructField("character", T.StringType())]))),
        T.StructField("crew", T.ArrayType(T.StructType([
            T.StructField("name", T.StringType()),
            T.StructField("job", T.StringType())]))),
    ])),
    # a line that is not valid json lands here instead of failing the read
    T.StructField("_corrupt_record", T.StringType()),
])
```

- `StructType([...])` is a list of `StructField(name, type)`.
- A field can itself be a struct or an array: `credits` is a struct holding
  two arrays of structs.
- `_named()` is a small Python function that *returns a type*, so the
  `{id, name}` shape is written once and reused for genres and companies.
- **Plain fields are read as strings on purpose**, even `budget` and
  `vote_count`. The comment: "this is the raw layer, and convert_dtypes
  does the real typing with try_cast so one bad value turns into a null
  instead of killing the job." A budget of `"lots"` in the file is then
  just a string, not a read error.
- Only 2 of the cast struct's many fields are listed (`name`, `character`),
  and only `name` and `job` for crew. Spark skips the rest of each record.

The schema has 26 fields: 25 data fields plus `_corrupt_record`.

### printSchema()

`df.printSchema()` prints the schema as a tree. For the raw table, trimmed:

```
root
 |-- id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- budget: string (nullable = true)
 |-- belongs_to_collection: struct (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- name: string (nullable = true)
 |-- genres: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- name: string (nullable = true)
 |-- credits: struct (nullable = true)
 |    |-- cast: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- character: string (nullable = true)
 |    |-- crew: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- job: string (nullable = true)
 |-- _corrupt_record: string (nullable = true)
 ...
```

How to read it: **struct** = one nested object; **array** of **struct** = a
list of objects with the same fields; `nullable = true` = the column may
hold nulls.

After cleaning, the table is flat: 25 columns, all simple types (`idx`,
`id`, `release_date: date`, `genres: string`, `budget_musd: double`, ...).

## 4. Columns and expressions

A **Column** is not data. It is an **expression**: a recipe saying how to
compute a value for each row. `F.col("budget")` means "the budget column".
`F.col("budget") / 1_000_000` means "budget divided by a million". Nothing
is computed until the Column is used inside a DataFrame method and an
action runs.

| You write | Meaning |
|---|---|
| `F.col("title")` | The column named `title` |
| `F.lit(0)` | A constant value 0 on every row |
| `F.col("a") + F.col("b")`, `-`, `*`, `/` | Arithmetic, row by row |
| `F.col("a") > 0`, `==`, `!=`, `<=` | Comparisons, giving a boolean Column |
| `cond1 & cond2`, `cond1 \| cond2`, `~cond` | AND, OR, NOT. Use `&`, `\|`, `~`, never `and`/`or`/`not` |
| `.alias("name")` | Name the result column |
| `F.expr("try_cast(id AS BIGINT)")` | Write the expression in Spark SQL text |
| `F.coalesce(a, b)` | The first non-null of `a`, `b` |

Wrap each condition in brackets when combining:
`(F.col("vote_average") < 0) | (F.col("vote_average") > 10)` (from
`validate`). Python's `|` binds tighter than `<`.

Because a Column is just a value, a Python function can **return** one and
another function can use it. `kpis.has()` returns a Column; `rank()` takes
one as its `where` argument.

## 5. select, withColumn, drop, filter

```python
df = spark.createDataFrame(
    [(597, "Titanic", 200_000_000), (19995, "Avatar", 237_000_000)],
    ["id", "title", "budget"])

df.select("title", (F.col("budget") / 1_000_000).alias("budget_musd")).show()
df.withColumn("budget_musd", F.col("budget") / 1_000_000)   # add or replace one column
df.drop("budget")                                           # remove; unknown names are ignored
df.filter(F.col("budget") > 210_000_000)                    # keep matching rows (Avatar)
```

| Method | Does | Lab example |
|---|---|---|
| `select(...)` | Choose and compute columns; the result has **only** these | `reorder_and_reset`: `df.select(idx.alias("idx"), *FINAL_COLUMNS)` |
| `withColumn(name, col)` | Add a column, or **replace** it if the name exists | `pipe_join_names` replaces `genres` with a string |
| `drop(*names)` | Remove columns | `drop_irrelevant_columns`; `.drop("credits")` |
| `filter(cond)` (alias `where`) | Keep rows where `cond` is true | `.filter(F.col("status") == "Released")` |

Each returns a **new** DataFrame. `df.withColumn(...)` on its own line does
nothing to `df`; you must keep the result. `clean_unrealistic_values` shows
the loop form:

```python
for col in ["budget", "revenue", "runtime"]:
    df = df.withColumn(col, F.when(F.col(col) > 0, F.col(col)))
```

## 6. when / otherwise

`F.when(cond, value)` is Spark's if/else, row by row:

```python
F.when(F.col("belongs_to_collection").isNotNull(), "franchise").otherwise("standalone")
```

**No `otherwise` means null.** If no `when` matches and there is no
`otherwise`, the result is null. The lab uses this on purpose as a short
way to say "keep the value if it is valid, else null":

```python
df.withColumn(col, F.when(F.col(col) > 0, F.col(col)))
```

On three rows, `x = 5.0`, `null`, `0.0`:

| x | `x > 1` | `F.when(F.col("x") > 1, "big")` |
|---|---|---|
| 5.0 | true | big |
| NULL | NULL | NULL |
| 0.0 | false | NULL |

## 7. Null semantics

**Null** means "unknown / missing". Spark follows SQL rules, which differ
from Python's `None` habits:

1. **Any comparison with null gives null**, not true or false. `null > 1`
   is null. Even `F.col("x") == None` gives null on *every* row, so it
   never matches anything.
2. **`filter` keeps only rows where the condition is true.** Null counts as
   not true, so `filter(F.col("budget_musd") >= 10)` silently drops rows
   with a null budget. That is how the ROI threshold works, and it should
   be a choice you know about.
3. **Arithmetic with null gives null.** `revenue_musd - null` is null.
   That is why a movie with no budget gets `profit_musd = null`, never a
   fake profit.
4. **Aggregates skip nulls.** `F.avg("vote_average")` averages only the
   non-null values. `F.count("col")` counts non-nulls; `F.count("*")`
   counts rows.

Test for null with methods, never with `==`:

```python
F.col("_corrupt_record").isNull()
F.col("belongs_to_collection").isNotNull()
```

`F.coalesce(x, F.lit(0))` replaces a null with 0. `extract_credits` uses it
so `cast_size` is 0, not null, when the credits are missing.

### Dropping rows with nulls: `dropna`

```python
df.dropna(subset=["id", "title"])   # drop a row if id OR title is null
df.dropna(thresh=10)                # keep a row only if it has >= 10 non-null values
df.dropna()                         # drop a row if ANY column is null
```

`thresh` is exactly spec Step 2.8. An empty list `[]` is not null, so it
counts as a value.

## 8. Struct fields: `col("a.b")`

A dot reaches inside a struct:

```python
F.col("belongs_to_collection.name")     # "The Avengers Collection", or null
F.col("credits.cast")                   # the cast array inside the credits struct
```

If the struct itself is null (Titanic has no collection), the field is
null too. No error.

**On an array of structs, the dot takes that field from every element**
and gives an array:

```python
recs = [{"id": 1, "genres": [{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}]},
        {"id": 2, "genres": []}]
schema = "id long, genres array<struct<id:long,name:string>>"
df = spark.read.schema(schema).json(spark.sparkContext.parallelize([json.dumps(r) for r in recs]))
df.select("id", F.col("genres.name").alias("names")).show()
# id=1 -> [Action, Adventure]     id=2 -> []
```

(A schema can also be written as a DDL string like this one, short for the
`T.StructType` form.) This is the trick behind `pipe_join_names` and
`F.col("credits.cast.name")` in `cleaning.py`.

## 9. Array functions

Results below are from running them on a 3-row test frame: row 1 has
genres `[Action, Adventure]`, row 2 has `[]`, row 3 has `null`:

| Function | Does | Row 1 | Row 2 `[]` | Row 3 null |
|---|---|---|---|---|
| `F.size("genres")` | Number of elements | 2 | 0 | NULL |
| `F.concat_ws("\|", col)` | Join elements into one string | `Action\|Adventure` | `""` | `""` |
| `F.array_contains(col, "Action")` | Is this exact value in the array? | true | false | NULL |
| `F.explode(col)` | One output row per element | 2 rows | **0 rows** | **0 rows** |

Consequences used in the lab:

- `concat_ws` gives `""` (empty string) for an empty or null array, never
  null. So `pipe_join_names` adds `F.when(joined != "", joined)` to turn
  `""` into a real null.
- `size` of a null array is null, so `extract_credits` wraps it:
  `F.coalesce(F.size("credits.cast"), F.lit(0))`.
- `explode` **drops** rows whose array is empty or null. `top_directors`
  filters out null directors first, so nothing is lost silently.
  (`F.explode_outer` would keep a null row instead.)

### `split` and the regex trap

`F.split(col, pattern)` turns a string into an array. The pattern is a
**regular expression** (regex), a mini-language where `|` means "or". So:

```python
F.split("a|b", "|")      # [a, |, b, ]   wrong: "|" alone matches the empty string
F.split("a|b", r"\|")    # [a, b]        right: "\|" means a literal pipe
```

Every split in the lab uses `r"\|"`. The `r` makes it a **raw string**, so
Python passes the backslash to Spark unchanged.

`split` + `array_contains` together give a whole-item search on a
"|"-joined string, which is what `kpis.has()` does:

```python
F.array_contains(F.split(F.col(col), r"\|"), value)
```

### Higher-order `F.filter`

`F.filter(array_col, function)` keeps the array elements for which the
function returns true. Do not confuse it with `DataFrame.filter`, which
keeps **rows**.

```python
F.filter("credits.crew", lambda c: c["job"] == "Director")["name"]   # the directors' names
```

It is called **higher-order** because it takes a function as an argument.
The lambda is turned into a Spark expression and runs inside the JVM, so
it is as fast as any built-in.

## 10. `DataFrame.transform`: chaining your own functions

`df.transform(fn, *args)` calls `fn(df, *args)` and returns its result. It
lets your own functions sit in a method chain, with extra arguments if
they need them:

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

`.transform(pipe_join_names, "genres", "name")` calls
`pipe_join_names(df, "genres", "name")`. Without `transform` the chain
would read inside out, the innermost call first. With it, the steps read
top to bottom in spec order, and each function can be tested on its own.

## 11. Aggregating the whole table at once

`df.agg(...)` with no `groupBy` computes over all rows and returns one
row. `cleaning.validate` uses it to run six checks in **one pass**:

```python
checks = df.agg(
    F.count("*").alias("rows"),
    F.count_distinct("id").alias("distinct_ids"),
    F.count_if(F.col("title").isNull()).alias("null_titles"),
    ...
).first()
```

`F.count_if(cond)` counts rows where `cond` is true. Grouped aggregation
(`groupBy().agg()`) is the subject of
[07 · Aggregations, windows, sorting](07-aggregations-windows-sorting.md).

## 12. Looking at results

| Call | Gives | Action? |
|---|---|---|
| `df.show(n=20, truncate=True)` | Prints a text table | yes |
| `df.count()` | Number of rows | yes |
| `df.first()` | One `Row`; read fields as `row.title` or `row["count"]` | yes |
| `df.collect()` | A Python list of all rows (careful on big data) | yes |
| `df.toPandas()` | A pandas DataFrame (careful on big data) | yes |
| `row.asDict()` | The `Row` as a plain dict (`validate` returns this) | — |
| `df.columns` | List of column names | no (schema only) |
| `df.printSchema()` | Prints the schema | no (schema only) |

`r["count"]` is needed in `test_inspect_anomalies_splits_cells` because
`r.count` would be the `Row`'s built-in `count` method, not the column.

## Self-check

1. Name two reasons the lab uses `RAW_SCHEMA` instead of inference. [No extra pass over the file; stable types (empty cast lists no longer break); unknown fields skipped.]
2. What happens to a line that is not valid JSON under `mode=PERMISSIVE`? [Its text goes into `_corrupt_record` and the other columns are null.]
3. What is the result of `F.when(F.col("x") > 0, F.col("x"))` for `x = 0`? [Null: no match and no `otherwise`.]
4. Why `r"\|"` and not `"|"` in `split`? [The pattern is a regex; a bare `|` matches the empty string.]
5. What does `.transform(pipe_join_names, "genres", "name")` call? [`pipe_join_names(df, "genres", "name")`.]
