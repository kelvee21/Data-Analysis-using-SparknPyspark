# 07 — Aggregations, Windows and Sorting

How Spark turns 18 rows into the summary tables of Step 3: grouping and
aggregate functions, medians, sorting with tie-breakers, window functions,
and the explode-then-group pattern with its double-counting trap. Every
example uses this lab's code and the numbers in `reports/results.md`.

> **Help:** Column expressions, `when` and null rules are in
> [05 · DataFrame fundamentals](05-dataframe-fundamentals.md). Why a
> `groupBy` causes a shuffle is in [02 · What Spark is](02-what-is-spark.md#h-8-jobs-stages-tasks-shuffles-and-reading-a-plan).
> The functions that use all this are walked through in
> [08 · KPIs line by line](08-kpis-line-by-line.md).

## 1. Two kinds of aggregation

An **aggregation** turns many rows into one value: a count, a sum, an
average. Spark has two forms:

| Form | Result | Lab example |
|---|---|---|
| `df.agg(...)` | One row for the whole table | `cleaning.validate`: 6 checks in one row |
| `df.groupBy(key).agg(...)` | One row per distinct key value | `kpis.top_franchises`: one row per franchise |

```python
# whole table: one row
df_movies.agg(F.count("*").alias("rows"), F.count_distinct("id").alias("distinct_ids"))

# per group: one row per franchise
df_movies.groupBy("belongs_to_collection").agg(F.count("*").alias("movies"))
```

Think of `groupBy` as "sort the rows into buckets by key", and `agg` as
"reduce each bucket to one row". Every column in the result is either a
grouping key or an aggregate. A plain column like `title` cannot appear,
because a bucket has many titles and Spark would not know which one to
show.

`groupBy(...).count()` is a shortcut for `.agg(F.count("*").alias("count"))`.
`cleaning.inspect_anomalies` uses it.

## 2. The aggregate functions used

| Function | Does | Nulls | Lab use |
|---|---|---|---|
| `F.count("*")` | Number of rows | counted | `movies` in every summary |
| `F.count("col")` | Number of non-null values | skipped | — |
| `F.count_distinct("id")` | Number of different values | skipped | `validate`: duplicates check |
| `F.count_if(cond)` | Rows where `cond` is true | null cond not counted | `validate`; null counts in `main.py` |
| `F.sum("col")` | Total | skipped | `total_revenue`, `total_budget` |
| `F.avg("col")` | Mean (average) | skipped | `mean_revenue`, `mean_rating` |
| `F.min`, `F.max` | Smallest, largest | skipped | histogram range in `visualize.plot_distribution` |
| `F.percentile_approx("col", p)` | Approximate percentile | skipped | `median_roi`; boxplot quartiles |
| `F.round(x, d)` | Round to `d` decimals (not an aggregate; wraps one) | — | Every summary column |

"Nulls skipped" matters. In `franchise_vs_standalone`, `F.avg("vote_average")`
averages only films with a rating. If a film had no budget, it would be
left out of `mean_budget` instead of counting as 0. That is the honest
choice, and the reason zeros were turned into nulls during cleaning.

### A worked example: franchise vs standalone

`kpis.franchise_vs_standalone` groups by a computed label:

```python
group = F.when(F.col("belongs_to_collection").isNotNull(), "franchise").otherwise("standalone")
...
df.withColumn("group", group)
  .groupBy("group")
  .agg(
      F.count("*").alias("movies"),
      F.round(F.avg("revenue_musd"), 1).alias("mean_revenue"),
      F.round(F.percentile_approx("roi", 0.5), 2).alias("median_roi"),
      ...
  )
```

Result in `results.md`:

| group | movies | mean_revenue | median_roi | mean_budget | mean_popularity | mean_rating |
|---|---|---|---|---|---|---|
| franchise | 16 | 1,691.70 | 7.71 | 218.00 | 27.06 | 7.40 |
| standalone | 2 | 1,765.10 | 7.91 | 180.00 | 26.53 | 7.44 |

Check the standalone row by hand from the cleaned-movies table. The two
standalone films are Titanic (revenue 2,264.16, budget 200) and Beauty and
the Beast (1,266.12, budget 160):

- mean revenue = (2,264.16 + 1,266.12) / 2 = **1,765.14** → rounded to 1 decimal, `1,765.10` as printed
- mean budget = (200 + 160) / 2 = **180.00**

Doing one group by hand like this is the best way to trust an aggregate.

### `F.round` and display

`F.round(x, 1)` rounds inside Spark. `main.md_table` then prints every
float with two decimals (`f"{v:,.2f}"`), which is why a value rounded to
one decimal shows as `1,765.10`. The second decimal is always 0 in those
columns.

## 3. Medians and `percentile_approx`

The **median** is the middle value when the values are sorted. Half are
below it, half above. It is used for ROI because, as the code comment
says, "one huge film moves the mean and not the median."

`F.percentile_approx(col, 0.5)` returns an approximate median. Why not an
exact one? An exact median needs all values of the group **sorted
together** in one place. On a cluster with a billion rows, that is a huge
shuffle. `percentile_approx` keeps a small summary of each partition
(a "sketch") and merges the summaries, so its memory use stays small no
matter how many rows there are. The comment in `kpis.py`: "percentile_approx
because an exact median needs a full sort."

There is a catch with tiny groups: `percentile_approx` always returns a
value **that exists in the data**. It does not average the two middle
values the way a textbook median does. Look at the standalone group:

| Film | ROI |
|---|---|
| Titanic | 11.32 (from the "top roi" table) |
| Beauty and the Beast | 1,266.12 / 160 = 7.91 |

- `percentile_approx` gave **7.91**: the lower of the two real values.
- A textbook median would be (11.32 + 7.91) / 2 = **9.62**.

With 2 values, "the median" is mostly a matter of definition, and 2 films
are far too few to conclude anything anyway. With thousands of rows the
two definitions agree closely. Say this in a review if asked why the
standalone median ROI looks low.

The same function takes a **list** of percentiles. `visualize.plot_boxplot`
gets all three quartiles in one pass:

```python
quartiles = df.agg(F.percentile_approx(column, [0.25, 0.5, 0.75]).alias("q")).first()
assert quartiles is not None
q1, med, q3 = quartiles["q"]
```

## 4. Sorting: `orderBy`, direction, tie-breakers, nulls

`df.orderBy(...)` sorts the whole DataFrame. It causes a shuffle, because
rows from every partition have to be compared.

```python
df.orderBy("runtime")                       # ascending (smallest first)
df.orderBy(F.desc("vote_average"))          # descending
df.orderBy(F.col("roi").asc())              # same as ascending
df.orderBy(F.desc("total_revenue"), F.desc("mean_rating"))   # two keys
```

### Tie-breakers

With two keys, the second only matters when the first is equal. That is a
**tie-breaker**. `kpis.rank` uses `title`:

```python
order = F.col(col).asc() if ascending else F.col(col).desc()
...
.orderBy(order, "title")
```

Real ties in `results.md`:

| List | Tie | Order shown | Why |
|---|---|---|---|
| top budget | Avengers: Infinity War and Star Wars: The Last Jedi, both 300.00 | Infinity War first | Exact tie; title "A..." sorts before "S..." |
| top rated | Avengers: Endgame and Avengers: Infinity War, both shown as 8.25 | Endgame first | The table shows ratings rounded to 2 decimals. Endgame's raw rating is 8.247 ([04](04-tmdb-api-and-fetch.md#h-9-what-one-raw-record-looks-like)); whether the unrounded values tie or not, Endgame comes first (higher value, or "E" before "I" in the title) |
| top rated | The Avengers and Harry Potter ... Part 2, both shown as 8.08 | The Avengers first | An exact tie would put "Harry..." before "The...". So The Avengers' unrounded rating must be higher: rounding hides the difference |

Without a tie-breaker, Spark gives **no guarantee** about the order of tied
rows. It can differ between runs or between machines, depending on which
partition finished first. Then a "top 5" with a tie at place 5 could show a
different film each run. The docstring of `rank`: "title breaks ties so the
same input always gives the same list."

The same rule applies to the group tables. `top_directors` sorts by
`total_revenue`, then `mean_rating`. Joe Russo and Anthony Russo tie on
both (4,994.60 and 8.25; they co-directed the same two films), as do
Jennifer Lee and Chris Buck (2,727.90 and 7.23). With only those two keys,
their order within each pair was not guaranteed. The code now adds the name
as a last key, `.orderBy(F.desc("total_revenue"), F.desc("mean_rating"), "director")`,
and `top_franchises` ends with `"franchise"` in the same way. See
[08](08-kpis-line-by-line.md#h-9-top-directors).

### Nulls in a sort

Spark's default puts nulls **first** in ascending order and **last** in
descending order. So a "lowest ROI" list on data with a null ROI would
start with the null. That is why `rank` removes nulls before sorting:

```python
df.filter(F.col(col).isNotNull())
```

Its docstring: "otherwise a film with no budget would sit at the top of
'lowest roi'." `test_rank_skips_nulls_and_uses_threshold` checks that the
film with `budget=0` (so ROI null) is not in the ascending list.

## 5. `limit(n)`

`df.limit(n)` keeps the first `n` rows. After an `orderBy` that means the
top `n`. Spark combines the two into one efficient step: in the plan,
`orderBy` + `limit` becomes `TakeOrderedAndProject(limit=10, ...)`, which
keeps only the best 10 per partition instead of fully sorting everything.

`limit` without `orderBy` gives *some* `n` rows, not a meaningful top `n`.

In `rankings`, every list is `n=5`; `top_franchises` and `top_directors`
default to `n=10`. `main.md_table` also applies `limit(max_rows)` before
`collect()`, so a table never pulls more than 50 rows into Python.

## 6. Window functions

An aggregate collapses a group into **one** row. A **window function**
computes a value per row while looking at a group of rows (the *window*)
around it. The rows stay; a new column is added.

```python
from pyspark.sql.window import Window

w = Window.orderBy("id")                       # the window: all rows, sorted by id
F.row_number().over(w)                         # 1, 2, 3, ... in that order
```

The lab's one window is the index in `cleaning.reorder_and_reset`:

```python
idx = F.row_number().over(Window.orderBy("id")) - 1
return df.select(idx.alias("idx"), *FINAL_COLUMNS)
```

| Part | Meaning |
|---|---|
| `Window.orderBy("id")` | One window holding all rows, sorted by `id` |
| `.partitionBy(...)` (not used here) | Split into separate windows per key, each numbered from 1 |
| `F.row_number()` | 1, 2, 3... with no gaps, even for ties |
| `- 1` | 0-based, like a pandas index |

Result: Titanic (id 597) is `idx 0`, The Lion King (id 420818) is `idx 17`.

### The cost of a window with no partition

To number rows 1 to 18 in one sequence, Spark must see all of them in one
place. So a window with no `partitionBy` moves **every row to a single
partition** (`Exchange SinglePartition` in the plan) and Spark logs a
warning. With 18 rows that costs nothing. With a billion rows, one
executor would do all the work while the others wait. The README calls
this out as "one thing i left on purpose".

### Ranking within groups (for comparison)

With `partitionBy`, each group gets its own numbering. This is not in the
lab code, but it shows the real use of windows: ranking films **inside**
each franchise.

```python
w = Window.partitionBy("belongs_to_collection").orderBy(F.desc("revenue_musd"))
df_movies.filter(F.col("belongs_to_collection").isNotNull()) \
    .select("belongs_to_collection", "title", "revenue_musd",
            F.row_number().over(w).alias("rank_in_franchise"))
```

For The Avengers Collection, using the revenues in `results.md`:

| title | revenue_musd | rank_in_franchise |
|---|---|---|
| Avengers: Endgame | 2,942.19 | 1 |
| Avengers: Infinity War | 2,052.42 | 2 |
| The Avengers | 1,518.82 | 3 |
| Avengers: Age of Ultron | 1,405.40 | 4 |

The other franchises would each start again at 1. Three numbering
functions differ only on ties:

| Values | `row_number` | `rank` | `dense_rank` |
|---|---|---|---|
| 300, 300, 260 | 1, 2, 3 | 1, 1, 3 | 1, 1, 2 |

## 7. Grouping by a computed key

The key of a `groupBy` does not have to be an existing column. Any Column
expression works:

```python
# visualize.plot_yearly_box_office: group by the year of a date
df.groupBy(F.year("release_date").alias("year")).agg(F.avg("revenue_musd").alias("mean_revenue"))

# kpis.franchise_vs_standalone: group by a when/otherwise label (§2)

# visualize.plot_distribution: group by histogram bucket number
bucket = F.least(F.floor((F.col(column) - lo) / width), F.lit(bins - 1)).cast("int")
df.filter(F.col(column).isNotNull()).groupBy(bucket.alias("b")).count()
```

The year example on this data: 2015 has 4 films (Jurassic World, Furious 7,
Avengers: Age of Ultron, Star Wars: The Force Awakens), so its bar in chart
4 is an average of 4; 1997, 2009, 2011, 2012 and 2013 have a single film
each.

The histogram example is the "Spark counts, Python only draws" pattern:
each value is turned into a bucket number in Spark, the buckets are
counted with `groupBy`, and only `bins` (20) small numbers come back to
Python, however big the table. Charts are covered in
[09 · Charts with Matplotlib](09-visualisation.md).

## 8. Explode, then group, and the double-counting trap

Multi-valued cells like `genres = "Science Fiction|Action|Adventure"`
cannot be grouped directly. The pattern is: **split** into an array,
**explode** into one row per item, then **group**:

```python
# kpis.top_directors
df.filter(F.col("director").isNotNull())
  .withColumn("director", F.explode(F.split("director", r"\|")))
  .groupBy("director")
  .agg(F.count("*").alias("movies"),
       F.round(F.sum("revenue_musd"), 1).alias("total_revenue"),
       F.round(F.avg("vote_average"), 2).alias("mean_rating"))
  .orderBy(F.desc("total_revenue"), F.desc("mean_rating"), "director")
```

Before the explode, Avengers: Endgame is **one** row with
`director = "Joe Russo|Anthony Russo"`. After it, there are **two** rows,
one per director, and **each carries the full 2,942.19 revenue**.

That is intended here: the code comment says "explode gives a co-directed
film one row per director so both get credit", and
`test_codirectors_both_get_the_revenue` checks that directors A and B each
get 200.0 from one 200 M film. But it has a consequence you must know:

**Totals over exploded rows do not add up to the real total.**

| | Revenue (million USD) |
|---|---|
| Real total of all 18 films (sum of the cleaned-movies table) | 30,597.45 |
| Sum of the top-10 director rows alone | 30,474.20 |

The top 10 directors alone almost reach the total of all 18 films. The
full director table (16 different directors) adds up to 38,319.96, which
is the real total plus Endgame, Infinity War, Frozen and Frozen II a
second time, because each of those four has two directors. The same holds
for genres: 57 genre rows for 18 films (from the value counts), so the
per-genre revenues behind chart 2 add up to about three times the real
total.

Rules to follow:

| Question | Safe? |
|---|---|
| "Which director has the most revenue from films they directed?" | Yes: each director's own sum is correct |
| "What share of all revenue did each director bring in?" | No: shares would add up to more than 100% |
| "Average ROI per genre" (chart 2) | Yes, per genre, if you show how many films each bar has (the chart labels `n=`) |
| "Total revenue = sum of the genre totals" | No: double counted |

If you ever need shares, either split the value (each of 2 directors gets
half) or count each film once with `F.count_distinct("id")` style logic.

## Self-check

1. What can appear in the result of `groupBy("director").agg(...)`? [The key `director` and aggregate columns only.]
2. Why does `percentile_approx` give 7.91 for the standalone median ROI and not 9.62? [It returns a value that exists in the data (the lower middle one); it does not average the two middle values.]
3. Why does `rank` filter out nulls before sorting? [Ascending order puts nulls first, so a null ROI would top "low roi".]
4. What does a window with no `partitionBy` cost on big data? [Every row moves to one partition, so one task does all the work.]
5. Endgame has two directors. How much revenue does each get in `top_directors`, and what does that do to totals? [Each gets the full 2,942.19; summing director totals double counts the film.]
