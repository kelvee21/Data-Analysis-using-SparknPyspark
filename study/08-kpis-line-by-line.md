# 08 — KPIs, Line by Line (Step 3)

A walk through `src/kpis.py`: profit and ROI, the ranking function the
spec calls a "UDF", a real Spark UDF for comparison, the two searches, and
the franchise and director tables. Each section quotes the current code
and the real result from `reports/results.md`, and says what the result
does and does not prove.

> **Help:** `groupBy`, medians, sorting and the explode-then-group pattern
> are taught in [07 · Aggregations, windows, sorting](07-aggregations-windows-sorting.md).
> How these tables become charts is in [09 · Charts with Matplotlib](09-visualisation.md).

## 1. The map: spec step to function

A **KPI** (key performance indicator) is a number chosen to judge
performance: here revenue, profit, ROI, rating and popularity.

| Spec step | Function | Result in `results.md` |
|---|---|---|
| 3.1 profit, ROI | `add_profit_roi` | Columns `profit_musd`, `roi` (25 columns in total) |
| 3.1 ten best/worst lists | `rank`, called by `rankings` | 10 tables of 5 films |
| 3.1 "define a UDF" | `rank` (and, for comparison, `roi_udf`) | — |
| 3.2 Search 1 | `search_willis_scifi_action` (uses `has`) | 0 matches |
| 3.2 Search 2 | `search_thurman_tarantino` (uses `has`) | 0 matches |
| 3.3 Franchise vs standalone | `franchise_vs_standalone` | 2 rows: 16 vs 2 films |
| 3.4 Most successful franchises | `top_franchises` | 10 rows |
| 3.5 Most successful directors | `top_directors` | 10 rows |

`main.py` calls them all on the cached `df_movies`. Every function only
builds a plan; `main.md_table` runs it with `collect()`.

## 2. add_profit_roi

```python
def add_profit_roi(df: DataFrame) -> DataFrame:
    # null budget or revenue gives a null profit / roi, never a fake 0.
    # dividing by a null budget is null too, so no divide by zero.
    return (
        df.withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))
        .withColumn("roi", F.col("revenue_musd") / F.col("budget_musd"))
    )
```

| Metric | Formula | Unit | Endgame |
|---|---|---|---|
| **Profit** | revenue − budget | million USD | 2,942.19 − 356.00 = 2,586.19 |
| **ROI** (return on investment) | revenue / budget | a ratio: "times the budget" | 2,942.19 / 356.00 = 8.26 |

The comment carries the key decision. Because cleaning turned budget 0
into null, a film with an unknown budget gets a null profit and a null
ROI, not a profit equal to its whole revenue and not a division by zero.
Null arithmetic does the work; no `if` is needed (see
[05](05-dataframe-fundamentals.md#h-7-null-semantics)).

ROI here is "revenue / budget", as the spec defines it, so 1.0 means
break-even. (Finance often defines ROI as profit / budget, where break-even
is 0. Say which one you use.)

`test_profit_and_roi`: budget 100 M, revenue 450 M → profit 350.0, ROI 4.5.

## 3. rank and rankings: the spec's "UDF"

The spec says: "Define a User-Defined Function (UDF) to streamline ranking
operations." In plain Python terms that means "write one reusable function
for all ten lists". That is `rank`:

```python
def rank(df: DataFrame, col: str, n: int = 5, ascending: bool = False, where: Column | None = None) -> DataFrame:
    """The ranking function the brief asks for. every best / worst list uses it.

    rows where col is null are dropped first, otherwise a film with no budget
    would sit at the top of "lowest roi". title breaks ties so the same input
    always gives the same list.
    """
    if where is not None:
        df = df.filter(where)
    order = F.col(col).asc() if ascending else F.col(col).desc()
    return (
        df.filter(F.col(col).isNotNull())
        .orderBy(order, "title")
        .select("title", col)
        .limit(n)
    )
```

Line by line:

| Line | Why |
|---|---|
| `where=None` then `df.filter(where)` | An optional extra condition, passed in as a Column (e.g. budget ≥ 10). One function serves lists with and without thresholds |
| `order = ... asc() if ascending else ... desc()` | "Best" lists sort descending, "worst" lists ascending |
| `filter(F.col(col).isNotNull())` | Spark puts nulls **first** in ascending order, so a null ROI would top "low roi" (see [07](07-aggregations-windows-sorting.md#h-4-sorting-orderby-direction-tie-breakers-nulls)) |
| `orderBy(order, "title")` | `title` breaks ties, so the output is the same every run |
| `select("title", col)` | Only what the table needs |
| `limit(n)` | Top `n`, default 5 |

`rankings` builds all ten lists by name:

```python
ROI_MIN_BUDGET_MUSD = 10.0
MIN_VOTES = 10

def rankings(df: DataFrame, n: int = 5) -> dict[str, DataFrame]:
    """All ten lists from step 3.1, by name."""
    rich = F.col("budget_musd") >= ROI_MIN_BUDGET_MUSD
    voted = F.col("vote_count") >= MIN_VOTES
    return {
        "top revenue": rank(df, "revenue_musd", n),
        "top budget": rank(df, "budget_musd", n),
        "top profit": rank(df, "profit_musd", n),
        "low profit": rank(df, "profit_musd", n, ascending=True),
        "top roi (budget >= 10M)": rank(df, "roi", n, where=rich),
        "low roi (budget >= 10M)": rank(df, "roi", n, ascending=True, where=rich),
        "most voted": rank(df, "vote_count", n),
        "top rated (>= 10 votes)": rank(df, "vote_average", n, where=voted),
        "low rated (>= 10 votes)": rank(df, "vote_average", n, ascending=True, where=voted),
        "most popular": rank(df, "popularity", n),
    }
```

`rich` and `voted` are Column objects, built once and passed in. Nothing
runs here; the dict holds ten plans. `test_rankings_has_all_ten_lists`
checks there are 10.

### Why the thresholds

| Threshold | Problem it prevents |
|---|---|
| **ROI only for budget ≥ 10 M** | A film made for 0.1 M that earns 5 M has ROI 50 and would top the list. Tiny or misreported budgets make ROI meaningless |
| **Rating only for ≥ 10 votes** | A film with 2 votes, both 10/10, has rating 10.0 and would top "top rated". Few votes means the average is mostly noise |

Both values are module constants (`ROI_MIN_BUDGET_MUSD`, `MIN_VOTES`), so
the tests use the same number as the pipeline:
`test_rank_skips_nulls_and_uses_threshold` filters with
`kpis.ROI_MIN_BUDGET_MUSD` and checks that "Cheap" (budget 5 M, ROI 20) is
excluded, and that "NoBudget" (ROI null) never appears in the ascending
list.

**On this data, neither threshold removes anything.** The smallest budget
is 125.00 (Harry Potter ... Part 2) and the fewest votes 10,557 (Frozen
II). The rules exist for data that needs them, and the tests prove they
work.

### The results

| top revenue | | top profit | | top roi (budget ≥ 10M) | |
|---|---|---|---|---|---|
| Avengers: Endgame | 2,942.19 | Avatar | 2,686.71 | Avatar | 12.34 |
| Avatar | 2,923.71 | Avengers: Endgame | 2,586.19 | Titanic | 11.32 |
| Titanic | 2,264.16 | Titanic | 2,064.16 | Jurassic World | 11.14 |
| Star Wars: The Force Awakens | 2,068.22 | Star Wars: The Force Awakens | 1,823.22 | Harry Potter ... Part 2 | 10.73 |
| Avengers: Infinity War | 2,052.42 | Avengers: Infinity War | 1,752.42 | Frozen II | 9.69 |

| low profit | | low roi (budget ≥ 10M) | | low rated (≥ 10 votes) | |
|---|---|---|---|---|---|
| Star Wars: The Last Jedi | 1,034.41 | Star Wars: The Last Jedi | 4.45 | Jurassic World: Fallen Kingdom | 6.52 |
| Incredibles 2 | 1,043.23 | Avengers: Age of Ultron | 5.98 | Jurassic World | 6.70 |
| Beauty and the Beast | 1,106.12 | Incredibles 2 | 6.22 | Star Wars: The Last Jedi | 6.73 |
| Frozen | 1,124.22 | The Lion King | 6.39 | Beauty and the Beast | 6.97 |
| Jurassic World: Fallen Kingdom | 1,140.47 | Black Panther | 6.75 | The Lion King | 7.10 |

The other four lists (top budget, most voted, top rated, most popular) are
in `results.md`. What to read from them:

- **Endgame tops revenue, Avatar tops profit and ROI.** Endgame's 356.00
  budget is the largest, so its profit and ROI fall behind Avatar's.
  Revenue alone rewards big spending; profit and ROI do not.
- **"Low profit" is still over 1 billion.** The worst film here, The Last
  Jedi, made 1,034.41 M profit. These are the 18 biggest earners TMDB
  knows, so "worst" means "least spectacular hit", not "flop". The same
  goes for "low rated": 6.52 is the lowest rating in the set.
- **Popularity tracks the Avengers.** The top four "most popular" are
  Avengers films; popularity is TMDB's current-interest score, not
  quality.

## 4. A real Spark UDF: roi_udf

In Spark, **UDF** has a precise meaning: a Python function registered so
Spark can call it on every row. `kpis.py` has one, kept only for
comparison:

```python
# a real spark udf, only here to compare with the built in maths in
# add_profit_roi. a python udf ships every row from the jvm to a python
# worker and back, and spark can not optimise what is inside it. so the
# pipeline never calls this, the notebook just shows both give the same roi.
@F.udf(returnType=T.DoubleType())
def roi_udf(revenue: float | None, budget: float | None) -> float | None:
    if revenue is None or budget is None or budget == 0:
        return None
    return revenue / budget
```

- `@F.udf(returnType=T.DoubleType())` wraps the function; Spark must be
  told the return type because it cannot look inside Python code.
- The body is plain Python. It receives `None` for null, so it must handle
  nulls itself (built-in arithmetic does that automatically).

| | Built-in: `F.col("revenue_musd") / F.col("budget_musd")` | `roi_udf("revenue_musd", "budget_musd")` |
|---|---|---|
| Where it runs | Inside the JVM, as generated code | In a separate Python worker process |
| Data movement | None | Every row is serialised, sent to Python, computed, sent back |
| Optimiser | Catalyst understands it and can move or merge it | A black box: no pushdown, no reordering |
| Null handling | Automatic | Written by hand |
| Speed on big data | Fast | Much slower (often many times) |

The rule from the code comment and the README: use a Python UDF only when
no built-in function can do the job. Nothing in this lab needs one.

`test_udf_gives_same_roi_as_builtin` proves the two agree:

```python
def test_udf_gives_same_roi_as_builtin(spark: SparkSession) -> None:
    df = enriched(spark, movie(id=1), movie(id=2, budget=0))
    rows = df.select("roi", kpis.roi_udf("revenue_musd", "budget_musd").alias("u")).collect()
    assert all(r.roi == r.u for r in rows)
```

Movie 1 gives 5.0 from both. Movie 2 (budget 0, so `budget_musd` null)
gives `None` from both; in Python `None == None` is `True`. The notebook
shows the same comparison on the real 18 films.

**So, which is "the UDF" the spec asks for?** Answer in a review: "the
spec means a reusable ranking function; that is `rank`. I also wrote a real
Spark UDF, `roi_udf`, to show why the pipeline does not use one: same
answer, but every row leaves the JVM."

## 5. has(): whole-item matching

The searches need "does the cast include Bruce Willis?". After cleaning,
`cast` is a single string like `"Robert Downey Jr.|Chris Evans|..."`.

```python
def has(col: str, value: str) -> Column:
    """True when the "A|B|C" column has value as a whole item.

    split + array_contains, not contains(): "Bruce Willis Jr" must not match
    "Bruce Willis".
    """
    return F.array_contains(F.split(F.col(col), r"\|"), value)
```

| Approach | `"Bruce Willis Jr\|Actor Two"` contains "Bruce Willis"? |
|---|---|
| `F.col("cast").contains("Bruce Willis")` (substring) | **true**: wrong |
| `has("cast", "Bruce Willis")` (split, then exact item) | **false**: right |

`F.split(..., r"\|")` turns the string back into an array; `array_contains`
checks for an **exact** element. `test_has_matches_whole_names_only` builds
a film whose only cast member is "Bruce Willis Jr" and expects 0 matches.

`has` returns a **Column** (a true/false expression), so it can be
combined with `&` and passed to `filter`. It is also case-sensitive and
exact: "bruce willis" would not match.

## 6. The two searches, and why both return 0 rows

```python
def search_willis_scifi_action(df: DataFrame) -> DataFrame:
    return (
        df.filter(has("genres", "Science Fiction") & has("genres", "Action") & has("cast", "Bruce Willis"))
        .orderBy(F.desc("vote_average"))
        .select("title", "genres", "vote_average")
    )


def search_thurman_tarantino(df: DataFrame) -> DataFrame:
    return (
        df.filter(has("cast", "Uma Thurman") & has("director", "Quentin Tarantino"))
        .orderBy("runtime")
        .select("title", "director", "runtime")
    )
```

The spec's sort orders are followed: Search 1 "best-rated first"
(`F.desc("vote_average")`), Search 2 "shortest to longest"
(`orderBy("runtime")`, ascending by default).

Both return **0 matches** in `results.md`. That is the correct answer, not
a bug:

- The 18 films are listed in `results.md`. None is a Bruce Willis film and
  none is directed by Quentin Tarantino (the directors column shows
  Cameron, the Russos, Whedon, and so on).
- The search logic is proven on data where the answer is known:
  `test_willis_search` builds "Die Hard in Space" (Action + Science Fiction,
  Bruce Willis) and a Drama with Bruce Willis, and expects only the first;
  `test_tarantino_search_shortest_first` builds a 150-minute and a
  110-minute Tarantino film with Uma Thurman and expects
  `["Short", "Long"]`.

`main.py` caches each search result before counting it, so the `count()`
and the table come from one computation:

```python
found = frame.cache()
out.append(f"### {name}\n\n{found.count()} matches.\n\n" + md_table(found))
found.unpersist()
```

**Line to say in review:** "The searches are correct and tested on fake
films; the 18 films in the brief's ID list simply contain no Bruce Willis
or Tarantino film. An empty result is a valid answer, and the report says
'0 matches' instead of hiding it."

## 7. franchise_vs_standalone

```python
def franchise_vs_standalone(df: DataFrame) -> DataFrame:
    group = F.when(F.col("belongs_to_collection").isNotNull(), "franchise").otherwise("standalone")
    # roi is a median: one huge film moves the mean and not the median.
    # percentile_approx because an exact median needs a full sort.
    return (
        df.withColumn("group", group)
        .groupBy("group")
        .agg(
            F.count("*").alias("movies"),
            F.round(F.avg("revenue_musd"), 1).alias("mean_revenue"),
            F.round(F.percentile_approx("roi", 0.5), 2).alias("median_roi"),
            F.round(F.avg("budget_musd"), 1).alias("mean_budget"),
            F.round(F.avg("popularity"), 2).alias("mean_popularity"),
            F.round(F.avg("vote_average"), 2).alias("mean_rating"),
        )
        .orderBy("group")
    )
```

The group label comes from the null in `belongs_to_collection` that
cleaning kept on purpose. The five aggregates are exactly the spec's
list: mean revenue, median ROI, mean budget, mean popularity, mean rating.

| group | movies | mean_revenue | median_roi | mean_budget | mean_popularity | mean_rating |
|---|---|---|---|---|---|---|
| franchise | 16 | 1,691.70 | 7.71 | 218.00 | 27.06 | 7.40 |
| standalone | 2 | 1,765.10 | 7.91 | 180.00 | 26.53 | 7.44 |

**What this does and does not show.** On paper, standalone films win on
revenue, ROI and rating. But the standalone group is **2 films**: Titanic
and Beauty and the Beast. Its "mean revenue" is mostly Titanic, one of the
three highest-grossing films ever. Its "median ROI" with 2 values is one
of the two values (see [07](07-aggregations-windows-sorting.md#h-3-medians-and-percentile-approx)).
And the 18 IDs were picked by the lab author, all of them huge hits, not a
random sample of films.

So the honest conclusion is: **this data cannot say whether franchises
beat standalone films.** The differences (1,765.10 vs 1,691.70; 7.44 vs
7.40) are smaller than what swapping a single film would change. The one
solid pattern is cost: franchise films had the bigger mean budget (218.00
vs 180.00). `test_franchise_split` checks the grouping itself on one film
of each kind.

## 8. top_franchises

```python
def top_franchises(df: DataFrame, n: int = 10) -> DataFrame:
    # most successful = most total revenue, rating breaks ties
    return (
        df.filter(F.col("belongs_to_collection").isNotNull())
        .groupBy(F.col("belongs_to_collection").alias("franchise"))
        .agg(
            F.count("*").alias("movies"),
            F.round(F.sum("revenue_musd"), 1).alias("total_revenue"),
            F.round(F.avg("revenue_musd"), 1).alias("mean_revenue"),
            F.round(F.sum("budget_musd"), 1).alias("total_budget"),
            F.round(F.avg("budget_musd"), 1).alias("mean_budget"),
            F.round(F.avg("vote_average"), 2).alias("mean_rating"),
        )
        .orderBy(F.desc("total_revenue"), F.desc("mean_rating"), "franchise")
        .limit(n)
    )
```

- `filter(... isNotNull())` drops the 2 standalone films; otherwise they
  would form a "null franchise" group.
- `groupBy(F.col(...).alias("franchise"))` groups and renames in one step.
- **The definition of "most successful" is a choice**, written in the
  comment: highest total revenue, mean rating as tie-breaker. The spec
  lists the measures but not how to rank them.
- `"franchise"` is the **last sort key**. If two franchises ever tied on
  total revenue and mean rating, the name decides, so the top 10 is the
  same on every run (see §9 for the case where this really happens).

| franchise | movies | total_revenue | mean_revenue | total_budget | mean_budget | mean_rating |
|---|---|---|---|---|---|---|
| The Avengers Collection | 4 | 7,918.80 | 1,979.70 | 1,111.00 | 277.80 | 7.96 |
| Star Wars Collection | 2 | 3,402.60 | 1,701.30 | 545.00 | 272.50 | 6.98 |
| Jurassic Park Collection | 2 | 2,982.00 | 1,491.00 | 320.00 | 160.00 | 6.61 |
| Avatar Collection | 1 | 2,923.70 | 2,923.70 | 237.00 | 237.00 | 7.61 |
| Frozen Collection | 2 | 2,727.90 | 1,364.00 | 300.00 | 150.00 | 7.23 |
| ... 5 more with 1 film each | | | | | | |

Read it carefully:

- **Total revenue rewards the number of films in the sample.** The
  Avengers lead because 4 of the 18 IDs are Avengers films. The real
  Star Wars or Jurassic franchises have more films than appear here.
- **By mean revenue, Avatar (1 film, 2,923.70) would be first.**
- **By mean rating, Harry Potter (8.08, 1 film) would be first.**

So "most successful franchise" depends on the measure. Say which one you
used and why: total revenue answers "which franchise brought in the most
money in this set". `test_top_franchises_sums_revenue` checks that two
films of 100 M and 300 M in one collection give `movies == 2` and
`total_revenue == 400.0`.

## 9. top_directors

```python
def top_directors(df: DataFrame, n: int = 10) -> DataFrame:
    # explode gives a co-directed film one row per director so both get credit.
    # co-directors tie on every number, so the name is the last sort key,
    # otherwise the russos can swap places from one run to the next.
    return (
        df.filter(F.col("director").isNotNull())
        .withColumn("director", F.explode(F.split("director", r"\|")))
        .groupBy("director")
        .agg(
            F.count("*").alias("movies"),
            F.round(F.sum("revenue_musd"), 1).alias("total_revenue"),
            F.round(F.avg("vote_average"), 2).alias("mean_rating"),
        )
        .orderBy(F.desc("total_revenue"), F.desc("mean_rating"), "director")
        .limit(n)
    )
```

The spec's three measures: number of films, total revenue, mean rating.
The `explode` gives each co-director a full row (pattern explained in
[07](07-aggregations-windows-sorting.md#h-8-explode-then-group-and-the-double-counting-trap)).

| director | movies | total_revenue | mean_rating |
|---|---|---|---|
| James Cameron | 2 | 5,187.90 | 7.76 |
| Anthony Russo | 2 | 4,994.60 | 8.25 |
| Joe Russo | 2 | 4,994.60 | 8.25 |
| Joss Whedon | 2 | 2,924.20 | 7.68 |
| Chris Buck | 2 | 2,727.90 | 7.23 |
| Jennifer Lee | 2 | 2,727.90 | 7.23 |
| J.J. Abrams | 1 | 2,068.20 | 7.24 |
| Colin Trevorrow | 1 | 1,671.50 | 6.70 |
| Jon Favreau | 1 | 1,662.00 | 7.10 |
| James Wan | 1 | 1,515.40 | 7.21 |

Check one by hand: James Cameron = Titanic 2,264.16 + Avatar 2,923.71 =
5,187.87 → 5,187.90. Mean rating (7.90 + 7.61) / 2 = 7.755, shown as 7.76.

Points for a review:

- **Co-directors share credit in full.** Joe and Anthony Russo each get the
  whole 4,994.60 (Endgame + Infinity War). Correct per director, but the
  column cannot be summed into a total. `test_codirectors_both_get_the_revenue`
  checks `{"A": 200.0, "B": 200.0}` for one 200 M film.
- **Ties in the order, and the fix.** Co-directors tie on every number,
  because they share the same films: Joe and Anthony Russo both have 2
  films, 4,994.60 and 8.25; Jennifer Lee and Chris Buck both have 2 films,
  2,727.90 and 7.23. With only `total_revenue` and `mean_rating` as sort
  keys, Spark gives no guarantee which of each pair comes first, so the
  table could change from one run to the next with no change in the data.
  That was a real risk in an earlier version of this function. The code
  now ends the sort with `"director"`, so ties are broken by name and the
  order is fixed: Anthony Russo before Joe Russo and Chris Buck before
  Jennifer Lee, on every run.
- **Rating as tie-breaker, not as a measure of success.** Ranking by
  mean rating instead would put the Russos (8.25) first.
- **Small numbers.** No director has more than 2 films here. "Most
  successful director" means "most revenue from the films in this list".

## 10. How the KPIs are tested

`tests/test_kpis.py` has 10 tests. They build tiny tables with
`enriched(spark, movie(...), ...)` from `tests/helpers.py`, which runs the
real `cleaning.clean` and `kpis.add_profit_roi`, so every KPI test also
exercises the cleaning path.

| Test | Proves |
|---|---|
| `test_profit_and_roi` | 450 − 100 = 350.0; 450 / 100 = 4.5 |
| `test_rank_skips_nulls_and_uses_threshold` | `where` excludes the 5 M budget film; null ROI never listed |
| `test_rankings_has_all_ten_lists` | 10 lists |
| `test_has_matches_whole_names_only` | "Bruce Willis Jr" is not "Bruce Willis" |
| `test_willis_search` | Only the Action + Sci-Fi film matches |
| `test_tarantino_search_shortest_first` | `["Short", "Long"]` |
| `test_codirectors_both_get_the_revenue` | Each co-director gets 200.0 |
| `test_franchise_split` | 1 franchise, 1 standalone |
| `test_top_franchises_sums_revenue` | 2 films, total 400.0 |
| `test_udf_gives_same_roi_as_builtin` | UDF and built-in agree, including null |

## Self-check

1. What does the spec mean by "define a UDF", and which function is it? [A reusable ranking function: `rank`. `roi_udf` is a real Spark UDF kept only for comparison.]
2. Why is `roi_udf` not used in the pipeline? [Every row goes from the JVM to a Python worker and back, and Catalyst cannot optimise it; the built-in division gives the same result.]
3. Why `has()` and not `contains()`? [`contains` is a substring test, so "Bruce Willis Jr" would match "Bruce Willis".]
4. Both searches return 0 rows. Is that a bug? [No. None of the 18 films has Bruce Willis or Tarantino; the tests prove the logic on fake films.]
5. Standalone films have the higher mean revenue. Can you conclude standalone films earn more? [No. There are only 2 (Titanic and Beauty and the Beast), and the 18 films are hand-picked hits.]
