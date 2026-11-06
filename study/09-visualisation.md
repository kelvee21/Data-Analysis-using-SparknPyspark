# 09 — Charts with Matplotlib

This page covers Step 4 of the lab: how the eight charts in `reports/`
(`chart1.png` to `chart8.png`) are made by `src/visualize.py`, why Spark hands
only small answers to pandas, and what each chart says about these 18 films.

> **Help:** the KPI tables behind the charts are explained in [08 · KPIs line by line](08-kpis-line-by-line.md). `groupBy`, `explode` and sorting are taught in [07 · Aggregations, windows, sorting](07-aggregations-windows-sorting.md).

## The one rule: Spark counts, Matplotlib draws

Matplotlib cannot read a Spark DataFrame. It wants plain Python lists or
pandas columns, all of which live in the memory of one Python process. So
every function in `visualize.py` follows the rule written in its docstring:

```python
"""...spark does the counting and grouping, and only the
small answer comes back to the driver with toPandas(). toPandas on the full
table would copy every row into driver memory, which is fine for 18 movies
and a crash for a real table. the two scatter plots are the only ones that
need one point per movie; on big data i would .sample() them first.
"""
```

Three steps per chart:

1. **Spark** selects, filters, groups or buckets the data into a small table.
2. **`.toPandas()`** (or `.collect()` / `.first()`) copies that small result to
   the **driver**, the one Python process that runs your script.
3. **Matplotlib** draws it.

### Why the order matters (the driver-memory risk)

In real Spark the data is split into **partitions** (chunks) spread over
**executors** (worker processes, often on other machines). No single machine
holds the whole table. `toPandas()` pulls every row into the driver at once.

| What is sent to the driver | Rows that land there |
|---|---|
| `plot_revenue_vs_budget`: two columns, one row per film | 18 |
| `plot_roi_by_genre`: one row per genre after `groupBy` | 11 |
| `plot_yearly_box_office`: one row per year after `groupBy` | 9 |
| `plot_franchise_vs_standalone`: one row per group | 2 |
| `plot_distribution`: one count per bucket, plus min/max/median | at most 20, plus 1 |
| `plot_boxplot`: three quartiles, two whisker ends, the outliers | 1 + 1 + a few |
| The whole TMDB catalogue (about a million films), without reducing first | a million: the driver may run out of memory |

The habit is: **reduce first, then convert**. Here the data is tiny, so the
rule protects nothing yet; it is the habit a reviewer wants to see.

> **Note:** `spark_session.py` turns on **Arrow**
> (`spark.sql.execution.arrow.pyspark.enabled`), a columnar format that makes
> `toPandas()` copy whole columns at once instead of row by row. PyArrow is in
> `requirements.txt`. The `fallback.enabled` setting means `toPandas()` still
> works, slowly, if PyArrow is missing.

## Matplotlib in ten minutes

**Matplotlib** is Python's standard plotting library. `matplotlib.pyplot`,
imported as `plt`, is its main interface.

### Figure and axes

- A **figure** (`fig`) is the whole image, the PNG file.
- An **axes** (`ax`) is one plot area inside it, with an x-axis and a y-axis.

```python
fig, ax = plt.subplots()                  # one figure, one axes, default 6.4 x 4.8 inches
fig, ax = plt.subplots(figsize=(10, 5))   # wider, used for the genre bars
```

Every `plot_*` function **returns `ax`**, not a file. The caller decides what
to do with it: `main.py` saves it, the notebook shows it with `plt.show()`,
and a test inspects it (`ax.patches`, `ax.get_title()`). From an `ax` you
reach its figure with `ax.figure`.

### The drawing methods this lab uses

| Method | Draws | Used in |
|---|---|---|
| `ax.scatter(x, y)` | one dot per (x, y) pair | revenue vs budget, popularity vs rating |
| `ax.plot(x, y, marker="o")` | a line through points, with a dot at each | yearly box office |
| `ax.plot([0, top], [0, top], linestyle="--")` | a dashed straight line | the break-even line |
| `ax.bar(x, heights)` | vertical bars | ROI by genre, franchise vs standalone |
| `ax.bar(lefts, heights, width=w, align="edge")` | bars that start at `lefts` and are `w` wide | the histogram buckets |
| `ax.axvline(x, linestyle="--")` | a vertical line across the plot | the median on the histogram |
| `ax.bxp([stats])` | a boxplot from numbers you already computed | runtime and popularity boxplots |

### Labels, ticks, saving

| Call | What it does |
|---|---|
| `ax.set_xlabel(...)`, `ax.set_ylabel(...)`, `ax.set_title(...)` | axis labels and title |
| `ax.legend()` | box naming every series that has a `label=` |
| `plt.setp(ax.get_xticklabels(), rotation=45, ha="right")` | tilt long x labels so they do not overlap |
| `ax.yaxis.set_major_locator(MaxNLocator(integer=True))` | whole-number ticks only ("no half movies") |
| `ax.set_yticks([])` | remove y ticks (the boxplot has only one box) |
| `fig.tight_layout()` | shrink margins so labels are not cut off |
| `fig.savefig(path, dpi=120)` | write the PNG at 120 dots per inch |
| `plt.close(fig)` | free the figure's memory |

### The Agg backend, and why it is set inside `main()`

A **backend** is the part of Matplotlib that turns a drawing into pixels.
Some open a window; `Agg` only writes image files.

```python
def main() -> None:
    ...
    # no window, just files. set here and not at import, so importing main
    # from the notebook does not turn off the inline charts.
    matplotlib.use("Agg")
```

If `matplotlib.use("Agg")` sat at the top of a module, a notebook that
imported that module would lose its inline charts. Inside `main()`, only the
command-line run switches to files. `tests/test_visualize.py` sets `Agg` at
its own top, because tests should never open windows.

## `visualize.py` and `save_chart`, function by function

### `save_chart(ax, name)` in `main.py`

```python
def save_chart(ax: Axes, name: str) -> Path:
    # root=True gives the Figure, not a SubFigure, so tight_layout is there
    fig = ax.get_figure(root=True)
    assert fig is not None
    fig.tight_layout()
    path = REPORTS / name
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path
```

One helper so every chart is saved the same way, into `reports/`. It returns
the path, which `run()` uses to write the `![chart1](chart1.png)` links in
`reports/results.md`. `plt.close(fig)` matters: Matplotlib keeps every open
figure in memory until it is closed, and warns after 20.

### `plot_revenue_vs_budget(df)` → chart 1

```python
pdf = df.select("title", "budget_musd", "revenue_musd").dropna().toPandas()
fig, ax = plt.subplots()
ax.scatter(pdf["budget_musd"], pdf["revenue_musd"])
# anything above this line made more than it cost
top = pdf["budget_musd"].max()
ax.plot([0, top], [0, top], linestyle="--", color="grey", label="revenue = budget")
```

- Spark keeps three columns and drops rows with a null budget or revenue
  (none here). `pdf` is the usual name for a **p**andas **D**ata**F**rame.
- The dashed line runs from (0, 0) to (top, top). Every point on it has
  revenue = budget: above it a film made money, below it lost money.

### `plot_roi_by_genre(df)` → chart 2

```python
pdf = (
    df.filter(F.col("roi").isNotNull())
    .withColumn("genre", F.explode(F.split("genres", r"\|")))
    .groupBy("genre")
    .agg(F.avg("roi").alias("mean_roi"), F.count("*").alias("n"))
    .orderBy(F.desc("mean_roi"))
    .toPandas()
)
labels = [f"{g}\n(n={n})" for g, n in zip(pdf["genre"], pdf["n"])]
```

- `F.split("genres", r"\|")` turns `"Action|Science Fiction"` into an array.
  `|` is escaped because `split` takes a regular expression, where a bare `|`
  means "or".
- `F.explode` makes one row per genre, so a film with three genres counts
  once in each. Spark then computes the mean ROI and the film count per
  genre. Only 11 rows come back.
- The label carries `n=`, so the reader sees which bars rest on one film. The
  docstring: "a tall bar can be one film."

### `plot_popularity_vs_rating(df)` → chart 3

```python
pdf = df.select("popularity", "vote_average").dropna().toPandas()
ax.scatter(pdf["popularity"], pdf["vote_average"])
```

One dot per film: popularity on x, rating on y.

### `plot_yearly_box_office(df)` → chart 4

```python
pdf = (
    df.filter(F.col("revenue_musd").isNotNull())
    .groupBy(F.year("release_date").alias("year"))
    .agg(F.avg("revenue_musd").alias("mean_revenue"))
    .orderBy("year")
    .toPandas()
)
ax.plot(pdf["year"], pdf["mean_revenue"], marker="o")
```

`F.year` pulls the year from the date. Spark averages revenue per year, so
9 rows come back. The line joins the yearly means in order.

### `plot_franchise_vs_standalone(df)` → chart 5

```python
group = F.when(F.col("belongs_to_collection").isNotNull(), "franchise").otherwise("standalone")
pdf = (
    df.groupBy(group.alias("group"))
    .agg(F.avg("revenue_musd").alias("mean_revenue"), F.count("*").alias("n"))
    .orderBy("group")
    .toPandas()
)
labels = [f"{g} (n={n})" for g, n in zip(pdf["group"], pdf["n"])]
```

Same split as `kpis.franchise_vs_standalone`: a film with a collection is a
franchise film. Two rows come back; the labels again show `n`.

### `plot_distribution(df, column, bins=20)` → chart 6 (a Spark-side histogram)

A **histogram** splits the range of a column into equal-width **buckets**
(bins) and counts the values in each. The usual Matplotlib way, `ax.hist`,
needs every value in Python. Here Spark does the bucketing and counting:

```python
stats = df.agg(
    F.min(column).alias("lo"),
    F.max(column).alias("hi"),
    F.percentile_approx(column, 0.5).alias("median"),
).first()
assert stats is not None  # agg with no groupBy always gives one row
...
width = (hi - lo) / bins if hi > lo else 1.0
# the max value would land in bucket number `bins`, least() puts it in the last one
bucket = F.least(F.floor((F.col(column) - lo) / width), F.lit(bins - 1)).cast("int")
rows = df.filter(F.col(column).isNotNull()).groupBy(bucket.alias("b")).count().collect()
counts: dict[int, int] = {r["b"]: r["count"] for r in rows}
lefts = [lo + i * width for i in range(bins)]
heights = [counts.get(i, 0) for i in range(bins)]
ax.bar(lefts, heights, width=width, align="edge")
ax.axvline(median, linestyle="--", color="black")
```

| Code | What it does |
|---|---|
| `df.agg(min, max, percentile_approx).first()` | one Spark pass gives the range and the median |
| `width = (hi - lo) / bins` | each bucket's width; `if hi > lo else 1.0` avoids dividing by 0 when all values are equal |
| `F.floor((col - lo) / width)` | bucket number 0, 1, 2 ... for each value |
| `F.least(..., F.lit(bins - 1))` | the maximum value computes to bucket `bins` (one past the end); this clamps it into the last bucket |
| `groupBy(bucket).count().collect()` | Spark counts per bucket; at most 20 rows come back |
| `dict(...)` | each `Row(b, count)` unpacks to a key and a value, e.g. `{0: 4, 1: 4, ...}` |
| `counts.get(i, 0)` | empty buckets are missing from the result, so they default to 0 |
| `align="edge"` | each bar starts at its left edge, so the bars touch like a histogram |
| `if lo is None: ...` | an all-null column gets an empty chart titled "(no values)" instead of a crash |

`test_histogram_counts_every_movie` checks that the bar heights add up to the
number of films (5 films, 4 bins → total height 5).

### `plot_boxplot(df, column)` → charts 7 and 8 (a Spark-side boxplot)

A **boxplot** summarises a column in a few numbers: the **median**; the
**quartiles** Q1 and Q3 (25 % and 75 % of the values lie below them); the
**whiskers**; and **outliers** drawn as circles. The box spans Q1 to Q3; its
width is the **IQR** (interquartile range, Q3 − Q1). Points beyond the
**fences**, Q1 − 1.5 × IQR and Q3 + 1.5 × IQR, are outliers. The whiskers
reach the furthest values still inside the fences.

```python
quartiles = df.agg(F.percentile_approx(column, [0.25, 0.5, 0.75]).alias("q")).first()
assert quartiles is not None
q1, med, q3 = quartiles["q"]
iqr = q3 - q1
low_fence, high_fence = q1 - 1.5 * iqr, q3 + 1.5 * iqr
inside = df.filter(F.col(column).between(low_fence, high_fence))
whis = inside.agg(F.min(column).alias("lo"), F.max(column).alias("hi")).first()
assert whis is not None
fliers = [
    r[column]
    for r in df.filter((F.col(column) < low_fence) | (F.col(column) > high_fence))
    .select(column).collect()
]
box = {"med": med, "q1": q1, "q3": q3, "whislo": whis["lo"], "whishi": whis["hi"],
       "fliers": fliers, "label": ""}
ax.bxp([box], orientation="horizontal")
```

- `percentile_approx(column, [0.25, 0.5, 0.75])` returns all three quartiles
  in one pass, as an array that unpacks into `q1, med, q3`.
- `between(low_fence, high_fence)` keeps the non-outliers; their min and max
  are the whisker ends.
- Only the outliers ("fliers") come back as rows.
- `ax.bxp` draws a box **from numbers you give it**. The usual `ax.boxplot`
  needs every raw value and computes the numbers itself.

> **Watch out: Spark fences differ from pandas fences.** `percentile_approx`
> returns a value that is **in the data**; it never interpolates between two
> values. pandas and NumPy (and so `ax.boxplot`) interpolate by default. On
> 18 rows that moves Q1 and Q3, and so the fences. The notebook records the
> effect on revenue: with Spark quartiles, Avatar (2,924) and Endgame (2,942)
> fall inside the high fence (3,129), while the pandas version of this lab
> flagged both as outliers. Neither is wrong; they use two definitions of a
> quartile. Read them as "the top two films", not as bad data.

`test_boxplot_draws` builds five films with runtimes 101–105 and one at 400,
and checks the chart is drawn with the title "runtime boxplot".

## What the eight charts show

### Chart 1 — revenue vs budget

**What you see.** 18 dots with budgets between 125 and 356 and revenues
between about 1,240 and 2,940 (million USD). The dashed break-even line runs
along the bottom. Every dot is far above it. The two highest dots are Avatar
(budget 237, revenue 2,923.71) and Endgame (356, 2,942.19).

**What to say.** "Every film earned at least 4.45 times its budget (the
lowest ROI, Star Wars: The Last Jedi). That is **survivorship bias**: the
spec's IDs are among the highest-grossing films ever, so only winners are in
the sample. Budget barely predicts revenue: The Last Jedi has the joint
second-biggest budget (300 M), yet the lowest profit, 1,034.41 M."

> **Survivorship bias:** conclusions drawn only from cases that "survived" a
> filter. Here the filter is "became a blockbuster".

### Chart 2 — ROI by genre

**What you see.** 11 bars of mean ROI, sorted high to low, each labelled
with its film count. Comedy (n=1) is highest at about 9.7, then Romance (n=2)
and Fantasy (n=4). Action (n=10) is lowest at about 7.4. Adventure (n=15)
sits near 8.

**What to say.** "The tallest bar is one film: Comedy is only Frozen II, whose
ROI is 9.69. The two big genres, Adventure (15 films) and Action (10), sit in
the middle and at the bottom. With 18 films I would not claim any genre is
more profitable. A film with three genres also counts in three bars, so the
bars are not independent."

### Chart 3 — popularity vs rating

**What you see.** Popularity on x (about 14 to 65), rating on y (6.52 to
8.25). Most films sit at popularity 14–23. The two dots at the far right,
popularity 64.93 and 54.46, are Endgame and Infinity War, both rated 8.25.

**What to say.** "Higher-rated films tend to be more popular here, but the
trend is driven by a few Avengers films. TMDB popularity is a current-activity
score (page views, votes, watch-list adds), so it rewards films people are
looking at now, not total audience."

### Chart 4 — box office by year

**What you see.** A line through 9 points, one per release year with a film:
1997, 2009, 2011, 2012, 2013, 2015, 2017, 2018, 2019. It peaks at 2009
(Avatar alone, 2,923.71), drops to about 1,250–1,700 for most later years,
and rises again in 2019.

**What to say.** "Most years hold one film, so the line follows which films
are in the list: 2009 is Avatar, 1997 is Titanic. A line also suggests values
between the points, but 1998–2008 have no films at all. It is not a market
trend, and revenue is not adjusted for inflation."

### Chart 5 — franchise vs standalone

**What you see.** Two bars of mean revenue: franchise (n=16) 1,691.70 and
standalone (n=2) 1,765.10.

**What to say.** "The standalone bar is two films, Titanic and Beauty and the
Beast. Titanic alone (2,264.16) lifts it above the franchise mean. Two data
points support no conclusion."

> **Follow-up trap:** "Is Beauty and the Beast really standalone?" TMDB gives
> it no collection, so by the spec's rule (`belongs_to_collection` is null) it
> is. A person might call it part of Disney's remake series.

### Chart 6 — revenue distribution

**What you see.** A histogram of `revenue_musd` in 20 buckets from the lowest
revenue (Incredibles 2, 1,243.23) to the highest (Endgame, 2,942.19). Most
films sit in the buckets around 1,250–1,750. A dashed line marks the median,
1,453.68 (that is Frozen II's revenue: `percentile_approx` returns a real
value). After a gap come films around 2,050, one near 2,264 and two near
2,930.

**What to say.** "Revenue is right-skewed: most films earn 1.2 to 1.7 billion
and a few earn far more. The mean is pulled above the median, so the median
is the fairer 'typical film'."

### Chart 7 — runtime boxplot

**What you see.** A box from about 124 to 149 minutes, median about 135,
whiskers from 102 (Frozen) to 181 (Endgame), and one outlier circle at 194:
Titanic.

**What to say.** "Titanic is the only runtime outlier. It is a real 194-minute
film, not a data error, so it stays."

### Chart 8 — popularity boxplot

**What you see.** A narrow box from about 15 to 35, median about 19, and a
long right whisker reaching 64.93 (Endgame). No outlier circles.

**What to say.** "Popularity is the most skewed column. Endgame sits right at
the high fence, so it is a whisker end here, not an outlier. With
pandas-style quartiles the answer could flip, because the fences move."

## Things a reviewer may point at

| Comment | Honest answer |
|---|---|
| "Why not `ax.hist` and `ax.boxplot`?" | They need every value in Python. Bucketing and quartiles in Spark send back only the numbers, which is what scales. |
| "Your boxplot differs from pandas." | `percentile_approx` picks real values; pandas interpolates. Different quartile definitions, different fences. |
| "Chart 4 draws a line across 11 empty years." | True. Markers only, or bars, would be more honest for gappy years. |
| "Why not plot directly from Spark?" | Spark has no plotting. Matplotlib needs data in local memory. |
| "Chart 1 has no titles on the dots." | True; the top films are in the revenue table in `results.md`. `ax.annotate` could label them. |

## Self-check

1. Why do the `plot_*` functions return `ax` instead of saving a file?
2. In `plot_distribution`, what does `F.least(..., F.lit(bins - 1))` fix?
3. Why is `matplotlib.use("Agg")` inside `main()` and not at the top of a module?
4. Why can a Spark boxplot and a pandas boxplot of the same column show different outliers?
5. Comedy has the highest bar in chart 2. Why should you not trust it?

Answers: (1) So the caller decides: `main.py` saves, the notebook shows,
tests inspect. (2) The maximum value would compute to bucket number `bins`,
one past the last; `least` clamps it into the last bucket. (3) So importing
the code from the notebook does not switch off inline charts. (4)
`percentile_approx` returns real values while pandas interpolates, so Q1, Q3
and the fences differ. (5) It is one film (n=1, Frozen II).

Next: [10 · Pipeline and tests](10-pipeline-and-tests.md)
