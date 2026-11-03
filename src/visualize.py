"""Step 4: the charts. same 7 plots as the pandas version.

rule i follow here: spark does the counting and grouping, and only the
small answer comes back to the driver with toPandas(). toPandas on the full
table would copy every row into driver memory, which is fine for 18 movies
and a crash for a real table. the two scatter plots are the only ones that
need one point per movie; on big data i would .sample() them first.
"""
from typing import Any

import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.ticker import MaxNLocator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def plot_revenue_vs_budget(df: DataFrame) -> Axes:
    pdf = df.select("title", "budget_musd", "revenue_musd").dropna().toPandas()
    fig, ax = plt.subplots()
    ax.scatter(pdf["budget_musd"], pdf["revenue_musd"])
    # anything above this line made more than it cost
    top = pdf["budget_musd"].max()
    ax.plot([0, top], [0, top], linestyle="--", color="grey", label="revenue = budget")
    ax.set_xlabel("budget (musd)")
    ax.set_ylabel("revenue (musd)")
    ax.set_title("revenue vs budget")
    ax.legend()
    return ax


def plot_roi_by_genre(df: DataFrame) -> Axes:
    """Mean roi per genre, sorted, with the film count on the label.

    with 18 films some genres have only 1 or 2, so a tall bar can be one
    film. the n= on the label shows which bars are too thin to mean much.
    """
    pdf = (
        df.filter(F.col("roi").isNotNull())
        .withColumn("genre", F.explode(F.split("genres", r"\|")))
        .groupBy("genre")
        .agg(F.avg("roi").alias("mean_roi"), F.count("*").alias("n"))
        .orderBy(F.desc("mean_roi"))
        .toPandas()
    )
    labels = [f"{g}\n(n={n})" for g, n in zip(pdf["genre"], pdf["n"])]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(labels, pdf["mean_roi"])
    ax.set_xlabel("genre, with number of films")
    ax.set_ylabel("mean roi")
    ax.set_title("roi by genre")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    fig.tight_layout()
    return ax


def plot_popularity_vs_rating(df: DataFrame) -> Axes:
    pdf = df.select("popularity", "vote_average").dropna().toPandas()
    fig, ax = plt.subplots()
    ax.scatter(pdf["popularity"], pdf["vote_average"])
    ax.set_xlabel("popularity")
    ax.set_ylabel("rating")
    ax.set_title("popularity vs rating")
    return ax


def plot_yearly_box_office(df: DataFrame) -> Axes:
    pdf = (
        df.filter(F.col("revenue_musd").isNotNull())
        .groupBy(F.year("release_date").alias("year"))
        .agg(F.avg("revenue_musd").alias("mean_revenue"))
        .orderBy("year")
        .toPandas()
    )
    fig, ax = plt.subplots()
    ax.plot(pdf["year"], pdf["mean_revenue"], marker="o")
    ax.set_xlabel("year")
    ax.set_ylabel("mean revenue (musd)")
    ax.set_title("box office by year")
    return ax


def plot_franchise_vs_standalone(df: DataFrame) -> Axes:
    group = F.when(F.col("belongs_to_collection").isNotNull(), "franchise").otherwise("standalone")
    pdf = (
        df.groupBy(group.alias("group"))
        .agg(F.avg("revenue_musd").alias("mean_revenue"), F.count("*").alias("n"))
        .orderBy("group")
        .toPandas()
    )
    labels = [f"{g} (n={n})" for g, n in zip(pdf["group"], pdf["n"])]
    fig, ax = plt.subplots()
    ax.bar(labels, pdf["mean_revenue"])
    ax.set_xlabel("group")
    ax.set_ylabel("mean revenue (musd)")
    ax.set_title("franchise vs standalone")
    return ax


def plot_distribution(df: DataFrame, column: str, bins: int = 20) -> Axes:
    """Histogram of one numeric column with the median marked.

    the pandas version handed all values to ax.hist. here spark puts each
    value in a bucket and counts them, so only `bins` numbers come back no
    matter how big the table is.
    """
    stats = df.agg(
        F.min(column).alias("lo"),
        F.max(column).alias("hi"),
        F.percentile_approx(column, 0.5).alias("median"),
    ).first()
    assert stats is not None  # agg with no groupBy always gives one row
    lo, hi, median = stats["lo"], stats["hi"], stats["median"]
    fig, ax = plt.subplots()
    if lo is None:
        ax.set_title(f"{column} distribution (no values)")
        return ax

    width = (hi - lo) / bins if hi > lo else 1.0
    # the max value would land in bucket number `bins`, least() puts it in the last one
    bucket = F.least(F.floor((F.col(column) - lo) / width), F.lit(bins - 1)).cast("int")
    rows = df.filter(F.col(column).isNotNull()).groupBy(bucket.alias("b")).count().collect()
    counts: dict[int, int] = {r["b"]: r["count"] for r in rows}
    lefts = [lo + i * width for i in range(bins)]
    heights = [counts.get(i, 0) for i in range(bins)]
    ax.bar(lefts, heights, width=width, align="edge")
    ax.axvline(median, linestyle="--", color="black")
    ax.set_xlabel(column)
    ax.set_ylabel("movies")
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))  # no half movies on the axis
    ax.set_title(f"{column} distribution (median={median:g})")
    return ax


def plot_boxplot(df: DataFrame, column: str) -> Axes:
    """Horizontal boxplot of one column, quick outlier view.

    the box numbers (quartiles, whiskers) are worked out in spark and given
    to ax.bxp, which draws a box from numbers instead of from raw values.
    only the outliers themselves come back as rows.
    """
    quartiles = df.agg(F.percentile_approx(column, [0.25, 0.5, 0.75]).alias("q")).first()
    assert quartiles is not None
    q1, med, q3 = quartiles["q"]
    iqr = q3 - q1
    low_fence, high_fence = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    inside = df.filter(F.col(column).between(low_fence, high_fence))
    whis = inside.agg(F.min(column).alias("lo"), F.max(column).alias("hi")).first()
    assert whis is not None
    fliers: list[float] = [
        r[column]
        for r in df.filter((F.col(column) < low_fence) | (F.col(column) > high_fence))
        .select(column).collect()
    ]
    box: dict[str, Any] = {"med": med, "q1": q1, "q3": q3, "whislo": whis["lo"], "whishi": whis["hi"],
           "fliers": fliers, "label": ""}
    fig, ax = plt.subplots()
    ax.bxp([box], orientation="horizontal")
    ax.set_yticks([])
    ax.set_xlabel(column)
    ax.set_title(f"{column} boxplot")
    return ax
