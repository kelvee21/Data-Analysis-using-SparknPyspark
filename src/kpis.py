"""Step 3: profit, roi, best / worst lists, searches, franchise and director tables."""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

ROI_MIN_BUDGET_MUSD = 10.0
MIN_VOTES = 10


def add_profit_roi(df: DataFrame) -> DataFrame:
    # null budget or revenue gives a null profit / roi, never a fake 0.
    # dividing by a null budget is null too, so no divide by zero.
    return (
        df.withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))
        .withColumn("roi", F.col("revenue_musd") / F.col("budget_musd"))
    )


def rank(
    df: DataFrame, col: str, n: int = 5, ascending: bool = False, where: Column | None = None
) -> DataFrame:
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


# a real spark udf, only here to compare with the built in maths in
# add_profit_roi. a python udf ships every row from the jvm to a python
# worker and back, and spark can not optimise what is inside it. so the
# pipeline never calls this, the notebook just shows both give the same roi.
@F.udf(returnType=T.DoubleType())
def roi_udf(revenue: float | None, budget: float | None) -> float | None:
    if revenue is None or budget is None or budget == 0:
        return None
    return revenue / budget


def has(col: str, value: str) -> Column:
    """True when the "A|B|C" column has value as a whole item.

    split + array_contains, not contains(): "Bruce Willis Jr" must not match
    "Bruce Willis".
    """
    return F.array_contains(F.split(F.col(col), r"\|"), value)


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
