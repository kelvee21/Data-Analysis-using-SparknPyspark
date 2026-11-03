"""Step 2: clean the raw movies with spark.

same steps and names as my pandas version (src/cleaning.py in DEM02), but
every function takes a spark DataFrame and gives a new one back. nothing
runs until something asks for a result (count, show, write), so clean() is
just building a plan.
"""
import logging
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

log = logging.getLogger(__name__)

DROP_COLUMNS = ["adult", "imdb_id", "original_title", "video", "homepage"]

FINAL_COLUMNS = [
    "id", "title", "tagline", "release_date", "genres", "belongs_to_collection",
    "original_language", "budget_musd", "revenue_musd", "production_companies",
    "production_countries", "vote_count", "vote_average", "popularity", "runtime",
    "overview", "spoken_languages", "poster_path", "cast", "cast_size", "director", "crew_size",
]

# text tmdb (and people) use when there is no value
PLACEHOLDERS = ["", "No Data", "No overview found.", "N/A", "n/a"]


def _named(*extra: str) -> T.ArrayType:
    # array of {id, name, ...} structs, the shape tmdb uses for genres,
    # companies and friends
    fields = [T.StructField("id", T.LongType()), T.StructField("name", T.StringType())]
    fields += [T.StructField(f, T.StringType()) for f in extra]
    return T.ArrayType(T.StructType(fields))


# i write the schema out instead of letting spark guess it.
# - guessing (inferSchema) reads the whole file once just to work out types.
#   fine for 18 movies, slow for 18 million.
# - a guess can change from one file to the next. a file where every cast
#   list is empty gets guessed as array<string> and then cast.name breaks.
# - fields i do not list are just skipped, so new api fields can not break me.
# the plain fields come in as strings on purpose: this is the raw layer, and
# convert_dtypes does the real typing with try_cast so one bad value turns
# into a null instead of killing the job.
RAW_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("tagline", T.StringType()),
    T.StructField("release_date", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("adult", T.StringType()),
    T.StructField("imdb_id", T.StringType()),
    T.StructField("original_title", T.StringType()),
    T.StructField("video", T.StringType()),
    T.StructField("homepage", T.StringType()),
    T.StructField("original_language", T.StringType()),
    T.StructField("overview", T.StringType()),
    T.StructField("poster_path", T.StringType()),
    T.StructField("budget", T.StringType()),
    T.StructField("revenue", T.StringType()),
    T.StructField("runtime", T.StringType()),
    T.StructField("vote_count", T.StringType()),
    T.StructField("vote_average", T.StringType()),
    T.StructField("popularity", T.StringType()),
    T.StructField("belongs_to_collection", T.StructType([
        T.StructField("id", T.LongType()), T.StructField("name", T.StringType())])),
    T.StructField("genres", _named()),
    T.StructField("production_companies", _named("origin_country")),
    T.StructField("production_countries", T.ArrayType(T.StructType([
        T.StructField("iso_3166_1", T.StringType()), T.StructField("name", T.StringType())]))),
    T.StructField("spoken_languages", T.ArrayType(T.StructType([
        T.StructField("english_name", T.StringType()),
        T.StructField("iso_639_1", T.StringType()),
        T.StructField("name", T.StringType())]))),
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


def load_raw(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read the json lines file with my schema. bad lines go to _corrupt_record."""
    return (
        spark.read
        .schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(str(path))
    )


def drop_corrupt_rows(df: DataFrame) -> DataFrame:
    # i count these in main.py and log them. here they just get dropped.
    return df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")


def drop_irrelevant_columns(df: DataFrame) -> DataFrame:
    # drop() skips names that are not there, so no "if col in columns" check
    return df.drop(*DROP_COLUMNS)


def extract_collection_name(df: DataFrame) -> DataFrame:
    # belongs_to_collection is {id, name} or null. .name on a null struct is
    # just null, so no special case for standalone films.
    return df.withColumn("belongs_to_collection", F.col("belongs_to_collection.name"))


def pipe_join_names(df: DataFrame, column: str, name_key: str) -> DataFrame:
    """[{name: "Action"}, {name: "Drama"}] -> "Action|Drama", empty list -> null.

    genres.name on an array of structs gives an array of only the names,
    so concat_ws can join it. no python loop, this all stays in the jvm.
    """
    joined = F.concat_ws("|", F.col(f"{column}.{name_key}"))
    # concat_ws gives "" for an empty list. when() with no otherwise() is null.
    return df.withColumn(column, F.when(joined != "", joined))


def extract_credits(df: DataFrame) -> DataFrame:
    """cast, cast_size, director and crew_size out of the credits struct.

    the pandas version did this in the api client. here the raw file keeps
    the credits as they came and spark does it, so i can redo it without
    calling the api again.
    """
    cast_names = F.concat_ws("|", F.col("credits.cast.name"))
    # F.filter keeps only the crew entries whose job is Director. a film can
    # have two directors, so i keep all of them, not just the first one.
    directors = F.concat_ws("|", F.filter("credits.crew", lambda c: c["job"] == "Director")["name"])
    return (
        df.withColumn("cast", F.when(cast_names != "", cast_names))
        .withColumn("cast_size", F.coalesce(F.size("credits.cast"), F.lit(0)))
        .withColumn("director", F.when(directors != "", directors))
        .withColumn("crew_size", F.coalesce(F.size("credits.crew"), F.lit(0)))
        .drop("credits")
    )


def inspect_anomalies(df: DataFrame, columns: list[str], n: int = 10) -> dict[str, DataFrame]:
    """value_counts for the columns i built, to spot odd values before i trust them.

    a "A|B" cell counts once for A and once for B. gives back the small count
    tables so a test can check this ran.
    """
    out: dict[str, DataFrame] = {}
    for col in columns:
        counts = (
            df.select(F.explode(F.split(F.col(col), r"\|")).alias(col))
            .groupBy(col).count()
            .orderBy(F.desc("count"), col)
            .limit(n)
        )
        out[col] = counts
    return out


def convert_dtypes(df: DataFrame) -> DataFrame:
    # spark 4 has ansi mode on, so a plain .cast("double") on "abc" throws and
    # the whole job dies. try_cast gives null instead, which is what
    # pd.to_numeric(errors="coerce") did in the pandas version.
    return (
        df.withColumn("id", F.expr("try_cast(id AS BIGINT)"))
        .withColumn("budget", F.expr("try_cast(budget AS DOUBLE)"))
        .withColumn("revenue", F.expr("try_cast(revenue AS DOUBLE)"))
        .withColumn("popularity", F.expr("try_cast(popularity AS DOUBLE)"))
        .withColumn("vote_count", F.expr("try_cast(vote_count AS BIGINT)"))
        .withColumn("vote_average", F.expr("try_cast(vote_average AS DOUBLE)"))
        .withColumn("runtime", F.expr("try_cast(runtime AS DOUBLE)"))
        .withColumn("release_date", F.expr("CAST(try_to_timestamp(release_date, 'yyyy-MM-dd') AS DATE)"))
    )


def clean_unrealistic_values(df: DataFrame) -> DataFrame:
    # 0 budget / revenue / runtime means "tmdb does not know", not zero
    for col in ["budget", "revenue", "runtime"]:
        df = df.withColumn(col, F.when(F.col(col) > 0, F.col(col)))
    df = (
        df.withColumn("budget_musd", F.col("budget") / 1_000_000)
        .withColumn("revenue_musd", F.col("revenue") / 1_000_000)
        .drop("budget", "revenue")
    )
    # a movie nobody voted on shows 0.0, that is not a rating
    df = df.withColumn("vote_average", F.when(F.col("vote_count") > 0, F.col("vote_average")))
    for col in ["overview", "tagline"]:
        df = df.withColumn(col, F.when(~F.trim(F.col(col)).isin(PLACEHOLDERS), F.col(col)))
    return df


def filter_released_and_dedup(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("status") == "Released")
        .drop("status")
        # same id twice -> keep one. which one is not fixed in spark, fine here
        # because a repeat is the same api answer twice.
        .dropDuplicates(["id"])
        .dropna(subset=["id", "title"])
        # keep rows that have at least 10 real values
        .dropna(thresh=10)
    )


def reorder_and_reset(df: DataFrame) -> DataFrame:
    """Final column order plus a 0-based idx, like reset_index in pandas.

    spark rows have no index. row_number over a window with no partitionBy
    pulls every row into one partition to number them. ok for 18 movies;
    on a big table i would drop idx and just use id as the key.
    """
    idx = F.row_number().over(Window.orderBy("id")) - 1
    return df.select(idx.alias("idx"), *FINAL_COLUMNS)


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


def validate(df: DataFrame) -> dict[str, Any]:
    """Checks that must hold before anyone reads a number from this table.

    one pass over the data with a single agg, not one count() per rule.
    raises instead of printing, so a bad run stops here and does not write
    a wrong parquet or wrong charts.
    """
    checks = df.agg(
        F.count("*").alias("rows"),
        F.count_distinct("id").alias("distinct_ids"),
        F.count_if(F.col("title").isNull()).alias("null_titles"),
        F.count_if(F.col("budget_musd") <= 0).alias("bad_budget"),
        F.count_if(F.col("revenue_musd") <= 0).alias("bad_revenue"),
        F.count_if((F.col("vote_average") < 0) | (F.col("vote_average") > 10)).alias("bad_rating"),
    ).first()
    # an agg with no groupBy always gives exactly one row, this is for the type checker
    assert checks is not None
    problems: list[str] = []
    if checks["rows"] == 0:
        problems.append("no rows left after cleaning")
    if checks["distinct_ids"] != checks["rows"]:
        problems.append("duplicate ids")
    for name in ["null_titles", "bad_budget", "bad_revenue", "bad_rating"]:
        if checks[name] > 0:
            problems.append(f"{checks[name]} rows with {name}")
    if problems:
        raise ValueError("data checks failed: " + "; ".join(problems))
    log.info("data checks passed on %d rows", checks["rows"])
    return checks.asDict()
