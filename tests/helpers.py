"""Fake movie records so the tests never need the api or the raw file.

same shape as one line of data/raw_movies.jsonl (a /movie/{id} answer with
append_to_response=credits), with only the fields the code reads.
"""
import json
from collections.abc import Sequence

from pyspark.sql import DataFrame, Row, SparkSession

from src import cleaning, kpis
from src.tmdb_client import Movie


def movie(**changes: object) -> Movie:
    m: Movie = {
        "adult": False, "id": 1, "imdb_id": "tt1", "original_title": "T", "video": False,
        "homepage": "", "title": "Test Movie", "tagline": "a tagline", "status": "Released",
        "release_date": "2019-04-24", "original_language": "en", "overview": "some plot.",
        "budget": 100_000_000, "revenue": 500_000_000, "runtime": 120,
        "vote_count": 100, "vote_average": 7.5, "popularity": 20.0, "poster_path": "/p.jpg",
        "belongs_to_collection": {"id": 9, "name": "Test Collection"},
        "genres": [{"id": 28, "name": "Action"}, {"id": 878, "name": "Science Fiction"}],
        "production_companies": [{"id": 1, "name": "Studio A"}],
        "production_countries": [{"iso_3166_1": "US", "name": "United States of America"}],
        "spoken_languages": [{"english_name": "English", "iso_639_1": "en", "name": "English"}],
        "credits": {"cast": [{"name": "Actor One"}, {"name": "Actor Two"}],
                    "crew": [{"name": "Dir One", "job": "Director"},
                             {"name": "Writer", "job": "Screenplay"}]},
    }
    m.update(changes)
    return m


def raw_df(spark: SparkSession, records: list[Movie], extra_lines: Sequence[str] = ()) -> DataFrame:
    """Read fake records the same way main.py reads the file: json text + my schema."""
    lines = [json.dumps(r) for r in records] + list(extra_lines)
    rdd = spark.sparkContext.parallelize(lines)
    return (spark.read.schema(cleaning.RAW_SCHEMA)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(rdd))


def cleaned(spark: SparkSession, *records: Movie) -> DataFrame:
    return cleaning.clean(raw_df(spark, list(records)))


def enriched(spark: SparkSession, *records: Movie) -> DataFrame:
    return kpis.add_profit_roi(cleaned(spark, *records))


def first(df: DataFrame) -> Row:
    """df.first() is typed Row | None. every test table here has rows, so
    this saves an "is not None" check in each test."""
    row = df.first()
    assert row is not None, "expected at least one row"
    return row
