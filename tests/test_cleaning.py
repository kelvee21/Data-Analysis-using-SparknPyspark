import pytest
from pyspark.sql import SparkSession

from src import cleaning
from tests.helpers import cleaned, first, movie, raw_df


def test_final_columns() -> None:
    assert len(cleaning.FINAL_COLUMNS) == 22
    assert cleaning.FINAL_COLUMNS[0] == "id"


def test_columns_come_out_in_brief_order(spark: SparkSession) -> None:
    assert cleaned(spark, movie()).columns == ["idx", *cleaning.FINAL_COLUMNS]


def test_nested_columns_become_pipe_text(spark: SparkSession) -> None:
    row = first(cleaned(spark, movie()))
    assert row.genres == "Action|Science Fiction"
    assert row.belongs_to_collection == "Test Collection"
    assert row.spoken_languages == "English"
    assert row.cast == "Actor One|Actor Two"
    assert row.cast_size == 2 and row.crew_size == 2
    assert row.director == "Dir One"


def test_two_directors_both_kept(spark: SparkSession) -> None:
    crew = [{"name": "A", "job": "Director"}, {"name": "B", "job": "Director"}]
    row = first(cleaned(spark, movie(credits={"cast": [], "crew": crew})))
    assert row.director == "A|B"


def test_empty_lists_become_null(spark: SparkSession) -> None:
    # with inferSchema this broke: all empty lists got guessed as array<string>.
    # the fixed schema means one movie on its own is enough now.
    row = first(cleaned(spark, movie(credits={"cast": [], "crew": []}, genres=[])))
    assert row.cast is None and row.director is None and row.genres is None
    assert row.cast_size == 0


def test_zero_money_is_null_and_rest_is_millions(spark: SparkSession) -> None:
    row = first(cleaned(spark, movie(budget=0, revenue=2_500_000)))
    assert row.budget_musd is None
    assert row.revenue_musd == 2.5


def test_no_votes_means_no_rating(spark: SparkSession) -> None:
    row = first(cleaned(spark, movie(vote_count=0, vote_average=0.0)))
    assert row.vote_average is None


def test_placeholders_become_null(spark: SparkSession) -> None:
    row = first(cleaned(spark, movie(overview="No Data", tagline="  ")))
    assert row.overview is None and row.tagline is None


def test_bad_values_become_null_not_a_crash(spark: SparkSession) -> None:
    row = first(cleaned(spark, movie(release_date="not a date", budget="lots")))
    assert row.release_date is None and row.budget_musd is None


def test_broken_json_line_is_dropped(spark: SparkSession) -> None:
    df = cleaning.clean(raw_df(spark, [movie()], extra_lines=['{"id": 2, "title": ']))
    assert df.count() == 1


def test_dupes_unreleased_and_no_title_removed(spark: SparkSession) -> None:
    df = cleaned(spark,
                 movie(id=1), movie(id=1),
                 movie(id=2, status="Post Production"),
                 movie(id=3, title=None),
                 movie(id=4))
    assert sorted(r.id for r in df.collect()) == [1, 4]


def test_idx_starts_at_zero(spark: SparkSession) -> None:
    df = cleaned(spark, movie(id=30), movie(id=10), movie(id=20))
    assert [(r.idx, r.id) for r in df.orderBy("idx").collect()] == [(0, 10), (1, 20), (2, 30)]


def test_inspect_anomalies_splits_cells(spark: SparkSession) -> None:
    df = cleaned(spark, movie(id=1), movie(id=2, genres=[{"id": 28, "name": "Action"}]))
    counts = cleaning.inspect_anomalies(df, ["genres"])["genres"]
    assert {r.genres: r["count"] for r in counts.collect()} == {"Action": 2, "Science Fiction": 1}


def test_validate_passes_on_good_data(spark: SparkSession) -> None:
    assert cleaning.validate(cleaned(spark, movie(id=1), movie(id=2)))["rows"] == 2


def test_validate_stops_on_empty_table(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="no rows"):
        cleaning.validate(cleaned(spark, movie(status="Rumored")))
