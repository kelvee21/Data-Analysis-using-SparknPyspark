from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import kpis
from tests.helpers import enriched, first, movie


def test_profit_and_roi(spark: SparkSession) -> None:
    row = first(enriched(spark, movie(budget=100_000_000, revenue=450_000_000)))
    assert row.profit_musd == 350.0
    assert row.roi == 4.5


def test_rank_skips_nulls_and_uses_threshold(spark: SparkSession) -> None:
    df = enriched(spark,
                  movie(id=1, title="Cheap", budget=5_000_000, revenue=100_000_000),  # roi 20, budget < 10M
                  movie(id=2, title="Big", budget=100_000_000, revenue=300_000_000),  # roi 3
                  movie(id=3, title="NoBudget", budget=0, revenue=900_000_000))       # roi null
    top = kpis.rank(df, "roi", where=F.col("budget_musd") >= kpis.ROI_MIN_BUDGET_MUSD).collect()
    assert [r.title for r in top] == ["Big"]
    low = [r.title for r in kpis.rank(df, "roi", ascending=True).collect()]
    assert low[0] == "Big" and "NoBudget" not in low


def test_rankings_has_all_ten_lists(spark: SparkSession) -> None:
    assert len(kpis.rankings(enriched(spark, movie()))) == 10


def test_has_matches_whole_names_only(spark: SparkSession) -> None:
    df = enriched(spark, movie(credits={"cast": [{"name": "Bruce Willis Jr"}], "crew": []}))
    assert df.filter(kpis.has("cast", "Bruce Willis")).count() == 0


def test_willis_search(spark: SparkSession) -> None:
    credits = {"cast": [{"name": "Bruce Willis"}], "crew": []}
    hit = movie(id=1, title="Die Hard in Space", credits=credits)
    miss = movie(id=2, title="Other", genres=[{"id": 18, "name": "Drama"}], credits=credits)
    titles = [r.title for r in kpis.search_willis_scifi_action(enriched(spark, hit, miss)).collect()]
    assert titles == ["Die Hard in Space"]


def test_tarantino_search_shortest_first(spark: SparkSession) -> None:
    credits = {"cast": [{"name": "Uma Thurman"}],
               "crew": [{"name": "Quentin Tarantino", "job": "Director"}]}
    df = enriched(spark, movie(id=1, title="Long", runtime=150, credits=credits),
                  movie(id=2, title="Short", runtime=110, credits=credits))
    assert [r.title for r in kpis.search_thurman_tarantino(df).collect()] == ["Short", "Long"]


def test_codirectors_both_get_the_revenue(spark: SparkSession) -> None:
    crew = [{"name": "A", "job": "Director"}, {"name": "B", "job": "Director"}]
    df = enriched(spark, movie(credits={"cast": [], "crew": crew}, revenue=200_000_000))
    assert {r.director: r.total_revenue for r in kpis.top_directors(df).collect()} == {"A": 200.0, "B": 200.0}


def test_franchise_split(spark: SparkSession) -> None:
    df = enriched(spark, movie(id=1), movie(id=2, belongs_to_collection=None))
    assert {r.group: r.movies for r in kpis.franchise_vs_standalone(df).collect()} == {
        "franchise": 1, "standalone": 1}


def test_top_franchises_sums_revenue(spark: SparkSession) -> None:
    df = enriched(spark, movie(id=1, revenue=100_000_000), movie(id=2, revenue=300_000_000))
    row = first(kpis.top_franchises(df))
    assert row.franchise == "Test Collection" and row.movies == 2 and row.total_revenue == 400.0


def test_udf_gives_same_roi_as_builtin(spark: SparkSession) -> None:
    df = enriched(spark, movie(id=1), movie(id=2, budget=0))
    rows = df.select("roi", kpis.roi_udf("revenue_musd", "budget_musd").alias("u")).collect()
    assert all(r.roi == r.u for r in rows)
