"""Run the whole lab: fetch (if needed) -> clean -> checks -> kpis -> charts.

    python -m src.main            # reuse data/raw_movies.jsonl if it is there
    python -m src.main --fetch    # call the api again first

writes data/movies_clean.parquet, reports/results.md and reports/chart*.png.
the notebook runs the same functions step by step.
"""
import argparse
import logging
import sys
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import cleaning, kpis, visualize
from src.spark_session import get_spark
from src.tmdb_client import RAW_PATH, fetch_all, save_jsonl

ROOT = Path(__file__).resolve().parents[1]
CLEAN_PATH = ROOT / "data" / "movies_clean.parquet"
REPORTS = ROOT / "reports"

log = logging.getLogger("main")


def md_table(df: DataFrame, max_rows: int = 50) -> str:
    """Small spark DataFrame -> markdown table for results.md."""
    rows = df.limit(max_rows).collect()
    if not rows:
        return "_no rows_\n"

    def fmt(v: object) -> str:
        if v is None:
            return ""
        if isinstance(v, float):
            return f"{v:,.2f}"
        return str(v).replace("|", "\\|")

    head = "| " + " | ".join(df.columns) + " |\n|" + "---|" * len(df.columns) + "\n"
    return head + "".join("| " + " | ".join(fmt(r[c]) for c in df.columns) + " |\n" for r in rows)


def save_chart(ax: Axes, name: str) -> Path:
    # root=True gives the Figure, not a SubFigure, so tight_layout is there
    fig = ax.get_figure(root=True)
    assert fig is not None
    fig.tight_layout()
    path = REPORTS / name
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path


def run(spark: SparkSession, out: list[str]) -> list[Path]:
    df_raw = cleaning.load_raw(spark, RAW_PATH).cache()
    # counting only _corrupt_record straight off the file is not allowed in
    # spark, the cache above is what makes this count work
    bad = df_raw.filter(F.col("_corrupt_record").isNotNull()).count()
    raw_rows = df_raw.count()
    log.info("raw rows: %d, unreadable lines: %d", raw_rows, bad)

    # cache because every kpi, table and chart below reads this same table.
    # without it spark would redo the read + clean for each one.
    df_movies = kpis.add_profit_roi(cleaning.clean(df_raw)).cache()
    checks = cleaning.validate(df_movies)
    df_raw.unpersist()

    # parquet keeps the types (dates stay dates) and is columnar, so the next
    # job only reads the columns it needs. coalesce(1): 18 rows do not need
    # 4 tiny files. overwrite makes a rerun give the same output.
    df_movies.coalesce(1).write.mode("overwrite").parquet(str(CLEAN_PATH))
    log.info("wrote %s", CLEAN_PATH)

    out.append("# results\n\nmade by `python -m src.main`. money columns are in million usd.\n")
    out.append(f"## data\n\nraw: {raw_rows} movies ({bad} unreadable lines). after cleaning: "
               f"{checks['rows']} movies, {len(df_movies.columns)} columns. data checks passed.\n")
    out.append("### cleaned movies\n\n" + md_table(df_movies.select(
        "idx", "id", "title", "release_date", "budget_musd", "revenue_musd", "vote_count",
        "vote_average", "popularity", "runtime", "director")))

    out.append("## value counts (step 2.4)\n")
    extracted = ["genres", "original_language", "production_countries", "spoken_languages",
                 "belongs_to_collection"]
    for col, counts in cleaning.inspect_anomalies(df_movies, extracted, n=15).items():
        out.append(f"### {col}\n\n" + md_table(counts))
    nulls = df_movies.select([F.count_if(F.col(c).isNull()).alias(c) for c in df_movies.columns])
    out.append("### nulls per column after cleaning\n\n" + md_table(nulls))

    out.append("## best and worst movies (step 3.1)\n")
    for name, frame in kpis.rankings(df_movies).items():
        out.append(f"### {name}\n\n" + md_table(frame))

    out.append("## searches (step 3.2)\n")
    for name, frame in [("search 1: sci-fi action with bruce willis",
                         kpis.search_willis_scifi_action(df_movies)),
                        ("search 2: uma thurman, directed by quentin tarantino",
                         kpis.search_thurman_tarantino(df_movies))]:
        found = frame.cache()
        out.append(f"### {name}\n\n{found.count()} matches.\n\n" + md_table(found))
        found.unpersist()

    out.append("## franchise vs standalone (step 3.3)\n\n" + md_table(kpis.franchise_vs_standalone(df_movies)))
    out.append("## most successful franchises (step 3.4)\n\n" + md_table(kpis.top_franchises(df_movies)))
    out.append("## most successful directors (step 3.5)\n\n" + md_table(kpis.top_directors(df_movies)))

    charts = [
        save_chart(visualize.plot_revenue_vs_budget(df_movies), "chart1.png"),
        save_chart(visualize.plot_roi_by_genre(df_movies), "chart2.png"),
        save_chart(visualize.plot_popularity_vs_rating(df_movies), "chart3.png"),
        save_chart(visualize.plot_yearly_box_office(df_movies), "chart4.png"),
        save_chart(visualize.plot_franchise_vs_standalone(df_movies), "chart5.png"),
        save_chart(visualize.plot_distribution(df_movies, "revenue_musd"), "chart6.png"),
        save_chart(visualize.plot_boxplot(df_movies, "runtime"), "chart7.png"),
        save_chart(visualize.plot_boxplot(df_movies, "popularity"), "chart8.png"),
    ]
    out.append("## charts (step 4)\n\n" + "".join(f"![{p.stem}]({p.name})\n\n" for p in charts))
    df_movies.unpersist()
    return charts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fetch", action="store_true", help="call the tmdb api even if the raw file exists")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    # no window, just files. set here and not at import, so importing main
    # from the notebook does not turn off the inline charts.
    matplotlib.use("Agg")

    if args.fetch or not RAW_PATH.exists():
        save_jsonl(fetch_all())

    REPORTS.mkdir(exist_ok=True)
    spark = get_spark()
    out: list[str] = []
    try:
        charts = run(spark, out)
    except Exception:
        log.exception("pipeline failed, nothing more written")
        sys.exit(1)
    finally:
        # always give the jvm back, even when a step failed
        spark.stop()

    (REPORTS / "results.md").write_text("\n".join(out), encoding="utf-8")
    log.info("wrote reports/results.md and %d charts", len(charts))


if __name__ == "__main__":
    main()
