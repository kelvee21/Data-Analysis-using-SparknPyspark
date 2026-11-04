import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from pyspark.sql import SparkSession

from src import visualize
from tests.helpers import enriched, movie


def test_histogram_counts_every_movie(spark: SparkSession) -> None:
    df = enriched(spark, *[movie(id=i, runtime=90 + i * 10) for i in range(1, 6)])
    ax = visualize.plot_distribution(df, "runtime", bins=4)
    assert sum(p.get_height() for p in ax.patches if isinstance(p, Rectangle)) == 5
    plt.close("all")


def test_boxplot_draws(spark: SparkSession) -> None:
    df = enriched(spark, *[movie(id=i, runtime=100 + i) for i in range(1, 6)], movie(id=9, runtime=400))
    ax = visualize.plot_boxplot(df, "runtime")
    assert ax.get_title() == "runtime boxplot"
    plt.close("all")
