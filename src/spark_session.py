import logging
import os
import sys
import warnings
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)


def _saved_user_env(name: str) -> str | None:
    """Read a user env var straight from the registry (windows only)."""
    if sys.platform != "win32":
        return None
    import winreg

    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
            value, _ = winreg.QueryValueEx(key, name)
    except OSError:
        return None
    return str(value)


def ensure_hadoop_home() -> None:
    """On windows, point the jvm at winutils.exe before spark starts.

    spark needs winutils.exe + hadoop.dll to write any file on windows.
    setx only reaches programs opened after it, so a vs code that was
    already open (and the jupyter kernel it starts) never sees HADOOP_HOME
    and the parquet write dies with "HADOOP_HOME and hadoop.home.dir are
    unset". so: real env var first, then .env, then the value saved in the
    registry. the bin folder goes on PATH too, that is where hadoop.dll is.
    """
    if sys.platform != "win32":
        return
    home = os.getenv("HADOOP_HOME") or _saved_user_env("HADOOP_HOME")
    if not home or not (Path(home) / "bin" / "winutils.exe").exists():
        warnings.warn(
            "HADOOP_HOME is not set or has no bin\\winutils.exe, "
            "reads work but writing parquet will fail (see study/01 §4)",
            stacklevel=2,
        )
        return
    os.environ["HADOOP_HOME"] = home
    bin_dir = str(Path(home) / "bin")
    if bin_dir.lower() not in os.environ.get("PATH", "").lower():
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")


def get_spark(app_name: str = "tmdb-movie-analysis") -> SparkSession:
    """Build (or reuse) the one SparkSession the notebook, main and tests share.

    every setting comes from here so the notebook and the script never
    disagree. the master comes from SPARK_MASTER so the same code can be
    pointed at a cluster later; on my laptop it stays local[*].
    """
    # on windows the workers can pick up a different python than the driver
    # and then crash on import. pin both to the python running this file.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # pyspark 4.3 warns on every toPandas() when pandas 3 is installed. the
    # calls i make (small tables, plain columns) work fine, so hide that one.
    warnings.filterwarnings("ignore", message="PySpark does not yet fully support pandas")

    load_dotenv()  # so the SPARK_ settings (and HADOOP_HOME) in .env count, not only real env vars
    # has to happen before getOrCreate, the jvm copies the env when it starts
    ensure_hadoop_home()
    master = os.getenv("SPARK_MASTER", "local[*]")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # default is 200 shuffle partitions. for 18 rows that is 200 near
        # empty tasks after every groupBy. AQE would merge them anyway, but
        # starting small keeps the spark ui readable.
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # dates: same answer on every machine, no matter the laptop timezone
        .config("spark.sql.session.timeZone", "UTC")
        # arrow makes toPandas() a columnar copy instead of row by row.
        # fallback means it still works if pyarrow is missing.
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    log.info("spark %s on %s, ui at %s", spark.version, master, spark.sparkContext.uiWebUrl)
    return spark
