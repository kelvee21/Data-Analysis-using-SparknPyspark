# tmdb movie analysis with spark

DEM05 lab 1. same lab as my pandas one (DEM02 tmdb-movie-analysis), redone
with pyspark: fetch 18 movies, clean the table, check profit, ratings and
distributions, then chart it. same repo layout and same function names as
the pandas version so the two are easy to compare.

- lab brief: [Data-Analysis-using-SparknPyspark.md](Data-Analysis-using-SparknPyspark.md)
- report: [reports/final_report.md](reports/final_report.md), all tables in [reports/results.md](reports/results.md)
- study notes: [study/](study/), open `study/index.html` in a browser

## run

needs java 17 and python 3.10+. on windows also winutils.exe with HADOOP_HOME
set (see [study/01-setup-windows.md](study/01-setup-windows.md)). `get_spark()`
also reads HADOOP_HOME from `.env` or from the value windows saved for your
account, so a vs code / jupyter opened before you set it still works.

```
pip install -r requirements.txt
copy .env.example .env
pytest
pyright
python -m src.tmdb_client
python -m src.main
```

`pyright` is the type check. every function has type hints and
`pyrightconfig.json` puts it in strict mode, so vscode shows real types
(DataFrame, Column, Axes) on hover and in autocomplete instead of Unknown.
it should say 0 errors.

open analysis.ipynb and run it top to bottom. it calls the same functions as
`src.main`, one step per section.

`src.tmdb_client` writes data/raw_movies.jsonl. after that nothing needs the
api: `src.main` and the notebook read the file. if the file is missing,
`src.main` fetches it first. `python -m src.main --fetch` forces a new fetch.

## files

- src/ - all source lives here
- src/spark_session.py - the one SparkSession everything uses, settings explained inside
- src/tmdb_client.py - fetch from the api with retries, save as json lines
- src/cleaning.py - raw schema, one function per cleaning rule, data checks
- src/kpis.py - profit, roi, the ranking function, searches, groups
- src/visualize.py - 7 plots (5 question charts + distribution + boxplot)
- src/main.py - glue: runs it all, writes parquet, results.md and 8 charts
- tests/ - 32 pytest tests on fake rows, no network needed
- pyrightconfig.json - strict type check settings, vscode / pylance read it too
- data/ - raw json lines and the cleaned parquet (not committed)
- reports/ - final report, results.md, chart1-8.png
- study/ - my study notes for this lab
- analysis.ipynb - all steps incl. eda, runs top to bottom

## spark choices

the pandas version could get away with anything at 18 rows. here i wrote it
the way it would have to run on a real table:

- **schema on read.** `cleaning.RAW_SCHEMA` instead of letting spark guess.
  no extra pass over the file, the types never change between runs, and a
  broken json line goes to `_corrupt_record` and gets counted, not crashed on.
- **built in functions, no python udf in the pipeline.** everything is
  `F.` column code so it stays in the jvm and catalyst can optimise it.
  `kpis.roi_udf` exists only so the notebook can show the difference.
- **try_cast.** spark 4 runs in ansi mode, a bad value would kill the job.
  try_cast turns it into a null like `errors="coerce"` did in pandas.
- **cache what gets reused, then unpersist.** the cleaned table feeds ten
  rankings, three groupings and eight charts. without cache spark redoes the
  read and clean for each one.
- **only small results go to the driver.** histograms and boxplots are
  counted in spark (buckets, percentile_approx) and only the numbers come
  back. the two scatter plots are the only toPandas() of whole columns.
- **data checks that stop the run.** `cleaning.validate` checks row count,
  unique ids and value ranges in one aggregation and raises if anything is
  off, so a bad run never writes a parquet or charts.
- **parquet output, overwrite mode.** typed and columnar, and a rerun gives
  the same result instead of adding to it.
- **config from env.** api key, spark master, shuffle partitions and log
  level come from env vars / .env, nothing secret in code.
- **spark.stop() in finally.** the jvm is released even when a step fails.

one thing i left on purpose: `idx` uses `row_number()` over a window with no
partition, which moves every row to one partition (spark warns about it in
the log). the brief asks for a reset index; on a big table i would drop idx
and key on `id`.
