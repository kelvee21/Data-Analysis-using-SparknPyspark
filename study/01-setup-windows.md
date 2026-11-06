# 01 — Setting Up Spark on Windows

What you need installed to run this lab on a Windows laptop, what each piece
does, how to check it works, and how to fix the errors that actually came up.

> **Help:** if words like "driver", "JVM" or "partition" are new, read
> [02 · What Spark is](02-what-is-spark.md) first. This page only says enough
> to get things running.

## 1. The pieces and why each one is needed

PySpark looks like a normal Python library, but it is not pure Python. The
real work is done by Spark, which is written in Scala and runs on the Java
Virtual Machine. Your Python code sends instructions to that Java process.
That is why a Python lab needs Java.

| Piece | What it is | Why this lab needs it | Version on this laptop |
|---|---|---|---|
| **Java (JDK)** | The Java Development Kit. It contains the **JVM** (Java Virtual Machine), the program that runs Java code | Spark itself runs inside a JVM | Temurin OpenJDK 17 |
| **Python** | The language you write the lab in | Your scripts, `requests`, Matplotlib, pytest, the notebook | 3.13 |
| **PySpark** | The Python package that starts Spark and talks to it. It ships the Spark jars (Java libraries) inside it | Steps 2, 3 and the counting behind Step 4 | 4.3 |
| **PyArrow** | A library for moving whole columns of data between programs | Makes `toPandas()` fast (see §9) | from `requirements.txt` |
| **winutils.exe + hadoop.dll** | Two small Windows helper files from the Hadoop project | Spark uses Hadoop code to write files. On Windows that code calls these native helpers | from `hadoop-3.3.6` |
| **HADOOP_HOME** | An environment variable pointing at the folder that holds `bin\winutils.exe` | Tells Spark where to find the helpers | `C:\Users\User\hadoop` |
| **TMDB API key** | A secret string from themoviedb.org that identifies you to the API | Step 1 downloads the movies | stored in `.env` |
| **`.env` file** | A text file of `NAME=value` lines, read by `python-dotenv` | Holds the key and optional Spark settings, out of the code and out of git | copy of `.env.example` |

An **environment variable** is a named value that the operating system
hands to every program it starts. `HADOOP_HOME`, `PATH` and `JAVA_HOME` are
examples. `PATH` is a list of folders Windows searches when you type a
command name like `java` or `winutils`.

## 2. Install Java 17

Spark 4 needs Java 17 or 21. This laptop uses Temurin 17 (a free build of
OpenJDK from adoptium.net). Install it with the option "Set JAVA_HOME"
ticked. Then open a **new** terminal and check:

```bash
java -version
```

The first line should start with `openjdk version "17.`.

If `java` is "not recognized", the installer did not add Java to `PATH`.
Reinstall with the PATH option, or add the JDK's `bin` folder to `PATH` by
hand.

## 3. Install the Python packages

From the project folder:

```bash
pip install -r requirements.txt
```

`requirements.txt`:

```
# DEM05 lab 1: tmdb analysis with pyspark. tested on python 3.13, pyspark 4.3, java 17
pyspark>=4.0
pyarrow>=15.0
pandas>=2.0
requests>=2.31
matplotlib>=3.10
python-dotenv>=1.0
jupyter>=1.0
pytest>=8.0
```

| Package | Used for |
|---|---|
| `pyspark` | Everything in `src/cleaning.py` and `src/kpis.py` |
| `pyarrow` | Fast `toPandas()` for the charts |
| `pandas` | Only the small results handed to Matplotlib |
| `requests` | The 19 API calls in `src/tmdb_client.py` |
| `matplotlib` | The 8 charts in `reports/` |
| `python-dotenv` | Reads `.env` |
| `jupyter` | Runs `analysis.ipynb` |
| `pytest` | The 32 tests in `tests/` |

Check they import:

```bash
python -c "import pyspark, pyarrow, pandas, matplotlib, pytest; print(pyspark.__version__)"
```

## 4. winutils and HADOOP_HOME (Windows only)

Spark borrows Hadoop's file-system code to read and write files. On Linux
and macOS that code works on its own. On Windows it calls two native files,
`winutils.exe` and `hadoop.dll`, which are not shipped with PySpark.
Without them, reading works, but **writing Parquet fails**.

Steps used on this laptop:

1. Download `winutils.exe` and `hadoop.dll` from the `hadoop-3.3.6/bin`
   folder of `github.com/cdarlint/winutils`.
2. Put both in `C:\Users\User\hadoop\bin`.
3. Set two **user-level** environment variables (Start → "Edit environment
   variables for your account"):
   - `HADOOP_HOME` = `C:\Users\User\hadoop` (the folder *above* `bin`)
   - add `%HADOOP_HOME%\bin` to `PATH`
4. **Close and reopen every terminal** (and VS Code, and Jupyter). A
   program reads its environment once, when it starts. Terminals opened
   before the change still have the old values.

Check in a new PowerShell window:

```powershell
echo $env:HADOOP_HOME          # C:\Users\User\hadoop
where.exe winutils             # C:\Users\User\hadoop\bin\winutils.exe
```

**What `get_spark()` does for you.** Step 4 is easy to forget, and VS Code
starts its Jupyter kernel with VS Code's own (old) environment. The kernel
then has no `HADOOP_HOME`, and the first Parquet write fails with
`HADOOP_HOME and hadoop.home.dir are unset`. So `ensure_hadoop_home()` in
`src/spark_session.py` runs just before Spark starts. It looks for the
folder in this order: the real environment variable, a `HADOOP_HOME=` line in
`.env`, then the user variable Windows saved in the registry (where step 3
put it). It also adds `bin` to `PATH` for `hadoop.dll`. This only helps a
*new* session. If Spark is already running in the kernel, restart the
kernel, because the JVM copied its environment when it started.

If you cannot change the user variables, set them for one PowerShell
session only:

```powershell
$env:HADOOP_HOME = "C:\Users\User\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
```

## 5. The TMDB key and `.env`

1. Create a free account on themoviedb.org, then Settings → API, and copy
   the **API Key** (the short "v3" one, not the long token).
2. Copy the template and fill it in:

```bash
copy .env.example .env      # PowerShell / cmd
```

`.env.example` holds the key plus three optional Spark settings, shown with
their defaults:

```
# copy to .env and fill in. .env is gitignored, never commit it.
# key from themoviedb.org -> settings -> api -> "api key" (v3)
TMDB_API_KEY=
TMDB_BASE_URL=https://api.themoviedb.org/3
# optional spark settings, these are the defaults
SPARK_MASTER=local[*]
SPARK_SHUFFLE_PARTITIONS=4
SPARK_LOG_LEVEL=WARN
```

| Variable | Read by | Meaning |
|---|---|---|
| `TMDB_API_KEY` | `tmdb_client.fetch_all` | Your key. Empty → the fetch stops with a message |
| `TMDB_BASE_URL` | `tmdb_client.fetch_all` | The API's address |
| `SPARK_MASTER` | `spark_session.get_spark` | Where Spark runs; `local[*]` = all cores of this laptop |
| `SPARK_SHUFFLE_PARTITIONS` | `get_spark` | Pieces after a `groupBy` or sort (see [02](02-what-is-spark.md#h-4-partitions)) |
| `SPARK_LOG_LEVEL` | `get_spark` | How much Spark prints: `WARN`, `ERROR`, `INFO` |

`.env` is listed in `.gitignore`, so git never commits it. A key pushed to
GitHub is public and must be revoked.

## 6. A five-line smoke test

A **smoke test** is the smallest run that proves the setup works before you
try the real thing. Save as `smoke.py` anywhere and run `python smoke.py`:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
df = spark.createDataFrame([(1, "Avatar"), (2, "Titanic")], ["id", "title"])
df.show()
spark.stop()
```

Expected (after about 20 seconds of start-up and some log lines):

```
+---+-------+
| id|  title|
+---+-------+
|  1| Avatar|
|  2|Titanic|
+---+-------+
```

This proves Java, PySpark and Python talk to each other. It does **not**
prove winutils works, because nothing is written to disk. To test that, add
one line before `spark.stop()`:

```python
df.write.mode("overwrite").parquet("smoke.parquet")
```

If a `smoke.parquet` folder appears, writing works. Delete it afterwards.

## 7. Running the lab

The code is a flat package in `src/`, so every command runs from the
**project root** with `python -m`:

```bash
python -m src.tmdb_client      # Step 1 only: call the API, write data/raw_movies.jsonl
python -m src.main             # everything; fetches first only if the raw file is missing
python -m src.main --fetch     # force a new fetch, then everything
```

`python -m src.main` means "run the module `main` inside the package
`src`". Running `python src/main.py` instead fails, because
`from src import cleaning` only works when the project root is the
starting point (see [03 · The Python you need](03-python-you-need.md#h-5-modules-packages-and-imports)).

What a full run produces (from `reports/results.md`):

| Stage | Result |
|---|---|
| Fetch | 19 requests; ID `0` returns 404 and is skipped; 18 movies saved to `data/raw_movies.jsonl` |
| Load | `raw: 18 movies (0 unreadable lines)` |
| Clean, profit/ROI, checks | `after cleaning: 18 movies, 25 columns. data checks passed.` |
| Write | `data/movies_clean.parquet` (a folder; needs winutils) |
| Report | `reports/results.md` and `reports/chart1.png` to `chart8.png` |

The notebook `analysis.ipynb` (project root) calls the same functions one
step per section. Open it with `jupyter notebook analysis.ipynb` and run it
top to bottom.

## 8. Running the tests

```bash
pytest
```

There are **32 tests** in four files: `test_cleaning.py` (15),
`test_kpis.py` (10), `test_tmdb_client.py` (5) and `test_visualize.py` (2).
They need no network and no raw file: `tests/helpers.py` builds fake movie
records. The 5 client tests use a fake HTTP session, so they never call
TMDB.

Expect the Spark tests to be slow on this laptop: one full run printed
`32 passed in 634.08s (0:10:34)`, about ten minutes. That is not a bug. Every test ends in a Spark **action** (`.first()`,
`.collect()`, `.count()`), and each action has a fixed cost of planning,
scheduling and starting tasks, whether the data has 1 row or 1 million.
See [10 · The pipeline script and tests](10-pipeline-and-tests.md).

## 9. Console noise you can ignore

Spark prints a lot. These lines are harmless on this laptop:

| Message | Why it appears | Action |
|---|---|---|
| `Missing Python executable 'C:\Program Files\Python313\python.exe', defaulting to ... for SPARK_HOME` | The `pyspark` launcher script looks for `python.exe` in a fixed place and guesses `SPARK_HOME` from the package folder instead. The guess is correct | Ignore |
| `The system cannot find the path specified.` | Printed by the same Windows launcher script | Ignore |
| A `WARN` about "No Partition Defined for Window operation" | `reorder_and_reset` numbers rows with a window that has no partition, which moves every row to one partition. Intentional for 18 rows (see [06](06-cleaning-line-by-line.md#h-11-reorder-and-reset-steps-2-10-and-2-11)) | Ignore |
| `[Stage 12:> (0 + 4) / 4]` progress bars | Spark showing tasks finishing | Ignore |
| An error deleting a temp folder (jar files) at shutdown | Windows will not delete a file another process still has open. The run already finished | Ignore; delete the temp folder by hand later |

Two warnings from older versions of this lab no longer appear:

- `FutureWarning: PySpark does not yet fully support pandas >= 3.0.0` is
  hidden on purpose in `get_spark()` with `warnings.filterwarnings(...)`.
- The "PyArrow not installed, falling back" warning is gone because
  `pyarrow` is now in `requirements.txt`, and `get_spark()` turns Arrow on.

## 10. Troubleshooting: real errors and their fixes

| Error you see | Cause | Fix |
|---|---|---|
| `HADOOP_HOME and hadoop.home.dir are unset` on the Parquet write in the notebook | The kernel started before `HADOOP_HOME` existed and Spark was already running | Restart the kernel and run all; `get_spark()` now finds it (§4) |
| `java.lang.UnsatisfiedLinkError: ... NativeIO$Windows.access0` when writing Parquet | `winutils.exe` / `hadoop.dll` missing, or `HADOOP_HOME` not set in *this* terminal | §4; then open a new terminal |
| `no TMDB_API_KEY in .env. copy .env.example to .env and add the key.` | No `.env` file, or the key line is empty | §5 |
| `HTTPError: 401 Client Error` during the fetch | The key is wrong (or you pasted the long v4 token) | Copy the v3 "API Key" again |
| `ModuleNotFoundError: No module named 'src'` | You ran `python src/main.py`, or you are not in the project root | `cd` to the project root, then `python -m src.main` |
| `AnalysisException ... cast[name] ... requires the "INTEGRAL" type` | Seen in an earlier version that let Spark guess the schema: when every test record had an empty `cast` list, Spark guessed `array<string>`, and a string has no `name` field | Fixed for good by the explicit `RAW_SCHEMA`; see [05 · DataFrame fundamentals](05-dataframe-fundamentals.md#h-3-schemas-inference-versus-an-explicit-schema) |
| `[CAST_INVALID_INPUT] The value 'abc' ... cannot be cast to "INT"` | PySpark 4 has **ANSI mode** on: `.cast()` fails on bad input instead of returning null | Use `try_cast` as `cleaning.convert_dtypes` does; see [06](06-cleaning-line-by-line.md#h-8-convert-dtypes-step-2-5) |
| `ValueError: data checks failed: ...` | `cleaning.validate` found a problem (no rows, duplicate ids, bad values) and stopped the run on purpose | Read the message; fix the data or the cleaning rule. See [06](06-cleaning-line-by-line.md#h-12-validate-data-checks-that-stop-the-run) |

## Self-check

1. Why does a Python lab need Java installed? [Spark runs inside a JVM; PySpark only sends it instructions.]
2. The smoke test passes but `python -m src.main` fails writing Parquet. What is missing? [winutils.exe/hadoop.dll and `HADOOP_HOME`, or the terminal was opened before setting them.]
3. Why `python -m src.main` and not `python src/main.py`? [`main.py` imports `from src import ...`, which works only when Python starts from the project root as a package.]
4. Which `.env` setting would point the same code at a cluster? [`SPARK_MASTER`.]
5. Why are the Spark tests slow even on tiny fake data? [Each Spark action has a fixed overhead regardless of data size.]
