# 15 — Review-Day Runbook

Everything needed for the review is already on disk: the raw API file, the
clean Parquet, `reports/results.md`, the final report and the eight charts.
Nothing has to be fetched during the review. This page is the order to show
things in, the exact commands, what each prints, and how long it takes on
this laptop.

> **Help:** if a reviewer asks "why?" at any step, the answers are in [12 · Code-review defense](12-code-review-defense.md) and [11 · Hard concepts](11-hard-concepts-explained.md). If Spark itself will not start, see [01 · Setting up Spark on Windows](01-setup-windows.md).

## What state the project is in

| Item | Where | State |
|---|---|---|
| Raw API answers | `data/raw_movies.jsonl` | 18 lines, fetched 4 November 2026; no API key needed to rerun |
| Clean table | `data/movies_clean.parquet/` | 18 rows, 25 columns |
| All tables | `reports/results.md` | made by `python -m src.main` |
| Report | `reports/final_report.md` | summary, method, results, limitations |
| Charts | `reports/chart1.png` … `chart8.png` | 5 question charts + revenue histogram + 2 boxplots |
| Notebook | `analysis.ipynb` | runs the same functions step by step |
| Tests | `tests/` | 32 tests, all pass |

`data/` is git-ignored (except `data/README.md`), so the raw file exists on
this laptop but not in a fresh clone.

**Measured on this laptop** (Windows 11, Python 3.13, PySpark 4.3, Java 17):

| Command | Time | Result |
|---|---|---|
| `python -m src.main` | **39.3 s**; an earlier run the same morning took 82.5 s | `wrote reports/results.md and 8 charts` |
| Notebook, run top to bottom | **50.3 s** | all cells run, Parquet rewritten, Spark stopped |
| `pytest` (all 32) | **590 s** (9 min 50 s) | `32 passed` |
| `pytest tests/test_tmdb_client.py` | **4 s** (`5 passed in 0.53s`) | no Spark needed |

The full test run is too slow to do live. Run it the evening before and
show the result line, or run only the client tests live.

---

## 10 minutes before the review

Open PowerShell in the lab folder. Every Spark command below starts with the
two Windows variables, so it works in a fresh window:

```powershell
cd C:\Workstation\Amalitech\Week-5\Data-Analysis-using-SparknPyspark
$env:HADOOP_HOME="C:\Users\User\hadoop"; $env:PATH="C:\Users\User\hadoop\bin;"+$env:PATH
```

**1. Java and PySpark are there** (a few seconds):

```powershell
java -version
python -c "import pyspark; print(pyspark.__version__)"
```

Must show Java 17 and `4.3.0`.

**2. The raw file is there** (instant):

```powershell
(Get-Content data\raw_movies.jsonl | Measure-Object -Line).Lines
```

Must print `18`. If the file is missing, `python -m src.main` would try to
call the API; see "If something fails live" below.

**3. Close other Spark sessions.** A Jupyter kernel or a test run still open
holds port 4040. Spark then moves to 4041 and logs `WARN Utils: Service
'SparkUI' could not bind on port 4040`. Harmless, but two JVMs compete for
memory and the run gets slower. Restart or shut down any open notebook
kernel.

**4. Open these:**
- VS Code on the lab folder, with `src/cleaning.py` and `src/main.py` ready
- `reports/final_report.md` in the Markdown preview
- `analysis.ipynb` in Jupyter or VS Code (do not run it yet)
- the study browser: `study/index.html`
- the PowerShell window with the two variables set

---

## The demo, in order (about 15 minutes)

For each stage: open the file, run the command if there is one, point at the
result, say the one line.

### Stage 1 — The task and the layout (1 min)

Open `Data-Analysis-using-SparknPyspark.md` (the spec) and `README.md`.

> "Four steps: fetch 19 TMDB IDs, clean, KPIs, charts. The repo mirrors my
> pandas version: one module per step in `src/`, a `main.py` that runs it all,
> a notebook that runs the same functions step by step, and 32 tests."

Point at the README's "spark choices" list: it is your outline for the
"why Spark like this?" questions.

### Stage 2 — Step 1, fetching (2 min)

Open `src/tmdb_client.py`. Point at `append_to_response=credits`, the
`RETRYABLE` set, `time.sleep(2 ** attempt)`, the 404 branch, and the
temp-file-then-`os.replace` in `save_jsonl`.

> "Plain Python, one call per film with credits included. ID 0 is a 404 and
> is skipped, so 18 films. Busy and server errors retry with back-off; a bad
> key stops at once. The file is written to a temp name and renamed, so Spark
> never reads a half-written file."

Do **not** run `--fetch` live. Show the file instead:

```powershell
Get-Content data\raw_movies.jsonl -TotalCount 1
```

Prints the first film (Avengers: Endgame) as one long JSON line.

### Stage 3 — Step 2, cleaning (3 min)

Open `src/cleaning.py`. Show, in this order:

1. `RAW_SCHEMA` and its comment: schema on read, strings in the raw layer.
2. `load_raw`: PERMISSIVE and `_corrupt_record`.
3. `clean()`: twelve `.transform` steps in spec order.
4. `convert_dtypes`: `try_cast` because of ANSI mode.
5. `reorder_and_reset`: the window with no `partitionBy`, and its docstring.
6. `validate`: six checks in one `agg`, raises on failure.

> "Each rule is its own function, chained in spec order. Bad values become
> null instead of failing, then `validate` checks the result in one pass and
> stops the run if anything is off."

### Stage 4 — Run the pipeline (about 40 s, up to 1 min 20 s)

```powershell
$env:HADOOP_HOME="C:\Users\User\hadoop"; $env:PATH="C:\Users\User\hadoop\bin;"+$env:PATH; python -m src.main
```

While it runs (39 s here, up to 82 s if the laptop is busy), say what is
happening. The lines that matter, in order:

```text
INFO src.spark_session: spark 4.3.0 on local[*], ui at http://...:4040
INFO main: raw rows: 18, unreadable lines: 0
WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, ...
INFO src.cleaning: data checks passed on 18 rows
INFO main: wrote ...\data\movies_clean.parquet
INFO main: wrote reports/results.md and 8 charts
```

Noise you will also see, all harmless here:

| Line | Why it is harmless |
|---|---|
| `Missing Python executable 'C:\Program Files\Python313\python.exe'...` | the launcher looks for Python in the wrong place; `get_spark()` sets `PYSPARK_PYTHON` itself |
| `The system cannot find the path specified.` | same launcher check |
| `WARNING: Using incubator modules: jdk.incubator.vector` | Java 17 message at JVM start |
| `[Stage 3:> (0 + 1) / 1]` progress bars | Spark's job progress; one per job |
| `WARN SparkStringUtils: Truncated the string representation of a plan` | the plan of the wide raw table is long; only its log text is cut |
| `WARN WindowExec: No Partition Defined ...` (several times) | the `idx` window; expected, named in the docstring and README |
| `SUCCESS: The process with PID ... has been terminated.` | Windows cleaning up the Spark processes at `spark.stop()` |

Point at the WindowExec warning on purpose:

> "That warning is the reset index: a window with no partition puts all rows
> in one partition. Fine for 18 rows; the docstring says I would key on `id`
> instead at scale."

### Stage 5 — The results (3 min)

Open `reports/final_report.md` (the summary for people) and then
`reports/results.md` (every table). Read out:

| Point at | Say |
|---|---|
| `## data` line | "18 raw, 0 unreadable lines, 18 clean, 25 columns, checks passed." |
| nulls per column | "Only `belongs_to_collection` has nulls: the 2 standalone films." |
| top revenue | "Endgame 2,942.19 million." |
| low profit | "The 'worst' film still made 1,034.41 million profit: survivorship bias." |
| top / low roi | "Avatar 12.34, The Last Jedi 4.45." |
| searches | "0 matches for both; the tests prove the searches work." |
| franchise vs standalone | "16 vs 2 films; the standalone side is Titanic and Beauty and the Beast." |
| franchises | "The Avengers Collection: 4 films, 7,918.80." |
| directors | "James Cameron first; the Russos each get full credit for co-directed films." |

### Stage 6 — The charts (2 min)

Open `reports/chart1.png` to `chart8.png` in order. One line each (detail in
[09 · Charts](09-visualisation.md)):

1. Revenue vs budget: every film far above the break-even line.
2. ROI by genre: the top bars rest on 1–4 films; the `n=` labels show it.
3. Popularity vs rating: the Avengers films pull the trend.
4. Box office by year: mostly one film per year; not a market trend.
5. Franchise vs standalone: 16 vs 2 films.
6. Revenue histogram: right-skewed, median 1,453.68; buckets counted in Spark.
7. Runtime boxplot: Titanic (194 min) is the one outlier.
8. Popularity boxplot: Endgame at the fence; quartiles from `percentile_approx`.

### Stage 7 — The notebook (3 min; 50 s to run in full)

Open `analysis.ipynb`. If the reviewer wants to see it run, use
**Restart Kernel and Run All** and talk over it. Otherwise scroll through the
saved outputs. Sections, and what to point at:

| Section | Point at |
|---|---|
| intro cell | "what changes from pandas": lazy plan, schema, cache, Spark-side charts |
| fetch | the API is called only if the raw file is missing; `df_raw.printSchema()` shows the strings-first raw schema |
| clean | `validate(df_cleaned)` prints the check counts; `df_cleaned.explain()`: read bottom up, `InMemoryTableScan` at the top because it is cached, `Window` for `idx` |
| eda | `summary(...)` table, then histograms and boxplots; the markdown cell explaining why Spark and pandas boxplots disagree on Avatar and Endgame |
| check the extracted columns | `inspect_anomalies` tables |
| profit and roi, more rankings | the same lists as `results.md` |
| udf vs built in | "rows where they differ: 0", and `ArrowEvalPython` in the UDF plan |
| searches | 0 and 0, with the note that the tests cover them |
| franchise and directors, charts | same tables and charts as Stages 5–6 |
| save and stop | `coalesce(1)` Parquet write, read back, then `unpersist` and `spark.stop()` |

> "The notebook and `main.py` call the same functions, so they cannot
> disagree. The notebook is for reading step by step; `main.py` is for
> running it in one go."

Run the notebook **or** `main.py` live, not both at once: two Spark sessions
compete for the same memory.

### Stage 8 — Tests (1 min)

Show the full-run result you saved beforehand (`32 passed in 590.11s`), then
run the fast file live:

```powershell
python -m pytest tests/test_tmdb_client.py -q
```

Prints `.....` and `5 passed in 0.53s`; about 4 s including Python start-up. These tests use a fake HTTP
session and a patched `time.sleep`, so they need no network, no Spark and no
waiting.

> "32 tests on fake rows: one per cleaning rule, KPI rule, client behaviour
> and chart helper. The Spark ones are slow on Windows because every action
> has a fixed cost; I listed how I would speed them up."

---

## The 2-minute verbal summary

Practise this out loud until it fits in two minutes.

> "This lab redoes my pandas TMDB analysis in PySpark. Step 1 is plain
> Python: 19 API calls with credits included, retries with back-off, ID 0
> skipped as a 404, so 18 films saved as JSON Lines.
>
> Step 2 reads that file with a schema I wrote, in permissive mode, so a
> broken line is counted instead of crashing the job; here there were none.
> Twelve small functions flatten the nested JSON into pipe-separated text,
> type the columns with `try_cast` because Spark 4 is in ANSI mode, turn
> zeros and placeholders into nulls, filter and de-duplicate, and add a
> 0-based index. `validate` then checks the result in one aggregation and
> stops the run if anything is wrong.
>
> Step 3 builds ten best and worst lists from one `rank` function, two
> searches, a franchise comparison and franchise and director tables. Endgame
> has the highest revenue, Avatar the highest profit and ROI, and The Avengers
> Collection is the top franchise. Both searches are empty for these IDs; the
> tests prove they work.
>
> Step 4 draws eight charts where Spark does the grouping, histogram buckets
> and quartiles, and only small results go to Matplotlib.
>
> The main finding is a warning: all 18 films are blockbusters, so every
> one earned at least 4.45 times its budget, and the standalone group is only
> 2 films. Nothing here generalises to films in general. Spark is more than 18
> rows need, but the code is written as it would have to be for a real table:
> explicit schema, built-ins instead of UDFs, cache and unpersist, small
> results to the driver, and data checks that stop a bad run."

---

## If something fails live

| What happens | What to do |
|---|---|
| No network | Nothing to do: the raw file is on disk, and `python -m src.main` without `--fetch` never calls the API. Do not pass `--fetch`. |
| `data/raw_movies.jsonl` missing and no network | `main.py` would try to fetch and fail with a network error. Say so, and show `reports/results.md` and the charts, which are already generated. |
| `no TMDB_API_KEY in .env` | You ran `--fetch` or the raw file is missing. Run without `--fetch` after restoring the file; never type the key on screen. |
| `UnsatisfiedLinkError ... NativeIO$Windows` at the Parquet write | `HADOOP_HOME` is not set in this window. Re-run with the `$env:HADOOP_HOME=...; $env:PATH=...` prefix. |
| `ModuleNotFoundError: No module named 'src'` | You ran `python src/main.py` or are in the wrong folder. `cd` to the lab folder and use `python -m src.main`. |
| `JAVA_HOME is not set` / Java errors at start | Check `java -version` shows 17; see [01 · Setup](01-setup-windows.md). |
| `SparkUI could not bind on port 4040` | Another Spark is running (often a notebook kernel). Harmless; close the other one if things feel slow. |
| Run is much slower than 80 s | Another Spark JVM or the test suite is running. Stop it and rerun. |
| `data checks failed: ...` | `validate` stopped the run on purpose; nothing was written. Read the message: it names the rule and the count. That is the feature working. |
| A notebook cell fails after `spark.stop()` | The last cell stops Spark. Restart the kernel before running cells again. |
| Tests take too long to show | Show the saved `32 passed` line and run `tests/test_tmdb_client.py` only. |
| Out-of-memory or the laptop freezes | Close the notebook kernel and other Spark runs; rerun `main.py` alone. |

## Self-check

1. Which two lines must you set in a fresh PowerShell window before any Spark command?
2. Why is it safe to have no network during the review?
3. What does the `WindowExec` warning mean, and what do you say about it?
4. Why should the notebook and `main.py` not run at the same time?
5. Which test file can you run live, and why is it fast?

Answers: (1) `$env:HADOOP_HOME="C:\Users\User\hadoop"` and the `PATH`
line adding its `bin`. (2) `data/raw_movies.jsonl` is on disk, and
`python -m src.main` only fetches with `--fetch` or when that file is
missing. (3) The `idx` window has no `partitionBy`, so all rows go to one
partition; fine for 18 rows, named in the code as the part that would not
scale. (4) Two Spark JVMs compete for memory and the port, and both rewrite
the same Parquet. (5) `tests/test_tmdb_client.py`: it uses a fake session and
a patched `time.sleep`, and needs no Spark.
