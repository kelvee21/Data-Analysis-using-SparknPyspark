# 12 — Defending This Project in Code Review

The review will probe three things: do you understand **why** the code is
written this way, can you back it with **numbers** from `reports/results.md`,
and do you know the **weak spots**. Below are likely questions with short
model answers tied to this repo's files and functions.

> **Help:** if a reviewer points at a concept ("why cache?", "what is a shuffle?"), the long answer is in [11 · Hard concepts](11-hard-concepts-explained.md). If they point at a function, see [14 · PySpark function reference](14-pyspark-function-reference.md). Line-by-line code is in [06 · Cleaning](06-cleaning-line-by-line.md) and [08 · KPIs](08-kpis-line-by-line.md).

## The opening (60 seconds)

> "`src/tmdb_client.py` fetches the 19 IDs in plain Python, keeps the 18 that
> exist, and saves the raw answers as JSON Lines. Spark reads that file with
> a schema I wrote, flattens the nested JSON, fixes types and placeholder
> values, checks the result with `validate()`, and writes a 25-column Parquet
> table. On top of it, `kpis.py` builds ten best/worst lists with one `rank`
> function, two searches, a franchise comparison and franchise and director
> tables, and `visualize.py` draws eight charts with the counting done in
> Spark. `python -m src.main` runs it all, `analysis.ipynb` runs the same
> functions step by step, and 32 pytest tests cover each rule on fake rows."

## Numbers to know by heart (all from `results.md`)

| Fact | Value |
|---|---|
| IDs requested / movies read | 19 / 18 (ID 0 → 404, skipped); 0 unreadable lines |
| Rows / columns after cleaning | 18 / 25 (22 spec + `idx`, `profit_musd`, `roi`) |
| Nulls after cleaning | only `belongs_to_collection`: 2 |
| Highest revenue | Avengers: Endgame, 2,942.19 |
| Highest profit | Avatar, 2,686.71 |
| Lowest profit | Star Wars: The Last Jedi, 1,034.41 |
| Highest / lowest ROI | Avatar 12.34 / Star Wars: The Last Jedi 4.45 |
| Most voted | The Avengers, 40259 votes |
| Highest rated | Endgame and Infinity War, 8.25 |
| Most popular | Endgame, 64.93 |
| Search 1 / Search 2 | 0 matches / 0 matches |
| Franchise vs standalone | 16 vs 2 films; mean revenue 1,691.70 vs 1,765.10 |
| Top franchise | The Avengers Collection, 4 films, 7,918.80 |
| Top director | James Cameron, 2 films, 5,187.90 |

Money is in million USD.

## Step 1 — Fetching (`src/tmdb_client.py`)

**Q1: Why is the fetch plain Python and not Spark?**
A: It is 19 small HTTP calls. Spark would only add start-up cost. The module
docstring: "19 http calls do not need a cluster. spark starts at step 2 when
it reads the file this writes."

**Q2: How do you get cast and director without a second call per film?**
A: `params={"append_to_response": "credits"}` in `fetch_movie`. TMDB adds
cast and crew to the movie answer, so one call per film. The credits are
kept raw and flattened in Spark, "so i can redo it without calling the api
again."

**Q3: What happens with ID 0?**
A: TMDB returns 404, `fetch_movie` logs "not found, skip it" and returns
`None`, and `fetch_all` keeps only non-`None` results: 18 lines.
`test_404_is_skipped` covers it.

**Q4: What if the network fails or TMDB is busy?**
A: Statuses 429, 500, 502, 503, 504 and connection errors are retried up to
3 times with exponential back-off, `time.sleep(2 ** attempt)`: 2, 4, 8 s.
Then it logs and skips the film. A 401 (bad key) is not retried:
`raise_for_status()` stops the program. Tests: `test_429_then_ok_retries`,
`test_gives_up_after_three_tries`, using a fake session and a patched
`time.sleep`.

**Q5: Where is the API key, and could it leak?**
A: In `.env`, read with `load_dotenv()`. `.env` is git-ignored. The key goes
in `session.params`, "so it is never part of a url i log." If it is missing,
`fetch_all` exits with a message saying how to fix it.

**Q6: Why JSON Lines, and why the temp file?**
A: One object per line lets Spark split and read the file in parallel; a
single JSON array can only be read as one piece. `save_jsonl` writes to a
`.tmp` file and then `os.replace`s it, so a crash halfway never leaves Spark
a half-written file. `test_save_jsonl_one_movie_per_line` checks no `.tmp`
is left.

## Step 2 — Cleaning (`src/cleaning.py`)

**Q7: Walk me through `clean()`.**
A: Twelve steps chained with `.transform`, in spec order:
`drop_corrupt_rows`, `drop_irrelevant_columns`, `extract_collection_name`,
`pipe_join_names` four times (genres, spoken languages, countries,
companies), `extract_credits`, `convert_dtypes`, `clean_unrealistic_values`,
`filter_released_and_dedup`, `reorder_and_reset`. Each takes a DataFrame and
returns a new one, so each is testable alone.

**Q8: Why write `RAW_SCHEMA` instead of letting Spark infer it?**
A: Inference reads the file once just to guess types; the guess can change
with the data (an all-empty cast list was guessed as `array<string>` and
broke `cast.name` in an earlier version); unlisted fields are skipped, so new
API fields cannot break the code. Plain fields are strings on purpose and get
typed by `try_cast`.

**Q9: What happens to a broken line in the raw file?**
A: PERMISSIVE mode puts its text in `_corrupt_record` instead of failing the
read. `main.py` counts those lines ("0 unreadable lines" here) and
`drop_corrupt_rows` removes them. `test_broken_json_line_is_dropped` covers
it.

**Q10: How do you turn `genres` into "Action|Adventure"?**
A: `pipe_join_names(df, "genres", "name")`: `F.col("genres.name")` pulls the
name of every struct in the array, `F.concat_ws("|", ...)` joins them, and
`F.when(joined != "", joined)` turns an empty result into null.
`spoken_languages` uses `english_name`.

**Q11: How do you find the director?**
A: In `extract_credits`, `F.filter("credits.crew", lambda c: c["job"] ==
"Director")` keeps crew entries whose job is Director, and `concat_ws` joins
all their names. So Frozen is `Jennifer Lee|Chris Buck`, not just the first.
`test_two_directors_both_kept` checks it.

**Q12: Why `try_cast` and not `.cast()`?**
A: Spark 4 is in ANSI mode: a plain cast of a bad value fails the whole job.
`try_cast` and `try_to_timestamp` return null, like `pd.to_numeric(...,
errors="coerce")`. `test_bad_values_become_null_not_a_crash` feeds "not a
date" and "lots".

**Q13: Budget 0: why null and not "infer from similar movies"?**
A: The spec allows either. Null is honest; an invented budget would put
made-up numbers into ROI. Null also means `avg` skips the row and ROI is
null, not a divide-by-zero error. In this data no film had a zero, so nothing
changed.

**Q14: What did you do with `vote_count = 0`?**
A: TMDB shows 0.0 when nobody voted, which is not a rating, so
`vote_average` becomes null. `test_no_votes_means_no_rating`.

**Q15: What did `inspect_anomalies` show?**
A: Nothing to fix, which itself describes the sample: `original_language`
is `en` for all 18, every film lists the United States as a production
country (the UK on 2), English is spoken in all 18. The genre counts add up
to more than 18 because a film counts once per genre (Adventure 15, Action
10, Science Fiction 10).

**Q16: Which duplicate survives `dropDuplicates`?**
A: One per id, not a fixed one. The code says so: "which one is not fixed in
spark, fine here because a repeat is the same api answer twice." If copies
could differ I would rank them by a fetch time and keep the newest.

**Q17: How did you "reset the index"?**
A: Spark has no index, so `reorder_and_reset` adds `idx = row_number() over
(order by id) - 1`: 0-based and continuous. Ordered by `id` because Spark
keeps no reliable original order, so `idx 0` is Titanic (id 597). The window
has no `partitionBy`, so all rows go to one partition: fine for 18, named in
the docstring as the one thing that would not scale.

**Q18: What does `validate()` check, and why raise?**
A: Rows > 0, unique ids, no null titles, budget and revenue > 0, rating in
0–10, all in one `agg`. It raises so `main()` stops before writing any
Parquet, results or charts. Tests: `test_validate_passes_on_good_data`,
`test_validate_stops_on_empty_table`.

**Q19: 25 columns, but the spec lists 22?**
A: `reorder_and_reset` gives exactly `idx` + the 22 spec columns in order
(`test_columns_come_out_in_brief_order`). `add_profit_roi` then adds
`profit_musd` and `roi`, computed once before the cache instead of in every
ranking.

## Step 3 — KPIs (`src/kpis.py`)

**Q20: The spec asks for a UDF for ranking. Where is it?**
A: `rank(df, col, n=5, ascending=False, where=None)` is the reusable function
behind all 10 lists in `rankings()`. It is plain Spark column code on
purpose. `roi_udf` is a real `@F.udf`, kept so the notebook can compare;
`test_udf_gives_same_roi_as_builtin` shows the same values, and the notebook
plan shows `ArrowEvalPython`, the step where rows go to Python and back.

**Q21: What does `rank` do with nulls and ties?**
A: It drops rows where the column is null, so a film with no budget never
tops "low roi" (an ascending sort puts nulls first). Ties are broken by
`title`: Endgame and Infinity War, both 8.25, always come out in the same
order.

**Q22: Why `budget_musd >= 10` and not `>= 10_000_000`?**
A: Money is in millions after `clean_unrealistic_values`.
`ROI_MIN_BUDGET_MUSD = 10.0` and `MIN_VOTES = 10` are named constants.
Every film here passes both; `test_rank_skips_nulls_and_uses_threshold`
shows the threshold excluding a 5 M film.

**Q23: Both searches return 0 rows. Is the code broken?**
A: No. None of the 18 IDs stars Bruce Willis or is a Tarantino film.
`test_willis_search` and `test_tarantino_search_shortest_first` build
matching films and show the searches find and sort them.

**Q24: Why `has()` instead of `F.col("cast").contains("Bruce Willis")`?**
A: `contains` is a substring test and would match "Bruce Willis Jr". `has()`
splits the pipe string and uses `array_contains`, which matches whole items.
`test_has_matches_whole_names_only`.

**Q25: How do you define franchise vs standalone?**
A: `belongs_to_collection` not null → `"franchise"`, else `"standalone"`,
in a column called `group`. One `groupBy("group").agg(...)` computes all six
columns. 16 vs 2 films; the standalones are Titanic and Beauty and the
Beast.

**Q26: What does "most successful franchise" mean?**
A: Highest `total_revenue`, ties broken by `mean_rating`, with film count,
total and mean budget and revenue shown as the spec lists. The Avengers
Collection: 4 films, 7,918.80. `test_top_franchises_sums_revenue`.

**Q27: Joe and Anthony Russo both show 4,994.60. Double counting?**
A: `top_directors` explodes the director string so each co-director gets
full credit, which is right for a per-director ranking
(`test_codirectors_both_get_the_revenue`). The column is not additive:
summing it would count Endgame twice.

**Q28: Is your median ROI exact?**
A: No. `percentile_approx` returns one of the real values and never
averages two. For the 2 standalone films it shows 7.91, the lower film's
ROI; the exact median would be the average of 7.91 and 11.32 (Titanic),
about 9.62.

## Step 4 — Charts (`src/visualize.py`)

**Q29: How do you get Spark data into Matplotlib?**
A: Spark selects, groups or buckets first; only the small result goes to the
driver with `toPandas()` or `collect()`. The two scatter plots are the only
ones that send one row per film (18).

**Q30: How is the histogram built without `ax.hist`?**
A: `plot_distribution` gets min, max and median in one `agg`, computes a
bucket number per row with `floor((x - lo) / width)`, clamps the maximum
into the last bucket with `least`, and counts per bucket with `groupBy`. At
most 20 numbers come back, whatever the table size.

**Q31: Your runtime boxplot: how, and why might it differ from pandas?**
A: `plot_boxplot` gets Q1, median and Q3 from one `percentile_approx`,
computes the fences, the whisker ends and the outliers in Spark, and draws
with `ax.bxp`. `percentile_approx` picks real values while pandas
interpolates, so on 18 rows the fences move: the notebook notes Avatar and
Endgame are inside the Spark fence but were outliers in the pandas version.

**Q32: What do the charts tell you?**
A: Every film is far above break-even (survivorship bias); the genre bars
with the highest ROI rest on 1–4 films; popularity rises with rating, driven
by the Avengers films; the yearly line follows which films are in the list;
franchise vs standalone is 16 vs 2; revenue is right-skewed; Titanic (194
min) is the one runtime outlier.

**Q33: Why `matplotlib.use("Agg")` inside `main()`?**
A: Agg draws to files with no window. Setting it at import time would turn
off inline charts in the notebook, which imports the same modules.

## Code quality

**Q34: How is the code organised?**
A: Flat `src/` package mirroring the pandas version: `spark_session.py`
(one SparkSession for script, notebook and tests), `tmdb_client.py` (Step 1),
`cleaning.py` (Step 2), `kpis.py` (Step 3), `visualize.py` (Step 4),
`main.py` (glue). Constants live next to the code that uses them
(`MOVIE_IDS`, `DROP_COLUMNS`, `FINAL_COLUMNS`, `ROI_MIN_BUDGET_MUSD`).

**Q35: What makes it efficient?**
A: Schema on read (no inference pass); built-ins instead of Python UDFs;
`cache()` on reused tables and `unpersist()` after; 4 shuffle partitions
with AQE on; reduce before `toPandas`; histograms and quartiles in Spark;
`validate` as one aggregation; one API call per film; Arrow for
`toPandas`; `coalesce(1)` Parquet output.

**Q36: What is configurable without code changes?**
A: From `.env` or environment variables: `TMDB_API_KEY`, `TMDB_BASE_URL`,
`SPARK_MASTER` (default `local[*]`), `SPARK_SHUFFLE_PARTITIONS` (default 4),
`SPARK_LOG_LEVEL` (default WARN). The same code could point at a cluster.

**Q37: What happens if a step fails?**
A: `main()` wraps `run()` in `try / except / finally`: the error is logged
with its stack trace, the script exits with code 1, `results.md` is not
written, and `spark.stop()` in `finally` always releases the JVM.

**Q38: How do the tests avoid the network and the raw file?**
A: `tests/helpers.py` builds fake TMDB records (`movie(**changes)`) and reads
them through the real `RAW_SCHEMA` (`raw_df`). The client tests use a fake
session and patch `time.sleep`. 32 tests, no network.

## Git

**Q39: How did you structure your commits?**
A: One commit per lab step: skeleton, Step 1 client + tests, Step 2 cleaning
+ tests, Step 3 KPIs + tests, Step 4 charts + `main.py`, then notebook,
report and README. See
[10 · Pipeline and tests](10-pipeline-and-tests.md#h-part-3-git-commit-plan).

**Q40: What is not in git, and why?**
A: `.env` (the key), all of `data/` except `data/README.md` (rebuilt by the
code), `__pycache__/`, `.pytest_cache/`, `.ipynb_checkpoints/`,
`spark-warehouse/`, `metastore_db/`, `derby.log`, `*.crc`.

## Weak spots to admit honestly

Say these before the reviewer finds them. Each has a fix ready.

| Weak spot | What to say | Fix |
|---|---|---|
| Small, biased sample | "18 English-language US blockbusters, lowest ROI 4.45. Nothing generalises to films in general." | A random or full sample of TMDB films |
| Both searches empty | "The required searches return 0 rows on these IDs; tests on fake films prove they work." | Nothing in code; the data has no match |
| Standalone = 2 films | "The franchise comparison is 16 vs 2; the standalone mean is Titanic plus Beauty and the Beast." | More data, or show the two films as a list |
| Approximate median | "`percentile_approx` shows 7.91 for standalone; the exact median of the two films is about 9.62." | `F.median` on small groups |
| Boxplot fences differ from pandas | "Spark quartiles are real values, pandas interpolates, so outliers can differ on 18 rows." | State the quartile method on the chart |
| Reset-index ordering | "`idx` follows `id`, not the spec's list order, so `idx 0` is Titanic; and the window has no `partitionBy`." | Save a fetch-position column in Step 1, or drop `idx` at scale |
| Raw data not in git | "`data/` is ignored, so a fresh clone needs an API key to reproduce." | Commit the 18-line raw file, or document the fetch date (the report does) |
| Director credit not additive | "Co-directors each get full revenue; never sum the director table." | Documented in the code comment and a test |
| No inflation adjustment | "1997 Titanic is compared with 2019 Endgame in nominal dollars." | Adjust with a CPI table |
| Popularity is a snapshot | "TMDB popularity is today's activity; it changes daily." | Use votes or revenue for 'success' |
| Back-off sleeps after the last try | "`fetch_movie` waits 8 s after the third failure before giving up." | Skip the sleep on the final attempt |
| Line chart over gappy years | "Chart 4 draws a line across 1998–2008, where there are no films." | Markers only, or bars |

## Self-check

1. Why does `rank` drop nulls before sorting?
2. Which two tests prove the searches work, given the real data has 0 matches?
3. What does PERMISSIVE mode do, and how many lines did it catch here?
4. Name four things that make the code efficient.
5. Which weak spot would you mention first, and why?

Answers: (1) An ascending sort puts nulls first, so a film with no budget
would top "low roi". (2) `test_willis_search` and
`test_tarantino_search_shortest_first`. (3) Puts a broken line's text in
`_corrupt_record` instead of failing; 0 here. (4) Any four of: schema on
read, built-ins not UDFs, cache/unpersist, 4 shuffle partitions + AQE,
reduce before `toPandas`, Spark-side histogram and quartiles, one-pass
`validate`, Arrow, one call per film. (5) The small biased sample: every
conclusion depends on it.

Next: [13 · Glossary and practice](13-glossary-and-practice.md)
