# TMDB Movie Data Analysis with PySpark: Final Report

**Author:** Fabrice Sheja · **Data snapshot:** TMDB API, fetched 4 November 2026 ·
**Tools:** Python 3.13, PySpark 4.3 (local mode), Matplotlib

Every number here comes from `python -m src.main` (or `analysis.ipynb`, which runs the
same functions). The full tables are in [results.md](results.md) and the charts are
`chart1.png` to `chart8.png` in this folder.

## 1. Summary

The 18 films in the lab's ID list are among the highest-grossing films ever
made. Together they cost **$3.85 billion** to make and took **$30.6 billion**
at the box office. Every one of them earned at least 4.4 times its budget.

- **The Avengers films lead almost every list.** *Avengers: Endgame* has the
  highest revenue ($2,942M), the highest budget ($356M), the joint-highest
  rating (8.25, with *Infinity War*) and the highest popularity score.
  *The Avengers* (2012) has the most votes (40,259).
- **The best return on investment came from older, cheaper films.**
  *Avatar* (12.3×), *Titanic* (11.3×), *Jurassic World* (11.1×) and *Harry
  Potter and the Deathly Hallows: Part 2* (10.7×). The most expensive film
  in the set, *Star Wars: The Last Jedi* ($300M), has the lowest ROI (4.5×).
- **Franchise vs standalone shows almost no difference here.** Both groups
  have a median ROI near 8 and a mean rating near 7.4. But the standalone
  group is only 2 films (*Titanic*, *Beauty and the Beast*), so no
  conclusion can be drawn from it.
- **Popularity tracks rating closely in this set** (correlation 0.84). Budget
  explains revenue only partly (correlation 0.58).
- **Both search queries return no films.** No film in the list stars Bruce
  Willis, and none is a Tarantino film. The queries are correct and are
  covered by tests on small made-up data (`tests/test_kpis.py`).

## 2. Method

**Step 1: Extraction.** `src/tmdb_client.py` calls
`/movie/{id}?append_to_response=credits` once per ID, so the cast and crew
arrive in the same call. ID 0 returns 404 and is skipped. Busy (429) and
server (5xx) errors are retried with exponential back-off. The raw responses
are saved to `data/raw_movies.jsonl`, so every later step can be re-run
without calling the API. 18 of 19 IDs returned a film.

**Step 2: Cleaning** (`src/cleaning.py`, in spec order). The raw file is read with an explicit
schema (`RAW_SCHEMA`), not inferred, and lines that are not valid JSON are dropped
and counted:

| Rule | Decision |
|---|---|
| Irrelevant columns | `adult`, `imdb_id`, `original_title`, `video`, `homepage` dropped |
| JSON-like columns | Names joined with `"\|"`. For `spoken_languages` the English name is used (e.g. "Russian", not "Pусский") |
| Directors | **All** directors kept, joined with `"\|"`. 4 films have two (e.g. Joe and Anthony Russo). Keeping only the first would hide one of them |
| Types | `try_cast`: an invalid value becomes null instead of failing the job. PySpark 4 runs in ANSI mode, where a plain `cast` throws |
| Zeros | A budget, revenue or runtime of 0 means "unknown", so it becomes null. A fake 0 would make profit and ROI wrong |
| Money | Budget and revenue converted to million USD |
| No votes | When `vote_count = 0`, `vote_average` becomes null: a 0.0 rating from nobody is not a rating |
| Placeholders | `"No Data"`, `"No overview found."` and empty text become null |
| Rows | Duplicates on `id` removed. Rows with no `id`/`title`, fewer than 10 non-null values, or not `Released` removed |
| Index | Spark rows have no index, so `idx` is a `row_number()` ordered by `id`, starting at 0 |

After cleaning, `validate()` checks the table in one pass (rows left, unique
ids, titles present, budgets, revenues and ratings in range) and stops the run
if any check fails. No film was removed by these rules. All 18 are released, complete and unique.
After cleaning, only `belongs_to_collection` has nulls: the 2 standalone films.

**Step 3: KPIs** (`src/kpis.py`). One reusable function,
`rank`, produces all ten best/worst lists. It leaves out rows where the
metric is null and accepts an extra filter. ROI rankings only use films with
a budget of at least $10M, and rating rankings only films with at least 10
votes, as the spec asks (here every film passes both). A real Spark UDF
(`roi_udf`) is included for comparison. A test shows it gives the same ROI as
the built-in column arithmetic, which is faster because it never leaves the JVM.

"Most successful" franchise or director means **highest total revenue, ties
broken by mean rating**. For directors, a co-directed film counts in full for
each director.

**Step 4: Charts** (`src/visualize.py`). Spark does the grouping, the histogram
buckets and the box-plot quartiles. Only the small results are converted to
pandas and drawn with Matplotlib.

## 3. Results

### Best and worst films

| KPI | 1st | 2nd | 3rd |
|---|---|---|---|
| Highest revenue | Avengers: Endgame $2,942M | Avatar $2,924M | Titanic $2,264M |
| Highest budget | Avengers: Endgame $356M | Avengers: Infinity War $300M | Star Wars: The Last Jedi $300M |
| Highest profit | Avatar $2,687M | Avengers: Endgame $2,586M | Titanic $2,064M |
| Lowest profit | Star Wars: The Last Jedi $1,034M | Incredibles 2 $1,043M | Beauty and the Beast $1,106M |
| Highest ROI (budget ≥ $10M) | Avatar 12.34 | Titanic 11.32 | Jurassic World 11.14 |
| Lowest ROI (budget ≥ $10M) | Star Wars: The Last Jedi 4.45 | Avengers: Age of Ultron 5.98 | Incredibles 2 6.22 |
| Most votes | The Avengers 40,259 | Avatar 34,959 | Avengers: Infinity War 33,324 |
| Highest rated (≥ 10 votes) | Avengers: Endgame 8.25 | Avengers: Infinity War 8.25 | The Avengers 8.08 |
| Lowest rated (≥ 10 votes) | Jurassic World: Fallen Kingdom 6.52 | Jurassic World 6.70 | Star Wars: The Last Jedi 6.73 |
| Most popular | Avengers: Endgame 64.9 | Avengers: Infinity War 54.5 | The Avengers 42.4 |

"Lowest profit" is still more than $1 billion: no film in the set lost money.

### Franchise vs standalone

| | Films | Mean revenue | Median ROI | Mean budget | Mean popularity | Mean rating |
|---|---|---|---|---|---|---|
| Franchise | 16 | $1,692M | 7.71 | $218M | 27.1 | 7.40 |
| Standalone | 2 | $1,765M | 7.91 | $180M | 26.5 | 7.44 |

### Most successful franchises and directors

| Franchise | Films | Total revenue | Mean rating |
|---|---|---|---|
| The Avengers Collection | 4 | $7,919M | 7.96 |
| Star Wars Collection | 2 | $3,403M | 6.98 |
| Jurassic Park Collection | 2 | $2,982M | 6.61 |

| Director | Films | Total revenue | Mean rating |
|---|---|---|---|
| James Cameron | 2 | $5,188M | 7.76 |
| Joe Russo / Anthony Russo | 2 | $4,995M | 8.25 |
| Joss Whedon | 2 | $2,924M | 7.68 |

### What the charts show

1. **Revenue vs budget:** every point is far above the break-even line. A
   bigger budget goes with somewhat more revenue (correlation 0.58), but
   *Avatar* and *Titanic* earn far more than films that cost the same.
2. **ROI by genre:** Comedy (9.7), Romance (9.6) and Fantasy (9.2) have the
   highest mean ROI, but they hold only 1, 2 and 4 films. Action, one of the
   two most common genres here (10 films), has the lowest (7.4).
3. **Popularity vs rating:** the films rated above 8 are also the most
   popular. *Endgame* and *Infinity War* stand apart from the rest.
4. **Box office by year:** mean revenue per release year jumps around
   because most years hold one film: 2009 is *Avatar* alone ($2,924M) and 1997
   is *Titanic* alone. 2015, 2018 and 2019 hold 3–4 films each. The line
   reflects which films are in the list, not a market trend.
5. **Franchise vs standalone:** mean revenue is nearly equal ($1,692M vs
   $1,765M), and the standalone bar is only 2 films.
6. **Distributions (charts 6–8):** revenue is right-skewed (median $1,454M,
   mean $1,700M). *Titanic* (194 min) is the one runtime outlier.

## 4. Limitations

- **The sample is small and hand-picked.** 18 record-breaking films are not a
  sample of "movies". There are no flops, so "lowest profit" and "lowest ROI"
  only mean "least spectacular hit". This is survivorship bias.
- **The standalone group has 2 films,** so means and medians for it are
  anecdotes, not evidence.
- **Popularity and vote counts change daily** on TMDB. Re-fetching will give
  slightly different numbers. The saved raw file keeps this run reproducible.
- **Spark is not needed for 18 rows.** pandas would be faster here. The point
  of the lab is the PySpark API, which runs unchanged on millions of rows
  on a cluster.

## 5. How to reproduce

```bash
pip install -r requirements.txt
copy .env.example .env          # add TMDB_API_KEY
pytest                          # 32 tests on fake rows, no network
python -m src.tmdb_client       # calls the API, writes data/raw_movies.jsonl
python -m src.main              # clean, KPIs, results.md and charts
```

Then open `analysis.ipynb` and run it top to bottom.

On Windows, Spark also needs `HADOOP_HOME` and `winutils.exe`. See
[study/01-setup-windows.md](../study/01-setup-windows.md).
