# 04 — The TMDB API and tmdb_client.py (Step 1)

How the 18 movies get from TMDB's servers onto your disk. First the web
basics (requests, status codes, retries), then `src/tmdb_client.py` line
by line, its tests, and what one saved record really looks like.

> **Help:** Python syntax used here (`try/except/else`, f-strings,
> `load_dotenv`) is in [03 · The Python you need](03-python-you-need.md).
> Setting up the key is in [01 · Setup](01-setup-windows.md#h-5-the-tmdb-key-and-env).

## 1. REST and HTTP in five minutes

An **API** (Application Programming Interface) is a way for one program to
ask another for data. A **REST API** does it over the web: each kind of
thing has a URL, and you use **HTTP** (the web's request protocol) to ask
for it.

One HTTP exchange:

| Part | Example from this lab |
|---|---|
| **Method** | `GET` (read data; the only method this lab uses) |
| **URL** (endpoint) | `https://api.themoviedb.org/3/movie/299534` |
| **Query parameters** | `?api_key=...&append_to_response=credits`, added after `?`, joined by `&` |
| **Response status code** | `200` (a number saying how it went) |
| **Response body** | JSON text describing the movie |

The URL is built from a **base URL** plus a path. TMDB's v3 base is
`https://api.themoviedb.org/3`; the movie endpoint is `/movie/{id}`, where
`{id}` is replaced by the movie's number.

## 2. Authentication with `api_key`

TMDB needs to know who is asking (to enforce limits and block abuse). The
v3 API accepts your key as a query parameter named `api_key`.
`tmdb_client.py` attaches it to every request once, through the session:

```python
# one session reuses the tcp connection for all 19 calls. the key goes in
# the session params so it is never part of a url i log.
session = requests.Session()
session.params = {"api_key": api_key}
```

A **`requests.Session`** remembers settings (like these default params) and
reuses the network connection between requests, which is faster than
opening a new one 19 times. The URL the code builds and logs is
`f"{base_url}/movie/{movie_id}"`, without the key; the session adds the key
only when sending.

## 3. `append_to_response=credits`: one call instead of two

The spec's final columns include `cast`, `cast_size`, `director` and
`crew_size`. Those are not in the movie details. They live at a second
endpoint, `/movie/{id}/credits`.

TMDB lets you fold a second endpoint into the first with
`append_to_response`:

```python
r = session.get(url, params={"append_to_response": "credits"}, timeout=TIMEOUT)
```

The response then has an extra key, `credits`, holding `cast` and `crew`.

| Approach | Requests for 19 IDs |
|---|---|
| Details + separate credits call | 38 |
| `append_to_response=credits` | **19** |

Half the requests, half the chance of hitting a rate limit, and each
movie's details and credits come from the same moment.

The docstring adds a design choice: "i keep the credits raw here and
flatten them in spark, so the raw file is exactly what the api sent." The
client does no cleaning at all; `cleaning.extract_credits` does it later.

## 4. Status codes this code handles

A **status code** is a three-digit number. The first digit is the family:
2xx success, 4xx "you did something wrong", 5xx "the server failed".

| Code | Meaning | What `fetch_movie` does | Retry? |
|---|---|---|---|
| **200** | OK, body is the movie | Return `r.json()` | — |
| **401** | Unauthorized: bad or missing key | `r.raise_for_status()` raises `HTTPError`; the run stops | No: a wrong key stays wrong |
| **404** | Not found: no movie with that ID | Log a warning, return `None` (skip) | No |
| **429** | Too many requests: you hit the rate limit | Wait, try again | Yes |
| **500, 502, 503, 504** | Server error or overloaded gateway | Wait, try again | Yes |
| Network error (timeout, no connection) | `requests.RequestException` | Log, wait, try again | Yes |

The rule behind the table: **retry only what might succeed next time.** A
429 or a 503 is temporary. A 401 or a 404 will give the same answer
forever, so retrying just wastes time.

In this lab's run, ID `0` returned **404** (the code comment says "id 0
is not a real movie. the brief puts it in to see if the code copes."). It
was skipped, and 18 of 19 movies were saved. `results.md` starts from
"raw: 18 movies".

## 5. Retries with exponential backoff

**Exponential backoff** means waiting longer after each failure: 2 s, then
4 s, then 8 s. If the server is overloaded, hammering it again
immediately makes things worse; spacing the attempts out gives it time to
recover.

```python
# wait 2s, 4s, 8s. hammering a 429 just gets more 429s.
time.sleep(2 ** attempt)
```

`attempt` goes 1, 2, 3, so `2 ** attempt` is 2, 4, 8. After three failed
attempts the function logs an error and returns `None`, so one broken
movie never kills the whole fetch.

Separately, `fetch_all` sleeps `0.1` seconds between movies, which keeps
19 requests well under TMDB's rate limit.

## 6. Why save the raw JSON first

`tmdb_client.py` does not clean anything. It saves the API's answers
exactly as received to `data/raw_movies.jsonl`, and Spark starts from that
file.

| Reason | What it buys you |
|---|---|
| **Reproducible** | Every later run uses the same 18 records, so `results.md` does not change because TMDB updated a vote count overnight |
| **No re-calling the API** | `python -m src.main` reuses the file when it exists: no key needed, no requests spent. Tests, notebook and charts can rerun freely |
| **Debuggable** | If a cleaned value looks wrong, open the raw line and see what TMDB actually sent |
| **Separation** | Extract (fetch) and transform (clean) are independent steps; a cleaning bug never needs a new download |

This is the **raw zone** (or "bronze layer") pattern used in real data
platforms: land the source data untouched, then build cleaned versions
from it. `data/README.md` says the same: the raw file is "one api answer
per line, credits included, nothing changed".

**JSON Lines** (`.jsonl`) is plain JSON with one complete object per line.
The `save_jsonl` docstring gives the Spark reason: "spark reads json lines
in parallel, a single big json array it can only read as one piece
(multiLine)."

## 7. tmdb_client.py line by line

### Module docstring, imports and constants

```python
"""Step 1: get the 19 movie ids from the TMDB api and land them raw on disk.

this part is plain python, no spark. 19 http calls do not need a cluster.
spark starts at step 2 when it reads the file this writes.
"""
import json
import logging
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

log = logging.getLogger(__name__)

# id 0 is not a real movie. the brief puts it in to see if the code copes.
MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397,
    420818, 24428, 168259, 99861, 284054, 12445,
    181808, 330457, 351286, 109445, 321612, 260513,
]

RAW_PATH = Path(__file__).resolve().parents[1] / "data" / "raw_movies.jsonl"
TIMEOUT = 20
RETRYABLE = {429, 500, 502, 503, 504}
```

- No Spark import. The API calls are plain Python on the driver machine.
- `RAW_PATH` is computed from this file's location, so it works from any
  current folder. `main.py` imports it: `from src.tmdb_client import RAW_PATH, ...`.
- `RETRYABLE` is a set, so `r.status_code not in RETRYABLE` is a fast,
  readable check.

### `fetch_movie`: one movie, with retries

```python
def fetch_movie(session: requests.Session, base_url: str, movie_id: int, retries: int = 3) -> Movie | None:
    url = f"{base_url}/movie/{movie_id}"
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params={"append_to_response": "credits"}, timeout=TIMEOUT)
        except requests.RequestException as e:
            log.warning("movie %s: network error %s (try %d)", movie_id, e, attempt)
        else:
            if r.status_code == 404:
                log.warning("movie %s: not found, skip it", movie_id)
                return None
            if r.status_code not in RETRYABLE:
                # 401 means a bad key. retrying will not fix that so let it fail.
                r.raise_for_status()
                return r.json()
            log.warning("movie %s: http %s (try %d)", movie_id, r.status_code, attempt)
        # wait 2s, 4s, 8s. hammering a 429 just gets more 429s.
        time.sleep(2 ** attempt)
    log.error("movie %s: gave up after %d tries", movie_id, retries)
    return None
```

Walk through the paths:

1. `range(1, retries + 1)` gives attempts 1, 2, 3.
2. `timeout=TIMEOUT` (20 s): without a timeout, a stuck connection could
   hang forever.
3. Network error → `except` logs it → falls through to `sleep` → next attempt.
4. 404 → return `None` immediately. No sleep, no retry.
5. Not retryable (200, 401, 403, ...) → `raise_for_status()` does nothing
   for 200 and raises for 4xx/5xx. For 200 the function returns the
   parsed JSON dict.
6. Retryable (429, 5xx) → log, sleep, next attempt.
7. Loop ends without returning → all 3 attempts failed → log error, `None`.

`session` is a parameter, not created inside. That is what makes the
function testable with a fake session (§8).

### `fetch_all`: the key and the loop

```python
def fetch_all(movie_ids: list[int] | None = None) -> list[Movie]:
    load_dotenv()
    api_key = os.getenv("TMDB_API_KEY", "")
    base_url = os.getenv("TMDB_BASE_URL", "https://api.themoviedb.org/3")
    if movie_ids is None:
        movie_ids = MOVIE_IDS
    if api_key == "":
        raise SystemExit("no TMDB_API_KEY in .env. copy .env.example to .env and add the key.")

    session = requests.Session()
    session.params = {"api_key": api_key}

    out: list[Movie] = []
    for mid in movie_ids:
        m = fetch_movie(session, base_url, mid)
        if m is not None:
            out.append(m)
        time.sleep(0.1)
    log.info("fetched %d of %d ids", len(out), len(movie_ids))
    return out
```

- **Fail fast** on a missing key, with a message that says how to fix it,
  before any request is made. `os.getenv("TMDB_API_KEY", "")` turns "not
  set" into `""`, and an unedited `.env.example` line `TMDB_API_KEY=` is
  also `""`, so one check covers both.
- `movie_ids=None` then `MOVIE_IDS`: the default list, but a caller (or a
  test) can pass others.
- Skipped movies (`None`) are not appended. On the real run the log line
  reads "fetched 18 of 19 ids".

### `save_jsonl`: one line per movie, written safely

```python
def save_jsonl(records: list[Movie], path: str | Path = RAW_PATH) -> Path:
    """One movie per line. spark reads json lines in parallel, a single big
    json array it can only read as one piece (multiLine)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # write to a temp file then rename, so a crash halfway never leaves
    # spark a half written file to read
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    os.replace(tmp, path)
    log.info("saved %d movies to %s", len(records), path)
    return path
```

- `mkdir(parents=True, exist_ok=True)` creates `data/` on a fresh clone.
- **Write then rename.** The lines go to `raw_movies.tmp`; only when all
  are written does `os.replace` rename it to `raw_movies.jsonl`. A rename
  is all-or-nothing, so the real file is either the old complete one or the
  new complete one, never half written. This is called an **atomic write**.
- Mode `"w"` overwrites, so re-fetching never appends duplicates.
- `encoding="utf-8"` matters on Windows, where the default encoding is
  cp1252 and cannot store the Japanese, Korean or Hindi language names in
  `spoken_languages`.

### The `__main__` block

```python
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    save_jsonl(fetch_all())
```

Lets you run Step 1 alone with `python -m src.tmdb_client`. `main.py` calls
the same two functions when the raw file is missing or `--fetch` is given:

```python
if args.fetch or not RAW_PATH.exists():
    save_jsonl(fetch_all())
```

## 8. How the client is tested without the network

`tests/test_tmdb_client.py` has 5 tests. None of them calls TMDB. A
`FakeSession` returns a list of status codes, one per call, and counts the
calls:

```python
class FakeSession(requests.Session):
    def __init__(self, statuses: list[int], data: object = None) -> None:
        super().__init__()
        self.statuses = list(statuses)
        self.data = data
        self.calls = 0

    def get(self, url: str | bytes, **kwargs: Any) -> requests.Response:
        self.calls += 1
        return FakeResponse(self.data, self.statuses.pop(0))


def no_wait(seconds: float) -> None:
    pass


def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tmdb_client.time, "sleep", no_wait)
```

`monkeypatch` is a pytest fixture that swaps something out for one test
only. Replacing `time.sleep` means the backoff waits (2 + 4 + 8 s) take no
time in tests.

| Test | Fake statuses | Proves |
|---|---|---|
| `test_movie_ids_match_lab_spec` | — | 19 IDs, first is `0` |
| `test_404_is_skipped` | `[404]` | Returns `None` |
| `test_429_then_ok_retries` | `[429, 200]` | Returns Titanic; `s.calls == 2` |
| `test_gives_up_after_three_tries` | `[503, 503, 503]` | Returns `None`; `s.calls == 3` |
| `test_save_jsonl_one_movie_per_line` | — | Each line parses back; no `raw.tmp` left behind |

## 9. What one raw record looks like

Each line of `data/raw_movies.jsonl` is one movie. TMDB sends 28
top-level keys; most of the file's size is `credits` (thousands of crew
entries in total). Here is *Avengers: Endgame* (ID 299534), trimmed: long
text is cut and the `cast`/`crew` lists show one entry each.

```json
{
  "adult": false,
  "backdrop_path": "/7RyHsO4yDXtBv1zUU3mTpHeQ0d5.jpg",
  "belongs_to_collection": {"id": 86311, "name": "The Avengers Collection",
                            "poster_path": "/yFSI...jpg", "backdrop_path": "/2UNU...jpg"},
  "budget": 356000000,
  "genres": [{"id": 12, "name": "Adventure"}, {"id": 878, "name": "Science Fiction"},
             {"id": 28, "name": "Action"}],
  "homepage": "https://www.marvel.com/movies/avengers-endgame",
  "id": 299534,
  "imdb_id": "tt4154796",
  "origin_country": ["US"],
  "original_language": "en",
  "original_title": "Avengers: Endgame",
  "overview": "After the devastating events of Avengers: Infinity War, ...",
  "popularity": 64.9252,
  "poster_path": "/jtDv3lCnST0fZRvdYFwR5CVwK2x.jpg",
  "production_companies": [{"id": 420, "logo_path": "/hUze...png",
                            "name": "Marvel Studios", "origin_country": "US"}],
  "production_countries": [{"iso_3166_1": "US", "name": "United States of America"}],
  "release_date": "2019-04-24",
  "revenue": 2942186933,
  "runtime": 181,
  "softcore": false,
  "spoken_languages": [{"english_name": "English", "iso_639_1": "en", "name": "English"},
                       {"english_name": "Japanese", "iso_639_1": "ja", "name": "日本語"},
                       {"english_name": "Xhosa", "iso_639_1": "xh", "name": ""}],
  "status": "Released",
  "tagline": "Avenge the fallen.",
  "title": "Avengers: Endgame",
  "video": false,
  "vote_average": 8.247,
  "vote_count": 29209,
  "credits": {
    "cast": [{"adult": false, "gender": 2, "id": 3223, "known_for_department": "Acting",
              "name": "Robert Downey Jr.", "original_name": "Robert Downey Jr.",
              "popularity": 11.4825, "profile_path": "/5qHN...jpg", "cast_id": 493,
              "character": "Tony Stark / Iron Man", "credit_id": "5e85...", "order": 0},
             "... more cast"],
    "crew": [{"adult": false, "gender": 0, "id": 3019687, "known_for_department": "Directing",
              "name": "Paul Schneider", "original_name": "Paul Schneider", "popularity": 0.255,
              "profile_path": null, "credit_id": "6057...", "department": "Directing",
              "job": "Second Assistant Director"},
             "... more crew"]
  }
}
```

Things to notice, because the cleaning step depends on them:

| Observation | Consequence in `cleaning.py` |
|---|---|
| `belongs_to_collection` is an **object** (or `null` for Titanic and Beauty and the Beast) | `extract_collection_name` takes `.name`; null stays null |
| `genres`, `production_*`, `spoken_languages` are **lists of objects** | `pipe_join_names` pulls out one field and joins with `"\|"` |
| Xhosa's native `name` is `""`, Japanese's is `日本語` | Spoken languages use `english_name` |
| Money is in plain dollars (`356000000`) | Divide by 1,000,000 for `budget_musd` (Endgame: 356.00 in `results.md`) |
| `credits.crew` holds every job, including "Second Assistant Director" | Keep only `job == "Director"`, an exact match. Endgame's directors: `Joe Russo\|Anthony Russo` |
| `release_date` is a **string** | Convert to a real date |
| Keys the spec does not need: `backdrop_path`, `origin_country`, `softcore`, ... | Not listed in `RAW_SCHEMA`, so Spark never reads them (see [05](05-dataframe-fundamentals.md#h-3-schemas-inference-versus-an-explicit-schema)) |

## Self-check

1. What does `append_to_response=credits` save? [A second request per movie: 19 calls instead of 38.]
2. Why is 429 retried but 401 not? [429 is temporary (rate limit); a bad key stays bad.]
3. What are the waits between three failed attempts, and why are the tests not slow? [2 s, 4 s, 8 s; the tests replace `time.sleep` with a no-op through `monkeypatch`.]
4. Why write to `raw_movies.tmp` and then rename? [The rename is atomic, so a crash never leaves a half-written raw file for Spark.]
5. What happened to movie ID 0? [TMDB returned 404; it was logged and skipped. 18 of 19 saved.]
