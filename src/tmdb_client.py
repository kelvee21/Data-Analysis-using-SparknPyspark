"""Step 1: get the 19 movie ids from the TMDB api and land them raw on disk.

this part is plain python, no spark. 19 http calls do not need a cluster.
spark starts at step 2 when it reads the file this writes.
"""
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

log = logging.getLogger(__name__)

# one movie as tmdb sends it. nested and loose, so a plain dict, not a class
Movie = dict[str, Any]

# id 0 is not a real movie. the brief puts it in to see if the code copes.
MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397,
    420818, 24428, 168259, 99861, 284054, 12445,
    181808, 330457, 351286, 109445, 321612, 260513,
]

RAW_PATH = Path(__file__).resolve().parents[1] / "data" / "raw_movies.jsonl"
TIMEOUT = 20
RETRYABLE = {429, 500, 502, 503, 504}


def fetch_movie(
    session: requests.Session, base_url: str, movie_id: int, retries: int = 3
) -> Movie | None:
    """One movie plus its credits, or None if tmdb does not have it.

    append_to_response=credits puts cast and crew in the same answer, so it
    is one call per movie instead of two. i keep the credits raw here and
    flatten them in spark, so the raw file is exactly what the api sent.
    """
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


def fetch_all(movie_ids: list[int] | None = None) -> list[Movie]:
    load_dotenv()
    api_key = os.getenv("TMDB_API_KEY", "")
    base_url = os.getenv("TMDB_BASE_URL", "https://api.themoviedb.org/3")
    if movie_ids is None:
        movie_ids = MOVIE_IDS
    if api_key == "":
        raise SystemExit("no TMDB_API_KEY in .env. copy .env.example to .env and add the key.")

    # one session reuses the tcp connection for all 19 calls. the key goes in
    # the session params so it is never part of a url i log.
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    save_jsonl(fetch_all())
