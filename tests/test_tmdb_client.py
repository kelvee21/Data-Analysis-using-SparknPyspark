import json
from pathlib import Path
from typing import Any

import pytest
import requests

import src.tmdb_client as tmdb_client
from src.tmdb_client import MOVIE_IDS, fetch_movie, save_jsonl


class FakeResponse(requests.Response):
    # a real Response subclass, so fetch_movie gets the type it asks for
    def __init__(self, data: object, status: int) -> None:
        super().__init__()
        self.data = data
        self.status_code = status

    def json(self, **kwargs: Any) -> Any:
        return self.data

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")


class FakeSession(requests.Session):
    """Answers with the statuses in the list, one per call."""

    def __init__(self, statuses: list[int], data: object = None) -> None:
        super().__init__()
        self.statuses = list(statuses)
        self.data = data
        self.calls = 0

    def get(self, url: str | bytes, **kwargs: Any) -> requests.Response:  # pyright: ignore[reportIncompatibleMethodOverride]
        self.calls += 1
        return FakeResponse(self.data, self.statuses.pop(0))


def no_wait(seconds: float) -> None:
    pass


def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tmdb_client.time, "sleep", no_wait)


def test_movie_ids_match_lab_spec() -> None:
    assert len(MOVIE_IDS) == 19
    assert MOVIE_IDS[0] == 0


def test_404_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    no_sleep(monkeypatch)
    assert fetch_movie(FakeSession([404]), "http://x", 0) is None


def test_429_then_ok_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    no_sleep(monkeypatch)
    s = FakeSession([429, 200], data={"id": 597, "title": "Titanic"})
    found = fetch_movie(s, "http://x", 597)
    assert found is not None and found["title"] == "Titanic"
    assert s.calls == 2


def test_gives_up_after_three_tries(monkeypatch: pytest.MonkeyPatch) -> None:
    no_sleep(monkeypatch)
    s = FakeSession([503, 503, 503])
    assert fetch_movie(s, "http://x", 597) is None
    assert s.calls == 3


def test_save_jsonl_one_movie_per_line(tmp_path: Path) -> None:
    path = save_jsonl([{"id": 1}, {"id": 2}], tmp_path / "raw.jsonl")
    lines = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(x)["id"] for x in lines] == [1, 2]
    assert not (tmp_path / "raw.tmp").exists()
