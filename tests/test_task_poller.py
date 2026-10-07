import logging
from unittest.mock import MagicMock

import pytest
import requests

from emx2_hpc_bridge.job_store import JobStatus, JobStore
from emx2_hpc_bridge.task_poller import TaskPoller


def make_poller(jobs):
    session = MagicMock()
    session.headers = {}
    session.post.return_value.json.return_value = {"data": {"Jobs": jobs}}
    sleep = MagicMock()
    poller = TaskPoller(
        server="https://emx2.example/",
        schema="extractJobs",
        token="secret",
        poll_interval=5,
        session=session,
        sleep=sleep,
    )
    return poller, session, sleep


def test_builds_url_and_sets_token():
    poller, session, _ = make_poller([])
    assert poller.url == "https://emx2.example/extractJobs/graphql"
    assert session.headers["x-molgenis-token"] == "secret"


def test_fetch_next_job_returns_first_job():
    poller, _, _ = make_poller([{"id": "job-1"}])
    assert poller.fetch_next_job() == {"id": "job-1"}


def test_fetch_next_job_returns_none_when_empty():
    poller, _, _ = make_poller(None)
    assert poller.fetch_next_job() is None


def test_run_stops_after_max_polls():
    poller, session, sleep = make_poller([])
    poller.run(max_polls=3)
    assert session.post.call_count == 3
    sleep.assert_called_with(5)
    assert sleep.call_count == 3


def test_run_stores_claimed_job():
    poller, _, _ = make_poller([{"id": "job-1"}])
    poller.job_store = MagicMock()
    poller.run(max_polls=1)
    poller.job_store.add_claimed.assert_called_once_with("job-1")


def test_claim_job_uses_passed_job_id():
    poller, session, _ = make_poller([])
    assert poller.claim_job("job-42") == "job-42"
    variables = session.post.call_args.kwargs["json"]["variables"]
    assert variables["value"][0]["id"] == "job-42"


def test_fetch_next_job_returns_none_when_jobs_key_missing():
    poller, session, _ = make_poller([])
    session.post.return_value.json.return_value = {"data": {"Jobs_agg": {"count": 0}}}
    assert poller.fetch_next_job() is None


def test_run_does_nothing_when_no_jobs():
    poller, session, _ = make_poller([])
    session.post.return_value.json.return_value = {"data": {"Jobs_agg": {"count": 0}}}
    poller.job_store = MagicMock()
    poller.run(max_polls=2)
    assert session.post.call_count == 2
    poller.job_store.add_claimed.assert_not_called()


def test_fetch_next_job_raises_on_empty_response():
    poller, session, _ = make_poller([])
    session.post.return_value.content = b""
    with pytest.raises(ValueError):
        poller.fetch_next_job()


def test_run_logs_errors_and_keeps_polling(caplog):
    poller, session, sleep = make_poller([])
    session.post.side_effect = [
        requests.ConnectionError("boom"),
        session.post.return_value,
    ]
    with caplog.at_level(logging.ERROR):
        poller.run(max_polls=2)
    assert session.post.call_count == 2
    assert sleep.call_count == 2
    assert "Poll 1 failed" in caplog.text


def test_job_is_stored_before_server_claim():
    poller, session, _ = make_poller([{"id": "job-1"}])
    calls = MagicMock()
    poller.job_store = calls.store
    poller.claim_job = calls.claim
    poller.run(max_polls=1)
    names = [c[0] for c in calls.mock_calls if c[0] in ("store.add_claimed", "claim")]
    assert names == ["store.add_claimed", "claim"]


def test_failed_server_claim_is_retried_on_next_poll():
    poller, session, _ = make_poller([{"id": "job-1"}])
    poller.job_store = JobStore(":memory:")
    ok = session.post.return_value
    failed = MagicMock()
    failed.raise_for_status.side_effect = requests.HTTPError("500")
    # poll 1: fetch ok, claim fails; poll 2: fetch ok, claim ok
    session.post.side_effect = [ok, failed, ok, ok]
    poller.run(max_polls=2)
    assert session.post.call_count == 4
    assert poller.job_store.get("job-1").status is JobStatus.CLAIMED
