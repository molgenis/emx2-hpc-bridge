from unittest.mock import MagicMock

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
