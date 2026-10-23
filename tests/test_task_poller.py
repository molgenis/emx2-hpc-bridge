import logging
import subprocess
from unittest.mock import MagicMock

import pytest
import requests

from emx2_hpc_bridge.job_store import JobStatus, JobStore
from emx2_hpc_bridge.slurm_monitor import SlurmJob
from emx2_hpc_bridge.task_poller import TaskPoller


def make_poller(jobs):
    """Poller with a free cluster, an sbatch script and an in-memory job store."""
    session = MagicMock()
    session.headers = {}
    session.post.return_value.json.return_value = {"data": {"Jobs": jobs}}
    sleep = MagicMock()
    monitor = MagicMock()
    monitor.list_jobs.return_value = []
    monitor.submit_job.return_value = "999"
    poller = TaskPoller(
        server="https://emx2.example/",
        schema="extractJobs",
        token="secret",
        poll_interval=5,
        session=session,
        sleep=sleep,
        job_store=JobStore(":memory:"),
        slurm_monitor=monitor,
        sbatch_script="run_cluster_cohort.sbatch",
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


def test_run_claims_and_submits_job():
    poller, _, _ = make_poller([{"id": "job-1"}])
    poller.run(max_polls=1)
    poller.slurm_monitor.submit_job.assert_called_once_with(
        "run_cluster_cohort.sbatch", "job-1"
    )
    assert poller.job_store.get("job-1").status is JobStatus.SUBMITTED


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
    poller.run(max_polls=2)
    assert session.post.call_count == 2
    assert poller.job_store.list_by_status(JobStatus.CLAIMED) == []
    poller.slurm_monitor.submit_job.assert_not_called()


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
    calls.store.list_by_status.return_value = []
    poller.job_store = calls.store
    poller.claim_job = calls.claim
    poller.run(max_polls=1)
    names = [c[0] for c in calls.mock_calls if c[0] in ("store.add_claimed", "claim")]
    assert names == ["store.add_claimed", "claim"]


def test_failed_server_claim_is_reclaimed_and_submitted_on_next_poll():
    poller, session, _ = make_poller([{"id": "job-1"}])
    ok = session.post.return_value
    failed = MagicMock()
    failed.raise_for_status.side_effect = requests.HTTPError("500")
    # poll 1: fetch ok, claim fails; poll 2: claim of stored job ok, no fetch
    session.post.side_effect = [ok, failed, ok]
    poller.run(max_polls=2)
    assert session.post.call_count == 3
    poller.slurm_monitor.submit_job.assert_called_once_with(
        "run_cluster_cohort.sbatch", "job-1"
    )
    assert poller.job_store.get("job-1").status is JobStatus.SUBMITTED


def test_poll_checks_cluster_before_fetching_jobs():
    poller, _, _ = make_poller([])
    calls = MagicMock()
    calls.monitor.list_jobs.return_value = []
    calls.fetch.return_value = None
    poller.slurm_monitor = calls.monitor
    poller.fetch_next_job = calls.fetch
    poller.run(max_polls=1)
    assert [c[0] for c in calls.mock_calls] == ["monitor.list_jobs", "fetch"]


def test_check_cluster_reports_running_job():
    poller, _, _ = make_poller([])
    poller.slurm_monitor.list_jobs.return_value = [
        SlurmJob("123", "cohort_extract", "RUNNING", "5:01")
    ]
    assert poller.check_cluster() is True


def test_check_cluster_without_monitor_returns_none():
    poller, _, _ = make_poller([])
    poller.slurm_monitor = None
    assert poller.check_cluster() is None


def test_busy_cluster_does_not_claim_or_submit():
    poller, session, _ = make_poller([{"id": "job-1"}])
    poller.slurm_monitor.list_jobs.return_value = [
        SlurmJob("123", "cohort_extract", "PENDING", "0:00")
    ]
    poller.run(max_polls=1)
    session.post.assert_not_called()
    poller.slurm_monitor.submit_job.assert_not_called()
    assert poller.job_store.get("job-1") is None


def test_failed_cluster_check_does_not_claim(caplog):
    poller, session, _ = make_poller([{"id": "job-1"}])
    poller.slurm_monitor.list_jobs.side_effect = FileNotFoundError("squeue")
    with caplog.at_level(logging.ERROR):
        poller.run(max_polls=1)
    assert "Checking Slurm queue failed" in caplog.text
    session.post.assert_not_called()
    assert poller.job_store.get("job-1") is None


def test_nothing_claimed_without_monitor():
    poller, session, _ = make_poller([{"id": "job-1"}])
    poller.slurm_monitor = None
    poller.run(max_polls=1)
    session.post.assert_not_called()


def test_nothing_claimed_without_sbatch_script():
    poller, session, _ = make_poller([{"id": "job-1"}])
    poller.sbatch_script = None
    poller.run(max_polls=1)
    session.post.assert_not_called()
    poller.slurm_monitor.list_jobs.assert_not_called()


def test_nothing_submitted_when_no_job_found():
    poller, _, _ = make_poller([])
    poller.run(max_polls=1)
    poller.slurm_monitor.submit_job.assert_not_called()


def test_claimed_jobs_are_submitted_before_new_jobs():
    poller, session, _ = make_poller([{"id": "job-2"}])
    poller.job_store.add_claimed("job-1")
    poller.run(max_polls=1)
    # Only the re-claim of job-1; job-2 is not fetched or claimed
    assert session.post.call_count == 1
    variables = session.post.call_args.kwargs["json"]["variables"]
    assert variables["value"][0]["id"] == "job-1"
    poller.slurm_monitor.submit_job.assert_called_once_with(
        "run_cluster_cohort.sbatch", "job-1"
    )
    assert poller.job_store.get("job-1").status is JobStatus.SUBMITTED
    assert poller.job_store.get("job-2") is None


def test_failed_submit_is_retried_on_next_poll():
    poller, _, _ = make_poller([{"id": "job-1"}])
    poller.slurm_monitor.submit_job.side_effect = [
        subprocess.CalledProcessError(1, "sbatch"),
        "999",
    ]
    poller.run(max_polls=1)
    assert poller.job_store.get("job-1").status is JobStatus.CLAIMED
    poller.run(max_polls=1)
    assert poller.slurm_monitor.submit_job.call_count == 2
    assert poller.job_store.get("job-1").status is JobStatus.SUBMITTED
