import logging
from unittest.mock import MagicMock

import pytest
import requests

from emx2_hpc_bridge.emx2_status import Emx2JobStatus
from emx2_hpc_bridge.job_store import JobStatus, JobStore
from emx2_hpc_bridge.job_workspace import JobWorkspace
from emx2_hpc_bridge.slurm_monitor import SlurmJob
from emx2_hpc_bridge.task_poller import GraphQLError, TaskPoller


def is_mutation(body):
    return body["query"].lstrip().startswith("mutation")


def mutation_ok():
    response = MagicMock()
    response.json.return_value = {"data": {"update": {"message": "1 Jobs updated"}}}
    return response


def mutation_rejected(message="Status label FAILED not found"):
    response = MagicMock()
    response.json.return_value = {"errors": [{"message": message}]}
    return response


def make_poller(jobs):
    session = MagicMock()
    session.headers = {}
    # Queries get session.post.return_value, so tests can tweak it; mutations
    # get an explicit success response
    session.post.return_value.json.return_value = {"data": {"Jobs": jobs}}
    session.post.side_effect = lambda url, json, timeout: (
        mutation_ok() if is_mutation(json) else session.post.return_value
    )
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


def test_set_status_raises_on_graphql_errors():
    poller, session, _ = make_poller([])
    session.post.side_effect = [mutation_rejected("no such label")]
    with pytest.raises(GraphQLError, match="no such label"):
        poller.set_status("job-1", Emx2JobStatus.FAILED)


def test_rejected_fail_mutation_is_logged(tmp_path, caplog):
    poller, session, _ = make_download_poller(tmp_path, {"id": "job-1", "input": None})
    session.post.side_effect = lambda url, json, timeout: (
        mutation_rejected() if is_mutation(json) else session.post.return_value
    )
    poller.claim_job = MagicMock()
    with caplog.at_level(logging.ERROR):
        poller.run(max_polls=1)
    poller.claim_job.assert_not_called()
    assert "Poll 1 failed" in caplog.text
    assert "Setting job job-1 to FAILED was rejected" in caplog.text
    assert "Status label FAILED not found" in caplog.text


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
    session.post.side_effect = [ok, failed, ok, mutation_ok()]
    poller.run(max_polls=2)
    assert session.post.call_count == 4
    assert poller.job_store.get("job-1").status is JobStatus.CLAIMED


def test_poll_checks_cluster_before_fetching_jobs():
    poller, _, _ = make_poller([])
    calls = MagicMock()
    calls.monitor.running_jobs.return_value = []
    calls.fetch.return_value = None
    poller.slurm_monitor = calls.monitor
    poller.fetch_next_job = calls.fetch
    poller.run(max_polls=1)
    assert [c[0] for c in calls.mock_calls] == ["monitor.running_jobs", "fetch"]


def test_check_cluster_reports_running_job():
    poller, _, _ = make_poller([])
    poller.slurm_monitor = MagicMock()
    poller.slurm_monitor.running_jobs.return_value = [
        SlurmJob("123", "cohort_extract", "RUNNING", "5:01")
    ]
    assert poller.check_cluster() is True


def test_check_cluster_without_monitor_returns_none():
    poller, _, _ = make_poller([])
    assert poller.check_cluster() is None


def test_failed_cluster_check_does_not_stop_claiming(caplog):
    poller, _, _ = make_poller([{"id": "job-1"}])
    poller.slurm_monitor = MagicMock()
    poller.slurm_monitor.running_jobs.side_effect = FileNotFoundError("squeue")
    poller.job_store = MagicMock()
    with caplog.at_level(logging.ERROR):
        poller.run(max_polls=1)
    assert "Checking Slurm queue failed" in caplog.text
    poller.job_store.add_claimed.assert_called_once_with("job-1")


def make_download_poller(tmp_path, job):
    poller, session, _ = make_poller([job])
    poller.workspace = JobWorkspace(tmp_path)
    download = session.get.return_value.__enter__.return_value
    download.iter_content.return_value = [b"%PDF-", b"1.7"]
    return poller, session, download


PDF_JOB = {"id": "job-1", "input": {"url": "/extractJobs/api/file/Jobs/input/abc"}}


def test_claimed_job_input_is_downloaded_to_workspace(tmp_path):
    poller, session, _ = make_download_poller(tmp_path, PDF_JOB)
    poller.run(max_polls=1)
    url = session.get.call_args.args[0]
    assert url == "https://emx2.example/extractJobs/api/file/Jobs/input/abc"
    pdf = tmp_path / "job-job-1" / "input" / "document.pdf"
    assert pdf.read_bytes() == b"%PDF-1.7"
    assert not pdf.with_name("document.pdf.part").exists()
    assert (tmp_path / "job-job-1" / "logs").is_dir()
    assert (tmp_path / "job-job-1" / "output").is_dir()
    claim = session.post.call_args.kwargs["json"]["variables"]
    assert claim["value"][0]["status"] == {"label": "PENDING"}


def test_transient_download_error_leaves_job_for_retry(tmp_path, caplog):
    poller, session, download = make_download_poller(tmp_path, PDF_JOB)
    download.status_code = 500
    download.raise_for_status.side_effect = requests.HTTPError("500")
    poller.job_store = JobStore(":memory:")
    poller.claim_job = MagicMock()
    poller.fail_job = MagicMock()
    with caplog.at_level(logging.ERROR):
        poller.run(max_polls=1)
    poller.claim_job.assert_not_called()
    poller.fail_job.assert_not_called()
    assert poller.job_store.get("job-1") is None
    assert not (tmp_path / "job-job-1" / "input" / "document.pdf").exists()


def test_job_without_input_is_failed(tmp_path):
    poller, _, _ = make_download_poller(tmp_path, {"id": "job-1", "input": None})
    poller.job_store = JobStore(":memory:")
    poller.claim_job = MagicMock()
    poller.fail_job = MagicMock()
    poller.run(max_polls=1)
    poller.claim_job.assert_not_called()
    poller.fail_job.assert_called_once_with("job-1")
    assert poller.job_store.get("job-1") is None


def test_missing_input_file_is_failed(tmp_path):
    poller, _, download = make_download_poller(tmp_path, PDF_JOB)
    download.status_code = 404
    poller.claim_job = MagicMock()
    poller.fail_job = MagicMock()
    poller.run(max_polls=1)
    poller.claim_job.assert_not_called()
    poller.fail_job.assert_called_once_with("job-1")


class FakeJobsServer:
    """Answers the poller's GraphQL calls from an in-memory Jobs table."""

    def __init__(self, jobs):
        self.jobs = jobs  # oldest first
        self.status = {job["id"]: Emx2JobStatus.CREATED.value for job in jobs}

    def post(self, url, json, timeout):
        if is_mutation(json):
            for row in json["variables"]["value"]:
                self.status[row["id"]] = row["status"]["label"]
            return mutation_ok()
        response = MagicMock()
        created = [
            job
            for job in self.jobs
            if self.status[job["id"]] == Emx2JobStatus.CREATED.value
        ]
        response.json.return_value = {"data": {"Jobs": created[:1]}}
        return response


@pytest.mark.parametrize(
    "bad_job, http_status",
    [
        ({"id": "bad", "input": None}, 200),
        ({"id": "bad", "input": {"url": "/extractJobs/api/file/Jobs/input/gone"}}, 404),
    ],
)
def test_unavailable_oldest_job_does_not_block_later_jobs(
    tmp_path, bad_job, http_status
):
    good_job = {"id": "good", "input": {"url": "/extractJobs/api/file/Jobs/input/ok"}}
    server = FakeJobsServer([bad_job, good_job])
    poller, session, download = make_download_poller(tmp_path, good_job)
    session.post.side_effect = server.post

    def get(url, stream, timeout):
        download.status_code = http_status if url.endswith("/gone") else 200
        return session.get.return_value

    session.get.side_effect = get
    poller.run(max_polls=2)
    assert server.status == {
        "bad": Emx2JobStatus.FAILED.value,
        "good": Emx2JobStatus.PENDING.value,
    }
    assert (tmp_path / "job-good" / "input" / "document.pdf").exists()
