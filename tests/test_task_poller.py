import json as jsonlib
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
        self.outputs = {}  # job id -> (filename, content) of uploaded results

    def post(self, url, json=None, timeout=None, data=None, files=None):
        if data is not None:  # multipart upload
            json = {
                "query": data["query"],
                "variables": jsonlib.loads(data["variables"]),
            }
        if is_mutation(json):
            for row in json["variables"]["value"]:
                self.status[row["id"]] = row["status"]["label"]
                if "output" in row:
                    filename, f, _ = files[row["output"]]
                    self.outputs[row["id"]] = (filename, f.read())
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


class FakeSlurm:
    """Stands in for both the submitter and the monitor."""

    def __init__(self):
        self.states = {}
        self.submitted = []

    def submit(self, paths):
        slurm_job_id = str(100 + len(self.submitted))
        self.submitted.append(paths)
        self.states[slurm_job_id] = "PENDING"
        return slurm_job_id

    def job_state(self, slurm_job_id):
        return self.states.get(slurm_job_id)

    def complete(self, slurm_job_id, result=b"xlsx"):
        """Finish a job successfully, writing its result like the extraction."""
        paths = self.submitted[int(slurm_job_id) - 100]
        paths.output_file.write_bytes(result)
        self.states[slurm_job_id] = "COMPLETED"

    def list_jobs(self):
        return [
            SlurmJob(slurm_job_id, "cohort_extract", state, "0:00")
            for slurm_job_id, state in self.states.items()
            if state in ("PENDING", "RUNNING")
        ]

    def running_jobs(self):
        return []


class FakeJobsServerWithLookup(FakeJobsServer):
    def post(self, url, json=None, timeout=None, data=None, files=None):
        job_filter = ((json or {}).get("variables") or {}).get("filter", {})
        if "id" in job_filter:
            job_id = job_filter["id"]["equals"][0]
            response = MagicMock()
            jobs = []
            if job_id in self.status:
                jobs = [{"id": job_id, "status": {"label": self.status[job_id]}}]
            response.json.return_value = {"data": {"Jobs": jobs}}
            return response
        return super().post(url, json, timeout, data, files)


def make_submit_poller(tmp_path, jobs=(PDF_JOB,)):
    server = FakeJobsServerWithLookup(list(jobs))
    poller, session, _ = make_download_poller(tmp_path, jobs[0])
    session.post.side_effect = server.post
    slurm = FakeSlurm()
    poller.job_store = JobStore(":memory:")
    poller.slurm_monitor = slurm
    poller.submitter = slurm
    return poller, server, slurm


def test_claimed_job_is_submitted_in_same_poll(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.run(max_polls=1)
    stored = poller.job_store.get("job-1")
    assert stored.status is JobStatus.SUBMITTED
    assert stored.slurm_job_id == "100"
    assert slurm.submitted[0].input_file.read_bytes() == b"%PDF-1.7"
    assert server.status["job-1"] == Emx2JobStatus.PENDING.value


def test_job_not_claimed_on_server_is_not_submitted(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.job_store.add_claimed("job-1")  # e.g. the server claim failed
    poller.workspace.prepare("job-1")
    assert poller.submit_job("job-1") is None
    assert slurm.submitted == []
    assert poller.job_store.get("job-1").status is JobStatus.CLAIMED


def test_job_stopped_on_server_is_not_submitted(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.job_store.add_claimed("job-1")
    server.status["job-1"] = Emx2JobStatus.STOPPED.value
    poller.submit_claimed_jobs()
    assert slurm.submitted == []
    assert poller.job_store.get("job-1").status is JobStatus.COMPLETED


def test_incomplete_workspace_fails_job(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.job_store.add_claimed("job-1")
    server.status["job-1"] = Emx2JobStatus.PENDING.value
    poller.workspace.prepare("job-1")  # but no input downloaded
    poller.submit_claimed_jobs()
    assert slurm.submitted == []
    assert poller.job_store.get("job-1").status is JobStatus.COMPLETED
    assert server.status["job-1"] == Emx2JobStatus.FAILED.value


def test_failed_submission_is_retried_on_next_poll(tmp_path, caplog):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.submitter = MagicMock()
    poller.submitter.submit.side_effect = [RuntimeError("sbatch failed"), "100"]
    with caplog.at_level(logging.ERROR):
        poller.run(max_polls=1)
    assert "Submitting job job-1 failed" in caplog.text
    assert poller.job_store.get("job-1").status is JobStatus.CLAIMED
    poller.run(max_polls=1)
    assert poller.job_store.get("job-1").status is JobStatus.SUBMITTED


def test_submission_continues_when_claiming_fails(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.run(max_polls=1)
    poller.fetch_next_job = MagicMock(side_effect=requests.ConnectionError("boom"))
    slurm.states["100"] = "RUNNING"
    poller.run(max_polls=1)
    assert server.status["job-1"] == Emx2JobStatus.RUNNING.value


@pytest.mark.parametrize(
    "slurm_state, local, remote",
    [
        ("PENDING", JobStatus.SUBMITTED, Emx2JobStatus.PENDING),
        ("RUNNING", JobStatus.SUBMITTED, Emx2JobStatus.RUNNING),
        ("COMPLETED", JobStatus.COMPLETED, Emx2JobStatus.COMPLETED),
        ("CANCELLED", JobStatus.COMPLETED, Emx2JobStatus.STOPPED),
        ("TIMEOUT", JobStatus.COMPLETED, Emx2JobStatus.FAILED),
        ("OUT_OF_MEMORY", JobStatus.COMPLETED, Emx2JobStatus.FAILED),
    ],
)
def test_slurm_state_is_synced_to_store_and_server(
    tmp_path, slurm_state, local, remote
):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.run(max_polls=1)
    if slurm_state == "COMPLETED":
        slurm.complete("100")
    else:
        slurm.states["100"] = slurm_state
    poller.update_job_statuses()
    assert poller.job_store.get("job-1").status is local
    assert server.status["job-1"] == remote.value


def test_finished_jobs_are_no_longer_polled(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.run(max_polls=1)
    slurm.complete("100")
    poller.update_job_statuses()
    slurm.job_state = MagicMock()
    poller.update_job_statuses()
    slurm.job_state.assert_not_called()


def test_failed_server_update_keeps_local_status_for_retry(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.run(max_polls=1)
    slurm.complete("100")
    poller.post_result = MagicMock(side_effect=requests.ConnectionError("boom"))
    poller.update_job_statuses()
    assert poller.job_store.get("job-1").status is JobStatus.SUBMITTED
    assert server.status["job-1"] == Emx2JobStatus.PENDING.value
    del poller.post_result
    poller.update_job_statuses()
    assert poller.job_store.get("job-1").status is JobStatus.COMPLETED
    assert server.status["job-1"] == Emx2JobStatus.COMPLETED.value


def test_result_is_posted_to_output_column(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.run(max_polls=1)
    slurm.complete("100", result=b"PK spreadsheet")
    poller.run(max_polls=1)
    assert server.outputs == {"job-1": ("result.xlsx", b"PK spreadsheet")}
    assert server.status["job-1"] == Emx2JobStatus.COMPLETED.value
    assert poller.job_store.get("job-1").status is JobStatus.COMPLETED


def test_result_upload_is_multipart_graphql_request(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.run(max_polls=1)
    slurm.complete("100")
    poller.session.post.reset_mock()
    poller.update_job_statuses()
    _, kwargs = poller.session.post.call_args
    row = jsonlib.loads(kwargs["data"]["variables"])["value"][0]
    assert row["output"] in kwargs["files"]
    assert kwargs["data"]["query"].lstrip().startswith("mutation")


@pytest.mark.parametrize("result", [None, b""])
def test_completed_job_without_result_is_failed(tmp_path, caplog, result):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.run(max_polls=1)
    if result is not None:
        slurm.submitted[0].output_file.write_bytes(result)
    slurm.states["100"] = "COMPLETED"
    with caplog.at_level(logging.ERROR):
        poller.update_job_statuses()
    assert "completed without writing" in caplog.text
    assert server.status["job-1"] == Emx2JobStatus.FAILED.value
    assert server.outputs == {}
    assert poller.job_store.get("job-1").status is JobStatus.COMPLETED


def test_rejected_result_upload_is_retried(tmp_path, caplog):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.run(max_polls=1)
    slurm.complete("100")
    post = poller.session.post.side_effect
    poller.session.post.side_effect = lambda url, **kwargs: (
        mutation_rejected("upload failed") if "files" in kwargs else post(url, **kwargs)
    )
    with caplog.at_level(logging.ERROR):
        poller.update_job_statuses()
    assert "Posting result of job job-1 was rejected" in caplog.text
    assert poller.job_store.get("job-1").status is JobStatus.SUBMITTED
    poller.session.post.side_effect = post
    poller.update_job_statuses()
    assert server.outputs["job-1"][0] == "result.xlsx"
    assert poller.job_store.get("job-1").status is JobStatus.COMPLETED


def test_unchanged_slurm_state_is_not_pushed_again(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.run(max_polls=1)
    slurm.states["100"] = "RUNNING"
    poller.update_job_statuses()
    poller.set_status = MagicMock()
    poller.update_job_statuses()
    poller.set_status.assert_not_called()


def test_unknown_slurm_job_is_left_alone(tmp_path, caplog):
    poller, server, slurm = make_submit_poller(tmp_path)
    poller.run(max_polls=1)
    slurm.states.clear()
    with caplog.at_level(logging.WARNING):
        poller.update_job_statuses()
    assert "unknown Slurm state" in caplog.text
    assert poller.job_store.get("job-1").status is JobStatus.SUBMITTED


JOB_2 = {"id": "job-2", "input": {"url": "/extractJobs/api/file/Jobs/input/def"}}


def test_only_one_extraction_job_is_submitted_at_a_time(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path, jobs=(PDF_JOB, JOB_2))
    poller.run(max_polls=2)
    assert poller.job_store.get("job-1").status is JobStatus.SUBMITTED
    assert poller.job_store.get("job-2").status is JobStatus.CLAIMED
    assert len(slurm.submitted) == 1

    slurm.states["100"] = "RUNNING"
    poller.run(max_polls=1)
    assert len(slurm.submitted) == 1

    slurm.complete("100")
    poller.run(max_polls=1)
    assert poller.job_store.get("job-1").status is JobStatus.COMPLETED
    assert poller.job_store.get("job-2").status is JobStatus.SUBMITTED
    assert slurm.submitted[1].root.name == "job-job-2"


def test_job_in_slurm_queue_blocks_submission(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path)
    slurm.states["99"] = "PENDING"  # e.g. submitted by hand
    poller.run(max_polls=1)
    assert slurm.submitted == []
    assert poller.job_store.get("job-1").status is JobStatus.CLAIMED


def test_failing_slurm_check_blocks_submission(tmp_path, caplog):
    poller, server, slurm = make_submit_poller(tmp_path)
    slurm.list_jobs = MagicMock(side_effect=FileNotFoundError("squeue"))
    with caplog.at_level(logging.ERROR):
        poller.run(max_polls=1)
    assert "Checking for active extraction failed" in caplog.text
    assert slurm.submitted == []


def test_skipped_claimed_job_does_not_block_next_one(tmp_path):
    poller, server, slurm = make_submit_poller(tmp_path, jobs=(PDF_JOB, JOB_2))
    poller.claim_job = MagicMock()  # claims stay local, server keeps CREATED
    poller.run(max_polls=1)
    server.status["job-2"] = Emx2JobStatus.PENDING.value
    poller.job_store.add_claimed("job-2")
    poller.workspace.prepare("job-2").input_file.write_bytes(b"%PDF-1.7")
    poller.submit_claimed_jobs()
    assert poller.job_store.get("job-1").status is JobStatus.CLAIMED
    assert poller.job_store.get("job-2").status is JobStatus.SUBMITTED
