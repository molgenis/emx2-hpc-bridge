import subprocess
from unittest.mock import MagicMock

import pytest

from emx2_hpc_bridge.slurm_monitor import SlurmJob, SlurmMonitor


def make_monitor(stdout):
    run = MagicMock()
    run.return_value.stdout = stdout
    return SlurmMonitor(job_name="cohort_extract", user="umcg-test", run=run), run


def test_list_jobs_calls_squeue_for_user_and_job_name():
    monitor, run = make_monitor("")
    monitor.list_jobs()
    args = run.call_args.args[0]
    assert args[0] == "squeue"
    assert "--user=umcg-test" in args
    assert "--name=cohort_extract" in args
    assert run.call_args.kwargs["check"] is True


def test_list_jobs_parses_squeue_output():
    monitor, _ = make_monitor(
        "123|cohort_extract|RUNNING|5:01\n124|cohort_extract|PENDING|0:00\n"
    )
    assert monitor.list_jobs() == [
        SlurmJob("123", "cohort_extract", "RUNNING", "5:01"),
        SlurmJob("124", "cohort_extract", "PENDING", "0:00"),
    ]


def test_running_jobs_ignores_pending_jobs():
    monitor, _ = make_monitor("124|cohort_extract|PENDING|0:00\n")
    assert monitor.running_jobs() == []
    assert monitor.is_job_running() is False


def test_is_job_running_when_job_running():
    monitor, _ = make_monitor("123|cohort_extract|RUNNING|5:01\n")
    assert monitor.is_job_running() is True


def test_empty_queue_has_no_jobs():
    monitor, _ = make_monitor("\n")
    assert monitor.list_jobs() == []


def test_squeue_failure_is_raised():
    monitor, run = make_monitor("")
    run.side_effect = subprocess.CalledProcessError(1, "squeue")
    with pytest.raises(subprocess.CalledProcessError):
        monitor.list_jobs()
