import subprocess
from unittest.mock import MagicMock

import pytest

from emx2_hpc_bridge.job_workspace import JobWorkspace
from emx2_hpc_bridge.slurm_submitter import SlurmSubmitter


def make_submitter(tmp_path, stdout="4242\n"):
    run = MagicMock()
    run.return_value.stdout = stdout
    submitter = SlurmSubmitter(script_dir=tmp_path / "paper-extracting", run=run)
    paths = JobWorkspace(tmp_path / "jobs").prepare("7")
    return submitter, paths, run


def test_submit_returns_slurm_job_id(tmp_path):
    submitter, paths, run = make_submitter(tmp_path)
    assert submitter.submit(paths) == "4242"
    assert run.call_args.kwargs["check"] is True


def test_submit_strips_cluster_name(tmp_path):
    submitter, paths, _ = make_submitter(tmp_path, stdout="4242;gearshift\n")
    assert submitter.submit(paths) == "4242"


def test_command_uses_job_workspace(tmp_path):
    submitter, paths, run = make_submitter(tmp_path)
    submitter.submit(paths)
    args = run.call_args.args[0]
    job_dir = tmp_path / "jobs" / "job-7"
    assert args[:2] == ["sbatch", "--parsable"]
    assert "--job-name=cohort_extract" in args
    assert f"--chdir={tmp_path / 'paper-extracting'}" in args
    assert f"--output={job_dir}/logs/slurm-%j.out" in args
    assert f"--error={job_dir}/logs/slurm-%j.err" in args
    assert f"--export=ALL,RUN_DIR={job_dir}/logs/run,SYNC_OUTPUT_ENABLE=0" in args
    script = args.index(str(tmp_path / "paper-extracting/run_cluster_cohort.sbatch"))
    assert args[script + 1 :] == [
        "--pdfs",
        f"{job_dir}/input/document.pdf",
        "-p",
        "all",
        "-o",
        f"{job_dir}/output/result.xlsx",
    ]


def test_sbatch_failure_includes_stderr(tmp_path):
    submitter, paths, run = make_submitter(tmp_path)
    run.side_effect = subprocess.CalledProcessError(
        1, "sbatch", stderr="Invalid account\n"
    )
    with pytest.raises(RuntimeError, match="Invalid account"):
        submitter.submit(paths)


def test_unexpected_sbatch_output_raises(tmp_path):
    submitter, paths, _ = make_submitter(tmp_path, stdout="Submitted batch job\n")
    with pytest.raises(RuntimeError, match="Unexpected sbatch output"):
        submitter.submit(paths)


def test_comma_in_path_is_rejected(tmp_path):
    submitter, _, _ = make_submitter(tmp_path)
    paths = JobWorkspace(tmp_path / "a,b").paths("7")
    with pytest.raises(ValueError):
        submitter.command(paths)
