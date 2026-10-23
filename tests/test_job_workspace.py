import pytest

from emx2_hpc_bridge.job_workspace import JobWorkspace


def test_prepare_creates_job_directories(tmp_path):
    paths = JobWorkspace(tmp_path).prepare("123")
    assert paths.root == tmp_path / "job-123"
    assert (tmp_path / "job-123" / "input").is_dir()
    assert (tmp_path / "job-123" / "logs").is_dir()
    assert (tmp_path / "job-123" / "output").is_dir()
    assert paths.input_file == tmp_path / "job-123" / "input" / "document.pdf"


def test_prepare_is_idempotent(tmp_path):
    workspace = JobWorkspace(tmp_path)
    workspace.prepare("123")
    (tmp_path / "job-123" / "output" / "result.json").write_text("{}")
    workspace.prepare("123")
    assert (tmp_path / "job-123" / "output" / "result.json").exists()


def test_slurm_log_args_point_to_logs_dir(tmp_path):
    paths = JobWorkspace(tmp_path).paths("123")
    assert paths.slurm_log_args() == [
        f"--output={tmp_path}/job-123/logs/slurm-%j.out",
        f"--error={tmp_path}/job-123/logs/slurm-%j.err",
    ]


@pytest.mark.parametrize("job_id", ["", ".", "..", "../etc", "a/b", "a\\b"])
def test_rejects_ids_that_escape_root(tmp_path, job_id):
    with pytest.raises(ValueError):
        JobWorkspace(tmp_path).paths(job_id)
