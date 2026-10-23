import pytest

from emx2_hpc_bridge.job_workspace import (
    MAX_INPUT_NAME,
    JobWorkspace,
    input_filename,
)


def test_prepare_creates_job_directories(tmp_path):
    paths = JobWorkspace(tmp_path).prepare("123")
    assert paths.root == tmp_path / "job-123"
    assert (tmp_path / "job-123" / "input").is_dir()
    assert (tmp_path / "job-123" / "logs").is_dir()
    assert (tmp_path / "job-123" / "output").is_dir()
    assert (
        paths.input_file("paper.pdf", "pdf")
        == tmp_path / "job-123" / "input" / "paper.pdf"
    )


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


@pytest.mark.parametrize("job_id", ["", ".", "..", "../etc", "a/b", "a\\b", "a\0b"])
def test_rejects_ids_that_escape_root(tmp_path, job_id):
    with pytest.raises(ValueError):
        JobWorkspace(tmp_path).paths(job_id)


@pytest.mark.parametrize(
    "filename, extension, expected",
    [
        ("paper.pdf", "pdf", "paper.pdf"),
        ("paper.PDF", "pdf", "paper.PDF"),
        ("paper", "pdf", "paper.pdf"),
        ("paper", ".pdf", "paper.pdf"),
        ("paper.pdf", None, "paper.pdf"),
    ],
)
def test_input_filename_keeps_uploaded_name(filename, extension, expected):
    assert input_filename(filename, extension) == expected


@pytest.mark.parametrize(
    "filename, expected",
    [
        (None, "input.pdf"),
        ("", "input.pdf"),
        ("  ", "input.pdf"),
        (".", "input.pdf"),
        ("..", "input.pdf"),
        (".pdf", "pdf.pdf"),
        ("../x.pdf", "_x.pdf"),
        ("a\\b.pdf", "a_b.pdf"),
        ("a/b.pdf", "a_b.pdf"),
        ("-rf.pdf", "rf.pdf"),
        ("a\nb\0c.pdf", "a_b_c.pdf"),
        ("my paper (v2).pdf", "my_paper_v2_.pdf"),
    ],
)
def test_input_filename_sanitizes_unsafe_names(filename, expected):
    assert input_filename(filename, "pdf") == expected


def test_input_filename_caps_length():
    name = input_filename("a" * 300 + ".pdf", "pdf")
    assert len(name) == MAX_INPUT_NAME
    assert name.endswith(".pdf")


def test_input_filename_sanitizes_extension():
    assert input_filename("paper", "../p df") == "paper.pdf"
