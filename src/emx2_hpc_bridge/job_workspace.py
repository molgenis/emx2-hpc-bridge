from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

# Longest stored input name; leaves room for ".part" within the usual 255-byte
# NAME_MAX
MAX_INPUT_NAME = 200
DEFAULT_INPUT_STEM = "input"
UNSAFE_NAME_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


def safe_path_component(value: str | None, what: str) -> str:
    """Return `value` if it is a single path component that cannot escape its
    parent directory; raises ValueError otherwise."""
    if not value or any(c in value for c in "/\\\0") or value in (".", ".."):
        raise ValueError(f"Invalid {what}: {value!r}")
    return value


def input_filename(filename: str | None, extension: str | None) -> str:
    """Name for the stored input file: the uploaded file's name, with its
    extension appended when the name does not already end with it.

    The name only decides where the file is stored, so it is sanitized rather
    than rejected: characters outside [A-Za-z0-9._-] become "_", leading dots
    and dashes are dropped, the length is capped and a missing name falls back
    to "input".
    """
    extension = UNSAFE_NAME_CHARS.sub("", (extension or "").strip().lstrip("."))
    suffix = f".{extension}" if extension else ""
    name = UNSAFE_NAME_CHARS.sub("_", (filename or "").strip()).lstrip(".-")
    if suffix and name.lower().endswith(suffix.lower()):
        suffix, name = name[-len(suffix) :], name[: -len(suffix)]
    name = name[: MAX_INPUT_NAME - len(suffix)].rstrip(".") or DEFAULT_INPUT_STEM
    return safe_path_component(name + suffix, "input filename")


@dataclass
class JobPaths:
    root: Path
    input_dir: Path
    logs_dir: Path
    output_dir: Path

    def input_file(self, filename: str | None, extension: str | None) -> Path:
        return self.input_dir / input_filename(filename, extension)

    def slurm_log_args(self) -> list[str]:
        """sbatch arguments that write the job's stdout/stderr into logs/."""
        return [
            f"--output={self.logs_dir / 'slurm-%j.out'}",
            f"--error={self.logs_dir / 'slurm-%j.err'}",
        ]


class JobWorkspace:
    """Lays out a directory per claimed job on the cluster:

    <root>/job-<id>/input/<input filename>
    <root>/job-<id>/logs/slurm-<slurm id>.{out,err}
    <root>/job-<id>/output/
    """

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def paths(self, job_id: str) -> JobPaths:
        # The id comes from the server; never let it escape the jobs root
        safe_path_component(job_id, "job id")
        job_dir = self.root / f"job-{job_id}"
        return JobPaths(
            root=job_dir,
            input_dir=job_dir / "input",
            logs_dir=job_dir / "logs",
            output_dir=job_dir / "output",
        )

    def prepare(self, job_id: str) -> JobPaths:
        """Create the job's directories; idempotent so a claim can be retried."""
        paths = self.paths(job_id)
        for directory in (paths.input_dir, paths.logs_dir, paths.output_dir):
            directory.mkdir(parents=True, exist_ok=True)
        return paths
