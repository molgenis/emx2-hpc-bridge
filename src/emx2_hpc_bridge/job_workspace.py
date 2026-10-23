from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def input_filename(filename: str | None, extension: str | None) -> str:
    """Name for the stored input file: the uploaded file's name, with its
    extension appended when the name does not already end with it.

    Raises ValueError for names that are missing or could escape input/.
    """
    name = (filename or "").strip()
    if not name or "/" in name or "\\" in name or name in (".", ".."):
        raise ValueError(f"Invalid input filename: {filename!r}")
    extension = (extension or "").strip().lstrip(".")
    if extension and not name.lower().endswith(f".{extension.lower()}"):
        name = f"{name}.{extension}"
    return name


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
        if not job_id or "/" in job_id or "\\" in job_id or job_id in (".", ".."):
            raise ValueError(f"Invalid job id: {job_id!r}")
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
