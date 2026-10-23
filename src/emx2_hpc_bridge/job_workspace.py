from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

INPUT_FILENAME = "document.pdf"


@dataclass
class JobPaths:
    root: Path
    input_dir: Path
    logs_dir: Path
    output_dir: Path

    @property
    def input_file(self) -> Path:
        return self.input_dir / INPUT_FILENAME

    def slurm_log_args(self) -> list[str]:
        """sbatch arguments that write the job's stdout/stderr into logs/."""
        return [
            f"--output={self.logs_dir / 'slurm-%j.out'}",
            f"--error={self.logs_dir / 'slurm-%j.err'}",
        ]


class JobWorkspace:
    """Lays out a directory per claimed job on the cluster:

    <root>/job-<id>/input/document.pdf
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
