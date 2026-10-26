from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

INPUT_FILENAME = "document.pdf"
OUTPUT_FILENAME = "result.xlsx"


@dataclass
class JobPaths:
    root: Path
    input_dir: Path
    logs_dir: Path
    output_dir: Path

    @property
    def input_file(self) -> Path:
        return self.input_dir / INPUT_FILENAME

    @property
    def output_file(self) -> Path:
        return self.output_dir / OUTPUT_FILENAME

    @property
    def run_dir(self) -> Path:
        """Where paper-extracting writes its run artifacts and model logs."""
        return self.logs_dir / "run"

    def is_ready(self) -> bool:
        """Whether the directories exist and the input was fully downloaded.

        Downloads are renamed into place only once complete, so an existing,
        non-empty input file is a complete one.
        """
        return (
            all(d.is_dir() for d in (self.input_dir, self.logs_dir, self.output_dir))
            and self.input_file.is_file()
            and self.input_file.stat().st_size > 0
        )

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
    <root>/job-<id>/output/result.xlsx
    """

    def __init__(self, root: str | Path):
        # Absolute, since sbatch runs the job from the paper-extracting checkout
        self.root = Path(root).absolute()

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
