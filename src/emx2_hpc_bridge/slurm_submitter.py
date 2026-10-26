from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Callable

from .job_workspace import JobPaths
from .slurm_monitor import DEFAULT_JOB_NAME

# Slurm entry point in the paper-extracting checkout
DEFAULT_JOB_SCRIPT = "run_cluster_cohort.sbatch"


class SlurmSubmitter:
    """Submits paper-extraction jobs to Slurm with sbatch."""

    def __init__(
        self,
        script_dir: str | Path,
        script: str = DEFAULT_JOB_SCRIPT,
        job_name: str = DEFAULT_JOB_NAME,
        run: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    ):
        # The sbatch script cds into its submit dir and expects the repo there
        self.script_dir = Path(script_dir).absolute()
        self.script = self.script_dir / script
        self.job_name = job_name
        self._run = run

    def command(self, paths: JobPaths) -> list[str]:
        """sbatch command that runs the extraction on the job's input file."""
        env = {
            "RUN_DIR": paths.run_dir,
            # Results stay in the job's output dir; no rsync to elsewhere
            "SYNC_OUTPUT_ENABLE": "0",
        }
        for name, value in env.items():
            # --export separates variables with commas
            if "," in str(value):
                raise ValueError(f"{name} may not contain a comma: {value}")
        exports = ",".join(f"{name}={value}" for name, value in env.items())
        return [
            "sbatch",
            "--parsable",
            f"--job-name={self.job_name}",
            f"--chdir={self.script_dir}",
            *paths.slurm_log_args(),
            f"--export=ALL,{exports}",
            str(self.script),
            # Forwarded to paper-extracting's main_cohort.py
            "--pdfs",
            str(paths.input_file),
            "-p",
            "all",
            "-o",
            str(paths.output_file),
        ]

    def submit(self, paths: JobPaths) -> str:
        """Submit the job and return its Slurm job id."""
        try:
            result = self._run(
                self.command(paths),
                capture_output=True,
                text=True,
                timeout=60,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"sbatch failed: {(e.stderr or '').strip()}") from e
        # --parsable prints "<job id>" or "<job id>;<cluster>"
        slurm_job_id = result.stdout.strip().split(";")[0]
        if not slurm_job_id.isdigit():
            raise RuntimeError(f"Unexpected sbatch output: {result.stdout!r}")
        return slurm_job_id
