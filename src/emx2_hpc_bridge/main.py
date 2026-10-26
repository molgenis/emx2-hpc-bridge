import logging

from .config import Config
from .job_store import JobStore
from .job_workspace import JobWorkspace
from .slurm_monitor import SlurmMonitor
from .slurm_submitter import SlurmSubmitter
from .task_poller import TaskPoller


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    print(f"Connecting to {Config.emx2_server} with schema {Config.emx2_schema}")
    print(f"Polling every {Config.poll_interval} seconds")

    submitter = None
    if Config.hpc_script_dir:
        submitter = SlurmSubmitter(
            script_dir=Config.hpc_script_dir,
            script=Config.hpc_job_script,
            job_name=Config.hpc_job_name,
        )
        print(f"Submitting jobs with {submitter.script}")
    else:
        print("HPC_SCRIPT_DIR not set: claimed jobs are not submitted to Slurm")

    poller = TaskPoller(
        server=Config.emx2_server,
        schema=Config.emx2_schema,
        token=Config.emx2_poller_jwt_token,
        poll_interval=Config.poll_interval,
        job_store=JobStore(Config.db_path),
        slurm_monitor=SlurmMonitor(job_name=Config.hpc_job_name, user=Config.hpc_user),
        workspace=JobWorkspace(Config.jobs_dir),
        submitter=submitter,
    )
    poller.run()


if __name__ == "__main__":
    main()
