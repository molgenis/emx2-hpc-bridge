import logging

from .config import Config
from .job_store import JobStore
from .slurm_monitor import SlurmMonitor
from .task_poller import TaskPoller


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    print(f"Connecting to {Config.emx2_server} with schema {Config.emx2_schema}")
    print(f"Polling every {Config.poll_interval} seconds")

    poller = TaskPoller(
        server=Config.emx2_server,
        schema=Config.emx2_schema,
        token=Config.emx2_poller_jwt_token,
        poll_interval=Config.poll_interval,
        job_store=JobStore(Config.db_path),
        slurm_monitor=SlurmMonitor(job_name=Config.hpc_job_name, user=Config.hpc_user),
    )
    poller.run()


if __name__ == "__main__":
    main()
