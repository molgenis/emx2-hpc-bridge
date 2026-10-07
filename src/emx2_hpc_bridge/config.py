import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    emx2_server = os.environ["EMX2_SERVER"]
    emx2_schema = os.environ["EMX2_SCHEMA"]
    emx2_poller_jwt_token = os.environ["EMX2_POLLER_JWT_TOKEN"]
    poll_interval = int(os.getenv("POLL_INTERVAL", "30"))
    db_path = os.getenv("DB_PATH", "jobs.db")
    # Not prefixed with SLURM_: Slurm itself sets SLURM_JOB_NAME inside jobs
    hpc_job_name = os.getenv("HPC_JOB_NAME", "cohort_extract")
    hpc_user = os.getenv("HPC_USER") or None
