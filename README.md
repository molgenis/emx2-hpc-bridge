# EMX2 HPC Bridge

## intoduction

The repo contains a set of scripts, docs, and config files that enables the [paper-extracting](https://github.com/molgenis/paper-extracting) tool to run a extraction request submitted via [emx2](https://github.com/molgenis/molgenis-emx2) on the [high performance computing](https://umcgresearch.org/w/high-performance-computing) (HPC) cluster. 

## General design

<img width="1579" height="996" alt="image" src="https://github.com/user-attachments/assets/ffef9404-8617-426f-8c68-7d524031ae97" />

## Development

### Prerequisites

- Python 3.9 or newer
- git

### Setup

Clone the repository and create a virtual environment:

```bash
git clone git@github.com:molgenis/emx2-hpc-bridge.git
cd emx2-hpc-bridge
python3 -m venv .venv
source .venv/bin/activate
```

Install the package in editable mode, together with the development tools (pytest, flake8, black):

```bash
pip install -e '.[dev]'
```

### Configuration

The bridge reads its configuration from environment variables, which can be put in a `.env` file in the project root. Copy the example file and fill in the values:

```bash
cp .env.example .env
```

| Variable                | Description                                       | Required |
| ----------------------- | ------------------------------------------------- | -------- |
| `EMX2_SERVER`           | URL of the EMX2 instance                          | yes      |
| `EMX2_SCHEMA`           | EMX2 schema that holds the extraction jobs        | yes      |
| `EMX2_POLLER_JWT_TOKEN` | JWT token used to authenticate against EMX2       | yes      |
| `POLL_INTERVAL`         | Seconds between polls (default `30`)              | no       |
| `DB_PATH`               | Location to store sqlitedb (hpc que queue)        | no       |
| `JOBS_DIR`              | Directory holding a folder per claimed job (default `jobs`) | no |
| `HPC_JOB_NAME`          | Slurm job name of the extraction job (default `cohort_extract`) | no |
| `HPC_USER`              | Slurm user whose jobs are checked (default: current user) | no |
| `HPC_SCRIPT_DIR`        | paper-extracting checkout to submit jobs from; without it claimed jobs are not submitted | on the cluster |
| `HPC_JOB_SCRIPT`        | sbatch script in `HPC_SCRIPT_DIR` (default `run_cluster_cohort.sbatch`) | no |

`.env` is ignored by git; never commit real tokens.

### Job directories

For every claimed job the bridge creates a directory under `JOBS_DIR` and downloads the job's input PDF into it before claiming the job in EMX2:

```
<JOBS_DIR>/
└── job-<id>/
    ├── input/
    │   └── document.pdf
    ├── logs/        # slurm-<slurm job id>.out / .err
    │   └── run/     # paper-extracting run artifacts and model server logs
    └── output/
        └── result.xlsx
```

On the cluster, set `JOBS_DIR=/groups/umcg-as/tmp04/projects/paper-extracting/jobs`.

### Job lifecycle

Each poll the bridge:

1. claims the oldest `CREATED` job (workspace prepared, input downloaded, status `PENDING` on EMX2);
2. checks the Slurm state (`squeue`, then `sacct` for finished jobs) of the submitted job and copies status changes to the local database and EMX2 (see the table below);
3. submits the oldest claimed job with `sbatch`, but only when no extraction job is active: none is `submitted` locally and Slurm's queue has no `HPC_JOB_NAME` job (pending or running). It checks that EMX2 still has it as `PENDING` and that its workspace is complete. The job runs `HPC_JOB_SCRIPT --pdfs <input/document.pdf> -p all -o <output/result.xlsx>` with `RUN_DIR=<logs/run>` and output syncing disabled. A claimed job that has already ended on EMX2 (or was deleted there), or whose workspace is incomplete (the job is then set to `FAILED`), is not submitted and goes straight to `completed`.

When Slurm reports a job `COMPLETED`, the bridge uploads `output/result.xlsx` to the job's `output` column and sets it to `COMPLETED` on EMX2 in a single mutation (a multipart GraphQL request). If the job completed without writing a non-empty `result.xlsx`, it is set to `FAILED` instead. If the upload fails, the job stays `submitted` and the upload is retried on the next poll.

Only one extraction job runs at a time; further jobs are claimed and wait as `claimed` until the cluster is free.

| Slurm state                                   | Local status | EMX2 status |
| --------------------------------------------- | ------------ | ----------- |
| `PENDING`, `REQUEUED`, ...                    | `submitted`  | `PENDING`   |
| `RUNNING`, `COMPLETING`                       | `submitted`  | `RUNNING`   |
| `COMPLETED`                                   | `completed`  | `COMPLETED` (result uploaded), or `FAILED` without result |
| `CANCELLED`                                   | `completed`  | `STOPPED`   |
| `FAILED`, `TIMEOUT`, `OUT_OF_MEMORY`, ...     | `completed`  | `FAILED`    |

Locally a job is only `claimed` (not yet submitted), `submitted` (pending or running on the cluster) or `completed` (no longer pending or running); whether it succeeded is kept on EMX2.

### Running

With the virtual environment active, start the poller with:

```bash
emx2-hpc-bridge
```

### Tests, linting and formatting

These are the same checks that run in CircleCI (see [.circleci/config.yml](.circleci/config.yml)):

```bash
pytest -v                 # run the tests
flake8 src tests          # lint
black --check --diff .    # check formatting
```

Run `black .` to apply the formatting fixes.
