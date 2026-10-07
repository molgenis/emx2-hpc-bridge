# EMX2 HPC Bridge

## intoduction

The repo contains a set of scripts, docs, and config files that enables the [paper-extracting](https://github.com/molgenis/paper-extracting) tool to run a extraction request submitted via [emx2](https://github.com/molgenis/molgenis-emx2) on the [high performance computing](https://umcgresearch.org/w/high-performance-computing) (HPC) cluster. 

## General design

<img width="1579" height="996" alt="image" src="https://github.com/user-attachments/assets/ffef9404-8617-426f-8c68-7d524031ae97" />

## Development

### Prerequisites

- Python 3.11 or newer
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

`.env` is ignored by git; never commit real tokens.

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
