# Copilot instructions for this repository

Purpose: Help AI coding agents become productive quickly in this small data-pipeline repo that harvests and explores CGSpace OAI-PMH records.

- **Big picture:** This repository contains lightweight data-pipeline scripts under `scripts/` that fetch and inspect OAI-PMH records from CGSpace (`BASE_URL` in `scripts/00_explore_oai.py`). Data directories follow a simple staging layout (`data/raw`, `data/staging`, `data/db`) and outputs go to `outputs/` (tables, figures, notes).

- **Primary entrypoint:** `scripts/00_explore_oai.py` — an exploratory harvester that:
  - uses `requests` and `lxml.etree` to call the OAI endpoint and parse XML,
  - implements pagination via OAI `resumptionToken`,
  - uses simple print-based reporting (not a logging framework),
  - contains Spanish comments and variable names (keep new comments consistent with the repository tone).

- **Key constants & integration points (change with care):**
  - `BASE_URL` — the OAI-PMH endpoint (https://cgspace.cgiar.org/oai/request).
  - `METADATA_PREFIX` — default `oai_dc` in the script.
  - `PAUSE_SECS`, `RETRY_WAIT`, `MAX_RETRIES` — control throttling and retry behaviour; tests/changes should not remove these throttles.

- **Observed coding patterns & conventions:**
  - Scripts are numbered (`00_`, `01_`, ...) indicating pipeline stage ordering — preserve numbering for new pipeline stages.
  - Small single-file scripts in `scripts/` perform a single task (harvest, explore). Prefer adding new stages as new numbered scripts.
  - Minimal dependencies; network/time-sensitive code uses explicit sleeps and retries.

- **How to run locally (discovered from repo):**
  - Activate the repository virtualenv (Windows):

    PowerShell

    venv\\Scripts\\Activate.ps1

    or Bash

    source venv/Scripts/activate

  - Install runtime deps (this repo does not include a requirements.txt):

    pip install requests lxml

  - Run the exploratory script:

    python scripts/00_explore_oai.py

- **What to look for when modifying harvesting code:**
  - Keep network parameters configurable via top-level constants.
  - Preserve `resumptionToken` handling: the script switches params to `{"verb": "ListRecords", "resumptionToken": token}` when paginating.
  - Respect existing pauses to avoid 429 rate limits — incremental changes should keep `RETRY_WAIT` and `PAUSE_SECS` logic.

- **Repository structure to reference in PRs:**
  - `scripts/00_explore_oai.py` — main example of harvest code and conventions.
  - `data/` — raw/staging/db subfolders for data artifacts.
  - `outputs/` — where derived tables, figures and notes belong.

- **Tests & CI:** No test suite or CI config detected. If adding tests, place them in a `tests/` folder and keep them focused on pure parsing logic (e.g., `lxml` parsing of saved example responses) rather than live network calls.

- **Examples of quick edits:**
  - To increase sample size change `MAX_RECORDS` in `scripts/00_explore_oai.py`.
  - To change metadata format edit `METADATA_PREFIX` (beware of changed XML structure).

If any section is unclear or you want more automation (requirements.txt, CI, or sample saved responses for tests), tell me which to add next. I'll iterate on this file based on your feedback.
