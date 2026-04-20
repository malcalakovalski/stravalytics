# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Personal Strava data exporter. Single Python script (`strava_export.py`) that pulls running and soccer activities via the Strava API and writes CSVs to `data/`.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then fill in Strava API credentials
```

Strava API credentials come from https://www.strava.com/settings/api. First run triggers browser-based OAuth2 flow and persists tokens to `strava_tokens.json`.

## Running

```bash
python strava_export.py         # Incremental: only fetches activities since last export
python strava_export.py --full  # Full refresh: re-fetches all-time activities
```

Incremental by default. Reads existing CSVs to find the latest activity date, fetches only newer activities, and merges (dedup by activity_id). Stream files are cached by filename. Rate-limit aware (proactive pause at 90/100 15-min quota, retry on 429).

## Output

- `data/strava-runs-summary.csv`: All runs >= 0.5 miles with HR zones
- `data/strava-soccer-summary.csv`: All soccer/football activities with calories
- `data/activity-streams/*.csv`: Per-second time series (distance, pace, HR, altitude) for 15 most recent runs. Filename format: `YYYY-MM-DD-{activity_id}.csv`

## Architecture

Single-file design. Key sections in `strava_export.py`:
- **OAuth2** (lines ~44-123): Local HTTP server callback for auth code flow, token refresh with file persistence
- **API client** (lines ~128-177): Rate-limit-aware GET wrapper, paginated activity fetcher
- **Row formatters** (lines ~210-280): Transform Strava JSON → flat dicts for runs (with HR zone distribution), soccer (with calories from detail endpoint), and per-second streams
- **Incremental helpers** (lines ~299-327): `_latest_activity_ts` scans CSVs for newest date, `_read_csv_rows` loads existing data for merge
- **Main** (lines ~333-501): Orchestrates fetch → filter → merge → write pipeline

## Sensitive Files

- `.env`: API credentials (gitignored)
- `strava_tokens.json`: OAuth tokens (gitignored but currently tracked — should be untracked)
