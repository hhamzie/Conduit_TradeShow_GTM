# Trade Show Outbound MVP

This repo now contains two layers:

- the existing directory scraper engine in [`scraper.py`](/Users/hamzehhammad/Documents/Conduit/HPointScraper/scraper.py)
- a lightweight web app and worker for bulk trade-show scheduling, review, and orchestration

## What the MVP does

- Upload a CSV of trade shows with `Show`, `Date`, `Place`, and `Link`
- Store shows in a database
- Wait until the configured trigger window before the show date
- Run the scraper with input-driven `Conference` and `Location`
- Save the export path and scrape run history
- Push scraper rows into Clay when a supported Clay transport is configured
- Notify operators when a show is ready for review
- Support approval before downstream outreach syncs

## Local run without Docker

1. Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Start the web app:

```bash
make web
```

3. Start the worker in another shell:

```bash
make worker
```

The default local database is SQLite at `data/trade_show_app.db`.

## Docker run

1. Copy `.env.example` to `.env`
2. Start the stack:

```bash
docker compose up --build
```

The dashboard will be available at `http://localhost:8000`.

## CSV format

Expected columns:

- `Show`
- `Date`
- `Place`
- `Link`

Optional future columns can be added later, but the worker currently treats those four as the source of truth for scheduled scraping.

## Clay integration

The app supports two Clay delivery modes:

- `CLAY_WEBHOOK_URL`
  Use this if your Clay input table is configured with a webhook source. This is the recommended route for this app.
- `CLAY_SESSION_COOKIE` + `CLAY_INPUT_TABLE_ID`
  This uses Clay's live table HTTP endpoints. In my testing, `CLAY_API_KEY` alone was not enough for those endpoints; Clay returned `401 You must be logged in`.

If neither of those is configured, the app will still scrape successfully, but Clay sync will be marked as skipped.
