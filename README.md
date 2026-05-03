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
- Accept a Clay-enriched CSV upload per show, clean it, and keep a Smartlead-ready export
- Create or reuse one Smartlead campaign per trade show instead of merging every show into one campaign
- Notify operators by email when a show is ready for review
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

## Render deploy

This repo can now be deployed to Render with:

- one Render Postgres database
- one Render web service using the included [render.yaml](/Users/hamzehhammad/Documents/Conduit/HPointScraper/render.yaml)
- one persistent disk mounted at `/app/data/exports`

Why one web service instead of separate web + worker services:

- the app writes scraper exports to disk
- Render disks are mounted to a single service
- the included startup script runs both the worker and FastAPI app in the same container for the hosted deployment

Basic flow:

1. Push the repo to GitHub.
2. In Render, create a Blueprint deploy from `render.yaml`, or manually create:
   - a Postgres database
   - a Docker web service
   - a persistent disk mounted at `/app/data/exports`
3. Add the rest of your real env vars in Render:
   - `SESSION_SECRET`
   - `DASHBOARD_USERNAME`
   - `DASHBOARD_PASSWORD`
   - Clay settings like `CLAY_INPUT_TABLE_ID` or `CLAY_WEBHOOK_URL`
   - email settings if you want notifications
4. Deploy.

`DATABASE_URL` from Render is accepted directly; the app normalizes Render's Postgres URL format automatically.

## Handoff / deploy from image

If you are handing this to someone else as a prebuilt image:

1. Send these files:

- `conduit-tradeshow-dashboard.tar.gz`
- `.env.example`
- `scripts/start-handoff-stack.sh`
- `scripts/stop-handoff-stack.sh`

2. Load the image:

```bash
gunzip conduit-tradeshow-dashboard.tar.gz
docker load -i conduit-tradeshow-dashboard.tar
```

3. Put the real deployment env vars into `.env`

4. Start the deploy stack with plain Docker:

```bash
chmod +x scripts/start-handoff-stack.sh scripts/stop-handoff-stack.sh
./scripts/start-handoff-stack.sh
```

5. Or, if they prefer Compose and already have it installed:

```bash
docker compose -f docker-compose.deploy.yml up -d
```

The handoff script starts:

- `db` as Postgres
- `web` from the prebuilt image
- `worker` from the same prebuilt image

To stop the plain-Docker handoff stack later:

```bash
./scripts/stop-handoff-stack.sh
```

The deploy compose file runs:

- `web` from the prebuilt image
- `worker` from the same prebuilt image
- `db` as Postgres

Unlike the local dev compose file, it does not bind-mount the source repo into the containers.

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

## Smartlead integration

The Smartlead flow now works per show:

- the scraper export still goes into Clay
- Clay can automatically POST enriched rows back into this app through the callback endpoint
- the app cleans and deduplicates the rows, saves a Smartlead-ready CSV, and syncs it into a unique Smartlead campaign for that show
- if no other app-managed Smartlead campaign is active, the worker can activate the next ready show automatically; otherwise it waits its turn

Required setting:

- `SMARTLEAD_API_KEY`

Optional settings:

- `SMARTLEAD_BASE_URL`
- `SMARTLEAD_CLIENT_ID`
- `SMARTLEAD_TEMPLATE_CAMPAIGN_ID`
- `CLAY_CALLBACK_TOKEN`

If `SMARTLEAD_TEMPLATE_CAMPAIGN_ID` is set, newly created show-specific campaigns will attempt to copy the template campaign's sender accounts, sequences, and basic schedule/settings before importing leads.

### Clay callback automation

To remove the manual Clay CSV export/upload step, create an `HTTP API` enrichment/action in Clay that `POST`s each enriched row to:

`POST /api/clay/enriched-row`

Add header:

- `X-Clay-Token: <CLAY_CALLBACK_TOKEN>`

Include at least these fields in the JSON body:

- `show_id`
- `email`
- `first_name`
- `last_name`
- `company_name`

Recommended extras:

- `job_title` or `title`
- `website`
- `linkedin_url`
- `phone_number`
- `clay_row_id` or another stable row identifier

The app uses `show_id` to route the row back to the correct trade show and rebuilds the Smartlead-ready export automatically.

## Email notifications

When a scrape finishes, the app can email review notifications to the configured recipients.

Required settings:

- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USERNAME` and `SMTP_PASSWORD` if your mail server requires auth
- `NOTIFY_FROM_EMAIL`
- `NOTIFY_TO_EMAILS`
