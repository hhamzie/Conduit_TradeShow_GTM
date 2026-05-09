# Trade Show Outbound MVP

This repo now contains two layers:

- the existing directory scraper engine in [`scraper.py`](/Users/hamzehhammad/Documents/Conduit/HPointScraper/scraper.py)
- a lightweight web app and worker for bulk trade-show scheduling, review, and orchestration

## What the MVP does

- Upload a CSV of trade shows with `Show`, `Date`, `Place`, and `Link`
- Store shows in a database
- Wait until the configured trigger window before the show date
- Run the scraper with input-driven `Conference` and `Location`
- Use an OpenAI-backed fallback only when heuristic discovery stalls, if `OPENAI_API_KEY` is configured
- Save the export path and scrape run history
- Duplicate one Clay template table per show when template-table automation is configured
- Push scraper rows into that show-specific Clay table
- Poll Clay for row-level enrichment status and rebuild both the raw enriched CSV and Smartlead-ready CSV automatically
- Create or reuse one Smartlead campaign per trade show instead of merging every show into one campaign
- Import only newly ready Clay rows into Smartlead and avoid double-importing the same Clay row
- Keep launch manual, but pause every other active Smartlead campaign before starting the selected show
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

### Auto-deploy from GitHub Actions

This repo now includes a GitHub Action at [.github/workflows/render-deploy.yml](/Users/hamzehhammad/Documents/Conduit/HPointScraper/.github/workflows/render-deploy.yml).

To use it:

1. In GitHub, open the repo settings.
2. Add an Actions secret named `RENDER_DEPLOY_HOOK_URL`.
3. Set it to your full Render deploy hook URL.

After that, every push to `main` will trigger the Render deploy hook automatically.

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

For the automated per-show flow on this branch, use:

- `CLAY_SESSION_COOKIE`
- `CLAY_TEMPLATE_TABLE_ID`
- `CLAY_ROW_STATUS_COLUMN`

Recommended terminal values:

- `CLAY_READY_STATUS_VALUE=ready`
- `CLAY_FAILED_STATUS_VALUE=failed`
- `CLAY_SKIPPED_STATUS_VALUE=skip`

Flow:

1. The app duplicates the template table once per show.
2. The scraper export is pushed into that dedicated Clay table.
3. Clay enriches rows and sets the configured row status column.
4. The worker polls the table and imports only rows marked `ready`.
5. The show becomes launch-ready only after every row is terminal (`ready`, `failed`, or `skip`).

If neither template-table automation nor webhook/direct-table input is configured, the app will still scrape successfully, but Clay sync will be marked as skipped.

## Agent fallback

The scraper still uses the fast heuristic path first, but it can now call an OpenAI-backed directory recovery step when discovery stalls.

Relevant settings:

- `OPENAI_API_KEY`
- `SCRAPER_AGENT_MODE`
- `SCRAPER_AGENT_MODEL`
- `TRADE_SHOW_SCAN_MODEL`

Modes:

- `off`
- `fallback`
- `always`

Recommended default:

- `SCRAPER_AGENT_MODE=fallback`

That keeps normal directories fast and cheap, while giving weird layouts a second recovery pass before the scrape fails.

## Trade show feeder

The manual `Scan for upcoming trade shows` popup on the dashboard is already wired. To turn it on later, you only need:

- `OPENAI_API_KEY`
- optional `TRADE_SHOW_SCAN_MODEL` (defaults to `gpt-5`)

If the key is missing, the popup will return a clean setup message instead of failing silently.

## Smartlead integration

The Smartlead flow works per show:

- the scraper export still goes into Clay first
- the app polls Clay back instead of relying on a callback
- the app cleans and deduplicates ready rows, saves a Smartlead-ready CSV, and syncs them into a unique Smartlead campaign for that show
- the campaign stays paused until you approve and launch it from the dashboard
- when you launch one show, the app pauses every other active Smartlead campaign first

Required setting:

- `SMARTLEAD_API_KEY`

Optional settings:

- `SMARTLEAD_BASE_URL`
- `SMARTLEAD_CLIENT_ID`
- `SMARTLEAD_TEMPLATE_CAMPAIGN_ID`

If `SMARTLEAD_TEMPLATE_CAMPAIGN_ID` is set, newly created show-specific campaigns will attempt to copy the template campaign's sender accounts, sequences, and basic schedule/settings before importing leads.

### Clay template table

Create one master Clay template table before testing the automated flow. That table should include:

- the incoming scraper columns you want to enrich
- `show_id`
- `show_name`
- `show_date`
- `show_place`
- `scraped_at`
- `source_url`
- one row-level status column, usually `enriched_status`

Recommended status values:

- `ready`
- `failed`
- `skip`

The app duplicates that template per show and uses the row status column as the source of truth for readiness.

## Email notifications

When a scrape finishes, the app can email review notifications to the configured recipients.

Required settings:

- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USERNAME` and `SMTP_PASSWORD` if your mail server requires auth
- `NOTIFY_FROM_EMAIL`
- `NOTIFY_TO_EMAILS`
