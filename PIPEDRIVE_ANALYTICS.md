# Pipedrive sales analytics

The authenticated `/analytics` page is the default dashboard. It reads the
latest bounded snapshot from the application database; web requests never call
Pipedrive.

## Metric contract

- Source population: non-deleted Pipedrive deals created in the rolling
  30-day window.
- Followed up: a source deal whose Pipedrive `activities_count` is greater
  than zero.
- Follow-up coverage: followed-up deals divided by source deals.
- Best hour, best weekday, and top owner: the highest follow-up coverage among
  buckets with at least `PIPEDRIVE_ANALYTICS_MIN_SAMPLE` deals (default 10).
- Time attribution: deal `add_time`, converted to
  `PIPEDRIVE_ANALYTICS_TIMEZONE` (default `America/New_York`).
- Owner attribution: `owner_id` joined to the Pipedrive users endpoint. The
  snapshot stores owner display names, aggregate counts, and rates only.

This is deliberately not a win-rate dashboard. The account's current won/lost
history is too sparse for an honest 30-day outcome view.

## Visual map

| Section | Question | Visual |
|---|---|---|
| Headline cards | How much deal volume was created, how much was followed up, and which time/owner buckets perform best? | Five KPI cards |
| Owner leaderboard | Who created deals this week and what share received follow-up? | Daily count/rate table with 30-day rate |
| Weekday blend | Which weekdays combine follow-up coverage and deal volume? | Dual-axis line/area chart |
| Hour profile | At what creation hours are deals most likely to receive follow-up? | 24-hour bar chart |
| Weekday profile | On which creation weekdays are deals most likely to receive follow-up? | Seven-day bar chart |
| Hour × day pattern | Where are high- and low-coverage creation windows concentrated? | 24 × 7 heatmap |

## Worker

`app.worker` calls `refresh_pipedrive_analytics_if_due` in an isolated error
boundary. The refresh runs at most once per local report date after
`PIPEDRIVE_ANALYTICS_REFRESH_HOUR` (default 6). It uses read-only `GET`
requests to:

- `/api/v2/deals`, cursor-paginated and sorted by newest `add_time`;
- `/v1/users`, for owner display names.

Required worker environment:

```text
PIPEDRIVE_API_TOKEN
DATABASE_URL
```

Optional worker environment:

```text
PIPEDRIVE_BASE_URL=https://api.pipedrive.com/v1
PIPEDRIVE_ANALYTICS_TIMEZONE=America/New_York
PIPEDRIVE_ANALYTICS_REFRESH_HOUR=6
PIPEDRIVE_ANALYTICS_LOOKBACK_DAYS=30
PIPEDRIVE_ANALYTICS_MIN_SAMPLE=10
```

Keep `PIPEDRIVE_API_TOKEN` on the worker only. The web service needs the shared
`DATABASE_URL` but does not need Pipedrive credentials.
