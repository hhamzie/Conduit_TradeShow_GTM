# OpenPhone call analytics

The authenticated `/analytics` page is the default dashboard. It renders the
latest bounded OpenPhone snapshot from the application database; dashboard web
requests never call OpenPhone directly. The legacy filename is retained so
existing deployment references continue to work.

## Source and metric contract

The source is OpenPhone's read-only API: `/v1/users` resolves rep names,
`/v1/conversations` discovers the phone-number/participant pairs, and
`/v1/calls` supplies the call facts. All times are converted to
`OPENPHONE_ANALYTICS_TIMEZONE` (default `America/New_York`) before bucketing.
The report excludes the partial current day.

- Eligible call: an outbound call in the reporting window.
- Connected call: an eligible call with completed status and duration of at
  least 90 seconds.
- Connect rate: connected calls divided by eligible outbound calls.
- Total calls and connect rate: rolling 30 complete days.
- Best hour and best day: the highest 30-day connect rate among buckets with at
  least `OPENPHONE_ANALYTICS_MIN_SAMPLE` calls (default 15).
- Top rep: the highest connect rate across the last seven completed local days
  among reps meeting the same minimum sample.
- Rep leaderboard: each rep's calls and connect rate from Monday through
  yesterday in the current report week, plus the rolling seven-completed-day
  connect rate.
- Connected percentage by weekday: connect rate blended across the last four
  complete weeks, paired with average calls per occurrence of that weekday.
- Hour and weekday profiles: 30-day connect rate for each hour and weekday.
- Heatmap: connect rate for every hour × weekday bucket blended across the last
  four complete weeks. Gray means no calls; colors are `<5%` red, `<10%`
  orange, `<15%` yellow, `<25%` light green, and `≥25%` green.

No Pipedrive deal or activity proxy is used for these call metrics.

## Dashboard sections

| Section | Variables shown |
|---|---|
| Headline cards | Total calls (30d), connected count, connect rate (30d), best hour, best day, and top rep (7d) |
| Rep leaderboard | This week's daily calls and connect percentages, plus 7d percentage |
| Weekday blend | Connected percentage and average calls/day across the last four weeks |
| Hour profile | Connection rate by hour of day over 30 days |
| Weekday profile | Connection rate by day of week over 30 days |
| Hour × day heatmap | Connection rate by hour and weekday across the last four weeks |

## Worker configuration

The Render worker refreshes the snapshot three times per day at the local
hours in `OPENPHONE_ANALYTICS_REFRESH_HOURS` (default `9,13,17`) in the
analytics timezone. These defaults correspond to 9:00 AM, 1:00 PM, and
5:00 PM Eastern. Each slot runs at most once. If the worker is unavailable at
a scheduled time, its next successful poll catches up only the latest missed
slot instead of replaying every missed refresh.

Required worker environment:

```text
OPENPHONE_API_KEY
DATABASE_URL
```

Optional worker environment:

```text
OPENPHONE_BASE_URL=https://api.openphone.com
OPENPHONE_ANALYTICS_TIMEZONE=America/New_York
OPENPHONE_ANALYTICS_REFRESH_HOURS=9,13,17
OPENPHONE_ANALYTICS_LOOKBACK_DAYS=30
OPENPHONE_ANALYTICS_MIN_SAMPLE=15
```

Keep `OPENPHONE_API_KEY` on the worker only. Do not put it in browser code,
committed files, URLs, snapshots, or logs. The web service needs the shared
`DATABASE_URL`, but it does not need OpenPhone credentials.

## Privacy boundary

The worker requests only the conversation, call, and user metadata needed to
aggregate this report. Conversation participants and phone-number IDs are used
in memory to retrieve calls, then discarded. The stored snapshot contains
aggregate call counts/rates and rep display names. It does not store phone
numbers, contact names, call recordings, transcripts, voicemail audio, or
message content. The browser receives only the bounded aggregate snapshot.
