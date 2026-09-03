# sub2api Input

The `sub2api` input always collects balance, effective group ratios, current
per-group model availability, and incremental user usage. It uses only
downstream user APIs.

## Authentication and state

- `refresh_token` is the only configured user credential. On the first
  collection, it is exchanged for an access/refresh token pair. The access
  token is then used for balance, groups, `/api/v1/keys`, and `/api/v1/usage`.
- `model_api_keys` contains only model API key secrets. The input discovers
  each key's `group_id` through `/api/v1/keys`; groups without a configured key
  are intentionally skipped for model collection.
- A configured model API key with no assigned `group_id` is reported as a
  warning and skipped. It does not produce group model metrics or make the
  models resource fail; the key value is never included in the warning.
- A single `state_file` stores rotated tokens, the integer usage cursor, and
  cumulative usage counters/histograms. It is atomically replaced with mode
  `0600`; newly created parent directories use mode `0700`.
- Tokens use `config.Secret` and are never included in logs or metric labels.
- A corrupt or unsupported state file prevents startup rather than silently
  rebuilding the cursor.

### Dedicated refresh-token session

sub2api refresh tokens are single-use rotating credentials. A successful
`/api/v1/auth/refresh` request immediately invalidates the old refresh token
and returns a new token pair. Do not share one refresh token between Categraf
and an active browser session: whichever client refreshes first makes the
other client's saved refresh token invalid.

Use a dedicated login session for each Categraf instance:

1. Log in using a separate API login or an incognito browser window.
2. Configure only the `refresh_token` returned by that login response.
3. If using an incognito window, close the window after obtaining the tokens.
   Do not click **Log out**, because logout revokes the refresh token supplied
   by that session.
4. Do not use or probe the refresh token elsewhere. Calling the refresh API as
   a test consumes and rotates a valid token.

A later normal login to the same account creates a separate token family and
does not invalidate the Categraf session. The Categraf token is invalidated by
logging out that specific session, using **Log out all sessions**, changing or
resetting the password, disabling the user, clearing the upstream refresh-token
store (normally Redis), or another client using the same refresh token.

On startup, non-empty tokens already stored in `state_file` take precedence
over the configured refresh token. If no access token has been persisted, the
first collection exchanges the effective refresh token and saves the returned
token pair before requesting user resources. If a stored refresh token becomes
invalid, merely changing `SUB2API_REFRESH_TOKEN` is not sufficient. Stop
Categraf, configure a fresh dedicated refresh token, and clear only the token
fields while preserving the Usage cursor and counters:

```bash
STATE_FILE=/var/lib/categraf/sub2api/supplier-state.json
tmp=$(mktemp "$(dirname "$STATE_FILE")/.sub2api-state.XXXXXX")
jq '.access_token = "" | .refresh_token = ""' "$STATE_FILE" > "$tmp"
chmod 600 "$tmp"
mv "$tmp" "$STATE_FILE"
```

Back up the state file before editing it. Restart Categraf only after the
replacement is complete.

## Collection frequency

The plugin defaults to a five-minute interval instead of Categraf's global
15-second default. Set the plugin-level `interval` and optional per-instance
`interval_times` to reduce supplier traffic:

```toml
interval = "5m"

[[instances]]
interval_times = 1
request_rps_limit = 1.0
request_burst = 1

[[instances.model_api_keys]]
api_key = "@{env:SUB2API_MODEL_KEY_1}"
```

All requests to the same normalized upstream origin share a
`golang.org/x/time/rate` token bucket and a one-request concurrency gate. The
defaults are one request per second with a burst of one. A bounded random delay
is applied after token acquisition to avoid a perfectly periodic request
pattern. The first collection refreshes once when no access token is present;
if that authentication preflight fails, all resources fail without making user
API requests and the next collection tries again. After bootstrap, HTTP 429 and
5xx responses are not retried; only a 401 can refresh the token and retry the
current request once. Limiter waiting and jitter do not consume the HTTP
timeout; the timeout starts when the actual upstream request begins.

## Incremental usage

Usage pages use the upstream-supported fixed page size of 20 and are requested
with `sort_by=id&sort_order=desc`. The input processes only rows whose positive
integer ID is greater than the persisted cursor. As soon as any
`id <= last_usage_id` is seen, it stops the current page and all remaining
pagination. Duplicate new IDs across moving offset pages are ignored.

`start_date` is the cursor's calendar date in `usage_timezone`; it only narrows
the server query. The ID cursor is the authoritative incremental boundary. On
the first run, the input reads only the latest page and saves its highest ID as
the baseline without adding those records to counters or histograms. If the
page is empty, the process start time is saved as the empty baseline.

There is no page or record-count cap. Scanning continues until the cursor, an
empty/short page, or the server-declared final page. A failed page discards the
entire pending batch, so the persisted cursor and cumulative metrics never
represent a partial scan. Records are aggregated directly into a temporary
state copy rather than retained as an unbounded in-memory list. A large backlog
can make one collection exceed the configured plugin interval; Categraf waits
for it to finish instead of running the same input concurrently.

## Metrics

All metrics include `site`. Group/model metrics also include `group`,
`platform`, and `model`; usage series additionally include `request_type`.

- `sub2api_up`
- `sub2api_scrape_success{resource="balance|groups|models|usage"}`
- `sub2api_scrape_duration_seconds{resource=...}`
- `sub2api_last_success_timestamp_seconds{resource=...}`
- `sub2api_balance_usd`
- `sub2api_frozen_balance_usd`
- `sub2api_group_count`
- `sub2api_group_available{group,platform}`
- `sub2api_group_ratio{group,platform,kind="default|effective|peak"}`
- `sub2api_model_count{group,platform}`
- `sub2api_model_available{group,platform,model}`
- `sub2api_model_scrape_success{group,platform}`
- `sub2api_usage_initialized`
- `sub2api_usage_last_processed_id`
- `sub2api_usage_new_records`
- `sub2api_usage_scan_pages`
- `sub2api_usage_scan_records`
- `sub2api_usage_scan_complete`
- `sub2api_usage_model_requests_total`
- `sub2api_usage_model_tokens_total{kind="input|output|cache_creation|cache_read|total"}`
- `sub2api_usage_model_cost_usd_total{kind="standard|actual"}`
- `sub2api_usage_model_duration_seconds_bucket{le}` / `_sum` / `_count`
- `sub2api_usage_model_first_token_latency_seconds_bucket{le}` / `_sum` / `_count`

`first_token_ms` is TTFT for requests where sub2api recorded a first token. A
null value does not enter the TTFT histogram. It is not TCP time-to-first-byte.

## PromQL examples

Requests during the last ten minutes:

```promql
sum by (site, model) (
  increase(sub2api_usage_model_requests_total[10m])
)
```

Per-model p95 TTFT with a healthy usage collector:

```promql
histogram_quantile(
  0.95,
  sum by (site, group, platform, model, le) (
    rate(sub2api_usage_model_first_token_latency_seconds_bucket[10m])
  )
)
and on(site) sub2api_scrape_success{resource="usage"} == 1
```

Model additions and removals still use set differences on
`sub2api_model_available` with successful current and historical model scrapes.
Usage inactivity must not be interpreted as a model going offline.
