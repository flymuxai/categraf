# sub2api Input

Monitor a sub2api API key's **remaining balance/quota** through the downstream
`GET /v1/usage` endpoint so an alert can fire before the key runs dry. One
`[[instances]]` block = one API key = one subject. There is no refresh-token
dance, no state file, no per-record usage scan.

## Authentication

Set `api_key` to a sub2api API key. Every request goes out as
`Authorization: Bearer <api_key>`. Nothing else is needed — bootstrap and
rotation logic have been removed together with the state file.

The raw key never appears in metrics, labels, or logs. Error messages from
the upstream are scrubbed for the configured key and for anything that looks
like a bearer token before being surfaced.

## Collection frequency

The plugin defaults to a five-minute interval instead of Categraf's global
15-second default. All requests to the same normalized upstream origin share a
`golang.org/x/time/rate` token bucket (default 1 rps, burst 1). A bounded
random delay is applied after token acquisition to avoid perfectly periodic
traffic. Limiter waiting and jitter do not consume the HTTP timeout; the
timeout starts when the actual upstream request begins. Failures are not
retried within a collection — 401 means the key is invalid, and the metric
`sub2api_scrape_success` drops to 0 until the next interval.

## What is collected

Every collection calls one endpoint:

- `GET /v1/usage` — mode-aware summary that carries the remaining balance,
  usage totals, and per-model breakdown. sub2api replies with one of three
  modes depending on the key configuration:
  - `quota_limited` — the key has a per-key USD cap and/or rate windows
  - `subscription` — the key belongs to a group with daily/weekly/monthly
    limits
  - `wallet` (an `unrestricted` key without a subscription) — the key spends
    from the user's wallet balance

Two optional collections can be turned on per instance:

- `collect_billing = true` → `GET /v1/sub2api/billing` (per-key rate
  multipliers)
- `collect_models  = true` → `GET /v1/models` (advertised model list)

## The single alerting metric

The plugin normalizes the "how much money is left" concept across all three
key modes:

```promql
sub2api_key_remaining_usd{key="prod-wallet-key"} < 5
```

- `mode="wallet"` — the user's wallet balance.
- `mode="quota_limited"` — the key's remaining USD quota, or, when the key
  has only rate limits configured, the minimum remaining across the 5h/1d/7d
  windows.
- `mode="subscription"` — the smallest remaining amount across the daily,
  weekly, and monthly limits configured on the key's group.

Combine with the up metric to avoid alerting on stale samples:

```promql
sub2api_key_remaining_usd{key="prod-wallet-key"} < 5
  and on(key) sub2api_scrape_success{resource="usage"} == 1
```

## Metrics

All metrics carry `{key = <instance name>}`. Additional labels are noted per
metric.

### Balance / quota

- `sub2api_key_remaining_usd{key, mode="quota_limited|subscription|wallet"}`
- `sub2api_key_wallet_balance_usd{key}` — wallet mode
- `sub2api_key_quota_limit_usd{key}` / `sub2api_key_quota_used_usd{key}` —
  quota_limited mode
- `sub2api_key_rate_limit_limit{key, window="5h|1d|7d"}`
- `sub2api_key_rate_limit_used{key, window}`
- `sub2api_key_rate_limit_remaining{key, window}`
- `sub2api_key_rate_limit_reset_timestamp{key, window}`
- `sub2api_key_subscription_usage_usd{key, window="daily|weekly|monthly"}`
- `sub2api_key_subscription_limit_usd{key, window}`
- `sub2api_key_expires_timestamp{key}` / `sub2api_key_days_until_expiry{key}`

### Usage (lifetime counters emitted from `usage.total.*`)

- `sub2api_key_requests_total{key}`
- `sub2api_key_tokens_total{key, kind="input|output|cache_creation|cache_read|total"}`
- `sub2api_key_cost_usd_total{key, kind="standard|actual"}`
- `sub2api_key_rpm{key}` / `sub2api_key_tpm{key}` (gauges)
- `sub2api_key_average_duration_seconds{key}` (gauge)

### Per-model breakdown (`window="30d"` gauges)

- `sub2api_key_model_requests{key, model, window}`
- `sub2api_key_model_tokens{key, model, window, kind}`
- `sub2api_key_model_cost_usd{key, model, window, kind}`

### Billing (opt-in)

- `sub2api_key_rate_multiplier{key, kind="group|user|resolved|peak_configured|peak_applied|effective"}`
- `sub2api_key_peak_rate_enabled{key}` (0/1)

### Models (opt-in)

- `sub2api_key_model_count{key}`
- `sub2api_key_model_available{key, model}`

### Collector health

- `sub2api_up{key}`
- `sub2api_scrape_success{key, resource="usage|billing|models"}`
- `sub2api_scrape_duration_seconds{key, resource}`
- `sub2api_last_success_timestamp_seconds{key, resource}`

## Trade-offs vs the previous design

- **Removed metrics** — `sub2api_frozen_balance_usd` (not exposed to API
  keys), `sub2api_group_count` / `sub2api_group_available` (a key only sees
  its own group), and TTFT/duration histograms (the `/v1/usage` endpoint only
  surfaces `average_duration_ms`, so `p95`/`p99` cannot be reconstructed).
- **Lifetime counters** — `sub2api_key_*_total` values come from the server's
  own per-key accumulators rather than from a local ID cursor. Rate/increase
  queries behave the same; a server-side reset would show as a counter reset.

If you still need per-record precision or TTFT histograms, keep using the
sibling `sub2api_admin` collector (which authenticates as an administrator
and has a different data model).
