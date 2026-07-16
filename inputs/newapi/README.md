# New API Input

The `newapi` input monitors New API. It always collects balance, effective
group ratios, per-group model availability, and incremental user usage through
downstream user APIs.

## Authentication and collection frequency

Configure the system access token generated from the user's profile. It is not
an `sk-*` model API key or a browser login session. Requests send the token in
`Authorization` together with `New-Api-User`.

The system access token does not expire automatically, but generating a new one
immediately invalidates the old value. Update the Categraf secret whenever the
token is regenerated.

The plugin defaults to a five-minute interval. Requests to the same normalized
upstream origin share a token bucket and a one-request concurrency gate with
other newapi and sub2api instances. The defaults are one request per second
with a burst of one. Token waiting and the bounded `0..250ms` jitter do not
consume the HTTP timeout; the timeout starts when the actual upstream request
begins. HTTP failures, including 401, 429, and 5xx, are not retried.

## Incremental usage state

New API exposes the response `id` as a display position, so the input never
uses it as a cursor. It persists a composite watermark: the latest completed
Unix second and every `request_id` observed in that second. Known IDs in the
boundary second are skipped, while unknown IDs are counted as new. The complete
boundary second must still be scanned because old and new IDs can be
interleaved.

Each scan fixes `end_timestamp` to the second before the scan began and uses the
server maximum page size of 100. There is no page or record-count cap. Page
totals must remain stable and the number of unique request IDs must match the
declared total. Records are aggregated directly into a temporary state copy;
the scanner retains request IDs for deduplication but does not retain every
decoded record. An incomplete or invalid scan does not update counters or the
watermark. The initial scan excludes the process startup second to avoid
historical backfill, which can omit requests created later in that same second.

`state_file` stores the watermark and cumulative counters/histograms. It is
atomically replaced with mode `0600`; newly created parent directories use
mode `0700`. A corrupt or unsupported state file prevents startup.

## Metrics

All metrics include `site`. Group and model metrics use `platform="newapi"`.
Usage metrics also include `group`, `model`, and
`request_type="stream|non_stream"`.

- `newapi_up`
- `newapi_scrape_success{resource="balance|groups|models|usage"}`
- `newapi_scrape_duration_seconds{resource=...}`
- `newapi_last_success_timestamp_seconds{resource=...}`
- `newapi_balance_usd`
- `newapi_group_count`
- `newapi_group_available{group,platform}`
- `newapi_group_ratio{group,platform,kind="effective"}`
- `newapi_model_count{group,platform}`
- `newapi_model_available{group,platform,model}`
- `newapi_model_scrape_success{group,platform}`
- `newapi_usage_initialized`
- `newapi_usage_last_processed_timestamp_seconds`
- `newapi_usage_new_records`
- `newapi_usage_scan_pages`
- `newapi_usage_scan_records`
- `newapi_usage_scan_complete`
- `newapi_usage_model_requests_total`
- `newapi_usage_model_tokens_total{kind="input|output|cache_read|cache_creation|total"}`
- `newapi_usage_model_cost_usd_total{kind="actual"}`
- `newapi_usage_model_duration_seconds_bucket` / `_sum` / `_count`
- `newapi_usage_model_first_token_latency_seconds_bucket` / `_sum` / `_count`

`total` is prompt plus completion tokens. Cache values are overlapping input
breakdowns and are not added to total. Actual cost is the charged quota divided
by the current `quota_per_unit`. TTFT uses positive `other.frt` values from
streaming logs and converts milliseconds to seconds. Request IDs, token names,
IP addresses, channels, and API keys are never metric labels.

## PromQL examples

Requests during the last ten minutes:

```promql
sum by (site, model) (
  increase(newapi_usage_model_requests_total[10m])
)
```

Actual cost during the last hour:

```promql
sum by (site, model) (
  increase(newapi_usage_model_cost_usd_total{kind="actual"}[1h])
)
```

Per-model p95 first-token latency with a healthy usage collector:

```promql
histogram_quantile(
  0.95,
  sum by (site, group, platform, model, le) (
    rate(newapi_usage_model_first_token_latency_seconds_bucket[10m])
  )
)
and on(site) newapi_scrape_success{resource="usage"} == 1
```

Model additions and removals still use set differences on
`newapi_model_available` with successful current and historical model scrapes.
Usage inactivity must not be interpreted as a model going offline.
