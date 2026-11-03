# supplier Input

The `supplier` input watches prepaid balances at upstream suppliers so an
alert can fire before an account runs dry. One `[[instances]]` block = one
supplier account. `provider` selects the upstream API shape; every provider
reports the same `supplier_balance` metric, so one set of alert rules covers
all suppliers.

| provider | Request | Credential |
|---|---|---|
| `lynxton` | `GET <base_url>/v1/account/balance` | `Authorization: Bearer sk-...` |
| `epay` | `GET <base_url>/api.php?act=query&pid=<pid>&key=<key>` (彩虹易支付 merchant query) | merchant `pid` + `key` |

`base_url` is the site root. The plugin appends the path, and Init rejects a
`base_url` that already ends in the API path or carries a query string.

## Configuration

See `conf/input.supplier/supplier.toml`. The interval defaults to 5m. Requests
to the same upstream origin share a token bucket (1 rps, burst 1 by default)
with every other `supplier`, `newapi` and `sub2api` instance. Failures are not
retried within a collection.

Common keys: `supplier` (label value), `provider`, `base_url`, `api_key`,
plus the usual Categraf HTTP options (`timeout`, `http_proxy`, TLS). The
plugin only reports values; alert thresholds belong in the alert rules.

### lynxton

- The balance belongs to the account, not to the token, so any `sk-` key of
  the account works. Create a dedicated monitoring token and give that token a
  tiny quota.
- `balance_scope` follows the account that owns the key: owner/admin keys see
  the organization pool (`org_pool`), member keys see their own allowance
  (`personal`). Set `expect_scope` so a swapped key fails the scrape instead of
  silently changing what the number means.
- `balance_display` is reported as the balance, in `display_unit`.
- `used_quota` is converted with `quota_per_unit` (default 500000) into
  `supplier_spent_total`. The plugin compares the configured ratio with
  `balance / balance_display`; if they differ by more than 1% it logs a
  warning and skips `supplier_spent_total` for that scrape. It never uses the
  observed ratio directly, so rounding cannot make the counter go backwards.

### epay

- `pid` accepts an integer or a string. `unit` defaults to `CNY` because the
  response has no currency field.
- A wrong key or pid still returns HTTP 200; any `code` other than `1` fails
  the scrape and logs the upstream `msg`.
- `money`, `active` and the order counts arrive as strings and are parsed as
  numbers. Both `orders_today`/`orders_lastday` and the documented
  `order_today`/`order_lastday` spellings are accepted.

## Key handling

The epay merchant key is also the signing key for payment requests and
asynchronous callbacks: anyone holding it can forge a "payment succeeded"
notification. Configure it on one trusted collector and keep it out of git.

`api_key` accepts `@{env:NAME}`, resolved once at Init from the Categraf
process environment; an unset variable fails Init with the variable's name.
No other secret store is supported.

The plugin never puts a key in metrics or labels. Every logged error is
scrubbed for the key as configured and in its URL-encoded forms, because the
epay key travels in the query string and Go's network errors include the full
request URL. Response bodies are never logged (the epay response echoes the
key), and the response struct has no field for it.

## Metrics

Every metric carries `supplier` and `provider`.

| Metric | Labels | Providers | Notes |
|---|---|---|---|
| `supplier_balance` | `unit`, `scope` (lynxton) | all | Remaining prepaid balance |
| `supplier_spent_total` | `unit`, `scope` | lynxton | Cumulative spend |
| `supplier_account_active` | | epay | 1 = enabled, 0 = disabled |
| `supplier_orders_total` | | epay | Lifetime order count |
| `supplier_orders_today` / `supplier_orders_yesterday` | | epay | |
| `supplier_up` | | all | 1 when the scrape succeeded |
| `supplier_scrape_duration_seconds` | | all | |
| `supplier_last_success_timestamp_seconds` | | all | Only on success |

The balance metrics are emitted only when the scrape succeeds, so a failing
collector does not trigger balance alerts. Alert on `supplier_up` instead.

## Alert rules

```promql
# Balance below a per-supplier threshold, in the balance's unit (CNY here).
supplier_balance{supplier="lynxton"} < 50
supplier_balance{supplier="payqixiang"} < 100

# At the trend of the last 6 hours, the balance reaches 0 within 24 hours.
# A top-up makes the slope positive, so it does not fire right after one.
predict_linear(supplier_balance[6h], 86400) < 0

# Collector failing for 15 minutes: revoked key, disabled account, scope
# mismatch, API change. Without this, a broken collector never alerts.
supplier_up == 0

# epay merchant disabled.
supplier_account_active == 0
```

Collect each supplier on one Categraf instance only. Several collectors would
send duplicate series and fire duplicate alerts.
