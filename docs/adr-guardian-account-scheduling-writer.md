# ADR: Guardian writes Sub2API account scheduling fields

- Status: Accepted
- Date: 2026-08-25
- Authoritative Sub2API revision: `aa2c4e8d136b13553ac7bae3d76c25715333a554`
- Discovery mode: source review and local fakes only; no live Sub2API mutation was sent

## Decision

Guardian's UI calls its scored objects “channels”, but Sub2API's official `channels` resource is
the billing/pricing channel and does not expose `load_factor`, `priority`, or `schedulable`.
Those three fields belong to Sub2API **accounts**. Guardian must therefore resolve a monitor to
one unique group, enumerate the accounts in that group from the shared inventory, and write each
eligible account by account ID. A monitor ID or billing-channel ID must never be used as an
account mutation ID.

Official sources:

- Admin routes: account read/update and the dedicated schedulable endpoint are registered at
  [admin.go lines 360–400](https://github.com/Wei-Shaw/sub2api/blob/aa2c4e8d136b13553ac7bae3d76c25715333a554/backend/internal/server/routes/admin.go#L360-L400).
- Partial account update fields are pointer-valued in
  [account_handler.go lines 133–153](https://github.com/Wei-Shaw/sub2api/blob/aa2c4e8d136b13553ac7bae3d76c25715333a554/backend/internal/handler/admin/account_handler.go#L133-L153).
- The update handler passes `priority` and `load_factor` through and returns the updated account in
  [account_handler.go lines 953–1018](https://github.com/Wei-Shaw/sub2api/blob/aa2c4e8d136b13553ac7bae3d76c25715333a554/backend/internal/handler/admin/account_handler.go#L953-L1018).
- The dedicated schedulable handler is defined in
  [account_handler.go lines 2533–2560](https://github.com/Wei-Shaw/sub2api/blob/aa2c4e8d136b13553ac7bae3d76c25715333a554/backend/internal/handler/admin/account_handler.go#L2533-L2560).
- Official field semantics and defaults are defined in
  [account.go lines 98–149](https://github.com/Wei-Shaw/sub2api/blob/aa2c4e8d136b13553ac7bae3d76c25715333a554/backend/ent/schema/account.go#L98-L149).
- `load_factor <= 0` clears the override and values above 10000 are rejected in
  [admin_account.go lines 751–779](https://github.com/Wei-Shaw/sub2api/blob/aa2c4e8d136b13553ac7bae3d76c25715333a554/backend/internal/service/admin_account.go#L751-L779).

## Verified HTTP contract

All paths are relative to the configured Sub2API admin base URL. Requests carry the existing
`x-api-key` admin credential and JSON content type. Redirects are rejected by the shared client.

| Operation | Method and path | Exact request body | Required read-back |
|---|---|---|---|
| Read state | `GET /api/v1/admin/accounts/{account_id}` | none | `code=0`; `data.id` exactly matches; strict `status`, `schedulable`, `priority`, `load_factor`, and `concurrency` |
| Set load | `PUT /api/v1/admin/accounts/{account_id}` | `{"load_factor": N}` only | A separate GET must return the same positive integer |
| Set priority | `PUT /api/v1/admin/accounts/{account_id}` | `{"priority": N}` only | A separate GET must return the same integer |
| Set scheduling | `POST /api/v1/admin/accounts/{account_id}/schedulable` | `{"schedulable": true|false}` only | A separate GET must return the same boolean |

The update response is not treated as verification. Guardian always performs a separate GET so a
timeout, partial write, stale response, wrong identity, or malformed envelope cannot be reported
as success.

## Field semantics

- `load_factor` is nullable. A positive value is the explicit routing weight. Null, zero, or a
  negative legacy value falls back to positive `concurrency`, then to 1. Guardian only writes
  explicit values from 1 through 10000; it never sends zero because zero means “clear”, not a
  routing weight.
- `priority` is an account integer and smaller values have higher priority. The official default is
  50 and the official update handler does not impose a 1–5 range. Guardian therefore preserves the
  observed baseline and applies small positive degradation offsets. It must not clamp a baseline
  such as 50 down to 5. Guardian reserves priority 0 and writes values from 1 through 1,000,000.
- `schedulable=false` removes an account from scheduling. `active + schedulable=false` remains a
  protected manual pause unless Guardian has durable ownership evidence for that exact account.

## Failure-closed rules

No write is allowed when any of the following is true:

1. monitor-to-group mapping is absent or ambiguous;
2. the account is not present in the latest canonical inventory;
3. current-state GET redirects, fails, has `code != 0`, has a wrong account ID, or omits/malforms a
   scheduling field;
4. the account is a manual pause, expired, or temporarily unavailable;
5. the one-field write fails or redirects;
6. the separate read-back does not exactly match the target;
7. Guardian loses its write lease or field ownership before verification.

An indeterminate result freezes the current and remaining writes in that run. It is never coerced
to success.

## Compatibility consequence

The earlier 1–5 priority assumption came from the learned WogHub UI, not the Sub2API API. It is
not authoritative for Sub2API accounts and is replaced by baseline-relative integer priority.
The Guardian UI will be updated in GR14 to display the actual baseline and remove the 1–5 limit.
