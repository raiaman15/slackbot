# Proposal

## Why

Engineers need to inspect and recover Dagster failures from the existing Slack alert thread. This change defines the initial bot; implementation is pending.

## What Changes

- Add deterministic `@bot` commands, structured error details and same-thread progress.
- Let allowed-channel members prepare operations with requester confirmation.
- Connect FastAPI directly to the private Dagster webserver in the same Kubernetes cluster.
- Coordinate two bot replicas through PostgreSQL, with one active bot-created run family and conservative recovery from uncertain launches.
- Preserve source inputs, use current code and respect Dagster retries. Enable logs/status/retry first, then verified catalog operations.
- Keep application policies reusable for a later company AI gateway; phase one has no LLM.

## Capabilities

### New Capabilities

- `slack-operations`: Trusted thread binding, commands, authorization, confirmation, evidence and notifications.
- `dagster-operations`: Supported operations, faithful execution inputs and retry-family semantics.
- `execution-runtime`: Durable dispatch, reconciliation, private connectivity, lifecycle and availability.

### Modified Capabilities

None; no implemented baseline exists.

## Impact

Requires Python application code, PostgreSQL migrations, Slack app configuration, Kubernetes deployment settings, tests and runbooks. Dagster retains execution ownership. Keep its existing alert publisher; update it only if the full run UUID cannot be recovered from trusted alerts. Verify actual environment contracts in DEV before enabling PROD mutations.
