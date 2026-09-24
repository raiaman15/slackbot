# Proposal

## Why

Engineers need to inspect and recover Dagster failures from the existing Slack alert thread. The [infrastructure audit](environment-audit.md) grounds this initial bot plan in the reported DEV/PROD deployment; implementation is pending.

## What Changes

- Add deterministic `@bot` commands, structured error details and same-thread progress.
- Let allowed-channel members prepare operations with requester confirmation.
- Connect FastAPI to Dagster 1.13.1 over the private HTTP Service on port 80. Keep separate namespaces and explicitly unblock narrow webserver access through the Dagster deployment repository.
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

Requires Python code, an isolated bot database schema/role, Slack configuration, deployment pipeline/manifests, cross-repository network changes, tests and runbooks. Dagster retains execution ownership. The existing alert button supplies the full UUID; preserve its publisher. Verify retry/selection behavior, actual bot connectivity and production-representative database recovery before enabling PROD mutations. Raw compute logs are unavailable; `logs` exposes structured errors.
