# Proposal

## Why

Engineers need to inspect a Dagster failure and safely control its recovery from the existing Slack alert thread. The repository currently contains a detailed design but no runnable bot; this change defines the initial implementation contract using OpenSpec.

## What Changes

- Add deterministic `@bot` commands inside trusted failure threads, with same-thread evidence, action choices, confirmation and progress.
- Let allowed-channel members inspect Dagster runs and prepare explicitly confirmed operations using one backend service identity.
- Implement direct private API access from the existing FastAPI application to the existing Dagster webserver in the same Kubernetes cluster.
- Preserve the v0.3 architecture: one bot deployment with two production replicas, durable state in PostgreSQL, and supervised background work in each application process.
- Support whole-run re-execution and fresh-copy launch with preserved logical inputs and current deployed code, while observing Dagster's own retries.
- Add durable receipt, confirmation, launch admission, uncertain-submission reconciliation, audit, redaction and notification behavior.
- Retain the named operation catalog; enable logs/status/retry first, then other verified operations. Preset-dependent launch/materialization stays disabled until mappings exist.
- Preserve an extension boundary for phase-two natural-language interpretation; phase one has no model dependency.

This is a new system capability proposal, not a declaration that implementation or production rollout has completed. Existing Dagster alert publication remains owned by its current integration. A publisher change is required only if a complete run ID cannot be recovered from the existing trusted alert.

## Capabilities

### New Capabilities

- `slack-thread-interaction`: Explicit mention parsing, exact trusted root/run binding, same-thread user interaction and fallback behavior.
- `operation-authorization`: Workspace/channel/member/scope enforcement and requester-bound, expiring action confirmation.
- `dagster-control`: Version-compatible named Dagster operations, execution-mode semantics, selection preservation and automatic-retry coordination.
- `durable-execution`: Durable requests, deduplication, single-family admission, one automatic dispatch attempt, reconciliation and operator recovery.
- `evidence-and-notifications`: Structured Dagster evidence, safe rendering, independent notification delivery, audit and retention.
- `cluster-runtime`: Private same-cluster connectivity, application lifecycle, replica recovery, health and deployment-readiness guarantees.

### Modified Capabilities

None. No implemented capability baseline exists in `openspec/specs/` yet.

## Impact

Planned code belongs in the Python FastAPI application, with Slack, Dagster and PostgreSQL adapters, migrations, Kubernetes deployment configuration, tests and runbooks. Integrations require Slack app credentials/scopes, approved outbound Slack access, the existing internal webserver Service, and a dedicated database role. Dagster continues to own execution; the bot needs no Kubernetes job-creation permissions or runtime Teleport session.

This conversion organizes the existing design. It does not deploy infrastructure, install a Slack app, change Dagster configuration or enable production mutations. Environment verification tasks remain part of the implementation plan.

