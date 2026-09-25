# Proposal

## Why

Engineers need to inspect and recover Dagster failures from their existing Slack threads. Database provisioning is a constraint, and core development must proceed independently of Slack setup. The [infrastructure audit](environment-audit.md) grounds the implementation plan.

## What Changes

- Use Dagster GraphQL and bounded memory, with one bot pod/process and no bot database or persistent work store.
- Require current requester confirmation and fresh evidence before actions; reconcile uncertain outcomes without automatic resubmission. Restart invalidates controls and requires operator rearming.
- Build a minimal private admin panel for DEV testing and direct PROD operation through existing Teleport port-forward access over shared application services, independently of Slack credentials.
- Implement Slack mentions, trusted thread binding, membership checks and rendering as a separate adapter/workstream.
- Adopt LangGraph in phase one with scoped conversation/turn state, deterministic model placeholders and typed Dagster read/preparation tools. Later company-gateway implementations replace model interfaces; approval and dispatch remain application-owned.
- Preserve both retry modes, logical inputs/current code, Dagster retry/queue rules and the scoped operation catalog.
- Keep Dagster private on audited HTTP port 80, with explicit cross-namespace policy remediation and deployment admission checks.

The no-database decision replaces two-replica coordination, durable receipt/outbox and automatic failover. GraphQL checks cannot guarantee exactly-once execution or recovery of lost pre-run interactions. Phase one has no LLM; future adapters reuse the same policies.

## Capabilities

### New Capabilities

- `dagster-operations`: Named GraphQL operations, faithful execution and authoritative run evidence.
- `execution-runtime`: Single-process admission, volatile state, uncertainty/restart recovery and private deployment.
- `admin-panel`: Private administration UI that opens directly without application sign-in, with the full approved catalog, automatic browser context and isolated DEV mocks.
- `slack-operations`: Optional Slack transport, exact thread identity, channel authorization and best-effort replies.

### Modified Capabilities

None; no implemented capability baseline exists.

## Impact

Requires Python application/UI code, a pinned LangGraph dependency with necessary tool-schema support, Slack configuration, delivery manifests, a narrow policy change in the private Dagster deployment repository, tests and operator runbooks. No bot database, schema, migrations or storage provisioning is required. Existing Dagster persistence remains untouched. Raw compute logs are unavailable; expose structured errors. Deliver core/admin UI, live DEV execution and Teleport port-forward access, then Slack integration and controlled PROD rollout.
