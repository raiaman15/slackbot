# Tasks

Implementation is pending. The [audit](environment-audit.md) provides reported facts, not completed work. All checkboxes remain unchecked until their verification passes. Core/DEV UI work requires neither Slack setup nor a bot database; Slack integration is a separate workstream.

## 1. Shared application and offline contracts

- [ ] 1.1 Build the FastAPI lifespan, typed actor/target/intent/result contracts, finite parser and named Dagster adapter boundary; verify shared services have no Slack dependency and startup works with Slack disabled.
- [ ] 1.2 Add validated mock/live and DEV/PROD settings, single-process runtime, bounded RAM queues/maps and boot identity; verify no database/persistent-store dependency, all mutations disabled every boot, and invalid/mixed profiles rejected.
- [ ] 1.3 Implement redaction, safe evidence models and structured telemetry; fixture-test secrets/PII, markup, truncation, selection/configuration handling and absence of raw errors/credentials in outputs or tags.

## 2. DEV workbench — independently deliverable

- [ ] 2.1 Serve the small `/dev` page and typed session/command/operation routes; verify thread-like logs/status/config, mode choice, preview, confirmation and separate proposal/run cancellation use shared services without Slack credentials.
- [ ] 2.2 Implement fixed startup mock/live mode, curated alert/run fixtures and server-bound DEV targets; verify no browser endpoint/environment/GraphQL override and no mock identity can reach real Dagster.
- [ ] 2.3 Add DEV authentication, bounded in-memory sessions, CSRF and Origin/Host checks; verify unauthorized/cross-origin access fails, credentials avoid URLs/browser storage, and PROD exposes neither UI assets nor DEV routes.
- [ ] 2.4 Add polling and explicit busy/expired/unknown/restarted states; verify dropped HTTP responses never auto-replay commands, old-boot controls fail, and mocked failure/retry scenarios are reproducible through automated tests.

## 3. Infrastructure and live DEV GraphQL

- [ ] 3.1 Platform/application owners: satisfy Kyverno labels and registry rules, publish a pullable image, and complete existing ACD/ArgoCD/ExternalSecrets delivery templates. Verify admission, image pull, startup and required egress as separate gates.
- [ ] 3.2 Platform/Dagster owners: apply narrow DEV/PROD webserver ingress TCP 80 from bot namespace AND pod selectors, preserving existing traffic. Verify real bot-to-Service GraphQL success and denied unrelated workloads; failed probes/port-forwards do not qualify.
- [ ] 3.3 Verify private inventory endpoints, network-only auth, deployed location/repository and pinned 1.13.1 schema/union/tag contracts; record ambient-mesh/retry behavior and intended webserver count versus DEV drift. Disable incompatible capabilities.
- [ ] 3.4 Implement bounded reads, full input/selection snapshots, event pagination and complete provenance/retry discovery. Verify op/asset/check/partition/null-empty fidelity, queue-policy tags, ordinary op/dbt no-retry failure, worker-crash family and pending-child gaps against live DEV.

## 4. Database-free execution and recovery

- [ ] 4.1 Implement current-boot requester-bound five-minute proposals and one-use confirmation; verify changed inputs/code/prior attempts, other users, expired state and duplicate controls cannot dispatch. Keep all approved inputs in memory only.
- [ ] 4.2 Implement local admission, complete GraphQL prechecks and stable installation/request/operation/source tags; verify same-request discovery, prior attempts across both modes, incomplete scans, active families and retry gaps block duplicate work without claiming distributed exclusion.
- [ ] 4.3 Implement whole-run re-execution/fresh-copy plans and consume dispatch authority before one POST; verify latest code, faithful selections/lineage, zero transport/proxy retries, and no resubmission after timeout, lost response or generic post-creation error.
- [ ] 4.4 Implement observation, positive reconciliation and mutation disarming on uncertainty; verify queued/active/pending families retain admission, missing results never prove absence, and failed notifications cannot trigger execution.
- [ ] 4.5 Implement the operator CLI/local socket through audited platform exec; drill first commissioning, graceful restart, node-partition/stale submitter isolation and unknown launch/automation outcomes. Verify boot/evidence binding, old controls invalidation, no remote unlock or automatic rearm from an empty scan, and zero POSTs if disarming/uncertainty occurs while an approved action awaits checks.
- [ ] 4.6 Add per-target cancellation and schedule/sensor controls plus optional validated launch/materialization mappings; verify termination-versus-terminal status, late conflicting automation requests even when reads show the desired state, preserved cursors, explicit repeat acknowledgement and excluded/bulk effects remain unavailable.

## 5. Slack adapter — separate from core/UI

- [ ] 5.1 Slack/data owners: supply real app/publisher/workspace/channel identities, scopes, captured alert payload and approved redaction/retention rules. Verify exact full UUID extraction from the audited localhost/double-slash button without publisher changes or URL fetching.
- [ ] 5.2 Implement optional Socket Mode intake and volatile bounded ACK/deduplication; verify prompt acknowledgement, duplicate events, reconnects, overload and loss on process exit without promising durable receipt or guaranteed redelivery.
- [ ] 5.3 Implement exact trusted-root resolution, volatile/manual binding and paginated membership policy; verify shortened IDs, edits, ambiguous roots, unauthorized/shared channels and lost binding state fail safely.
- [ ] 5.4 Map Slack commands/buttons/cards to shared services; verify requester identity, current-boot controls, membership freshness, repeat-attempt previews and same-thread responses without introducing another execution path.
- [ ] 5.5 Implement bounded in-memory notification retry/coalescing and scoped run lookup after restart; verify revoked destinations, ambiguous posts, exhausted retries and dropped state never reroute evidence or repeat execution.
- [ ] 5.6 Run real Slack DEV acceptance for logs/status, both retry modes and cancellation; verify scopes, identity, event delivery, root lookup, rendering and membership independently of UI/service tests.

## 6. Production readiness and completion

- [ ] 6.1 Enforce one replica/process, `Recreate`, no HPA/overlapping installation and audited recovery access; verify replacement remains read-only even if an old pod/request may survive. Verify SIGTERM drain, dead-loop failure and no Dagster restart storm.
- [ ] 6.2 Measure memory/queue bounds, receipt latency, query/scan budgets, observation and delivery behavior; verify active/unknown state is never evicted to admit work and independent alarms detect saturation, missing connectivity and rearm-required state.
- [ ] 6.3 Drill process-state loss, tagged-run recovery, Dagster outage/storage evidence loss and rollback. Verify no old command/confirmation is replayed, operator evidence gates reopening, and limitations are visible in help/runbooks.
- [ ] 6.4 Deploy PROD read-only with DEV UI/mock routes absent; repeat actual workload and Slack checks, confirm data-owner approvals and operator isolation procedure, then explicitly commission validated mutations and monitor controlled initial operations.
- [ ] 6.5 Reconcile delivered behavior and scenario evidence, document no-database availability/delivery limits and remaining owner gates, validate OpenSpec, and archive only after implementation acceptance.
