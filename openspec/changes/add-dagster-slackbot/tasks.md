# Tasks

Planning is complete; implementation and environment acceptance remain pending. Check a task only after its code and verification pass. Capability specs define observable behavior; `design.md` owns exact names, schemas, routes, states and defaults. Implement each requirement's WHEN/THEN scenarios at service, adapter or live-environment level. Use sanitized fixtures; do not create placeholder directories or files before needed.

Start core and mock-backed UI work now. In parallel, prioritize the live adapter proof (3.3–3.4) and restart/rearm drill (4.5), since retry observability and loss of process state carry the main remaining risks. Live reads require the infrastructure path; live writes additionally require validated dispatch/recovery and explicit commissioning. Slack remains independent; its missing identities do not block core/UI development.

## 1. Shared application and contracts

- [ ] 1.1 Create the minimal Python package, locked dependencies, validated settings and typed command/result/error models from the design. Verify invalid/extra fields, mixed environments and PROD mock mode fail; startup needs no disabled-transport credentials.
- [ ] 1.2 Compile the async LangGraph workflow once with deterministic interpreter/formatter implementations and trusted runtime context. Verify every route with scripted inputs, no model calls/checkpointing/tracing, bounded steps/deadlines, and incomplete background discovery returned within the turn budget.
- [ ] 1.3 Implement the exact command grammar and typed read/preparation tool mapping. Verify text/form parity, lazy job/preset and schedule/sensor/tick details, quoting, argument case, unknown flags, out-of-scope targets and no raw GraphQL or execution tool exposure.
- [ ] 1.4 Implement redaction and safe formatting. Test credentials/PII, markup, nested causes, truncation and selection/config handling; model/scripted prose cannot change immutable preview fields, status or authorization.
- [ ] 1.5 Implement RAM state, atomic request reservation and canonical operation states. Verify identical/concurrent requests return one receipt, changed payload conflicts, expiration is not extended, capacity is bounded, accepted-worker failures remain inspectable and all mutations start disarmed.

## 2. Admin panel and browser API

- [ ] 2.1 Implement the canonical `/api/v1` routes/envelopes, server-issued contexts and scoped Activity lookups. Contract-test status codes, duplicate/lost-response lookup, unauthorized cached reads, pagination cursors, unknown operation semantics and accepted-then-rejected Operation.error responses.
- [ ] 2.2 Implement automatic bounded RAM browser sessions through bootstrap, with no sign-in in DEV or PROD. Verify common configured capabilities, session/boot isolation, automatic expiry renewal, invalidation of old previews and no claim of individual Teleport identity.
- [ ] 2.3 Add CSRF/Origin/Host checks, installation-qualified cookies, cache/CSP headers and loopback port-forward configuration. Verify forged origins, cross-site requests, altered session IDs, mixed DEV/PROD contexts and PROD mock routes fail; no login routes or identity-provider credentials exist.
- [ ] 2.4 Build the six-view admin shell, responsive navigation, tables/filters, detail panel and command input. Verify empty/loading/stale/error/unauthorized states, page-size limits and no public/CDN dependency.
- [ ] 2.5 Wire every approved read/action form to shared commands. Verify configured read-only state, unavailable presets/mappings, explicit target/mode, required partitions, immutable preview, discard versus cancel, and same-session one-use confirmation, explicit repeat controls and run-bound versus explicit catalog contexts.
- [ ] 2.6 Implement safe polling and response-loss handling. Verify no POST auto-replay, hidden-page backoff, automatic session renewal after idle expiry despite polling, preserved preview after formatting failure, pending-confirmation polling/control behavior and restart with unavailable history.
- [ ] 2.7 Run browser acceptance at desktop and narrow widths, keyboard/focus/contrast checks and mock fault fixtures. Cover concurrent actors, context races, malicious evidence, accepted-but-timeout submission and all command mappings; record functional/accessibility findings.

## 3. Infrastructure and live Dagster contract

- [ ] 3.1 Platform/application owners: provide Kyverno labels, a pullable approved image, existing ACD/ArgoCD/ExternalSecrets delivery and existing Teleport permissions, loopback port-forward and restricted bot ingress. Verify admission, pull, startup, secret injection, the specified bot 8000/Service 80 mapping, local DEV/PROD origins, probes/metrics, denied unintended bot ingress and each required egress path separately.
- [ ] 3.2 Platform/Dagster owners: apply scoped DEV/PROD webserver ingress TCP 80 from bot namespace AND pod selectors, preserving legitimate traffic. Prove real allowed/denied workload paths; port-forwards or failed probes do not qualify.
- [ ] 3.3 Verify inventory URL/auth, deployed location/repository, pinned 1.13.1 query/input/result/tag contracts and mesh retry behavior. Capture the final audit's execution/configuration, run/filter and lineage contracts as sanitized fixtures; introspection does not prove runtime semantics. Disable unsupported capabilities and reconcile DEV webserver replica drift.
- [ ] 3.4 Implement bounded GraphQL reads, exact private configuration snapshots and derived retry assessment. Verify scalar conversion/type/null-empty fidelity, pre-dispatch fingerprints, partial/top-level/union errors, complete selections/partitions and queue tags. Pin retry tag/event semantics; test op/dbt no-retry, worker-crash pending-child gaps, failure before decision publication, missing history, resolved parent/child markers and terminal success without a retry marker against DEV fixtures.

## 4. Confirmed execution and recovery

- [ ] 4.1 Implement immutable five-minute previews and requester/session/boot-bound controls. Verify changed input/code/prior attempts, expired/replaced browser sessions, other actors, duplicate controls and stale context cannot dispatch; repeat-labelled controls bind the displayed prior-attempt history.
- [ ] 4.2 Implement complete background discovery plus fresh targeted prechecks, provenance tags, one family slot and target serialization. Verify incomplete/aged-out scans and active/PENDING/UNKNOWN prior attempts across both modes block launch without claiming distributed exclusion.
- [ ] 4.3 Implement whole-run re-execution and fresh-copy submission with one consumed dispatch attempt. Verify logical inputs/current code, lineage and tags; zero graph/client/proxy mutation retries; cancel/timeout/response loss never resubmits.
- [ ] 4.4 Implement family observation and positive reconciliation. Verify queued descendants and PENDING/UNKNOWN assessments hold the slot, complete NOT_PENDING plus terminal family permits release, failure versus success stays distinct from operation completion, ambiguous outcomes disarm, and delivery failure cannot affect execution.
- [ ] 4.5 Implement authenticated operator CLI/local socket and explicit rearming. Drill first commissioning, restart, stale submitter isolation, uncertain launch/automation, rollback and evidence loss; disarm during prechecks must prevent uncommitted dispatch.
- [ ] 4.6 Implement exact-run graceful cancellation. Verify already-terminal no-op, queued/active targets, descendant selection, cancellation-requested versus observed canceled, and uncertain termination recovery.
- [ ] 4.7 Implement schedule/sensor start/stop with per-target ordering. Verify already-desired no-op, late conflicting requests, cursor preservation and warnings about future runs outside the bot slot.
- [ ] 4.8 Implement optional preset launch and mapped asset materialization. Verify happy paths, current definition validation, existing partition keys, complete effects, missing config and excluded multi-run/bulk behavior.

## 5. Slack adapter — independent workstream

- [ ] 5.1 Slack/data owners: supply own app/workspace identities, publisher identities, specified scopes/event/interactivity settings, channel types, captured alert and approved disclosure rules. Verify exact full UUID extraction from the trusted localhost/double-slash button without fetching or modifying the publisher.
- [ ] 5.2 Implement Socket Mode bounded intake/ACK and request identity mapping. Verify mention/button ACK within three seconds, duplicate events, rejected/full-queue ACKs, reconnects, overload and volatile loss without promising durable receipt or redelivery.
- [ ] 5.3 Implement trusted root/manual binding and fully paginated membership checks. Verify shortened IDs, exact-root timestamp/deleted-root behavior, edits, unknown roots, shared/unapproved channels, revocation and missing binding state fail safely. Verify local binding confirmation works while mutations are disarmed without a Dagster mutation.
- [ ] 5.4 Route Slack command turns through the same graph and confirmations through the same confirmation service. Verify all catalog mappings, root reply routing, actor isolation, immutable previews and current-boot controls.
- [ ] 5.5 Implement bounded notification retry/coalescing and authorized state lookup. Verify revoked destinations, ambiguous posts, rate limits, exhausted retries and restart never reroute evidence or repeat execution.
- [ ] 5.6 Run real Slack DEV acceptance plus context-provider fixtures for pagination, history gaps and concurrent turns. Verify scopes/identity/events/thread rendering separately from UI; broader history/model use remains disabled in phase one.

## 6. Production release and completion

- [ ] 6.1 Verify one replica/process, Recreate/no overlap, no HPA, shutdown drain, loop supervision and independent operator access. Replacement must remain read-only despite possible surviving old requests.
- [ ] 6.2 Measure configured RAM/query/graph budgets, receipt latency, observation and delivery. Verify the 100-entry unresolved-mutation budget includes cancellation/automation, preserves reads at saturation and never evicts active/unknown state to admit work; independent alarms detect saturation, outages and rearm-required state.
- [ ] 6.3 Drill process loss, tagged-run recovery, Dagster outage/storage evidence loss and rollback. Record platform evidence or the remaining G4 uncertainty about the existing Dagster storage recovery semantics; do not provision a bot database or infer failover guarantees. Verify no recreated approval/replayed command, explicit rearm and accurate limitations in panel/help/runbook.
- [ ] 6.4 Deploy enabled PROD transports read-only. Repeat actual workload, Teleport/port-forward boundary and Slack checks; close data-owner gates, then explicitly commission only validated mutations and monitor initial controlled actions.
- [ ] 6.5 Reconcile every capability scenario with test/evidence results, run Ruff/mypy/pytest and required browser/live checks, validate OpenSpec, and archive only after acceptance. Documentation validation alone never completes an implementation task.
