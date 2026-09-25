# Tasks

Planning is complete; implementation and environment acceptance remain pending. Check a task only after its code and verification pass. Capability specs define observable behavior; `design.md` owns exact names, schemas, routes, states and defaults. Implement each requirement's WHEN/THEN scenarios at service, adapter or live-environment level. Use sanitized fixtures; do not create placeholder directories or files before needed.

## 1. Shared application and contracts

- [ ] 1.1 Create the minimal Python package, locked dependencies, validated settings and typed command/result/error models from the design. Verify invalid/extra fields, mixed environments and PROD mock mode fail; startup needs no disabled-transport credentials.
- [ ] 1.2 Compile the async LangGraph workflow once with deterministic interpreter/formatter implementations and trusted runtime context. Verify every route with scripted inputs, no model calls/checkpointing/tracing, and bounded steps/deadlines.
- [ ] 1.3 Implement the exact command grammar and typed read/preparation tool mapping. Verify text/form parity, quoting, argument case, unknown flags, out-of-scope targets and no raw GraphQL or execution tool exposure.
- [ ] 1.4 Implement redaction and safe formatting. Test credentials/PII, markup, nested causes, truncation and selection/config handling; model/scripted prose cannot change immutable preview fields, status or authorization.
- [ ] 1.5 Implement RAM state, atomic request reservation and canonical operation states. Verify identical/concurrent requests return one receipt, changed payload conflicts, expiration is not extended, capacity is bounded and all mutations start disarmed.

## 2. Admin panel and browser API

- [ ] 2.1 Implement the canonical `/api/v1` routes/envelopes, server-issued contexts and scoped Activity lookups. Contract-test status codes, duplicate/lost-response lookup, unauthorized cached reads, pagination cursors and unknown operation semantics.
- [ ] 2.2 Implement OIDC authorization-code/PKCE with RAM login state/sessions and exact subject-role allowlists. Verify issuer/audience/signature/nonce/state, viewer/operator denial, session rotation/expiry, recent auth_time for PROD actions and missing-OIDC isolation from Slack.
- [ ] 2.3 Add CSRF/Origin/Host checks, safe cookies, cache/CSP headers, private access configuration and DEV-only local login. Verify cross-origin requests, forged identity and PROD mock/test routes cannot authorize access; credentials never enter logs/browser storage.
- [ ] 2.4 Build the six-view admin shell, responsive navigation, tables/filters, detail panel and command input. Verify empty/loading/stale/error/unauthorized states, page-size limits and no public/CDN dependency.
- [ ] 2.5 Wire every approved read/action form to shared commands. Verify read-only roles, unavailable presets/mappings, explicit target/mode, required partitions, immutable preview, discard versus cancel, and same-session one-use confirmation.
- [ ] 2.6 Implement safe polling and response-loss handling. Verify no POST auto-replay, hidden-page backoff, idle-session expiry despite polling, preserved preview after formatting failure and restart with unavailable history.
- [ ] 2.7 Run browser acceptance at desktop and narrow widths, keyboard/focus/contrast checks and mock fault fixtures. Cover concurrent actors, context races, malicious evidence, accepted-but-timeout submission and all command mappings; record functional/accessibility findings.

## 3. Infrastructure and live Dagster contract

- [ ] 3.1 Platform/application owners: provide Kyverno labels, a pullable approved image, existing ACD/ArgoCD/ExternalSecrets delivery and private admin/OIDC callback routing. Verify admission, pull, startup, secret injection and each required egress path separately.
- [ ] 3.2 Platform/Dagster owners: apply scoped DEV/PROD webserver ingress TCP 80 from bot namespace AND pod selectors, preserving legitimate traffic. Prove real allowed/denied workload paths; port-forwards or failed probes do not qualify.
- [ ] 3.3 Verify inventory URL/auth, deployed location/repository, pinned 1.13.1 query/input/result/tag contracts and mesh retry behavior. Capture sanitized fixtures; disable unsupported capabilities and reconcile DEV webserver replica drift.
- [ ] 3.4 Implement bounded GraphQL reads and faithful source snapshots. Verify partial/top-level/union errors, full selection/partition/null-empty fidelity, queue tags, op/dbt no-retry failures, worker-crash families and pending-child gaps against live DEV.

## 4. Confirmed execution and recovery

- [ ] 4.1 Implement immutable five-minute previews and requester/session/boot-bound controls. Verify changed input/code/prior attempts, reauthentication, other actors, duplicate controls and stale context cannot dispatch.
- [ ] 4.2 Implement complete background discovery plus fresh targeted prechecks, provenance tags, one family slot and target serialization. Verify incomplete/aged-out scans and active/pending prior attempts across both modes block launch without claiming distributed exclusion.
- [ ] 4.3 Implement whole-run re-execution and fresh-copy submission with one consumed dispatch attempt. Verify logical inputs/current code, lineage and tags; zero graph/client/proxy mutation retries; cancel/timeout/response loss never resubmits.
- [ ] 4.4 Implement family observation and positive reconciliation. Verify queued/pending descendants retain admission, failure versus success stays distinct from operation completion, ambiguous outcomes disarm, and delivery failure cannot affect execution.
- [ ] 4.5 Implement authenticated operator CLI/local socket and explicit rearming. Drill first commissioning, restart, stale submitter isolation, uncertain launch/automation, rollback and evidence loss; disarm during prechecks must prevent uncommitted dispatch.
- [ ] 4.6 Implement exact-run graceful cancellation. Verify already-terminal no-op, queued/active targets, descendant selection, cancellation-requested versus observed canceled, and uncertain termination recovery.
- [ ] 4.7 Implement schedule/sensor start/stop with per-target ordering. Verify already-desired no-op, late conflicting requests, cursor preservation and warnings about future runs outside the bot slot.
- [ ] 4.8 Implement optional preset launch and mapped asset materialization. Verify happy paths, current definition validation, existing partition keys, complete effects, missing config and excluded multi-run/bulk behavior.

## 5. Slack adapter — independent workstream

- [ ] 5.1 Slack/data owners: supply real identities, scopes, channel types, captured alert and approved disclosure rules. Verify exact full UUID extraction from the trusted localhost/double-slash button without fetching or modifying the publisher.
- [ ] 5.2 Implement Socket Mode bounded intake/ACK and request identity mapping. Verify duplicate events, reconnects, overload and volatile loss without promising durable receipt or redelivery.
- [ ] 5.3 Implement trusted root/manual binding and fully paginated membership checks. Verify shortened IDs, edits, unknown roots, shared/unapproved channels, revocation and missing binding state fail safely.
- [ ] 5.4 Route Slack command turns through the same graph and confirmations through the same confirmation service. Verify all catalog mappings, root reply routing, actor isolation, immutable previews and current-boot controls.
- [ ] 5.5 Implement bounded notification retry/coalescing and authorized state lookup. Verify revoked destinations, ambiguous posts, rate limits, exhausted retries and restart never reroute evidence or repeat execution.
- [ ] 5.6 Run real Slack DEV acceptance plus context-provider fixtures for pagination, history gaps and concurrent turns. Verify scopes/identity/events/thread rendering separately from UI; broader history/model use remains disabled in phase one.

## 6. Production release and completion

- [ ] 6.1 Verify one replica/process, Recreate/no overlap, no HPA, shutdown drain, loop supervision and independent operator access. Replacement must remain read-only despite possible surviving old requests.
- [ ] 6.2 Measure configured RAM/query/graph budgets, receipt latency, observation and delivery. Verify active/unknown state is never evicted to admit work; independent alarms detect saturation, outages and rearm-required state.
- [ ] 6.3 Drill process loss, tagged-run recovery, Dagster outage/storage evidence loss and rollback. Verify no recreated approval/replayed command, explicit rearm and accurate limitations in panel/help/runbook.
- [ ] 6.4 Deploy enabled PROD transports read-only. Repeat actual workload, OIDC/role and Slack checks; close data-owner gates, then explicitly commission only validated mutations and monitor initial controlled actions.
- [ ] 6.5 Reconcile every capability scenario with test/evidence results, run Ruff/mypy/pytest and required browser/live checks, validate OpenSpec, and archive only after acceptance. Documentation validation alone never completes an implementation task.
