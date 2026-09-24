# Tasks

All tasks track implementation and remain unchecked. Keep verification evidence with the code or controlled environment records; never commit secrets.

## 1. Verify environment contracts

- [ ] 1.1 Record Dagster versions, schema, launcher, selection and retry behavior; capture DEV fixtures proving complete input recovery, lineage and pending/exhausted retries, including the gap before a child exists.
- [ ] 1.2 Verify private GraphQL connectivity/authentication from the actual FastAPI workload and exact UUID extraction from a trusted alert; update the publisher if necessary.
- [ ] 1.3 Record approved Slack identities/channels, DEV/PROD separation, data policy, key management, database failover guarantees and independent alarm route; obtain owner sign-off on unresolved environment facts.

## 2. Build application and persistence

- [ ] 2.1 Create the Python application with pinned async clients, lifespan and validated settings; verify clean DEV startup/shutdown, invalid-config rejection and dispatch disabled by default.
- [ ] 2.2 Add the design's ledger migrations, encrypted snapshots and transactional repositories; verify unique keys, state transitions, append-only audit, key handling and clean initialization.
- [ ] 2.3 Implement bounded pools, receipt capacity reservation, durable work claims and lease fencing; verify saturation preserves receipt deadlines and competing/stale workers cannot overwrite state.

## 3. Implement the Dagster adapter

- [ ] 3.1 Add fixed-endpoint reads and submit-once mutations with schema/union checks; verify TLS/auth, timeouts, redirect refusal, zero launch retries and capability degradation on mismatch.
- [ ] 3.2 Implement complete run/evidence snapshots and both execution modes; verify configuration, op/asset/check/partition selection, null versus empty, lineage and current-code/image conflicts in DEV.
- [ ] 3.3 Add retry-family and operation-tag discovery; verify pending retries/budgets, inherited tags, actual descendants and ambiguous or persisted-but-unsubmitted runs.

## 4. Implement Slack interaction and policy

- [ ] 4.1 Configure DEV/PROD Socket Mode apps and persist-before-ACK receipt handling; verify duplicate delivery, uncertain database commits, reconnects and bounded acknowledgement latency.
- [ ] 4.2 Implement deterministic mention parsing, exact trusted-root binding and the full-ID fallback; verify unsupported prose, edits, bots, suffixes and neighboring/child runs cannot silently select an action or target.
- [ ] 4.3 Add scope and paginated membership checks plus requester-bound expiring confirmations; verify forged/stale/double clicks, another user, removed membership and changed previews cannot dispatch.
- [ ] 4.4 Build bot-owned thread cards and command help; verify mode choice, preview, confirmation, cancellation of proposals, operation lookup and progress remain in the original thread.

## 5. Implement dispatch and recovery

- [ ] 5.1 Add final prechecks and atomic global launch admission; verify stale inputs return to confirmation and simultaneous requests admit one family, with busy responses instead of queued launches.
- [ ] 5.2 Persist the unique dispatch marker and permit only its creator's single send after a known commit; fault-test uncertain commits, crashes, lost responses and generic post-creation errors without automatic resubmission.
- [ ] 5.3 Track known and unknown submissions; verify empty searches, retry gaps, cancellation pending and stale-worker evidence retain unresolved state until conclusive family completion.
- [ ] 5.4 Implement the operator recovery gate and runbook; drill submitter isolation, evidence-based resolution and database restore/lossy promotion before allowing a new confirmed operation.

## 6. Deliver safe evidence and the operation catalog

- [ ] 6.1 Implement bounded structured-error extraction, safe rendering and redaction before storage/output; test secrets/PII, markup injection, pagination, truncation and configured human links.
- [ ] 6.2 Implement transactional outbox, status coalescing and rate-limit handling; verify Slack failures never replay execution and revoked destinations stop delivery without rerouting.
- [ ] 6.3 Add audit and retention jobs; verify actor-to-run traceability, encrypted snapshot expiry and retention of every unresolved operation's recovery evidence.
- [ ] 6.4 Implement metadata commands and exact-run graceful cancellation; verify advertised capabilities match supported contracts and cancellation-requested is distinct from terminal.
- [ ] 6.5 Implement serialized schedule/sensor desired-state controls; verify uncertain changes block conflicting requests and preserve sensor cursors.
- [ ] 6.6 Add validated launch/materialization presets; verify absent mappings, expanded asset effects and unsupported ranges are refused, and excluded/bulk/destructive controls remain unavailable.

## 7. Deploy and verify

- [ ] 7.1 Add supervised loops, health endpoints and SIGTERM drain; verify dead loops fail health, database loss stops intake, Dagster outages avoid restart storms and shutdown preserves running work.
- [ ] 7.2 Add two-replica deployment templates, disruption protection, secrets, network restrictions and least privilege; verify rolling overlap and actual private connectivity without Kubernetes execution permissions.
- [ ] 7.3 Add metrics, independent alerts and deployment/rotation/upgrade/rollback runbooks; drill dependency failure, unknown-launch alarms and compatible rollback with the ledger retained.
- [ ] 7.4 Run DEV acceptance against every spec scenario, including two-replica races and database recovery; record end-to-end latency/recovery results and test/evidence references.
- [ ] 7.5 Deploy PROD read-only, then enable verified operations through the release gate; record actual environment checks and canary outcomes. Reconcile specs/tasks, validate OpenSpec and archive only after implementation acceptance.
