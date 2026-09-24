# Tasks

The [audit](environment-audit.md) supplies reported facts, not completed implementation. All tasks remain unchecked. Keep verification evidence with the code or controlled environment records; never commit secrets.

## 1. Close audit release gates

- [ ] 1.1 Dagster/job owners: capture pinned 1.13.1 schema/input/union and selection fixtures. Verify ordinary op/dbt failure without automatic run retry, induced worker-crash recovery, pending-child gaps and effective per-run overrides against DEV two / PROD three instance retry defaults.
- [ ] 1.2 Platform/Dagster owners: change the private Dagster deployment chart/values in DEV and PROD to admit only bot namespace AND pod selectors to webserver TCP 80, preserving existing traffic. Verify allowed bot access and denied unrelated pod/namespace traffic with effective CNI policies.
- [ ] 1.3 Application/platform owners: supply the private inventory's release-qualified HTTP URLs, deployed location/repository and explicit network-only auth. Verify access from actual bot pods, environment/preview isolation, ambient-mesh status and the intended webserver replica count versus DEV drift.
- [ ] 1.4 Slack/data owners: capture real publisher/app/workspace/channel IDs/types and the full-UUID button payload; approve membership, redaction/retention and human port-forward instructions. Verify exact trusted extraction without publisher changes or localhost fetching.
- [ ] 1.5 Database owner: provision an isolated bot schema/role (or dedicated database), restricted credentials and key management; record engine/topology/commit guarantees and bot DEV storage choice. Approve a production-representative non-production failover test environment; a single-pod DEV test cannot clear this gate.
- [ ] 1.6 Application/platform owners: complete delivery pipeline/configuration using existing ACD/ArgoCD, image tags and ExternalSecrets conventions; verify rendered DEV/PROD manifests, secret references, independent alarm route and cross-repository rollout order.

## 2. Build application and persistence

- [ ] 2.1 Create the Python application with pinned async clients, lifespan and validated settings; verify clean DEV startup/shutdown, invalid-config rejection and dispatch disabled by default.
- [ ] 2.2 Add the design's ledger migrations, encrypted snapshots and transactional repositories; verify unique keys, state transitions, append-only audit, key handling and clean initialization.
- [ ] 2.3 Implement bounded pools, receipt capacity reservation, durable work claims and lease fencing; verify saturation preserves receipt deadlines and competing/stale workers cannot overwrite state.

## 3. Implement the Dagster adapter

- [ ] 3.1 Add fixed-endpoint reads and submit-once mutations with schema/union checks; verify the configured HTTP/network-only contract, optional TLS validation, timeouts, redirect refusal, zero launch retries and capability degradation on mismatch. Pin `stopRunningSchedule` and reject excluded schema mutations.
- [ ] 3.2 Implement complete run/evidence snapshots and both execution modes; verify configuration, op/asset/check/partition selection, null versus empty, lineage and current-code/image conflicts in DEV.
- [ ] 3.3 Add retry-family and operation-tag discovery; verify effective retries/budgets, preserved queue-policy tags, inherited correlation, actual descendants and ambiguous or persisted-but-unsubmitted runs; never infer exhaustion from the instance flag alone.

## 4. Implement Slack interaction and policy

- [ ] 4.1 Configure DEV/PROD Socket Mode apps and persist-before-ACK receipt handling; verify duplicate delivery, uncertain database commits, reconnects and bounded acknowledgement latency.
- [ ] 4.2 Implement deterministic mentions, trusted button-URL binding and full-ID fallback; fixture-test `http://127.0.0.1:8080//runs/<full-uuid>`, first-segment display text, wrong publishers/URLs, edits and neighboring/child runs without fetching alert URLs.
- [ ] 4.3 Add scope and paginated membership checks plus requester-bound expiring confirmations; verify forged/stale/double clicks, another user, removed membership and changed previews cannot dispatch.
- [ ] 4.4 Build bot-owned thread cards and command help; verify mode choice, effective retry policy, possible queueing, confirmation, proposal cancellation, operation lookup and progress remain in the original thread.

## 5. Implement dispatch and recovery

- [ ] 5.1 Add final prechecks and atomic global launch admission; verify stale inputs return to confirmation and simultaneous requests admit one family, with busy responses instead of queued launches.
- [ ] 5.2 Persist the unique dispatch marker and permit only its creator's single send after a known commit; fault-test uncertain commits, crashes, lost responses and generic post-creation errors without automatic resubmission.
- [ ] 5.3 Track known and unknown submissions; verify empty searches, tag-limited queueing, monitoring delays, retry gaps, cancellation pending and stale-worker evidence retain ownership until conclusive family completion.
- [ ] 5.4 Implement the operator recovery gate and runbook; drill submitter isolation and evidence-based resolution, plus restore/lossy promotion on a database matching PROD failover semantics. Keep PROD writes blocked if that evidence is unavailable; local single-pod drills alone are insufficient.

## 6. Deliver safe evidence and the operation catalog

- [ ] 6.1 Implement bounded structured-error extraction, safe rendering and redaction before storage/output; test secrets/PII, markup injection, pagination, truncation, human port-forward guidance and explicit unavailable stdout/stderr under `NoOpComputeLogManager`; no Kubernetes-log fallback.
- [ ] 6.2 Implement transactional outbox, status coalescing and rate-limit handling; verify Slack failures never replay execution and revoked destinations stop delivery without rerouting.
- [ ] 6.3 Add audit and retention jobs; verify actor-to-run traceability, encrypted snapshot expiry and retention of every unresolved operation's recovery evidence.
- [ ] 6.4 Implement metadata commands and exact-run graceful cancellation; verify advertised capabilities match supported contracts and cancellation-requested is distinct from terminal.
- [ ] 6.5 Implement serialized schedule/sensor desired-state controls; verify uncertain changes block conflicting requests and preserve sensor cursors.
- [ ] 6.6 Add validated launch/materialization presets; verify absent mappings, expanded asset effects and unsupported ranges are refused, and excluded/bulk/destructive controls remain unavailable.

## 7. Deploy and verify

- [ ] 7.1 Add supervised loops, health endpoints and SIGTERM drain; verify dead loops fail health, database loss stops intake, single-webserver outages degrade capabilities without restart storms and shutdown preserves running work.
- [ ] 7.2 Add two-replica deployment templates, disruption protection, secrets, network restrictions and least privilege; verify GitOps rollout/overlap after the Dagster-side policy change, actual private connectivity and distinct bot/Dagster replica expectations without Kubernetes execution permissions.
- [ ] 7.3 Add metrics, independent alerts and deployment/rotation/upgrade/rollback runbooks; drill dependency failure, unknown-launch alarms and compatible rollback with the ledger retained.
- [ ] 7.4 Run DEV acceptance against every spec scenario, including two-replica races, queueing, both failure classes, single-webserver outage and production-representative database recovery; record measured latency/recovery and scenario evidence. Distinguish Dagster detection delay from bot observation latency.
- [ ] 7.5 Deploy PROD read-only, then enable verified operations through the release gate; record actual environment checks and canary outcomes. Reconcile specs/tasks, validate OpenSpec and archive only after implementation acceptance.
