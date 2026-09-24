# Tasks

All checkboxes track bot implementation, not the creation of these planning files. No implementation task is complete yet. Keep verification evidence with the implementing code or controlled environment records; never commit secrets.

## 1. Record environment contracts

- [ ] 1.1 Record installed Dagster/webserver/daemon/code-location/chart versions and launcher/executor behavior; verify a captured DEV schema supports required run, event and mutation documents.
- [ ] 1.2 Record webserver Service name, namespace, port, path, authentication and cluster DNS; verify a harmless GraphQL read from the actual FastAPI workload reaches the expected DEV code location.
- [ ] 1.3 Capture a redacted failure alert with publisher IDs and link targets; verify full UUID extraction or deliver a tested full-ID alert-publisher update.
- [ ] 1.4 Record automatic-retry configuration, lineage/tag propagation and reliable pending/exhausted observations; verify DEV fixtures cover the gap before a retry child exists.
- [ ] 1.5 Record approved workspace/channel policy, redaction samples and retention decisions; verify data-owner review and DEV/PROD identity separation are documented.
- [ ] 1.6 Record PostgreSQL commit/failover guarantees, secret management and independent alert route; verify a recovery plan covers potentially lossy promotion and restore.

## 2. Establish application and persistence foundations

- [ ] 2.1 Create the Python package and pinned dependencies with the single FastAPI entrypoint; verify startup/shutdown through lifespan without loading Dagster job code into the bot.
- [ ] 2.2 Implement validated settings and secret references with dispatch disabled by default; verify invalid/mixed DEV-PROD configuration fails startup without logging secrets.
- [ ] 2.3 Implement async database access, bounded pool acquisition and receipt capacity reservation; verify background saturation does not block the measured receipt deadline.
- [ ] 2.4 Add migrations for receipts, operations, confirmations, work, dispatch attempts, launch slot, bindings, observations, outbox, audit and config snapshots; verify migrations and unique/foreign-key/transition constraints against PostgreSQL.
- [ ] 2.5 Implement transactional repositories, safe work claims and lease-generation fencing; verify competing claims and stale updates cannot change another worker's state.
- [ ] 2.6 Document local DEV setup and migration order; verify a clean environment can reproduce initialization using the documented commands.

## 3. Implement private Dagster adapter

- [ ] 3.1 Implement a lifespan-owned async HTTP client and named read/submit-once paths; verify timeouts, redirect rejection, TLS/auth behavior and zero automatic mutation retries.
- [ ] 3.2 Implement schema/version checks and complete run/config/selection snapshots; verify every document and success/error union against the recorded DEV schema.
- [ ] 3.3 Implement bounded metadata/run reads and failure-event pagination; verify fixtures cover missing runs, partial pages and unsupported optional capabilities.
- [ ] 3.4 Implement whole-run re-execution and fresh-copy plan construction; verify complete op/asset/check/partition intent, null-versus-empty selection, configuration, lineage and current-code/image conflicts in DEV.
- [ ] 3.5 Implement retry-family and operation-tag observations; verify inherited tags distinguish descendants from duplicate primaries and pending retries block manual duplication.
- [ ] 3.6 Document supported releases/schema fingerprints and upgrades; verify a deliberate mismatch disables affected writes while compatible diagnostics remain usable.

## 4. Implement Slack receipt and thread interaction

- [ ] 4.1 Create DEV/PROD app configuration templates with required scopes and app_mention subscription; verify DEV mentions and button interactions arrive over Socket Mode without public ingress.
- [ ] 4.2 Implement the async SDK receiver and persist-before-ACK path; verify redelivery, uncertain commit, unsupported events and connection interruption against receipt deduplication.
- [ ] 4.3 Implement finite mention grammar, aliases and bare-mention menu; verify unknown prose, unmentioned replies, edits and self/bot commands cannot trigger a mutation.
- [ ] 4.4 Implement exact root retrieval, trusted publisher checks, UUID extraction and immutable binding; verify deep-thread mentions never select a neighboring/latest run or silently switch to a child.
- [ ] 4.5 Implement exceptional full-ID binding and same-thread operation lookup; verify suffix ambiguity, missing roots and cross-thread IDs produce specified refusal/confirmation behavior.
- [ ] 4.6 Build bot-owned thread cards and the command guide; verify logs/status, retry mode choice, confirmation and progress stay under the original alert without broadcast.

## 5. Implement authorization and preparation

- [ ] 5.1 Implement workspace/app/channel/environment/location/repository policy and paginated membership checks; verify disallowed contexts and unavailable membership evidence cannot read or dispatch.
- [ ] 5.2 Implement preparation with exact target/configuration/selection/policy previews; verify active sources, active backfills and incompatible/current-code conflicts cannot reach an executable proposal.
- [ ] 5.3 Implement requester-bound five-minute confirmations, opaque references, single-use nonces and preview invalidation; verify double clicks, forged values, other users, expiry and changed previews cannot launch.
- [ ] 5.4 Revalidate membership and current source/retry/configuration immediately before dispatch; verify removed members and stale prechecks deny or return to preparation.
- [ ] 5.5 Document permission/confirmation behavior; verify another channel member can create a separate request but cannot operate another member's proposal.

## 6. Implement durable dispatch and recovery

- [ ] 6.1 Implement atomic singleton launch admission with a busy response; verify concurrent confirmations admit one active family without queuing delayed production launches.
- [ ] 6.2 Persist a unique marker before one mutation invocation; verify unknown marker commits, lost mutation responses and generic post-creation errors never cause automatic resubmission.
- [ ] 6.3 Implement known-run tracking and retry-family reconciliation; verify queueing, cancellation pending and retry gaps retain the slot until terminal/no-pending evidence is conclusive.
- [ ] 6.4 Implement unknown-submission reconciliation by tag, job, selection/configuration and lineage; verify absent results retain uncertainty and duplicate primaries or persisted-but-unsubmitted runs require operator handling.
- [ ] 6.5 Implement controlled operator recovery and dispatch latch; verify the runbook isolates paused submitters, records evidence and requires a new confirmed operation for a new attempt.
- [ ] 6.6 Implement restore/lossy-promotion protection; verify state-loss simulation cannot reopen dispatch before ledger/run/slot reconciliation.
- [ ] 6.7 Document states, recovery ownership and remote retry races; verify fault tests cover death before/after the marker and late evidence from a stale worker.

## 7. Implement evidence, notifications and audit

- [ ] 7.1 Implement structured error/cause/stack extraction and redaction before rendering or result storage; verify credential/PII patterns, Slack markup injection, truncation and prohibited-content fixtures.
- [ ] 7.2 Implement transactional outbox, coalesced status, terminal messages and rate-limit handling; verify notification failure or lost post response cannot replay a launch.
- [ ] 7.3 Recheck destination authorization; verify channel removal/revocation stops delivery without rerouting logs to DMs or other channels.
- [ ] 7.4 Implement audit and restricted encrypted configuration snapshots; verify each mutation traces to authenticated actor, preview, decision and resulting run evidence without secret leakage.
- [ ] 7.5 Implement retention jobs and document workspace-copy limits; verify terminal data expires by policy while unresolved operation/slot/evidence is never purged.

## 8. Complete the named operation catalog

- [ ] 8.1 Implement graceful exact-run cancellation independent of the launch slot; verify cancellation-requested versus terminal state and active-descendant selection.
- [ ] 8.2 Implement bounded jobs/runs/failed/config/assets/partitions/automation metadata commands; verify the guide and capability inventory match enabled adapter contracts.
- [ ] 8.3 Implement confirmed schedule/sensor desired-state changes with per-target serialization; verify uncertain requests cannot be overtaken by opposite requests and sensor cursors remain unchanged.
- [ ] 8.4 Implement validated preset launch and single-asset/partition mappings; verify absent mappings, expanded multi-asset effects and unsupported ranges remain disabled/refused.
- [ ] 8.5 Enforce excluded operations and document indirect-run concurrency limits; verify arbitrary GraphQL/config, bulk retries/backfills, destructive controls and force termination are unavailable.

## 9. Deploy and operate the combined application

- [ ] 9.1 Implement supervised receiver/work/reconciliation/outbox lifecycles; verify a dead critical loop fails health and bounded asynchronous work leaves receipt handling responsive.
- [ ] 9.2 Implement readiness/liveness/dependency diagnostics; verify database failure explicitly stops durable intake and Dagster outage degrades capabilities without a restart storm.
- [ ] 9.3 Implement SIGTERM drain and recovery; verify replacement preserves unknown evidence, resumes tracking and never cancels a Dagster run.
- [ ] 9.4 Add two-replica templates, node spreading, disruption budget, rolling overlap, secrets and least privilege; verify DEV rollout and actual network boundaries without bot Kubernetes execution roles.
- [ ] 9.5 Add metrics/independent alerts for receipt latency, work age, connectivity, unknown dispatch, retry-pending and database health; verify injected failure reaches the external alarm route.
- [ ] 9.6 Document deployment, rotation, restore, upgrade and rollback runbooks; verify drills preserve state and leave existing runs under Dagster control.

## 10. End-to-end acceptance and completion

- [ ] 10.1 Run the DEV failure-thread journey for logs/status and both retry modes; verify all 28 carried-forward acceptance criteria and capability scenarios have test/evidence references.
- [ ] 10.2 Exercise two-replica concurrency, rolling overlap, dependency outages and database failover/restore; verify no automatic duplicate launch and measured latency/recovery objectives.
- [ ] 10.3 Deploy PROD read-only, then enable verified mutations through the release gate; verify actual Slack/Service/schema/data-policy settings and record canary outcomes.
- [ ] 10.4 Reconcile delivered behavior with specs and task evidence; verify strict OpenSpec validation and archive/sync only when the change is complete.

