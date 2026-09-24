## Purpose

Operate a database-free FastAPI application beside Dagster, with conservative mutation admission, explicit restart recovery, and independently testable transports.

The [environment audit](../../environment-audit.md) records reported infrastructure evidence; it does not establish bot workload connectivity.

## ADDED Requirements

### Requirement: Dagster is the only persistent execution source
The bot SHALL use Dagster GraphQL for persisted run evidence and SHALL have no bot database, SQLite, Redis, persistent files/volume, object-store ledger, or Kubernetes coordination objects. State SHALL remain in bounded process memory. Platform logs SHALL contain redacted diagnostic events, not function as a work queue or recovery ledger. The system SHALL NOT promise durable command receipt, durable approvals, complete audit history, reliable notification delivery, or exactly-once execution.

#### Scenario: Process state disappears
- **WHEN** a restart loses commands, proposals, bindings, or notifications
- **THEN** the bot SHALL report unavailable session context, reject old controls, and recover only facts positively established through scoped Dagster queries; missing memory SHALL NOT mean no launch occurred.

### Requirement: Private Dagster connection
The application SHALL use the configured environment's release-qualified private Service `/graphql`, with audited HTTP port 80 and network-only access; it SHALL NOT accept endpoints or credentials from commands. Redirects SHALL be disabled and later HTTPS SHALL verify certificates. The bot SHALL require no runtime Teleport tunnel, public ingress, Kubernetes execution/log privileges, or access to Dagster storage. Platform owners SHALL confirm mesh enrollment and request-retry behavior before mutations; absent sidecars or namespace labels SHALL NOT prove mesh absence.

Separate bot namespaces SHALL require a Dagster-side ingress allowance combining bot namespace AND pod selectors in one peer, restricted to webserver pods on TCP 80. Existing legitimate traffic SHALL remain intact. Effective additive policies, DNS, Slack when enabled, and monitoring paths SHALL be verified. Mutation enablement SHALL require allowed and denied workload-path tests; a port-forward result SHALL NOT qualify.

#### Scenario: Cross-namespace remediation remains unverified
- **WHEN** the actual bot cannot query GraphQL or an unauthorized workload passes the new allowance
- **THEN** mutations SHALL remain disabled until scoped policy remediation and both workload tests pass in that environment.

### Requirement: Single active process
Deployment SHALL use one replica, one Uvicorn process, a `Recreate` strategy, and no HPA, overlapping rollout, or second instance sharing an installation identity. A fresh unpredictable `boot_id` SHALL identify each process session. `SLACK_ENABLED=false` SHALL permit startup and DEV testing without Slack credentials or connectivity. No broker, worker service, or MCP server SHALL be required.

#### Scenario: Deployment replaces a pod
- **WHEN** a new process starts
- **THEN** it SHALL begin with every mutation disarmed; replica count, `Recreate`, process death, and local locks SHALL NOT be treated as remote request fencing or proof that an old submitter cannot still affect Dagster.

### Requirement: Explicit commissioning and restart recovery
Every startup SHALL require operator commissioning before any launch, cancellation, or automation mutation. Configuration SHALL set only capability ceilings, never automatically arm mutations. An operator CLI SHALL contact the current process through a local Unix socket accessed through authenticated, audited platform execution. It SHALL bind the decision to `boot_id` and an evidence/change reference; a supplied actor name SHALL NOT authenticate the operator. Slack, DEV UI, remote HTTP, environment flags, and force-reset controls SHALL NOT unlock mutations.

Commissioning SHALL account for previous submitters and in-flight requests, perform complete bounded GraphQL scans, and validate discovered run families. First installation SHALL require recorded no-prior-submitter evidence. Restart recovery SHALL isolate old submitters where needed. Empty queries, elapsed time, or an operator acknowledgement without evidence SHALL NOT prove non-execution. Insufficient evidence SHALL retain read-only operation. A safely attached active family MAY permit confirmed cancellation after commissioning but SHALL block another launch.

#### Scenario: Restart finds no tagged run
- **WHEN** prior dispatch outcome cannot be established
- **THEN** the operator SHALL keep mutations disarmed despite an empty scan and resolve the previous submitter and possible remote effects before commissioning.

### Requirement: Volatile admission and immutable approval
Memory SHALL hold the bounded queue, deduplication map, bindings, five-minute proposals, operation states, family slot, definition locks, and notification buffer. Initial limits SHALL be 100 queued commands, 10,000 deduplication entries retained up to 24 hours, 100 proposals, and 1,000 resolved operations retained up to 24 hours. Active or unknown operations SHALL NOT be evicted; exhausted capacity SHALL reject new work. Accepted acknowledgement SHALL mean volatile receipt only.

Within a session, deduplication SHALL use transport request identity, not command text; Slack mentions SHALL use workspace/event identity rather than envelope identity. Single-use confirmation SHALL consume an immutable requester-bound intent tied to current boot, target, mode, approved inputs, and expiry. Changing intent SHALL require a new proposal. New requests after restart SHALL never recreate approvals automatically.

#### Scenario: Duplicate event or confirmation arrives
- **WHEN** its identity is retained or its proposal is consumed, expired, or from another boot
- **THEN** it SHALL not create a second authorized dispatch; at capacity the bot SHALL reject intake without promising later processing or transport redelivery.

### Requirement: Process-local mutation admission
One asynchronous lock SHALL serialize admission and protect one bot-created primary family slot per installation/environment. Immediately before invocation, authorization, target, approved inputs, code identity, and retry checks SHALL be no older than five seconds. Changed evidence SHALL invalidate the preview. Competing launches SHALL receive busy responses without deferred automatic execution; reads SHALL remain available. Cancellation and automation SHALL require their own approvals and definition/target serialization.

Checks SHALL inspect existing request matches, source attempts across both rerun and fresh-copy modes, and scoped active or pending families including retry gaps. Queries SHALL exhaust required cursor pagination within bounded budgets; truncated, inaccessible, or ambiguous evidence SHALL defer mutations. Neither the latest run, first page, nor a recent time window SHALL prove absence. Proven prior requests SHALL return their existing result; repeating a completed attempt SHALL require a new preview and explicit repeat acknowledgement. Active, pending, or unknown attempts SHALL block parallel retry.

#### Scenario: Two users confirm concurrently
- **WHEN** both target an apparently available family slot
- **THEN** only one SHALL pass the serialized fresh checks; the other SHALL receive the current busy or changed-evidence result.

### Requirement: Single-attempt mutation dispatch
Immediately before `submit_once`, the process SHALL recheck boot identity, the mutation gate, operation ownership and unconsumed dispatch authority under synchronization shared with disarming. That final check, consuming authority in memory and starting invocation SHALL have no intervening asynchronous wait. Closing the gate SHALL prevent subsequent invocations, without claiming to retract requests already started. Each confirmation SHALL authorize at most one mutation POST; client, SDK, application, proxy, and mesh automatic mutation retries SHALL be disabled. Reads MAY retry within bounds. Dagster 1.13.1 provides no launch idempotency key. This guarantee SHALL be explicitly limited to the live process session and SHALL NOT imply server uniqueness or distributed locking.

After invocation begins, timeout, disconnect, cancellation, shutdown, or generic GraphQL/server failure SHALL yield `SUBMISSION_UNKNOWN` unless the adapter proves no side effect. Unknown outcome SHALL disable all new mutations while safe reads and observation continue. Cancellation and automation changes SHALL obey the same rule; an opposite action SHALL NOT overtake an ambiguous earlier one.

#### Scenario: Admission closes during fresh checks
- **WHEN** an approved operation awaits GraphQL checks and an operator disarms the process or another operation becomes uncertain
- **THEN** the final synchronized check SHALL reject dispatch without issuing a mutation POST.

#### Scenario: Response is lost after launch
- **WHEN** Dagster may have created or submitted a run
- **THEN** the bot SHALL neither resend nor free admission; it SHALL reconcile or require operator evidence, without an uncertainty expiry.

### Requirement: Positive GraphQL reconciliation
Created runs SHALL include stable installation/environment identifiers, unique operation ID, deterministic transport request ID, source ID when applicable, mode, protocol version, and safe intent fingerprint. The adapter SHALL verify tag support in its pinned mutation documents. Tags SHALL exclude credentials, configuration bodies, messages, and unapproved clear Slack identifiers; opaque keyed context identifiers MAY be used. Installation identity SHALL persist in deployment configuration and SHALL NOT rotate to evade history.

Tags SHALL support discovery only. Reconciliation SHALL validate scope, job, inputs/selections, and lineage before attaching a primary and actual automatic descendants. Conflicting primaries, created-but-unsubmitted incidents, or insufficient evidence SHALL retain the block. Positive evidence SHALL resolve only what it establishes; observing a current automation/run state SHALL NOT exclude a late earlier mutation. Unknown mutation blocks SHALL clear only after positive reconciliation accounts for remote effects or the controlled operator process supplies sufficient evidence. Restart commissioning SHALL remain mandatory independently.

#### Scenario: Tagged results include descendants
- **WHEN** lineage proves a single primary family
- **THEN** the bot SHALL track that family without treating tag count as proof of duplicates or replaying any request.

### Requirement: Conclusive family completion
The slot SHALL remain occupied through queueing, execution, cancellation, automatic retry gaps, and uncertainty. Release SHALL require terminal primary/descendants and authoritative evidence of no pending retry, or sufficient controlled operator incident resolution. Unavailable retry observability SHALL block new launches. Queued runs SHALL retain coordinator-policy tags and SHALL NOT be replaced merely because execution has not started.

#### Scenario: Parent is failed but retry is pending
- **WHEN** a child has not yet appeared or exhaustion cannot be established
- **THEN** the family slot SHALL remain held; quiet time SHALL NOT qualify as completion.

### Requirement: Best-effort transport delivery
Notification retries SHALL affect delivery only, preserve known operation identity, and use bounded memory with an initial ten-attempt/fifteen-minute budget. Restart MAY lose delivery work and duplicate messages MAY occur. Authorization revocation SHALL stop delivery without rerouting evidence. Recovery SHALL NOT infer Slack destinations from untrusted run tags; lost thread context SHALL require a fresh authorized user request. Status SHALL distinguish recovered Dagster facts from unavailable command history.

#### Scenario: Slack fails after a run starts
- **WHEN** notification delivery remains unsuccessful
- **THEN** observation SHALL continue while the process lives, and no notification retry SHALL relaunch or change execution outcome.

### Requirement: Supervised lifecycle and dependency recovery
Startup SHALL validate configuration, initialize bounded asynchronous clients, establish read capabilities, and supervise critical loops before intake. Shutdown SHALL stop admission, drain bounded work, close clients, and leave Dagster runs running; it SHALL NOT claim to persist memory or revoke remote requests. Unexpected critical-loop exit SHALL fail health and terminate; dependency outages SHALL back off without restart storms. Dagster storage recovery, restore, or changed history evidence SHALL disarm mutations pending rescan and operator assessment.

Liveness SHALL measure process/loop health without downstream calls. Readiness SHALL reflect initialization and enabled transport requirements separately from mutation readiness. Dagster outages SHALL degrade affected capabilities and identify the audited single webserver dependency. Disabled Slack SHALL not fail application readiness. Monitoring SHALL expose commissioning state, unknown operations, capacity pressure, dropped delivery, dependency health, and stale observations through an independent platform path.

#### Scenario: Process restarts during active work
- **WHEN** replacement becomes ready for reads
- **THEN** health SHALL still show mutations disarmed and lost session delivery state; readiness SHALL not imply recovered approvals or automatic resumption.

### Requirement: Isolated and admission-compliant deployment
DEV and PROD SHALL separate identities, credentials, endpoints, and allowed scopes. Kyverno-required compliance labels and an approved registry SHALL be incorporated into release manifests; successful admission, image pull, startup, and actual workload connectivity SHALL each be verified. Deployment SHALL use restricted secrets, non-root privileges, dropped capabilities, a read-only image filesystem where practical, no host mounts, and no Kubernetes token unless required for workload authentication. Failed audit probes SHALL NOT satisfy workload testing.

Release gates SHALL exercise duplicates, concurrent confirmations, both retry modes, queued families, response loss, restart commissioning, stale controls, automatic retry gaps, and dependency outages. PROD SHALL begin read-only and enable only tested capabilities. The DEV replica-count discrepancy SHALL be reconciled before capacity assumptions use chart values. Rollback SHALL start disarmed and account for prior submitters just like any replacement.

#### Scenario: Approved probe image cannot pull
- **WHEN** admission succeeds but the workload does not start
- **THEN** the deployment gate SHALL remain open until the actual bot image runs and passes connectivity checks.

### Requirement: Honest operational objectives
Under healthy dependencies, the application SHALL measure acknowledgement p99 below two seconds within Slack's three-second deadline, status/error p95 below five/ten seconds, active polling every fifteen seconds with jitter, live-session terminal notification p95 within thirty seconds, and uncertainty alerting within sixty seconds. These SHALL be measured targets, not durable-delivery guarantees. Restart-to-mutation recovery SHALL have no automatic deadline because commissioning requires evidence.

Documentation SHALL state single-bot downtime, the shared cluster failure domain, and audited single-replica Dagster dependencies; it SHALL NOT claim HA or the former durable-work recovery guarantee. Human fallback SHALL retain full run IDs and the approved Teleport access convention. Dagster's separate 120-second monitoring cycle and 600-second startup timeout SHALL NOT be confused with bot response targets.

#### Scenario: Pod or cluster fails
- **WHEN** availability and recovery are reported
- **THEN** the outage and lost session work SHALL be visible, and healthy read service after restart SHALL not be reported as restored mutation availability.
