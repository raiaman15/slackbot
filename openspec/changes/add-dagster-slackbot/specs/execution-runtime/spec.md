## Purpose

Keep Slack operations durable, prevent automatic duplicate launches, and operate FastAPI safely beside Dagster in the same Kubernetes cluster.

The supplied [environment audit](../../environment-audit.md) records the deployment baseline and outstanding platform evidence.

## ADDED Requirements

### Requirement: Private Dagster connection
FastAPI SHALL call the existing Dagster webserver `/graphql` through the configured environment's release-qualified private Kubernetes Service, using the audited HTTP port 80 baseline. Both audited endpoints have no authentication proxy or application credentials and no ingress; network admission is the access boundary. Slack input SHALL NOT select endpoints or credentials. Redirects SHALL be disabled; any later HTTPS configuration SHALL verify certificates. The absence of observed sidecars SHALL NOT establish mesh absence or mTLS; platform owners SHALL resolve ambient-mesh enrollment and retry behavior before dispatch enablement. The bot SHALL require no human tunnel, public Dagster ingress, Kubernetes mutation/log privileges, local execution, or shared Dagster storage. Dagster SHALL retain execution and retry responsibilities.

The bot SHALL use a separate namespace per environment. Existing Dagster policies select all namespace pods and allow only same-namespace ingress, so deployment SHALL depend on a change owned by the private Dagster deployment repository admitting only bot namespace AND pod selectors in one peer, to selected webserver pods on TCP 80. Existing allowed traffic SHALL remain intact. Deployment SHALL verify effective additive policies and required DNS, database, Slack, and monitoring paths. Tests SHALL run from actual bot and unauthorized workloads; successful audit port-forward queries SHALL NOT prove cross-namespace reachability.

#### Scenario: Applications occupy different namespaces
- **WHEN** FastAPI queries or launches a run
- **THEN** its actual workload identity SHALL use the verified private endpoint without Teleport or port forwarding; cluster placement alone SHALL NOT imply HTTP authentication.

#### Scenario: Network remediation has not passed workload tests
- **WHEN** the bot cannot reach the webserver or unrelated namespaces/pods can use the new allowance
- **THEN** dispatch SHALL remain disabled until the scoped Dagster policy change passes both allowed-path and denied-path tests in that environment.

### Requirement: One combined application deployment
Production SHALL use one FastAPI deployment with two identical replicas, each running Socket Mode intake and bounded, supervised asynchronous work in one process. Coordination SHALL use durable PostgreSQL and remain correct during rollout overlap. Socket Mode SHALL require outbound connectivity without public bot ingress. Optional HTTP delivery SHALL verify Slack signatures and timestamps and share the same policies. No separate broker, worker service, MCP server, or coordination service SHALL be required.

#### Scenario: Replica restarts during rollout
- **WHEN** old and new pods overlap or one stops
- **THEN** surviving replicas SHALL continue intake and recoverable work using shared durable state, without relying on stopped-process memory.

### Requirement: Durable receipt before acknowledgement
The system SHALL commit a deduplicated receipt and ready work before acknowledging supported events. Confirmation acceptance SHALL atomically consume the single-use confirmation and persist its authorized transition. This path SHALL exclude Slack-history and Dagster calls, reserve database capacity, and use an initial 500 ms transaction deadline. Mention identity SHALL be workspace plus event ID, not transport envelope ID; interactions SHALL use stable action/operation identity. Acknowledgement SHALL mean durable receipt only.

#### Scenario: Duplicate deliveries or double clicks arrive
- **WHEN** multiple replicas receive the same logical event or confirmation
- **THEN** only one receipt or confirmation transition SHALL succeed; separately posted identical mentions SHALL remain separate requests.

#### Scenario: Receipt persistence fails or has uncertain outcome
- **WHEN** successful commit is unconfirmed
- **THEN** intake SHALL not acknowledge acceptance, SHALL resolve uncertainty using the same identity, and SHALL raise independent health signals without promising eventual Slack redelivery.

### Requirement: Immutable intent and audit
Operations SHALL retain requester, exact target/thread, mode, approved preview/configuration identity, expiry, state version, ownership, and execution evidence. Changing mode before approval SHALL invalidate prior confirmation; after approval, edits SHALL NOT change intent. Legal state transitions and append-only audit SHALL identify the Slack human separately from backend identity and trace acceptance, approval, admission, dispatch, operator decisions, and notifications without raw secrets. Execution and notification state SHALL remain independent.

Configuration SHALL prefer faithful references; required snapshots SHALL be encrypted through company key management with restricted access and key rotation. Missing inputs or decryption failure SHALL block execution rather than substitute defaults.

#### Scenario: Old button targets confirmed work
- **WHEN** it requests a different mode or target
- **THEN** the system SHALL reject the change and preserve the approved intent and audit history.

### Requirement: Atomic global admission
Across replicas, at most one bot-created primary run family SHALL occupy the production launch slot. Admission SHALL atomically verify ownership after fresh authorization, source state, approved inputs, observable code identity, and retry checks. Pre-dispatch checks older than five seconds SHALL be repeated. Changed previews SHALL require new confirmation. Competing launches SHALL receive busy results rather than wait for unreviewed future execution; reads and cancellation SHALL remain available, with automation controls serialized independently per target.

#### Scenario: Two users confirm concurrently
- **WHEN** both request an empty slot
- **THEN** one SHALL acquire it and the other SHALL receive a busy result identifying the active operation safely.

#### Scenario: Automatic retry changes the source
- **WHEN** final checks find recovery or an active/pending retry
- **THEN** the previously approved casual retry SHALL NOT dispatch and the bot SHALL explain the updated state.

### Requirement: One automatic dispatch attempt
Before any launch/re-execution call, the system SHALL commit one immutable dispatch marker under current operation, slot, and worker ownership. Only its creator, after positively confirming commit, SHALL be authorized for one automatic invocation. Transport, SDK, application, mesh, and proxy launch retries SHALL be disabled. Reads MAY use bounded retries. Marker existence SHALL never authorize another process. The system SHALL NOT claim distributed exactly-once execution or ingestion deduplication.

The audited Dagster 1.13.1 launch mutations provide no idempotency key; correlation tags SHALL NOT be treated as a server-enforced substitute.

#### Scenario: Marker commit is ambiguous
- **WHEN** its creator cannot confirm success
- **THEN** it SHALL not call Dagster and SHALL reconcile the same operation.

#### Scenario: Process dies after marking
- **WHEN** recovery finds no recorded dispatch result
- **THEN** it SHALL reconcile without sending again, even if the original process died before making the call.

### Requirement: Ownership and uncertain outcomes
Safe work SHALL use bounded claims and reject stale-owner updates. Lease expiry SHALL NOT reauthorize marked dispatch or cancel a remote request. Submitting-process identity and late immutable response evidence SHALL be retained. After authorization, timeout, disconnect, cancellation, shutdown, or unclassified server failure SHALL become `SUBMISSION_UNKNOWN` unless the adapter proves no side effect. Created-but-unsubmitted runs SHALL remain tracked incidents rather than replacement opportunities.

#### Scenario: Paused worker resumes
- **WHEN** its claim was reassigned
- **THEN** it SHALL not acquire new dispatch authority or overwrite state; recovery SHALL still account for any previously authorized request it could send.

#### Scenario: Launch response is lost
- **WHEN** Dagster may have created the run
- **THEN** the bot SHALL retain admission and reconcile without resubmission; generic GraphQL errors SHALL NOT prove rejection before creation.

### Requirement: Evidence-based reconciliation and recovery
New runs SHALL carry operation, dispatch, and source provenance without Slack bodies or credentials. Reconciliation SHALL validate job, location, approved inputs/selections, and lineage. Tags SHALL aid discovery, not guarantee uniqueness. Bounded polling SHALL attach one verified primary and its real automatic descendants. Empty results SHALL NOT prove absence. Competing primaries or unresolved evidence SHALL enter `NEEDS_OPERATOR`, retain admission, and raise independent alerts.

#### Scenario: Tagged candidates include automatic descendants
- **WHEN** lineage proves one primary retry family
- **THEN** reconciliation SHALL track that family without treating tag count alone as duplicate execution.

#### Scenario: Operator resolves an unknown dispatch
- **WHEN** ordinary reconciliation cannot establish the outcome
- **THEN** a controlled operator path SHALL disable dispatch, terminate or isolate the original submitter as needed, account for in-flight requests, and audit evidence and actor before releasing admission; any later launch SHALL require a new confirmed operation, never reset the old one.

### Requirement: Conclusive family completion
Admission SHALL remain held through queueing, starting, execution, cancellation, retry gaps, and uncertainty. Release SHALL require terminal primary/descendants and authoritative evidence of no pending retry, or controlled audited incident resolution. Quiet periods SHALL NOT substitute for retry evidence; unavailable retry observability SHALL block launch readiness unless documented Dagster-side coordination resolves it.

#### Scenario: Parent failed but retry state is unknown
- **WHEN** exhaustion cannot be established
- **THEN** the slot SHALL remain occupied and the condition SHALL escalate.

#### Scenario: Concurrency tags keep the run queued
- **WHEN** Dagster creates a run that waits under preserved coordinator limits
- **THEN** the bot SHALL report queued state and retain its family slot without removing tags or launching a replacement.

#### Scenario: Family conclusively completes
- **WHEN** all completion conditions hold
- **THEN** final outcome, logical notification, audit evidence, and slot release SHALL be recorded atomically.

### Requirement: Independent durable notifications
State transitions SHALL durably create logical notifications for the saved authorized root thread. Delivery SHALL respect rate limits, coalesce progress, and prioritize start/end results. Failed or ambiguous Slack calls SHALL retry or reconcile notification work only; distinguishable duplicate messages MAY occur without duplicate execution. Removed channel authorization SHALL stop delivery there and alert operators without redirecting evidence elsewhere.

#### Scenario: Slack fails after launch
- **WHEN** a notification cannot be confirmed
- **THEN** execution tracking SHALL continue and notification recovery SHALL preserve operation identity without launching again or changing successful execution state.

### Requirement: Database recovery latch
The durable store SHALL have a fenced single writer and verified failover semantics preserving acknowledged dispatch commits. Backup restore or potentially lossy promotion SHALL latch dispatch disabled before writes resume. A controlled operator SHALL reconcile ledger, tagged runs, active families, and slot ownership before auditing explicit reopening. Safe reads and reconciliation SHOULD remain available.

The bot SHALL have its own database or schema and restricted role, never use Dagster's schema. The initial platform choice SHALL be a dedicated bot schema and role on the existing managed database platform, subject to owner-confirmed isolation and durability. The audit reports single-pod DEV storage and shared RDS-style PROD storage; Aurora is inferred, not confirmed. Platform owners SHALL verify engine, writer fencing, and acknowledged-commit durability. Production failover qualification SHALL use a representative managed test database; single-pod DEV restart/restore drills SHALL NOT qualify PROD failover.

#### Scenario: Restored database lacks a committed marker
- **WHEN** a previous dispatch may still exist in Dagster
- **THEN** missing ledger evidence SHALL NOT permit fresh dispatch before controlled recovery; a generic HA claim SHALL NOT waive this requirement.

#### Scenario: Only single-pod database drills have passed
- **WHEN** production failover behavior remains unverified
- **THEN** production dispatch SHALL remain gated pending representative failover evidence and platform-owner confirmation.

### Requirement: Supervised lifecycle and bounded work
Startup SHALL validate configuration and migrated schema, initialize asynchronous clients, verify Slack identity, recover work, and start supervised loops before intake. Migrations SHALL run once per deployment. Work, database acquisition/locks, calls, and response sizes SHALL be bounded; active/unknown reconciliation SHALL retain capacity. Transactions SHALL NOT span external calls. Shutdown SHALL stop intake/claims, drain bounded work, preserve dispatch evidence, and close clients last without canceling Dagster runs.

#### Scenario: Critical loop fails or database becomes unwritable
- **WHEN** intake can no longer process durably
- **THEN** it SHALL pause/disconnect explicitly; unexpected loop exit SHALL fail health and terminate, while dependency outages SHALL back off without restart storms.

#### Scenario: Health is evaluated
- **WHEN** probes run
- **THEN** liveness SHALL check process/loop supervision without downstream calls; readiness SHALL require initialization, writable storage, critical loops, and Slack connectivity, while Dagster outages SHALL degrade affected capabilities and identify the single-replica webserver dependency explicitly.

### Requirement: Isolated deployment and capability gates
DEV and PROD SHALL separate Slack apps/tokens, channels, endpoints, identities, and durable configuration. Deployment SHALL use restricted secrets, non-root privileges, dropped Linux capabilities, no host mounts, a read-only image filesystem where practical, and no Kubernetes token unless required by workload authentication. Dispatch SHALL default disabled until the actual workload verifies private access, expected location/schema, effective network restrictions, and disabled launch retries. DEV SHALL exercise thread interaction, both execution modes, selections, automatic retries, crashes, and failover. PROD SHALL begin read-only and enable only validated capabilities.

Environment configuration SHALL record the live Dagster 1.13.1 identities and effective retry defaults from the audit; preview namespaces SHALL resolve their own Service names. Platform owners SHALL reconcile the reported DEV chart declaration of two webserver replicas against the observed one before capacity or availability assumptions rely on it.

#### Scenario: Rollback occurs with active work
- **WHEN** a release is rolled back
- **THEN** new dispatch SHALL stop while ledger, admission, observation, and notifications remain recoverable and existing Dagster runs continue.

### Requirement: Honest operational objectives
Production SHALL spread replicas across nodes and preserve one during voluntary disruption. Independent monitoring SHALL cover Slack connectivity, storage/schema failure, unknown dispatch, stuck retries, and stale observations. Documentation SHALL acknowledge the shared cluster failure domain and retain human Dagster/Teleport fallback with usable links or full run IDs. Under normal dependency health, measured latency/recovery objectives SHALL be acknowledgement p99 below two seconds and Slack's three-second deadline; status/error p95 below five/ten seconds; bot durable-work recovery after pod replacement below 60 seconds; active polling every 15 seconds with jitter; terminal notification p95 within 30 seconds; and uncertainty alerting within 60 seconds. The monthly end-to-end availability objective SHALL be 99.9%, including dependency failures and synthetic read probes.

The audited webserver, daemon, and user-code deployments each have one replica in both environments. Two bot replicas SHALL NOT be presented as making those dependencies highly available; readiness reporting and outage drills SHALL account for the webserver's single point of failure. Bot recovery targets SHALL NOT bound Dagster's separate 120-second monitoring cycle, 600-second startup timeout, or uncertain-submission resolution.

#### Scenario: Cluster outage affects both replicas
- **WHEN** command availability is measured
- **THEN** independent monitoring SHALL detect the outage and include it in the metric; objectives SHALL NOT imply cross-cluster resilience or a guaranteed uncertainty-resolution deadline.
