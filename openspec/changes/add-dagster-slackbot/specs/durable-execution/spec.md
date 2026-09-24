## Purpose

Define durable acceptance, single automatic dispatch, global run-family admission, reconciliation, and recoverable Slack delivery without claiming distributed exactly-once execution.

## ADDED Requirements

### Requirement: Persist accepted work before transport acknowledgement
The system SHALL durably persist a deduplicated receipt and ready work before successfully acknowledging a supported Slack event. Confirmation acceptance SHALL atomically consume the valid confirmation and persist the authorized transition and work before acknowledgement. No Slack history or Dagster request SHALL be required on this bounded receipt path. An acknowledgement SHALL mean durable receipt only, not completed authorization or execution.

#### Scenario: Process exits immediately after acknowledgement
- **WHEN** another bot process recovers durable work after that exit
- **THEN** the accepted request or confirmation SHALL remain available for processing without relying on the old process's memory.

#### Scenario: Persistence is known to fail
- **WHEN** the durable receipt cannot be committed
- **THEN** the system MUST NOT acknowledge successful acceptance and SHALL expose a retryable transport failure and independent health alarm, without promising that finite Slack retries guarantee delivery.

#### Scenario: Receipt commit result is uncertain
- **WHEN** the database connection fails before commit outcome is known
- **THEN** recovery SHALL use the same event and operation identity to resolve the outcome rather than creating replacement work.

### Requirement: Stable event and interaction deduplication
Redelivered mention events SHALL resolve to the same durable receipt by workspace and event ID. Button deliveries and double clicks SHALL be controlled by their stable action/operation identity and single-use confirmation transition. Transport envelope IDs SHALL NOT replace event identity. Separately posted mentions SHALL remain distinct requests, even if their text is identical.

#### Scenario: Same mention is delivered to both replicas
- **WHEN** both deliveries carry the same workspace and event ID
- **THEN** exactly one logical receipt and operation SHALL be accepted.

#### Scenario: Same user intentionally posts a later identical command
- **WHEN** the new mention has a distinct event ID
- **THEN** the bot SHALL treat it as a new request requiring its own policy checks and any required confirmation.

### Requirement: Immutable approved intent and legal transitions
The durable operation SHALL retain requester, exact channel/thread/target, mode, approved preview and configuration identity, expiry, state version, and execution evidence. After confirmation, late mode changes or edited messages MUST NOT alter the approved intent. Before confirmation, a mode change SHALL invalidate the previous preview and confirmation reference. Execution and notification state SHALL remain independent.

#### Scenario: Old mode-selection button is clicked after confirmation
- **WHEN** that interaction targets an already confirmed operation
- **THEN** the system SHALL reject the change without modifying the saved mode or target.

#### Scenario: Run succeeds while Slack delivery is pending
- **WHEN** terminal execution evidence is recorded
- **THEN** the operation SHALL remain successful while notification retries proceed separately, and SHALL never return to launch-ready state.

### Requirement: Durable audit and traceable execution intent
The system SHALL maintain machine-readable legal transitions and append-only audit evidence identifying the authenticated Slack human separately from the backend identity. Accepted work, confirmation, admission, dispatch identity, outcome, operator decisions, and notifications SHALL be traceable by operation ID. Normal application operation MUST NOT silently edit audit history; audit records SHALL exclude raw secrets.

#### Scenario: Operator investigates an uncertain launch
- **WHEN** they inspect the operation audit
- **THEN** they SHALL be able to identify the original actor, exact approved target, submitting process, dispatch identity, and all recorded decisions without reading secret configuration.

### Requirement: One global active bot run family
Across all replicas and rollout overlap, the system SHALL admit at most one active bot-created primary run family in the configured production scope. Admission SHALL be atomic with ownership verification and shall follow current authorization, target, retry, and configuration checks. Another confirmed launch SHALL be rejected as busy rather than silently queued for an unreviewed future state. Reads and cancellation SHALL remain usable while the slot is occupied; automation controls SHALL use independent per-target serialization.

#### Scenario: Two users confirm different launches concurrently
- **WHEN** both operations compete for an empty slot
- **THEN** only one SHALL acquire admission and the other SHALL receive a busy result identifying the active operation safely.

#### Scenario: Active family is queued or canceling
- **WHEN** another launch is requested
- **THEN** the existing family SHALL retain the slot and the new operation MUST NOT dispatch.

### Requirement: Current pre-dispatch checks
Before creating dispatch authorization, the system SHALL verify fresh requester membership, configured policy, exact source state, compatible approved inputs, observable code identity, automatic-retry state, valid admission, and valid submitting-worker ownership. Initial maximum precheck age SHALL be five seconds; older checks SHALL be repeated. A changed preview SHALL require fresh confirmation and release unused admission safely.

#### Scenario: Prechecks become stale while waiting
- **WHEN** a worker reaches dispatch more than five seconds after its final checks
- **THEN** it SHALL revalidate before obtaining dispatch authorization.

#### Scenario: Source automatically recovers before dispatch
- **WHEN** final checks detect a successful automatic child or active/pending retry
- **THEN** the saved casual retry SHALL NOT dispatch and the system SHALL explain the updated source state.

### Requirement: One automatic dispatch attempt per operation
The system SHALL commit a unique immutable dispatch marker before any launch or re-execution call. Only the process that positively confirms creation of that marker under current operation, slot, and worker ownership SHALL be authorized to make one automatic invocation. No automatic transport, SDK, application decorator, or proxy retry SHALL resend that invocation. The system MUST NOT promise distributed exactly-once execution or ingestion-level data deduplication.

#### Scenario: Successful marker is followed by a normal launch response
- **WHEN** the authorized process makes its single invocation and receives a verified run ID
- **THEN** it SHALL record that run and begin tracking without issuing another launch.

#### Scenario: Process dies after marking but before sending
- **WHEN** recovery finds the committed dispatch marker without a recorded result
- **THEN** it SHALL reconcile the uncertain dispatch and MUST NOT grant a second automatic invocation, even if no run was actually created.

### Requirement: Uncertain dispatch-marker commit forbids sending
A worker SHALL require positive confirmation that the dispatch-marker transaction committed before calling Dagster. An ambiguous commit SHALL stop the external call and enter unresolved-dispatch recovery under the same operation identity. Inspecting an existing marker later MUST NOT become permission for another worker to dispatch.

#### Scenario: Database connection drops during marker commit
- **WHEN** the worker cannot establish whether its marker committed
- **THEN** it SHALL stop before calling Dagster and SHALL reconcile that operation rather than create a new marker or launch attempt.

### Requirement: Safe work recovery does not reauthorize launch
Reads, preparation, reconciliation, and notification work SHALL be recoverable after worker loss using bounded ownership claims. Reassigned safe work SHALL reject stale-owner state updates. Lease expiry, replica replacement, or rollout MUST NOT reassign a committed dispatch marker for another launch. The system SHALL retain submitting-process identity and accept late immutable response evidence without allowing it to overwrite newer operation state.

#### Scenario: A paused pre-dispatch worker resumes after reassignment
- **WHEN** its ownership generation is no longer current
- **THEN** it SHALL be unable to obtain dispatch authorization or modify authoritative state.

#### Scenario: Already-authorized submitter resumes late
- **WHEN** that process may still send its original request
- **THEN** recovery SHALL account for it explicitly and MUST NOT assume that database fencing has canceled a request to Dagster.

### Requirement: Classify uncertain mutation outcomes conservatively
Once dispatch is authorized, a timeout, connection loss, coroutine cancellation, shutdown, or unclassified server failure SHALL produce `SUBMISSION_UNKNOWN` unless a documented result proves no side effect. Generic GraphQL server errors SHALL NOT automatically be classified as pre-submission rejection. A returned or discovered created-but-unsubmitted run SHALL remain an incident to track, not an excuse to create a replacement.

#### Scenario: Dagster creates a run but the HTTP response is lost
- **WHEN** the bot times out
- **THEN** it SHALL retain admission, record uncertainty, notify the original thread, and reconcile without resending launch.

#### Scenario: Dagster provides a proven validation rejection
- **WHEN** the adapter contract establishes that the documented response occurred before any run creation
- **THEN** the system MAY record a terminal rejected operation and release unused admission with audit evidence.

### Requirement: Correlated run discovery without uniqueness claims
New runs SHALL carry verified operation, dispatch, and source provenance that excludes Slack bodies and credentials. Reconciliation SHALL query that provenance and verify candidate location, job, configuration/selection identity, and expected lineage. Tags SHALL be treated as discovery aids, never as an idempotency or uniqueness guarantee. Valid automatic descendants SHALL be distinguished from competing primary runs.

#### Scenario: Multiple tagged runs are one valid retry family
- **WHEN** one candidate is the expected primary and others are its actual automatic descendants
- **THEN** reconciliation SHALL attach the primary and track the family without declaring duplicates solely from tag count.

#### Scenario: Multiple competing primary runs match an operation
- **WHEN** reconciliation cannot identify one valid primary
- **THEN** the operation SHALL require operator review, keep admission blocked, and raise an anomaly alert.

### Requirement: Empty discovery never proves no launch
Unknown submission SHALL trigger bounded read polling with backoff. One valid primary SHALL be attached and tracked even if not started or queued. Empty results, delayed visibility, unavailable evidence, or prolonged uncertainty MUST NOT authorize a second launch. Unresolved cases SHALL enter `NEEDS_OPERATOR`, retain the slot, and raise an independent alert.

#### Scenario: Query finds no candidate after a timeout
- **WHEN** an original dispatch or paused submitter may still complete
- **THEN** the bot SHALL continue bounded reconciliation or escalate without releasing the slot or resubmitting.

### Requirement: Release admission only after conclusive family completion
The launch slot SHALL remain held through queueing, starting, running, cancellation pending, automatic-retry gaps, and submission uncertainty. It SHALL be released only after the primary and tracked descendants are terminal and verified Dagster state establishes no pending retry, or after an authorized audited incident resolution. Arbitrary quiet time SHALL NOT substitute for retry-state evidence; inability to observe it SHALL block launch readiness or require documented Dagster-side coordination.

#### Scenario: Parent is failed while retry status is unknown
- **WHEN** retry exhaustion cannot be established reliably
- **THEN** the system SHALL keep the slot occupied and escalate rather than release it after a timeout.

#### Scenario: Last descendant completes and no retry remains
- **WHEN** authoritative observations establish terminal family completion
- **THEN** the system SHALL record the final outcome, enqueue its notification, and release the slot atomically with appropriate audit evidence.

### Requirement: Controlled operator resolution of unknown dispatch
Only a controlled owner/operator recovery path SHALL resolve uncertain execution. It SHALL disable new dispatch, identify and terminate or isolate the original submitter as necessary, account for in-flight requests, inspect supported Dagster evidence, and record the decision and actor. Existing runs SHALL be attached only on positive identification. Any later execution SHALL use a new explicitly confirmed operation; the old operation MUST NOT be reset to ready-to-send.

#### Scenario: Operator believes no run was created
- **WHEN** a paused original process or in-flight request could still launch
- **THEN** recovery SHALL NOT release the slot or authorize another operation until that risk is resolved and supporting evidence is recorded.

#### Scenario: Ordinary channel member clicks recovery control
- **WHEN** an operation requires uncertain-launch incident resolution
- **THEN** normal Slack run permissions SHALL NOT authorize that resolution and the bot SHALL direct the case to the controlled operator path.

### Requirement: Durable same-thread notification outbox
Execution transitions SHALL create logical notification work durably with the state they report. Notifications SHALL target the saved authorized root thread, coalesce status updates, prioritize start/end results, and respect Slack rate limits. Failed or uncertain posts SHALL retry or reconcile notification work only. A possible duplicate message SHALL remain identifiable by operation ID and MUST NOT create duplicate execution.

#### Scenario: Slack is unavailable after Dagster launch
- **WHEN** execution succeeds but its message cannot be delivered
- **THEN** the system SHALL continue tracking and retry notification independently without launching again.

#### Scenario: Slack post succeeds but its response is lost
- **WHEN** notification outcome is uncertain
- **THEN** the system SHALL use the logical message identity and thread evidence where supported, tolerate distinguishable duplicate notification if necessary, and preserve one execution intent.

#### Scenario: Channel authorization is removed
- **WHEN** a pending delivery targets a removed or disallowed channel
- **THEN** the system SHALL stop data delivery there, retain operation/audit evidence, and alert operators without redirecting logs to an arbitrary DM or channel.

### Requirement: Recovery latch for database restore and lossy failover
The durable store SHALL provide a fenced single writer and a verified automatic-failover contract preserving acknowledged dispatch commits. After backup restoration or any promotion that may have lost committed ledger entries, dispatch SHALL remain latched disabled before resuming on the restored/promoted database. An operator SHALL reconcile retained ledger, tagged Dagster runs, active families, and slot ownership before explicitly reopening dispatch. Read and reconciliation functions SHOULD remain available where safe.

#### Scenario: Database is restored to an earlier point
- **WHEN** some previously acknowledged marker may be absent
- **THEN** the bot SHALL refuse new dispatch until controlled reconciliation establishes safe state and records the decision to reopen.

#### Scenario: Database service advertises HA without commit-preservation evidence
- **WHEN** a potentially lossy promotion can occur
- **THEN** automatic write resumption SHALL be prohibited unless the dispatch-disable latch and full recovery procedure apply.
