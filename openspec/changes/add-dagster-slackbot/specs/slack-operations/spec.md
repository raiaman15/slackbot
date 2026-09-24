## Purpose

Operate Dagster from authenticated mentions in existing failure threads, with exact targets, explicit approvals, bounded evidence, and recoverable delivery.

## ADDED Requirements

### Requirement: Deterministic human commands

The system SHALL accept supported new human messages containing its exact mention, derive the requester from authenticated message identity, and ignore bots, self-events, missing human identities, edits/deletions, unsupported subtypes, hidden/system messages, and unmentioned replies. Trusted bot-authored alerts remain eligible roots. Phase one SHALL use bounded, case-insensitive documented grammar without an LLM; unknown flags, oversized input, and unsupported prose SHALL return usage without execution. Bare mentions SHALL show actions. Aliases, if enabled, SHALL be documented; plain assent SHALL NOT confirm actions.

#### Scenario: Human requests logs
- **WHEN** a human mentions the bot beneath a trusted integration alert
- **THEN** the request belongs to that human, independently of installation identity or claims in message text.

#### Scenario: Unsupported invocation
- **WHEN** a message is edited, unmentioned, bot-authored, or outside the grammar
- **THEN** it creates no action; unsupported human commands receive bounded usage.

### Requirement: Exact trusted failure identity

Failure-specific `logs`, `status`, `config`, and `retry` SHALL require an existing root distinct from the command timestamp. Workspace, channel, and timestamps SHALL come from authenticated Slack context; timestamps remain strings. The system SHALL retrieve that exact root, verify configured publisher and alert shape, and extract a full run ID from trusted fields or link patterns without fetching arbitrary URLs or ingesting whole conversations. It SHALL verify the ID against configured environment, code location, repository, job, and supplied partition evidence. Missing, inaccessible, untrusted, ambiguous, or out-of-scope roots SHALL fail with an explanation; suffix matches and latest-run selection SHALL NOT establish identity.

#### Scenario: Root has a short link label
- **WHEN** the trusted root's link contains a full run ID
- **THEN** the system verifies that ID without requiring the user to copy it.

#### Scenario: Identity is incomplete
- **WHEN** only a suffix or neighboring message is available
- **THEN** the system refuses automatic binding, even if one search candidate appears.

### Requirement: Stable binding and explicit fallback

The workspace/channel/root binding SHALL retain the original failure. Children, edited alerts, listings, and status updates SHALL NOT retarget it. An explicit eligible related-run selection SHALL belong to its operation only. For trusted alerts lacking a full ID, `bind <full-run-id>` SHALL verify the run and available evidence, preview job, time, partition, and manual provenance, and require requester confirmation. Conflicts SHALL be surfaced without overwriting verified bindings. Revalidation SHALL detect changed root evidence.

#### Scenario: Recovery creates a child
- **WHEN** logs are requested afterward without an explicit target
- **THEN** they describe the original failure; status may separately identify descendants.

#### Scenario: Manual fallback conflicts
- **WHEN** the supplied run contradicts alert evidence or a verified binding
- **THEN** the system refuses replacement rather than silently accepting it.

### Requirement: Thread-scoped interaction and discovery

Evidence, menus, approvals, receipts, progress, and outcomes SHALL stay beneath the validated root without channel broadcast. The bot SHALL update only its saved messages, never the publisher's alert. Controls SHALL match saved channel, root, and card. `help` and `capabilities` SHALL expose the enabled operation catalog and sanitized unavailability reasons. Listings SHALL offer bounded explicit pagination without rebinding; optional presets/mappings SHALL NOT block logs, status, or retry. Top-level informational commands MAY reply beneath themselves; top-level failure commands SHALL request a failure-thread reply.

#### Scenario: Copied control or top-level retry
- **WHEN** a control has a different context or retry lacks an existing failure root
- **THEN** the system refuses execution and explains the required context.

### Requirement: Channel membership authorizes access

Before Dagster access, the system SHALL verify authenticated transport, configured app/workspace, allowlisted invoking channel, bot membership, human membership, enabled capability, and configured target scope. Human membership SHALL use complete pagination. All verified members SHALL have equal ordinary permissions without a separate Teleport login, including newly joined members of publicly joinable allowed channels. Company-controlled, non-Slack-Connect channels SHALL be the default. Invitation alone SHALL NOT authorize a channel. Endpoint/environment overrides SHALL be rejected; future interfaces SHALL use the same policy boundary.

#### Scenario: Membership is on a later page
- **WHEN** the complete result contains the requester
- **THEN** the system recognizes membership rather than checking only the first page.

#### Scenario: Context is unauthorized
- **WHEN** the channel is unapproved/shared under default policy, or the requester is absent
- **THEN** the system refuses before reading or changing Dagster data.

### Requirement: Fresh authorization and human attribution

Reads SHALL use membership evidence at most 30 seconds old. Mutations SHALL freshly check requester membership and policy immediately before execution; waits exceeding five seconds SHALL trigger refreshed authorization and target-state prechecks. Incomplete/unavailable checks SHALL deny or defer, and observed revocation SHALL invalidate approval. This is not an atomic guarantee against later membership changes. Protected audit records SHALL retain authenticated human, workspace, channel, root, operation, target, decisions, and transitions separately from the shared backend service identity, excluding secrets and raw errors.

#### Scenario: Requester leaves before dispatch
- **WHEN** final membership verification fails
- **THEN** the system invalidates approval and performs no mutation.

### Requirement: Exact requester-approved previews

Every mutation and manual binding SHALL require the original requester's explicit approval; reads require none. Other members SHALL NOT confirm, change mode, or cancel that proposal, but MAY create their own. Retry SHALL offer whole-run re-execution and a fresh copy, explaining lineage. Previews SHALL show exact target/effect, source, job, environment, partition/selection, current-code policy, verified retry policy, and incompatibilities. Schedule/sensor starts SHALL explain future runs outside the bot slot. Material changes to target, source state, configuration, selection, policy, or observable code SHALL require renewed preview and approval.

#### Scenario: Another member uses a control
- **WHEN** someone other than the requester clicks it
- **THEN** they receive private denial without altering the card or consuming approval.

#### Scenario: Preparation changes
- **WHEN** mode or material action details change
- **THEN** a new preview invalidates previous controls; confirmed actions remain immutable.

### Requirement: Expiring single-use controls

Confirmation SHALL expire five minutes after preview preparation and bind operation, requester, workspace, channel, root, saved bot card, mode, immutable request/preview identities, and a single-use interaction reference. All bindings and current state SHALL match before atomic consumption. Controls SHALL carry opaque references only; supplied actors, roles, targets, or configuration SHALL NOT override saved state.

#### Scenario: Duplicate or stale interaction
- **WHEN** confirmation is duplicated, expired, superseded, or altered
- **THEN** at most one valid transition occurs, without additional execution authority, even if the card still looks active.

### Requirement: Recoverable proposals and explicit cancellation

`operation <id>` SHALL be available to authorized users only in its saved channel/root; its requester MAY recover an unexpired proposal there. `Cancel request` SHALL cancel only an unsubmitted proposal. Message deletion SHALL NOT cancel execution. Dagster cancellation SHALL require exact-run selection and confirmation, including selection among multiple eligible descendants. Ordinary controls SHALL NOT reset uncertain dispatches; replacement after operator recovery SHALL require a new confirmed operation.

#### Scenario: Proposal disappears after confirmation
- **WHEN** its Slack message is deleted
- **THEN** the saved action continues; only an explicit authorized run-cancellation request can stop the run.

### Requirement: Bounded Dagster evidence

Logs SHALL use exact-run structured run/step failures, supported cause chains, ordered timestamps, and stack frames; absent exceptions or unsupported diagnoses SHALL be stated. Responses SHALL identify job, environment, full run ID, failed step, exception, retry state, requester, operation, and redaction/truncation. Status SHALL distinguish original and related runs; config SHALL show sanitized logical inputs only. Retrieval SHALL bound pages, events, bytes, depth, and runtime: initially 100 events/page, 1 MiB/request, a ten-second response target, 30 rendered frames, and approximately 6,000 total characters within individual Slack block limits. Bounds SHALL produce partial-result notices and explicit pagination or human access.

#### Scenario: Evidence exceeds limits
- **WHEN** any retrieval or rendering bound is reached
- **THEN** the response marks the excerpt as partial and offers continuation without silently dropping evidence.

### Requirement: Redaction before disclosure

Evidence SHALL remain Dagster-only, without external investigation or LLM disclosure. Before rendering, logging, caching, or audit export, the system SHALL redact credentials, tokens, passwords, authorization headers, secret URI components, configured sensitive keys, and owner-approved sensitive patterns. It SHALL exclude exception locals, raw connection strings, and secret-bearing request/response details; previews SHALL withhold secrets. Evidence markup SHALL be inert. Uncertain/prohibited bodies SHALL be withheld with safe class/metadata and protected human access. Full-log uploads and DM rerouting SHALL be excluded. Employee links SHALL use a valid configured convention; otherwise provide full UUID and established port-forward instructions, never fabricated public/internal Service links. Production enablement SHALL require owner review of representative errors and disclosure policy.

#### Scenario: Error contains sensitive or active markup
- **WHEN** evidence contains secrets, mention/link syntax, or unsafe content
- **THEN** the system sanitizes and escapes it before disclosure, withholding bodies it cannot safely render.

### Requirement: Notification delivery cannot change execution

Required notifications SHALL be durably recorded with their operation transitions and stable logical identities. Delivery SHALL remain independent of execution: failures SHALL NOT replay launches, reset dispatchability, stop observation, or change outcomes. Inspection SHALL distinguish transport acknowledgement, durable acceptance, creation, queued/running execution, cancellation requested, terminal outcome, uncertain submission, and pending delivery. Acceptance SHALL follow durable receipt; creation SHALL NOT imply running/success. Ambiguous Slack posts SHALL reconcile the saved thread where practical; identifiable duplicates are tolerated without claiming exactly-once delivery.

#### Scenario: Slack loses a creation response
- **WHEN** a run exists but its notification is missing or ambiguous
- **THEN** the system tracks that run and retries only delivery using its operation identity.

### Requirement: Rate-aware delivery and destination revocation

Progress SHALL coalesce to at most one update per 15 seconds per operation by default, prioritize start/end, respect `Retry-After`, and avoid streaming traces. Terminal replies SHALL include primary/relevant retry IDs and target p95 delivery within 30 seconds of Dagster-visible completion under healthy dependencies. Before disclosure the system SHALL recheck destination policy. Disallowed channels or lost bot access SHALL stop new dispatch/disclosure, preserve monitoring/results/audit, and signal independent operator monitoring without rerouting, replaying, or canceling execution.

#### Scenario: Channel access is revoked
- **WHEN** a result is pending for a now-unauthorized destination
- **THEN** delivery becomes an incident while execution monitoring continues independently.

### Requirement: Retention by data category

Unless approved company policy changes configurable periods, the system SHALL retain:
- Raw structured errors: transient only, never in its database.
- Redacted delivery payloads: seven days after delivery or terminal delivery incident.
- Normalized receipts/attempts: 90 days, then audit fields only.
- Operation metadata/audit: 365 days; confirmation metadata follows this period despite five-minute validity.
- Protected configuration snapshots: active lifetime plus seven days after terminal resolution.
- Terminal thread bindings: 90 days, then re-resolve on use.

Unresolved operation, admission, and recovery evidence SHALL survive cleanup without permitting raw-error persistence. Slack copies SHALL follow workspace retention independently.

#### Scenario: Cleanup encounters old state
- **WHEN** retention expires
- **THEN** resolved data is deleted/reduced by category, unresolved recovery evidence survives, and expired bindings require fresh verification.
