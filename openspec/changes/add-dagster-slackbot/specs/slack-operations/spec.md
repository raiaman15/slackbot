## Purpose

Provide an optional Slack adapter for shared Dagster application services, using trusted failure threads, explicit approval, bounded evidence, and honest process-lifetime delivery guarantees.

Publisher and log-storage findings are recorded in the [environment audit](../../environment-audit.md).

## ADDED Requirements

### Requirement: Independently enabled Slack adapter

Slack SHALL be a separate adapter and implementation workstream. With `SLACK_ENABLED=false`, the application and DEV workbench SHALL start without Slack credentials, API calls, connection tasks, or Slack-dependent readiness. When enabled, Socket Mode SHALL translate authenticated events and interactions into the same typed commands, actors, target contexts, approvals, and services used by other authorized adapters. Slack connection health SHALL be reported separately from Dagster, core, and workbench health. Slack failure SHALL NOT change an execution outcome or permit an alternative authorization path.

#### Scenario: Slack is not configured
- **WHEN** Slack is disabled and the DEV workbench is enabled
- **THEN** DEV commands and tests work without a Slack app or workspace.

### Requirement: Deterministic human commands and volatile receipt

The adapter SHALL accept supported new human messages containing its exact mention and derive the requester from authenticated context. It SHALL ignore bots, self-events, missing human identities, edits/deletions, unsupported subtypes, hidden/system messages, and unmentioned replies; trusted publisher messages remain eligible roots. Phase one SHALL use bounded, documented, case-insensitive grammar without an LLM. Bare mentions show actions; unknown flags, oversized input, and unsupported prose return usage. Plain assent SHALL NOT confirm actions.

Socket envelopes SHALL be acknowledged promptly after bounded in-memory admission; Dagster queries and membership checks follow acknowledgement. Acknowledgement means volatile receipt, not durable acceptance or execution. Full queues SHALL reject work with a best-effort busy response without depending on Slack redelivery. Event deduplication SHALL use workspace/event identity independently of envelope identity; interaction deduplication SHALL use saved approval state. Deduplication is bounded and lost on restart. Replayed commands MAY prepare a new preview but SHALL NOT execute without a current explicit confirmation and shared dispatch checks.

#### Scenario: Duplicate receipt or restart
- **WHEN** Slack delivers a repeated event during a process lifetime, or replays it after restart
- **THEN** remembered duplicates are ignored; forgotten requests cannot recreate approved execution authority.

### Requirement: Exact trusted failure identity

Failure-specific `logs`, `status`, `config`, and `retry` SHALL require an existing root distinct from the command timestamp. Workspace, channel, and timestamps SHALL come from authenticated context; timestamps remain strings. The adapter SHALL retrieve that exact root, verify configured publisher and alert shape, and extract one full UUID from trusted fields or configured patterns without fetching URLs or ingesting whole conversations. Shared services SHALL verify environment, code location, repository, job, and available partition evidence. Missing, inaccessible, untrusted, ambiguous, or out-of-scope roots SHALL fail with an explanation; suffix matches and latest-run selection SHALL NOT establish identity.

For the audited dagster-slack 1.13.1 publisher, primary extraction SHALL use the `View in Dagster UI` button matching `http://127.0.0.1:8080//runs/<full-uuid>`, including the doubled slash. The shortened visible UUID SHALL NOT identify the run. This pattern requires no publisher rewrite, but enabling trusted binding SHALL require a captured payload and verified publisher bot/app, workspace, and channel IDs. The URL is parsed, never fetched or offered as an employee link.

#### Scenario: Short text and full button UUID
- **WHEN** a verified root contains one matching full UUID in its button URL
- **THEN** the adapter binds that verified run without requiring copied IDs or fetching the URL.

#### Scenario: Identity is incomplete
- **WHEN** only a suffix, neighboring message, or unknown URL pattern is available
- **THEN** automatic binding fails even if one search candidate appears.

### Requirement: Stable process-lifetime binding

The in-memory workspace/channel/root binding SHALL retain the original failure. Children, listings, status updates, and edited alerts SHALL NOT retarget it. Explicit eligible related-run selection belongs only to its operation. `bind <full-run-id>` MAY recover a trusted alert lacking a full ID after evidence verification, a provenance/target preview, and requester confirmation; conflicts SHALL refuse replacement. Revalidation SHALL detect changed root evidence. After restart, automatic bindings require fresh root verification and manual bindings require renewed confirmation. An edited root without a saved, verifiable original SHALL NOT establish a binding.

#### Scenario: Child run or lost binding
- **WHEN** a retry creates a child, or the process restarts
- **THEN** logs continue to require the original failure identity; lost manual approval is never reconstructed from messages or run tags.

### Requirement: Thread-scoped interaction and discovery

Evidence, menus, approvals, receipts, and outcomes SHALL stay beneath the validated root without channel broadcasts or DM rerouting. The bot SHALL update only its own saved messages. Controls SHALL match saved channel, root, and card. `help` and `capabilities` SHALL expose enabled commands and sanitized unavailable reasons. Listings SHALL provide explicit bounded pagination without rebinding. Optional presets/mappings SHALL NOT block core reads or retry. Top-level informational commands MAY reply beneath themselves; top-level failure commands SHALL request a failure-thread reply.

#### Scenario: Copied control or top-level retry
- **WHEN** a control has a different context or retry lacks an existing failure root
- **THEN** the adapter refuses execution and explains the required context.

### Requirement: Conversation identity is separate from requester authority

The shared conversation reference SHALL include installation, environment, transport, workspace, channel and original root timestamp. Request/event ID and authenticated actor SHALL remain separate per-turn fields. Replies SHALL use that saved channel and root `thread_ts`, with `reply_broadcast=false`; interpreters SHALL NOT choose destinations. Per-conversation context updates SHALL serialize or check snapshot revisions, without holding a lock while awaiting approval or run completion. Delayed results SHALL retain their original requester/turn and SHALL NOT overwrite newer context. Clarifications and proposals SHALL remain requester-bound despite shared dialogue. Unmentioned replies SHALL NOT start an operation or confirm one.

#### Scenario: Two members share a failure thread
- **WHEN** one member requests a retry and another replies with assent while asking their own question
- **THEN** they MAY share verified conversational evidence, but neither the assent nor the second request SHALL approve or alter the first member's proposal.

### Requirement: Rebuildable bounded conversation context

Phase one SHALL supply verified root/current-command context and Dagster evidence through a versioned context-provider contract. Future explicitly enabled dialogue retrieval SHALL read only the authorized thread, verify installed token/scopes, paginate within configured message/byte/time/token limits, respect rate limits, and mark missing/truncated history. Snapshots SHALL retain author/message provenance, observation times and context revision. No channel-wide search, attachment/URL ingestion or cross-thread memory SHALL be required. Slack discussion and prior bot prose SHALL NOT establish execution facts or approval; fresh Dagster queries supply operational evidence. Cached text/summaries SHALL be bounded, sanitized and disposable, invalidated on detected edits/deletions or loss of authorization. Ambiguous references SHALL require clarification.

#### Scenario: Context is lost or unavailable
- **WHEN** a new authenticated turn follows restart or inaccessible/deleted history
- **THEN** the adapter SHALL rebuild only accessible verified context, disclose gaps, and ask for missing references without recreating approvals, pending clarification authority or graph execution state.

### Requirement: Slack membership proves actor scope

Before Dagster access, the adapter SHALL verify authenticated transport, configured app/workspace, allowlisted invoking channel, bot membership, human membership, enabled capability, and target scope. Membership SHALL use complete pagination. All verified channel members have equal ordinary permissions, including newly joined members of publicly joinable allowed channels. Company-controlled, non-Slack-Connect channels are the default; invitation alone does not authorize a channel. Endpoint/environment overrides SHALL be rejected. The adapter SHALL pass verified actor and scope to shared services; other adapters SHALL prove their own authorized principals without pretending to be Slack users.

Reads SHALL use membership evidence at most 30 seconds old. Mutations SHALL freshly check requester membership and policy within five seconds before invocation; waits require refreshed authorization and target prechecks. Incomplete checks deny or defer. Observed revocation invalidates approval. This cannot atomically prevent a later membership change. Redacted diagnostic events SHALL distinguish authenticated human and transport context from backend identity, without promising a durable application audit ledger.

#### Scenario: Later-page member leaves before dispatch
- **WHEN** complete pagination initially verifies membership but final verification fails
- **THEN** the system invalidates approval and performs no mutation.

### Requirement: Requester-approved immutable previews

Every Dagster mutation and manual binding SHALL require the original requester's explicit approval. Other members SHALL NOT confirm, change mode, or cancel that proposal, but MAY create their own. Retry SHALL offer whole-run re-execution and a fresh copy, explaining lineage, current-code behavior, verified automatic retry policy, and possible queueing. Previews SHALL identify exact target/effect, source, job, environment, partition/selection, and incompatibilities. Schedule/sensor starts SHALL explain future runs outside the bot slot. Material changes require renewed preview. A completed earlier attempt for the same source requires explicit repeat acknowledgement; active, pending, or unresolved attempts cannot be bypassed by changing mode.

#### Scenario: Different actor or changed preparation
- **WHEN** another member clicks a control, or material action details change
- **THEN** another actor receives denial without consuming approval; changed preparation invalidates old controls and requires a new preview.

### Requirement: Expiring process-bound controls

Confirmation SHALL expire five minutes after preview preparation and bind boot ID, operation, requester, workspace, channel, root, saved card, mode, immutable intent/preview, and single-use interaction reference. Under process-local synchronization, every binding and state SHALL match before consumption. Controls SHALL carry opaque references only; browser/message-supplied identities, targets, or configuration SHALL NOT override saved state. Restart, expiry, supersession, or eviction of an eligible proposal invalidates its controls. Fresh confirmation SHALL NOT override disabled mutations or uncertain dispatch.

#### Scenario: Duplicate or old-boot interaction
- **WHEN** a confirmation is duplicated or belongs to an earlier process
- **THEN** it cannot create additional execution authority; expired state requires a new request and preview.

### Requirement: Operation inspection and explicit cancellation

`operation <id>` SHALL expose retained operation state only to authorized users in its saved context. Its requester MAY recover an unexpired proposal there. Lost state SHALL be reported as unavailable, never as evidence that no run launched. Scoped Dagster queries MAY recover verified run facts but SHALL NOT fabricate command history or recover approvals. `Cancel request` cancels only an unsubmitted proposal; message deletion does not cancel a submitted run. Dagster cancellation requires exact-run selection and separate confirmation. Slack controls SHALL NOT arm mutations, reset uncertainty, or perform operator recovery.

#### Scenario: Process state is lost
- **WHEN** the user asks about an unknown operation after restart
- **THEN** the response explains the limitation and offers authorized run inspection without resubmission.

### Requirement: Shared bounded Dagster evidence

Evidence retrieval and sanitization SHALL be transport-independent application services used by Slack and the DEV workbench. Logs SHALL use exact-run structured run/step failures, supported cause chains, timestamps, and stack frames. Absent exceptions or unsupported diagnoses SHALL be stated. Responses SHALL identify job, environment, full run ID, failed step, retry state, and available requester/operation context without inventing missing facts. Status distinguishes original and related runs; config shows sanitized logical inputs only.

Retrieval SHALL bound pages, events, bytes, depth, and runtime: initially 100 events/page, 1 MiB/request, a ten-second response target, and 30 rendered frames. Slack SHALL additionally bound output to approximately 6,000 characters within individual block limits. Reaching bounds SHALL produce partial-result notices and explicit continuation or human access. Both audited instances use `NoOpComputeLogManager`; `logs` SHALL explain that raw stdout/stderr are not persisted, without adding Kubernetes log permissions or promising recoverable compute logs.

#### Scenario: Evidence exceeds limits
- **WHEN** retrieval or rendering reaches a bound
- **THEN** each adapter identifies partial evidence and offers authorized continuation without silently claiming completeness.

### Requirement: Redaction before any disclosure

Operational evidence SHALL remain Dagster-only, without external investigation; phase one SHALL make no LLM disclosure. A future model adapter SHALL require separate enablement and owner-approved disclosure through the company AI gateway only. Before rendering, diagnostic logging, caching, model input, or export, shared services SHALL redact credentials, tokens, passwords, authorization headers, secret URI components, configured sensitive keys, and owner-approved patterns. Exception locals, raw connection strings, and secret-bearing bodies SHALL be excluded; previews withhold secrets. Slack markup and workbench HTML SHALL be inert. Uncertain/prohibited content SHALL be withheld with safe metadata. Full-log uploads and DM rerouting SHALL be excluded. Human links SHALL use an approved convention; otherwise show the UUID and established port-forward instructions, never fabricated public/internal Service links. Production enablement requires owner review of representative errors and disclosure policy.

#### Scenario: Evidence contains secrets or active markup
- **WHEN** Dagster evidence includes sensitive or executable content
- **THEN** both adapters sanitize and escape it, withholding unsafe bodies.

### Requirement: Best-effort notifications and bounded memory

Notifications SHALL remain separate from execution and SHALL use only bounded, redacted in-memory state. Delivery failures SHALL NOT replay mutations, stop observation, or change outcomes. Replies SHALL distinguish volatile receipt, created, queued/running, cancellation requested, terminal, unknown submission, and delivery failure. Retry delivery at most ten times or fifteen minutes per notification, respecting `Retry-After`; coalesce progress to at most one update per fifteen seconds. Terminal delivery targets p95 within thirty seconds of visible completion under healthy dependencies, without promising durable or exactly-once delivery.

Before disclosure, recheck destination policy. Revocation stops new disclosure and dispatch from that context; active-run monitoring continues while the process lives, with independent redacted operator diagnostics. Restart loses pending notifications and thread routing. Recovered run tags SHALL NOT authorize routing; require a fresh authenticated request. Raw errors remain transient; resolved RAM state follows execution-runtime bounds. Unknown/active state SHALL NOT be evicted to free execution capacity. There SHALL be no bot database, durable outbox, retained configuration snapshots, or guaranteed audit retention; Dagster, Slack, and platform-log retention apply independently.

#### Scenario: Delivery fails or the process restarts
- **WHEN** a creation reply is missing, retries are exhausted, or notification state is lost
- **THEN** the run is never relaunched for delivery; subsequent authorized queries can obtain Dagster facts without promising recovery of the missing message.
