# Evidence and Notifications

## Purpose

Define safe, bounded Dagster evidence and reliable same-thread notifications whose delivery failures cannot change or repeat the underlying operational action.

## ADDED Requirements

### Requirement: Dagster-only structured failure evidence
The system SHALL retrieve structured run-failure and step-failure events, supported exception cause chains, and stack frames for the exact bound run from Dagster. It SHALL preserve event ordering and timestamps and identify failed step keys. A run failure without a step exception SHALL be reported as such. Phase one SHALL NOT investigate external log systems, send evidence to an LLM, or generate root-cause claims beyond the available Dagster evidence.

#### Scenario: Step failure has a nested cause
- **WHEN** the bound run contains a structured step failure with supported cause-chain and stack data
- **THEN** the system presents the failed step and ordered, sanitized Dagster evidence for that exact run

#### Scenario: Run failure has no step exception
- **WHEN** Dagster reports run failure but provides no associated step exception
- **THEN** the system explicitly states that no step exception is available rather than inventing a cause

#### Scenario: Evidence is insufficient for diagnosis
- **WHEN** the available Dagster events do not establish a root cause
- **THEN** the system reports the available evidence and its limits without using a model or external source to manufacture a diagnosis

### Requirement: Bounded evidence retrieval and rendering
Evidence retrieval SHALL enforce configured bounds on pages, event count, bytes, nesting depth, and runtime. Initial defaults SHALL use 100 events per page and at most 1 MiB of internal evidence per request, with a normal response target of ten seconds. Rendering SHALL be bounded to at most 30 stack frames and approximately 6,000 total characters across correctly sized Slack blocks, obeying each selected block's limit. Reaching any bound SHALL produce an explicit partial-result or truncation notice and offer explicit pagination or established Dagster access instead of silently omitting evidence.

#### Scenario: Failure evidence exceeds request bounds
- **WHEN** the evidence reaches a configured page, event, byte, nesting, or runtime limit
- **THEN** the system stops bounded retrieval and labels the response as partial
- **AND** it offers explicit pagination or the configured human-access route

#### Scenario: Stack trace exceeds the display budget
- **WHEN** the sanitized trace exceeds 30 frames or the configured display budget
- **THEN** the system shows a bounded excerpt and a truncation notice using Slack-valid blocks
- **AND** it does not rely on Slack silently truncating an oversized field

### Requirement: Evidence response identifies its context
A failure-log response SHALL identify the job, configured environment, full source run ID, failed step when present, exception and concise cause/stack evidence, automatic-retry state, requester, and operation ID. It SHALL identify redaction and truncation when applicable. Status SHALL distinguish the original run, related operation, and observed automatic-retry descendants rather than presenting them as a single run. Configuration responses SHALL contain only sanitized logical configuration, selections, and applicable tags.

#### Scenario: User receives a failure response
- **WHEN** an authorized `logs` request completes
- **THEN** the reply identifies the exact source run, job and environment, requesting user and operation, and available failure and automatic-retry evidence
- **AND** it marks any sanitization or truncation

#### Scenario: Retry descendant differs from the original failure
- **WHEN** status includes an original failure and an active automatic-retry descendant
- **THEN** the system identifies each run and its observed state explicitly rather than implying that the original run changed identity

### Requirement: Redaction precedes disclosure and result persistence
The system SHALL treat Slack output as data export from the VPC and redact credential patterns, tokens, passwords, authorization headers, secret URI components, configured sensitive keys, and owner-approved sensitive identifiers or payload patterns before rendering, operational logging, persistent result caching, or audit export. It SHALL exclude exception locals, raw connection strings, and secret-bearing request or response details. Previews and safe configuration summaries SHALL withhold secret values. Production enablement SHALL require owner review of representative error samples and the resulting data policy.

#### Scenario: Structured exception includes a secret
- **WHEN** an error or configuration summary contains a credential, authorization header, secret URI component, or configured sensitive value
- **THEN** the system removes or masks it before any Slack output, operational log, persistent result, or audit export

#### Scenario: Preview includes sensitive launch configuration
- **WHEN** launch preparation needs sensitive source configuration
- **THEN** the confirmation preview shows only a safe summary and logical identity without exposing secret values

#### Scenario: Company error samples have not been reviewed
- **WHEN** production write and evidence disclosure enablement is evaluated without owner review of representative errors and policy patterns
- **THEN** the system's release gate remains unsatisfied

### Requirement: Uncertain content and Slack markup are constrained
The system SHALL escape evidence-derived Slack markup and prevent log content from creating mentions, misleading links, or interactive controls. If content is prohibited or its suitability remains uncertain after redaction, it SHALL withhold the body and return only error class, safe metadata, and a protected human-access route when available. Phase one SHALL NOT upload full log files or silently redirect restricted operational output to direct messages.

#### Scenario: Error text contains Slack control syntax
- **WHEN** a structured error includes mention tokens, link syntax, or text resembling interactive content
- **THEN** the system renders it as inert evidence without notifying users or creating evidence-controlled actions

#### Scenario: Body cannot be safely disclosed
- **WHEN** an error body is prohibited or cannot be confidently sanitized for the allowed channel
- **THEN** the system withholds the body and provides safe metadata and the existing human-access route when configured
- **AND** it does not upload the full body or send it to a different destination

### Requirement: Human-access references are valid for people
The system SHALL include a human-access link only when its configured convention is valid for employees. It SHALL NOT fabricate a public URL or present a cluster-internal Service address as a usable employee link. If no stable link exists, it SHALL provide the full run UUID and established port-forward access instructions. Linking to human access SHALL NOT expose Dagster publicly or change the bot's configured internal target.

#### Scenario: No stable human URL is configured
- **WHEN** a response needs to direct a person to Dagster but only internal cluster access is configured
- **THEN** the system shows the full run identifier and the established access instructions without inventing a clickable URL

### Requirement: Durable notification work is independent of execution
Each operation state transition that requires a notification SHALL durably record that notification's logical identity and result with the transition. Slack delivery state SHALL remain separate from operation and Dagster execution state. Failure to send, update, or acknowledge a notification SHALL NOT reset an operation to dispatchable, repeat a launch, stop run observation, or change its observed outcome. An operation MAY finish while its notification remains pending, and operation inspection SHALL distinguish those states.

#### Scenario: Slack fails after Dagster creates a run
- **WHEN** Dagster creates a run and the creation notification cannot be delivered
- **THEN** the system continues tracking that run and retries only the notification work
- **AND** it does not submit another run

#### Scenario: Run succeeds while its final reply is pending
- **WHEN** Dagster proves the tracked family's success before Slack delivers the terminal reply
- **THEN** the operation remains successful with delivery reported separately as pending

#### Scenario: Process restarts before notification delivery
- **WHEN** the bot restarts after committing an operation transition but before sending its corresponding message
- **THEN** the pending logical notification remains recoverable without replaying the operation

### Requirement: Rate-aware same-thread progress and terminal delivery
The system SHALL post and update only its own status messages in the saved original thread and SHALL send a terminal reply containing the primary and relevant automatic-retry run identifiers. It SHALL respect Slack rate limits and `Retry-After`, coalesce intermediate updates, and default to at most one status update per 15 seconds per operation with start/end transitions prioritized. It SHALL NOT repeatedly stream stack traces as progress updates. Under healthy dependencies, terminal notification SHALL target the initial p95 objective of 30 seconds after Dagster-visible final state.

#### Scenario: Dagster reports frequent intermediate changes
- **WHEN** an operation produces multiple status changes within the default update interval
- **THEN** the system coalesces intermediate progress while preserving important start/end transitions and the final outcome

#### Scenario: Slack rate-limits a message
- **WHEN** Slack returns a rate limit with a retry delay
- **THEN** the system respects that delay for delivery while continuing independent run observation

#### Scenario: Automatic-retry family completes
- **WHEN** the primary and relevant retry descendants reach a verified final family outcome
- **THEN** the system posts the final result and applicable run identifiers beneath the original alert

### Requirement: Ambiguous Slack delivery does not imply ambiguous execution
The system SHALL assign a stable logical message identity to each notification and associate visible messages with the operation ID. When Slack may have accepted a message whose response was lost, it SHALL reconcile the known thread where practical and tolerate distinguishable duplicate notifications if exact message deduplication cannot be established. It SHALL NOT claim exactly-once Slack delivery or use notification uncertainty as evidence that a Dagster launch failed.

#### Scenario: Slack accepts a post but its response is lost
- **WHEN** the initial message may exist although no message timestamp was received
- **THEN** the system reconciles the saved thread where practical or retries the logical notification under its delivery policy
- **AND** any duplicate is identifiable by operation ID and creates no additional Dagster submission

### Requirement: Revoked destination produces a delivery incident
The system SHALL stop sending new evidence or operational updates when the destination becomes disallowed or the bot loses its channel access. It SHALL retain the result and audit under the retention policy and alert operators through an independent monitoring path. It SHALL NOT reroute the result to arbitrary direct messages or another channel, and the delivery incident SHALL NOT cancel or replay the underlying operation.

#### Scenario: Bot loses channel access with pending output
- **WHEN** Slack no longer permits access to the original allowed thread
- **THEN** the system records the delivery incident, preserves the operation result, and signals operators independently
- **AND** it sends no evidence to an alternative destination

### Requirement: Concrete retention and unresolved-state protection
The system SHALL apply the following default retention policy unless an approved company policy explicitly changes a configurable period: raw structured error bodies SHALL be transient only and SHALL NOT be retained in the bot database; redacted delivery payloads SHALL be retained for seven days after successful delivery or a terminal delivery incident; confirmation validity SHALL be five minutes while its metadata follows operation audit retention; normalized command receipts and execution attempts SHALL be retained for 90 days and then reduced to audit fields; operation metadata and audit records SHALL be retained for 365 days; encrypted launch configuration snapshots SHALL remain for the active operation lifetime plus seven days after terminal resolution; terminal thread bindings SHALL remain for 90 days and later requests SHALL re-resolve them. Unresolved operation, admission, and recovery evidence SHALL NOT be purged while unresolved, without using that exception to persist raw structured errors. Slack copies SHALL remain subject to workspace retention independently of bot-side deletion.

#### Scenario: Retention period ends for a resolved operation
- **WHEN** an applicable default retention period expires after terminal resolution or delivery
- **THEN** the system deletes or reduces that data according to its category while retaining audit fields for their required period

#### Scenario: An old operation remains unresolved
- **WHEN** normal age-based cleanup encounters an unresolved dispatch, admission record, or recovery evidence
- **THEN** the system preserves the unresolved state and evidence necessary for safe reconciliation
- **AND** the exception does not permit retention of raw structured error bodies

#### Scenario: Old thread binding expires
- **WHEN** a new command references a thread whose terminal binding has exceeded its retention period
- **THEN** the system re-resolves and verifies the root rather than using an expired mapping as authority

#### Scenario: Bot deletes a stored delivery payload
- **WHEN** bot-side cleanup removes a payload after its retention period
- **THEN** the system does not represent that cleanup as deletion of the already-posted Slack copy

### Requirement: Sensitive configuration snapshots are protected
When faithful execution requires retaining sensitive launch configuration, the system SHALL protect its snapshot with company-managed encryption, restricted access, documented key rotation, and the defined snapshot retention period. It SHALL retain a separate logical hash for comparison without exposing the secret content. A faithful configuration reference SHALL be preferred where possible, and secret values SHALL NOT be retained merely for convenience. Key or configuration failures SHALL produce sanitized operational errors rather than secret disclosure.

#### Scenario: Launch needs a saved sensitive configuration snapshot
- **WHEN** the approved action cannot be reproduced faithfully from a permitted configuration reference
- **THEN** the system stores only the necessary protected snapshot and comparison identity and removes it according to the terminal-resolution retention policy

#### Scenario: Snapshot cannot be safely accessed
- **WHEN** snapshot decryption or approved access fails
- **THEN** the system refuses or defers the affected action with a sanitized error instead of disclosing configuration or silently substituting different inputs
