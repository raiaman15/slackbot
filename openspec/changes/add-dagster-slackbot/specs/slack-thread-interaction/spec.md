# Slack Thread Interaction

## Purpose

Define deterministic Slack interactions that bind operational commands to the original Dagster failure alert and keep their evidence, controls, and progress in that thread.

## ADDED Requirements

### Requirement: Explicit human mention invocation
The system SHALL accept a command only from a supported new human message containing the installed bot's exact mention identity. It SHALL derive the requester from the authenticated invoking message's human user identity, never from installation authorization metadata, display names, or command text. It SHALL ignore self events, other bot-originated commands, absent human identities, message edits/deletions, unsupported subtypes, hidden/system messages, and unmentioned replies. A bot-authored failure alert SHALL remain eligible as a trusted root independently of these command filters.

#### Scenario: Human invokes the bot beneath an integration alert
- **WHEN** an authorized person sends `@bot logs` inside a thread rooted at a trusted bot-authored Dagster failure alert
- **THEN** the system accepts the human message as the command and evaluates the integration's message as the root alert
- **AND** the operation's requester is the person who sent the command

#### Scenario: Installation identity differs from the requester
- **WHEN** a valid mention's installation metadata identifies the bot while the message identifies a human sender
- **THEN** the system attributes the request to the human sender
- **AND** message text cannot replace that actor identity

#### Scenario: A reply or edit is not a new command
- **WHEN** the system observes an unmentioned reply, a bot-originated mention, or an edit to an earlier command
- **THEN** it creates no new operational request and changes no previously saved action

### Requirement: Finite phase-one command grammar
The system SHALL interpret phase-one commands using a bounded, case-insensitive, documented grammar after the exact bot mention. It SHALL support deterministic command names, reject unknown flags and oversized input, and SHALL NOT infer actions from arbitrary prose or call an LLM. An empty mention SHALL display the available actions; unknown wording SHALL return usage in the invoking thread. Optional aliases SHALL be explicitly documented; supported examples are `error` or `show logs` for `logs`, and `rerun` for `retry`.

#### Scenario: Empty mention inside a failure thread
- **WHEN** an authorized person sends only the bot mention inside a valid failure thread
- **THEN** the system displays an action menu for that failure in the same thread
- **AND** it does not submit a mutation

#### Scenario: Unsupported natural-language input
- **WHEN** a person mentions the bot with wording outside the documented grammar or an unknown flag
- **THEN** the system returns bounded usage guidance in the same thread
- **AND** it does not guess or execute an intended action

#### Scenario: Plain assent is not confirmation
- **WHEN** a person replies `yes` beneath a pending proposal
- **THEN** the system does not confirm or execute the proposal

### Requirement: Failure-specific commands require existing thread context
The system SHALL require a reply beneath an existing failure alert for failure-specific `logs`, `status`, `config`, and `retry` operations. The authenticated root-thread timestamp SHALL exist and differ from the invoking message timestamp. A top-level informational request MAY receive help or bounded listings beneath its own message, but SHALL NOT implicitly bind to a failure or choose the latest run.

#### Scenario: Top-level retry request
- **WHEN** a person sends `@bot retry` as a new top-level message
- **THEN** the system explains how to reply beneath the existing failure alert
- **AND** it neither chooses a run nor submits a launch

#### Scenario: Top-level help request
- **WHEN** an authorized person sends `@bot help` as a top-level message
- **THEN** the system replies beneath that message without creating a failure binding

### Requirement: Exact trusted root resolution
The system SHALL derive workspace, channel, root-thread timestamp, and command timestamp from authenticated Slack context, retaining timestamps without numeric conversion. It SHALL resolve the exact root message, verify that its timestamp equals the requested root, and verify its configured publisher identity and expected alert shape. It SHALL inspect trusted text, blocks, attachment links, or fields for a full run identifier without fetching arbitrary user-selected URLs or ingesting the whole conversation. A missing, inaccessible, untrusted, or multi-run root SHALL stop automatic resolution with an explanation in the invoking thread.

#### Scenario: Mention occurs several replies into a failure thread
- **WHEN** the command is nested among existing replies to a trusted failure alert
- **THEN** the system resolves the original root timestamp rather than a nearby reply or the command timestamp
- **AND** the user does not need to copy a permalink or run identifier

#### Scenario: History lookup does not return the exact root
- **WHEN** the available message has a timestamp different from the authenticated root-thread timestamp
- **THEN** the system reports that the alert cannot be resolved
- **AND** it does not use the neighboring message or latest failure

#### Scenario: Root publisher or target is ambiguous
- **WHEN** the root publisher is not trusted or the alert points to multiple runs
- **THEN** the system refuses automatic run selection and explains the resolution problem

### Requirement: Full run identity and scope verification
The system SHALL use a complete run identifier extracted from trusted configured alert fields or link patterns to query the configured Dagster environment. Before establishing a binding, it SHALL validate the run's environment, code location, repository, job, and any supplied partition evidence. A visible short label MAY identify a link whose target contains the full identifier. The system SHALL NOT treat a suffix, an incomplete candidate search, or a user-selected endpoint as authoritative execution identity.

#### Scenario: Short visible label links to a full UUID
- **WHEN** a trusted root displays a run suffix but its configured Dagster link contains the full run UUID
- **THEN** the system verifies that exact UUID against the configured Dagster scope and uses it as the source identity

#### Scenario: Only one suffix candidate appears in a bounded search
- **WHEN** the root contains only a suffix and a bounded search returns one candidate
- **THEN** the system does not claim global uniqueness or automatically bind the candidate
- **AND** it requests an exact-identifier fallback or an alert-format correction

#### Scenario: Alert references an out-of-scope run
- **WHEN** the extracted full run identifier resolves outside the configured environment, code location, repository, or supplied target evidence
- **THEN** the system refuses the operation rather than redirecting to another Dagster scope

### Requirement: Explicit manual binding fallback
The system SHALL offer `bind <full-run-id>` as an exceptional fallback when the trusted alert lacks a complete run identifier. It SHALL verify the exact run and available alert evidence, show job, time, partition, and binding provenance, and require the requesting person's explicit confirmation before saving the mapping. Suffix discovery MAY assist the person but SHALL NOT establish the binding. A mismatch SHALL be surfaced, and a requester-supplied binding SHALL remain distinguishable from publisher-verified identity. The system SHALL NOT silently overwrite an established verified binding.

#### Scenario: Person supplies an exact fallback identifier
- **WHEN** an authorized person sends `@bot bind <full-run-id>` for a trusted alert with incomplete run identity
- **THEN** the system presents the verified run's job, time, partition, and manual-mapping evidence for confirmation in that thread
- **AND** no mapping takes effect before valid requester confirmation

#### Scenario: Manual binding contradicts existing evidence
- **WHEN** the supplied exact identifier conflicts with alert evidence or an established verified binding
- **THEN** the system reports the conflict and does not silently replace the source run

### Requirement: Original source binding remains stable
The system SHALL preserve the original failure's binding for the workspace, channel, and root-thread identity. `logs`, `config`, and `retry` SHALL continue to target that original source unless an operation explicitly selects an eligible related run. Status MAY include related recovery operations and automatic-retry descendants. A new child run, an edited alert, a new list result, or an updated status card SHALL NOT silently retarget the thread. Any explicit child target SHALL be recorded on the operation without rewriting the original binding.

#### Scenario: A retry creates a new run
- **WHEN** a new recovery run exists and a person subsequently requests `@bot logs` in the original thread
- **THEN** the system returns the original failure's evidence
- **AND** related run evidence requires an explicit target selection

#### Scenario: Bound root is edited
- **WHEN** the alert's run reference changes after a verified binding was established
- **THEN** the system detects the conflict during required revalidation and refuses silent retargeting

#### Scenario: Cancellation has multiple eligible descendants
- **WHEN** a cancellation request could apply to more than one related active run
- **THEN** the system requires selection of an exact run and shows it before confirmation
- **AND** the selected run does not replace the thread's original source binding

### Requirement: Same-thread operational conversation
The system SHALL place evidence, mode selection, confirmation, visible receipts, status, and terminal outcomes beneath the validated original root without broadcasting replies to the channel. It SHALL update only its own saved messages and SHALL NOT edit the existing failure publisher's alert. Interaction payloads SHALL be checked against the saved channel, root, and bot-owned card identity; client-supplied values SHALL NOT redirect an operation or its output.

#### Scenario: Retry completes
- **WHEN** a confirmed retry is created and later reaches a final family outcome
- **THEN** the mode selection, confirmation, creation result, progress, and terminal reply all remain in the original failure thread
- **AND** the existing publisher's root alert remains unchanged

#### Scenario: A copied control appears in another context
- **WHEN** an interaction's channel, root, or card does not match the saved proposal
- **THEN** the system rejects the interaction without redirecting the operation or its evidence

### Requirement: Retry mode selection and preview
The system SHALL present `Re-execute whole run` and `Launch fresh copy` for an eligible retry request and explain their lineage distinction. After mode selection, it SHALL show the exact source run, job, production environment, partition and selection, current-code policy, applicable verified retry policy, and any incompatibility before confirmation. A material change to the prepared action SHALL require a new preview and confirmation. Changing mode SHALL be possible only before confirmation and SHALL invalidate the previous preview's controls.

#### Scenario: Person prepares a retry
- **WHEN** a verified source is eligible for retry
- **THEN** the system offers both supported modes in the original thread and presents a mode-specific preview before asking for confirmation

#### Scenario: Source or preview changes
- **WHEN** the target, configuration, observed definition, or policy changes materially before dispatch
- **THEN** the system requires renewed preparation and confirmation of the changed action
- **AND** the old approval cannot execute the new action

#### Scenario: Late mode-selection click
- **WHEN** a person clicks an old mode control after the operation was confirmed, admitted, or dispatched
- **THEN** the system does not change the saved mode or submitted action

### Requirement: Command discovery and bounded catalog responses
The system SHALL expose the enabled named operation catalog through `help` and `capabilities`, including availability explanations for unsupported or unverified capabilities. The catalog SHALL cover `health`, `jobs`, `runs`, `failed`, `status`, `logs`, `config`, `retry`, preset-based `launch`, exact-run `cancel`, `assets`, `asset`, `partitions`, mapped `materialize`, `schedules`, `sensors`, schedule/sensor start and stop, `operation`, and `bind`. Listing commands SHALL use bounded pagination in the invoking thread and SHALL NOT change its bound run. Optional launch and materialization mappings SHALL NOT be prerequisites for failure logs, status, or retry.

#### Scenario: Optional capability has not been verified
- **WHEN** a person requests a capability unsupported by the installed schema or missing its required preset or mapping
- **THEN** the system reports it as unavailable with a sanitized explanation
- **AND** it does not approximate the operation through an unapproved alternative

#### Scenario: Run listing is paginated
- **WHEN** a person requests more runs than fit in one bounded result
- **THEN** the system offers explicit pagination within the invoking thread
- **AND** selecting a page does not change the original failure binding

### Requirement: Recoverable proposals and operation status
The system SHALL let an authorized person inspect `operation <id>` only from that operation's saved channel and root thread. The original requester SHALL be able to recover an unexpired pending proposal there. Consumed or expired controls SHALL remain invalid even if their visual update fails. Deleting a Slack message SHALL NOT cancel an already confirmed run; `Cancel request` SHALL cancel only an unsubmitted proposal, while canceling a Dagster run SHALL require the explicit run-cancellation operation.

#### Scenario: Requester recovers an unexpired proposal
- **WHEN** the original requester sends `@bot operation <id>` in the operation's saved thread before confirmation expiry
- **THEN** the system displays the existing proposal and valid remaining action controls without creating a different action

#### Scenario: Old card still looks active
- **WHEN** a consumed or expired confirmation card could not be visually updated and a person clicks it
- **THEN** the system rejects the stale control and submits no new action

#### Scenario: Confirmed request message is deleted
- **WHEN** a person deletes the command or proposal message after confirmation
- **THEN** the system does not interpret deletion as a cancellation request or change the saved action

### Requirement: Accurate operational state language
The system SHALL distinguish transport acknowledgement, durable acceptance, run creation, queued execution, active execution, cancellation requested, terminal execution, submission uncertainty, and delayed notification. It SHALL NOT claim acceptance before durable receipt or claim a run is running or successful merely because creation succeeded. Refusals SHALL identify the relevant operation or run when safe and explain the next action without initiating an unconfirmed replacement.

#### Scenario: Dagster returns a queued run
- **WHEN** Dagster confirms creation of a run that remains queued
- **THEN** the system reports the full run identifier and queued status rather than claiming execution has started or completed

#### Scenario: Submission or notification is uncertain
- **WHEN** a launch result is unknown or a completed operation's Slack delivery is delayed
- **THEN** the system distinguishes those conditions and reports the existing operation for inspection
- **AND** it does not advise or perform an automatic replacement launch

#### Scenario: Another launch or automatic retry blocks admission
- **WHEN** an existing bot family, unresolved submission, or active or pending automatic retry prevents a new launch
- **THEN** the system explains the relevant busy or retry state in the same thread
- **AND** it does not silently queue a later production launch
