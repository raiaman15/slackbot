## Purpose

Define the supported Dagster control capabilities, faithful run reproduction, and observable retry semantics available through the Slack bot.

## ADDED Requirements

### Requirement: Version-verified Dagster capabilities
The system SHALL use named, schema-verified GraphQL operations against the configured Dagster webserver and SHALL enable a capability only after its query or mutation, variables, result unions, and DEV behavior pass contract checks for the deployed version. The system SHALL verify startup compatibility and disable affected mutations on incompatibility while retaining compatible diagnostics. HTTP success alone MUST NOT establish GraphQL success.

#### Scenario: Deployed schema differs from the supported contract
- **WHEN** startup or a subsequent compatibility check detects a schema mismatch affecting launch
- **THEN** launch SHALL be unavailable with an actionable capability error and compatible read-only commands SHALL remain available.

#### Scenario: GraphQL returns an error over HTTP success
- **WHEN** Dagster returns HTTP 200 with GraphQL errors or an error result union
- **THEN** the system SHALL classify the documented result and MUST NOT report the operation as successful solely because HTTP succeeded.

### Requirement: Named phase-one command catalog
The system SHALL expose the following finite command catalog after an explicit bot mention. Each capability SHALL be configurable and report unavailable when its verified schema or required configuration is missing. Reads SHALL be bounded, and list operations SHALL support bounded pagination in the invoking thread.

| Command | Required behavior |
|---|---|
| `help`, `health`, `capabilities` | Show usage, safe dependency health, and enabled capabilities |
| `jobs` | List configured-location jobs, repositories, and partitioning |
| `runs`, `failed` | List runs with bounded status, time, and job filters |
| `status` | Show the bound source run, steps, timing, lineage, operation, and retry state |
| `logs` | Show bounded, redacted Dagster structured errors, cause chains, and stacks |
| `config` | Show safe configuration, selections, and applicable tags without secret values |
| `retry` | Prepare a choice of whole-run re-execution or fresh copy of the source |
| `launch <job> --preset <name>` | Launch one run from an owner-maintained validated preset |
| `cancel [run-id]` | Prepare graceful cancellation of one exact eligible active or queued run |
| `assets`, `asset <key>` | List or inspect asset materialization and check state |
| `partitions <job-or-asset>` | List or validate existing partition keys |
| `materialize <asset> --partition <key>` | Materialize one asset and partition through a verified job/preset mapping |
| `schedules`, `sensors` | List or inspect definitions, state, and ticks |
| `schedule start/stop <name>` | Prepare a desired state for one schedule |
| `sensor start/stop <name>` | Prepare a desired state for one sensor without cursor changes |
| `operation <id>` | Show a current-thread operation and recover its requester's pending confirmation |
| `bind <full-run-id>` | Prepare an explicitly confirmed exceptional manual thread binding |

#### Scenario: Operator requests available actions
- **WHEN** an authorized person mentions the bot with `capabilities`
- **THEN** the bot SHALL distinguish enabled commands from unavailable commands and explain missing prerequisites without exposing infrastructure secrets.

#### Scenario: Optional capability is unsupported
- **WHEN** a command requires an unavailable schema capability or configuration mapping
- **THEN** the system SHALL explain that limitation and MUST NOT approximate the request through an unverified or private API.

### Requirement: Explicit production scope and immutable failure target
The system SHALL accept targets only in its configured environment, code location, and allowed repositories. Failure-specific `logs`, `status`, `config`, and `retry` SHALL default to the original verified root-alert run. Any explicit child selection SHALL be stored on that operation and MUST NOT replace the thread's source binding. Definition commands SHALL retain their explicit named targets.

#### Scenario: Recovery creates a new child run
- **WHEN** a retry has created a child and a person subsequently requests `logs` in the failure thread
- **THEN** the bot SHALL retrieve the original bound failure unless the person explicitly selects the child.

#### Scenario: Request names another environment
- **WHEN** a command or resolved target identifies a run outside the configured production scope
- **THEN** the system SHALL refuse the operation without selecting another endpoint or environment.

### Requirement: Optional launch presets and asset mappings
Direct launch SHALL require a named owner-maintained preset validated against the current job definition, with an explicit partition where required. Asset materialization SHALL require an approved asset-to-job/preset mapping that proves one run with known configuration and exactly the requested asset/partition effect. Missing mappings SHALL disable only those optional capabilities; logs, status, and retry MUST NOT depend on them.

#### Scenario: Preset is missing
- **WHEN** a person requests direct launch without an available validated preset
- **THEN** the bot SHALL refuse launch and SHALL keep independently verified failure-log and retry commands available.

#### Scenario: Asset selection expands to additional assets
- **WHEN** materializing the requested asset would execute a non-subsettable multi-asset with additional asset effects
- **THEN** the bot SHALL refuse the single-asset command rather than silently broaden it.

### Requirement: Phase-one prohibited actions
The system MUST NOT support bulk retries, multi-partition backfills, arbitrary GraphQL input, run deletion, asset wiping, code-location reload or shutdown, forced run-state changes, force termination, schedule-definition editing, sensor-cursor edits, dynamic-partition mutation, arbitrary configuration pasted into Slack, or external log-system access in phase one. A retry MUST NOT create a new schedule, sensor, or backfill.

#### Scenario: User requests a wider operation
- **WHEN** a person requests force termination, arbitrary GraphQL, a bulk backfill, or another prohibited action
- **THEN** the bot SHALL refuse that action and identify the supported scope without issuing its mutation.

### Requirement: Two explicit retry modes
For an eligible source, `retry` SHALL offer whole-run re-execution and a fresh copy, with a new run ID and current deployed code in both modes. Whole-run re-execution SHALL preserve supported Dagster parent/root lineage and execute the complete eligible source selection. Fresh copy SHALL create a new Dagster root with source provenance retained by the bot. Neither mode SHALL resume from failure by reusing successful outputs as retry inputs in phase one.

#### Scenario: Requester selects whole-run re-execution
- **WHEN** the requester confirms whole-run re-execution of a compatible eligible source
- **THEN** the system SHALL request a new run preserving supported lineage and complete original eligible selection, without enabling from-failure resume.

#### Scenario: Requester selects fresh copy
- **WHEN** the requester confirms a fresh copy
- **THEN** the system SHALL request a new root run with the approved copied logical inputs and source provenance, and SHALL explain that its retry budget may differ from linked re-execution.

### Requirement: Faithful logical input and selection preservation
Both retry modes SHALL retrieve and preserve complete source logical configuration, partition intent, op or step selection, asset selection, asset-check selection, and applicable user/policy tags through verified supported inputs. The system SHALL preserve the distinction between null and empty selections and SHALL refuse any source intent it cannot reproduce faithfully. Lineage or provenance alone MUST NOT be treated as selection preservation.

#### Scenario: Source has an op subset and explicit asset-check selection
- **WHEN** either retry mode is prepared for that source
- **THEN** the preview and eventual request SHALL preserve those exact selections, including null versus empty distinctions, or the operation SHALL be refused.

#### Scenario: Source selection cannot be represented
- **WHEN** a dynamic partition, partial step selection, or another source field cannot be mapped faithfully into supported inputs
- **THEN** the system SHALL refuse execution rather than broaden or reduce the selection.

### Requirement: Complete effect preview for partition ranges
The system SHALL preview the complete effect of any source run spanning a partition range and SHALL permit its retry only when the entire original range and selection are explicitly supported. A one-run response MUST NOT be presented as proof of a single-asset or single-partition effect.

#### Scenario: Source includes a partition range
- **WHEN** the adapter cannot reproduce and preview the complete original range
- **THEN** retry SHALL be unavailable instead of narrowing the request to a single partition or expanding its scope.

### Requirement: Current deployed code and preservation conflicts
The system SHALL validate approved logical inputs against current deployed definitions and SHALL use current deployed code without restoring historical images. If preserved configuration or tags would force historical code, the system SHALL reject the conflict rather than silently modify inputs. Observable code, source, definition, or policy changes that invalidate the preview SHALL require a new preview and confirmation.

#### Scenario: Original configuration contains an old image override
- **WHEN** preserving a launcher or image override would execute historical code
- **THEN** the system SHALL explain the conflict and refuse launch until a separately reviewed supported input is available.

#### Scenario: Definition changes after preview
- **WHEN** the system detects a change that invalidates the saved preview before dispatch
- **THEN** it SHALL invalidate that approval and require fresh preparation and confirmation.

#### Scenario: Preserved inputs reference mutable external values
- **WHEN** a preview preserves environment or secret references under the latest-code policy
- **THEN** the bot SHALL describe the operation as a logical-input copy and MUST NOT promise a byte-for-byte historical replay.

### Requirement: Fresh execution identity and controlled tag handling
New runs SHALL receive new execution identity and appropriate mode-specific lineage. The system SHALL preserve supported business and retry-policy intent while replacing historical bot correlation tags and excluding stale retry counters, pending flags, retry-child pointers, resume markers, and other execution bookkeeping that is not a valid input for the new run.

#### Scenario: Source contains tags from an earlier bot operation
- **WHEN** a new run is prepared
- **THEN** the request SHALL use the new operation's provenance and verified retry-policy inputs rather than copying stale operation or retry-state identifiers.

### Requirement: Retry source eligibility
Only terminal failed sources SHALL be eligible for the default manual retry flow. A canceled or successful source SHALL require a fresh-copy flow that explicitly displays its current status. Active sources MUST NOT be casually cloned or re-executed through `retry`. Runs in an active backfill SHALL be ineligible, and completed-backfill copies SHALL require verified tag filtering and original-selection preservation.

#### Scenario: Source is still running
- **WHEN** a person requests `retry` for an active source
- **THEN** the system SHALL show its state and refuse the manual retry.

#### Scenario: Source succeeded or was canceled
- **WHEN** a person requests another execution of that source
- **THEN** the system SHALL require a fresh-copy preview explicitly acknowledging the source status.

#### Scenario: Source belongs to an active backfill
- **WHEN** either retry mode is requested
- **THEN** the system SHALL refuse the relaunch pending a separately supported backfill-coordination capability.

### Requirement: Dagster automatic retries remain authoritative
The system SHALL inspect the source's descendants and retry-pending state during preparation and again immediately before dispatch. A queued, running, or pending automatic retry SHALL block a duplicate manual launch. If an automatic retry already succeeded, a new execution SHALL require fresh preparation explicitly acknowledging recovery. The bot MUST NOT disable retry policy or reset retry counters to satisfy its own concurrency limit and MUST NOT create an independent automatic retry loop.

#### Scenario: Failed source is awaiting an automatic child
- **WHEN** Dagster reports an automatic retry as pending but no child run exists yet
- **THEN** the bot SHALL display the pending state and refuse a parallel manual retry.

#### Scenario: Retry budget is not observable
- **WHEN** preparing a linked re-execution or fresh copy without verified remaining-attempt information
- **THEN** the preview SHALL describe only verified installed behavior and MUST NOT invent a retry count.

### Requirement: Track the actual bot run family
The system SHALL track the new primary run and its actual automatic-retry descendants rather than every branch under a historical root. It SHALL report primary and relevant descendant run IDs, distinguish retry-pending gaps from final completion, and retain unresolved state when pending versus exhausted retry status cannot be established.

#### Scenario: Primary fails before its automatic child appears
- **WHEN** the primary becomes terminal but an automatic retry remains pending
- **THEN** the operation SHALL remain active, report retry pending, and continue observing descendants.

#### Scenario: Historical lineage contains an unrelated branch
- **WHEN** a linked re-execution shares an older root with another branch
- **THEN** only the new primary and its actual retry descendants SHALL determine this bot operation's completion.

### Requirement: Graceful cancellation and observed outcome
Cancellation SHALL target one exact supported active or queued run, display that run for requester confirmation, request normal supported termination, and track observed status. Ambiguous eligible descendants SHALL require explicit selection. `Cancellation requested` MUST NOT be reported as `Canceled` until Dagster confirms that outcome, and force termination SHALL remain unavailable.

#### Scenario: Multiple related runs could be canceled
- **WHEN** a person issues `cancel` without an unambiguous exact target
- **THEN** the bot SHALL offer eligible targets or require a full run ID and SHALL request confirmation before mutation.

#### Scenario: Dagster acknowledges a termination request
- **WHEN** the target remains active after the request
- **THEN** the bot SHALL report cancellation pending and continue observing actual terminal status.

### Requirement: Serialized desired-state automation control
Schedule and sensor start/stop SHALL set an explicitly previewed desired state for one named definition, with requester confirmation and serialization per target. After an ambiguous response, the system SHALL read current state before any supported idempotent convergence attempt. An unresolved older request SHALL block a contradictory dispatch until that request is resolved or isolated. Sensor cursors SHALL remain unchanged.

#### Scenario: Stop follows an uncertain start
- **WHEN** an old start request may still take effect
- **THEN** the system SHALL keep that target occupied and resolve or isolate the old request before sending the opposite desired state.

#### Scenario: A schedule is stopped
- **WHEN** Dagster confirms the desired stopped state
- **THEN** the bot SHALL report that state and MUST NOT imply that already-created runs were canceled.

### Requirement: Explain automation and external concurrency boundaries
Automation confirmation SHALL explain that enabling a schedule or sensor can create multiple future runs outside the bot's single-family launch slot. The system SHALL observe external UI and daemon changes without claiming atomic exclusion over them. It MUST NOT claim that the bot's own lock prevents a concurrent Dagster automatic retry or imposes instance-wide run concurrency.

#### Scenario: User confirms a sensor start
- **WHEN** the bot previews the state change
- **THEN** it SHALL state that future sensor-created runs are outside the bot's launch slot and may be multiple.

### Requirement: Typed application services remain the control boundary
All entry points SHALL use the same typed action preparation, authorization, confirmation, dispatch, and reconciliation contracts. Phase one SHALL require no LLM or MCP dependency. Any future language interpreter SHALL produce proposed typed intents only and MUST NOT choose actor identity, bypass confirmation, or gain direct Dagster execution authority.

#### Scenario: A future natural-language adapter proposes a retry
- **WHEN** the proposal reaches the application boundary
- **THEN** it SHALL require the same verified Slack actor, target, current authorization, preview, and confirmation as the deterministic command.
