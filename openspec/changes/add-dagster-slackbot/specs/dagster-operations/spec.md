## Purpose

Define Dagster commands, faithful run reproduction, and observable control outcomes.

Deployment facts come from the supplied [environment audit](../../environment-audit.md); deployment checks SHALL confirm they still hold.

## ADDED Requirements

### Requirement: Verified scoped command catalog
The system SHALL use named GraphQL operations verified against the deployed schema and DEV behavior, including inputs and result unions. Each capability SHALL be configurable. The system SHALL check startup compatibility, disable incompatible capabilities while retaining compatible diagnostics, and accept only the configured environment, code location, and repositories. HTTP success alone SHALL NOT mean GraphQL success. Reads and pagination SHALL be bounded. Required missing fields, unknown result variants and partial data SHALL be classified explicitly; incomplete evidence SHALL NOT authorize a mutation.

The initial adapter SHALL target audited Dagster 1.13.1 in DEV and PROD. Its pinned documents SHALL use `launchRun`, `launchRunReexecution`, `terminateRun`, `startSchedule`, `stopRunningSchedule`, `startSensor`, and `stopSensor` as applicable; there is no `stopSchedule`. Scope configuration SHALL use deployed workspace identities, initially location `k8s-example-user-code-1` and repository `__repository__`, rather than names from an undeployed workspace file.

The shared application services SHALL expose this catalog. Slack maps explicit mentions to it; the admin panel maps its commands and controls to the same services within its authenticated environment scope:

| Command | Behavior |
|---|---|
| `help`, `health`, `capabilities` | Usage, safe dependency health, enabled capabilities and missing prerequisites |
| `jobs` | Jobs, repositories, partitioning |
| `runs`, `failed` | Runs with status, time, job filters |
| `status` | Bound run, steps, timing, lineage, operation and retry state |
| `logs` | Redacted structured errors, cause chains and stacks |
| `config` | Safe configuration, selections and tags without secrets |
| `retry` | Choose whole-run re-execution or fresh copy |
| `launch <job> --preset <name>` | One run from a validated preset |
| `cancel [run-id]` | Gracefully cancel one exact active or queued run |
| `assets`, `asset <key>` | Asset materialization and check state |
| `partitions <job-or-asset>` | List or validate existing partition keys |
| `materialize <asset> --partition <key>` | One asset/partition through a verified mapping |
| `schedules`, `sensors` | Definitions, state and ticks |
| `schedule start/stop <name>` | Set one schedule's desired state |
| `sensor start/stop <name>` | Set one sensor's desired state, preserving its cursor |
| `operation <id>` | Current-context in-memory operation, or positively identified Dagster run facts; lost controls cannot be recovered |
| `bind <full-run-id>` | Slack-only exceptional manual thread binding, valid only within the current process |

#### Scenario: Listing and selecting an authorized target
- **WHEN** an authorized user requests a supported job, run, asset, partition or automation listing
- **THEN** results SHALL retain exact scoped identifiers and report truncation with a scoped pagination control; selecting an item SHALL validate that identifier without guessing a different target.

#### Scenario: Capability or target is unsupported
- **WHEN** an operation lacks verified schema support or targets another scope
- **THEN** the system SHALL refuse it with an actionable, non-secret explanation, without selecting another endpoint or approximating it through a private API.

#### Scenario: GraphQL fails over HTTP success
- **WHEN** HTTP 200 contains GraphQL errors or an error result union
- **THEN** the system SHALL classify that failure rather than report success.

#### Scenario: GraphQL returns partial data
- **WHEN** a response contains both data and errors or omits required evidence
- **THEN** diagnostics MAY display individually verified fields marked incomplete, but preparation/dispatch checks SHALL fail closed; a mutation whose side effects remain uncertain SHALL enter `SUBMISSION_UNKNOWN`.

#### Scenario: Audited schema exposes additional mutations
- **WHEN** introspection includes deletion, backfill, cursor editing, or other excluded mutations
- **THEN** their presence SHALL NOT enable them; only approved named operations with verified inputs and result unions SHALL be callable.

### Requirement: Optional inputs and phase-one exclusions
Direct launch SHALL require an owner-maintained preset validated against the current definition and an explicit partition when required. Materialization SHALL require an approved asset-to-job/preset mapping proving one run with known configuration and exactly the requested asset/partition effect. Missing mappings SHALL disable only those capabilities, independently of logs, status and retry.

Phase one SHALL exclude bulk retries, multi-partition backfills, arbitrary GraphQL/configuration, run deletion, asset wiping, code-location reload/shutdown, forced run states/termination, schedule-definition editing, sensor-cursor editing, dynamic-partition mutation and external log access. Retrying SHALL NOT create a schedule, sensor or backfill.

#### Scenario: Validated launch or materialization
- **WHEN** a permitted preset or asset mapping resolves to one supported run with valid current configuration and partition intent
- **THEN** the system SHALL prepare an exact effects preview and require the normal fresh checks and requester confirmation before dispatch.

#### Scenario: Optional owner configuration is absent
- **WHEN** no approved preset or exact asset mapping exists for the requested action
- **THEN** that action SHALL be unavailable with its missing prerequisite, while compatible read and retry capabilities remain available.

#### Scenario: Single-asset request expands
- **WHEN** a non-subsettable multi-asset would produce additional asset effects
- **THEN** the system SHALL refuse the command rather than broaden it.

### Requirement: Stable source and retry eligibility
Failure-specific `logs`, `status`, `config` and `retry` SHALL default to the verified Slack root-alert run or explicitly selected admin-panel run/DEV fixture. Explicit child selection SHALL belong to that operation without changing the thread binding; definition commands SHALL retain their named targets.

Default retry SHALL require a terminal failed source. Successful or canceled sources SHALL require fresh-copy preparation explicitly displaying their status; active sources SHALL be refused. Active-backfill members SHALL be ineligible. Completed-backfill copies SHALL require verified tag filtering and original-selection preservation.

#### Scenario: Retry created a child
- **WHEN** someone subsequently requests `logs` without selecting a child
- **THEN** the bot SHALL retrieve the original bound failure.

#### Scenario: Source is active or belongs to an active backfill
- **WHEN** either retry mode is requested
- **THEN** the system SHALL show the state and refuse execution.

### Requirement: Two explicit retry modes
For eligible sources, `retry` SHALL offer whole-run re-execution and fresh copy. Both SHALL create a new run ID using current deployed code. Whole-run re-execution SHALL preserve supported parent/root lineage and execute the complete eligible source selection. Fresh copy SHALL create a new Dagster root with source provenance in supported run tags. Neither mode SHALL resume from failure by reusing successful outputs as retry inputs.

#### Scenario: Requester chooses a mode
- **WHEN** the requester confirms an eligible retry
- **THEN** execution SHALL follow the selected lineage semantics and complete approved selection, explaining that fresh-copy retry budgets can differ from linked re-execution.

### Requirement: Faithful logical inputs and effects
Both modes SHALL preserve complete logical configuration, partition intent, op/step selection, asset selection, asset-check selection and applicable user/business/policy tags through supported inputs. Null and empty selections SHALL remain distinct. Lineage or provenance SHALL NOT substitute for selection preservation. The preview SHALL show complete partition-range effects; one run SHALL NOT imply one asset or partition. Unsupported source intent SHALL be refused rather than narrowed or broadened.

#### Scenario: Source has subsets or a partition range
- **WHEN** either retry mode is prepared
- **THEN** the preview and request SHALL reproduce the complete original selections and range, including null/empty distinctions, or refuse execution.

### Requirement: Current code and fresh execution bookkeeping
The system SHALL validate preserved inputs against current definitions without restoring historical images. The adapter SHALL record and recheck schema-supported deployment/definition identifiers and input validation evidence; unverifiable reproduction SHALL disable that action. Configuration or tags forcing historical code SHALL cause refusal, not silent modification. Observable source, code, definition or policy changes invalidating a preview SHALL require new preparation and confirmation.

New runs SHALL retain supported business/retry-policy intent, use fresh execution identity and mode-specific lineage, replace historical bot correlation, and exclude stale retry counters, pending flags, child pointers, resume markers and other invalid execution bookkeeping. Mutable environment/secret references SHALL be described as logical copies, not byte-for-byte historical replay.

Source concurrency-policy tags SHALL remain intact. Previews SHALL explain that the deployed queued coordinator may delay a created run under tag limits; the bot SHALL neither bypass those limits nor equate creation with running.

#### Scenario: Deployment changes after preparation
- **WHEN** fresh checks detect changed definitions or inputs that no longer validate
- **THEN** the system SHALL invalidate the preview and require new preparation/confirmation; these checks SHALL NOT claim atomic protection from a deployment after the final read.

#### Scenario: Historical inputs conflict with latest code
- **WHEN** an image or launcher override would select historical code
- **THEN** the bot SHALL refuse until separately reviewed supported input is available.

### Requirement: Dagster owns automatic retry policy
The system SHALL inspect descendants and retry-pending state during preparation and immediately before dispatch. Queued, running or pending automatic retries SHALL block manual duplicates. Prior automatic recovery SHALL require fresh preparation explicitly acknowledging it. The bot SHALL NOT disable retry policy, reset counters to satisfy its concurrency limit, invent unverified remaining attempts, or implement an independent automatic retry loop.

The audited instance defaults are DEV `max_retries: 2`, PROD `max_retries: 3`, and both `retry_on_asset_or_op_failure: false`. Previews and completion checks SHALL use verified effective policy, including run-level overrides and failure classification, rather than assume every failure retries or these defaults always apply. DEV fixtures SHALL cover an op/dbt failure without automatic retry and an induced worker-crash retry family, including its pending-child gap. Run monitoring SHALL remain authoritative for transitions it causes; its audited settings are a 600-second start timeout, zero resume attempts, and 120-second polling.

#### Scenario: Automatic child has not appeared
- **WHEN** Dagster reports an automatic retry pending
- **THEN** the bot SHALL report that state and refuse a parallel manual retry even without a child run ID.

#### Scenario: Ordinary code failure has no automatic retry
- **WHEN** effective policy and authoritative run evidence confirm no retry is pending for an op/dbt failure
- **THEN** the bot SHALL offer an eligible manual retry without inventing a pending child or unused-attempt count.

### Requirement: Actual run-family completion
An operation SHALL track its new primary and actual automatic-retry descendants, reporting relevant IDs and pending gaps. Unrelated historical-root branches SHALL NOT determine completion. Uncertain pending-versus-exhausted retry state SHALL remain unresolved.

#### Scenario: Primary fails before an automatic child appears
- **WHEN** an automatic retry remains pending
- **THEN** the operation SHALL remain active and continue observing descendants.

### Requirement: Graceful cancellation with observed outcome
Cancellation SHALL require requester confirmation for one exact supported active/queued run, normal termination and observed status tracking. Ambiguous descendants SHALL require explicit selection. Acknowledged termination SHALL NOT be reported as canceled until Dagster confirms that outcome.

#### Scenario: Target finishes before cancellation dispatch
- **WHEN** fresh checks find the exact target already terminal
- **THEN** the system SHALL return its actual outcome without sending termination or claiming that the bot canceled it.

#### Scenario: Target remains active after termination request
- **WHEN** Dagster acknowledges termination without a canceled status
- **THEN** the bot SHALL report cancellation pending and observe the eventual outcome.

### Requirement: Serialized automation desired state
Schedule/sensor start/stop SHALL preview and confirm one definition's desired state, serialize requests per target and preserve sensor cursors. After ambiguous responses, reads SHALL serve observation only; the old approval SHALL NOT authorize another mutation. Further changes SHALL require resolved uncertainty, fresh preparation/confirmation and the normal mutation gate. Unresolved older requests SHALL block contradictory dispatch until resolved or isolated.

Every automation/cancellation mutation SHALL obey the runtime boot gate and one-invocation rule. A lost response SHALL disarm mutations; observing a value once SHALL NOT prove an older request cannot arrive later. Lost process state SHALL require operator reconciliation before another mutation.

Previews SHALL explain that enabling automation can create multiple future runs outside the bot's single-family slot. Stopping SHALL NOT imply cancellation of existing runs. The bot SHALL observe external UI/daemon changes without claiming atomic exclusion or instance-wide concurrency control, including over automatic retries.

#### Scenario: Automation already has the requested state
- **WHEN** fresh evidence matches the requested state and no older operation is unresolved
- **THEN** the system SHALL return the observed state without a mutation; it SHALL NOT claim future schedule/sensor runs were created or existing runs canceled.

#### Scenario: Stop follows an uncertain start
- **WHEN** the older start may still take effect
- **THEN** the target SHALL remain occupied until that request is resolved or isolated before stop dispatch.

### Requirement: Shared command workflow
All entry points SHALL share typed command, preparation, authorization, confirmation, dispatch and reconciliation contracts. Actor context SHALL include transport, authenticated principal, authorized environment/scope and target context. Slack supplies verified membership/root evidence; the live admin panel supplies an authenticated, server-scoped session. DEV mock identities SHALL never authorize a live backend. Phase one SHALL interpret supported commands and render evidence deterministically without model-provider calls or credentials. Unsupported prose SHALL produce clarification/usage, not fabricated interpretation. Model-provider selection while disabled SHALL report unavailable without network access. Future language interpretation SHALL retain the same authorization, tool and execution boundaries.

Graph/model tools SHALL expose only authorized named reads and application-owned action preparation; they SHALL NOT choose actor/scope/destination, execute arbitrary GraphQL, dispatch mutations, consume confirmation or arm recovery. The graph MAY request preparation and return an opaque proposal reference and sanitized preview; confirmation and mutation dispatch SHALL execute separately from the graph. Graph completion/retry/resume SHALL NOT count as approval. Messages, logs and summaries SHALL remain untrusted data; model claims SHALL distinguish Dagster-backed facts from hypotheses. Ambiguous targets SHALL require clarification; invented identifiers SHALL fail normal target validation.

#### Scenario: No model service is configured
- **WHEN** a supported command enters from the admin panel or Slack
- **THEN** the shared workflow SHALL provide deterministic interpretation, scoped tool results and evidence-based responses without model credentials or provider calls.

#### Scenario: Future language adapter proposes retry
- **WHEN** its intent enters the application
- **THEN** the originating transport's authenticated actor, verified target, current authorization, preview and confirmation SHALL still be required.

#### Scenario: Retrieved text tries to authorize execution
- **WHEN** a message, error, summary or interpreter output says to bypass checks or reports an unverified successful run
- **THEN** it SHALL neither create execution authority nor be treated as a verified outcome; only the shared policy and Dagster evidence SHALL establish them.

### Requirement: Typed Dagster tool catalog
Named tools SHALL expose validated inputs/outputs, descriptions and explicit read/preparation classification. Phase one SHALL select tools deterministically; future model selection SHALL use the same allowed catalog. Reads SHALL return sanitized structured evidence; preparation SHALL produce only bounded in-memory proposals. Authenticated actor, endpoint, credentials, destination and policy scope SHALL come from trusted server runtime context, never tool inputs or model-mutable state. Every call SHALL enforce scope and freshness through shared services. Raw GraphQL and internal mutation executors SHALL NOT be registered as graph/model tools.

Repeated preparation for the same current-boot request and intent SHALL return its retained proposal/status; expiry, changed intent or missing state SHALL follow normal fresh-request rules. Preparation SHALL have no automatic retry policy and SHALL NOT expose confirmation nonces, full execution configuration or dispatch authority in graph state or model-visible results. Required preview target, mode, sanitized inputs, effects and warnings, plus operation outcome fields, SHALL be rendered directly from immutable application proposals/results. Interpreter/formatter prose MAY supplement but SHALL NOT alter or replace those fields.

#### Scenario: A proposed tool call supplies execution authority
- **WHEN** an interpreter requests an unknown tool, raw mutation, forged actor/scope, or an already expired proposal
- **THEN** the application SHALL reject that input without dispatching or recreating approval.

### Requirement: GraphQL evidence replaces a bot database
The bot SHALL use supported GraphQL queries and created-run tags for execution evidence, without a separate database, direct Dagster-storage access or a fabricated persistent operation store. The adapter SHALL support complete bounded discovery by stable installation, request, operation and source provenance, with validated job/configuration/selection, mode and lineage. Discovery SHALL include active runs and terminal parents with pending retries. Missing pages, inaccessible metadata, retention gaps or ambiguous tag/lineage matches SHALL defer launch rather than approximate absence. Tag-input and query support SHALL be contract-tested before launch capability is enabled.

#### Scenario: A request was submitted before process restart
- **WHEN** current authorized lookup positively matches its tagged Dagster run
- **THEN** the system SHALL return verified run facts without replaying submission or recreating lost confirmation authority; if no match can be established, it SHALL report unknown/unavailable rather than never submitted.

### Requirement: Previous attempts require explicit review
Before either retry mode, the system SHALL inspect bot-created attempts for the source across both modes, alongside the source's automatic retry family. A mode switch SHALL NOT bypass active, pending or unknown prior work. A positively matched original request SHALL return its existing result. A deliberately new request after conclusive completion SHALL show previous attempts and require explicit repeat acknowledgement in a fresh preview; changed prior-attempt evidence SHALL invalidate confirmation. These are evidence checks, not atomic exclusion of external Dagster activity.

#### Scenario: Fresh copy follows a linked re-execution
- **WHEN** the earlier bot-created family remains queued, active, pending retry or unresolved
- **THEN** the fresh-copy action SHALL be refused or deferred; changing lineage mode SHALL NOT create a parallel attempt.
