# Operation Authorization

## Purpose

Define the authorization and confirmation guarantees for operating Dagster from allowed Slack channels while preserving the human actor and exact approved action.

## ADDED Requirements

### Requirement: Authenticated and allowlisted request context
The system SHALL authorize requests only from authenticated Slack transport for the configured app and workspace, an allowlisted invoking channel, a bot that is a channel member, and a current human channel member. The target channel and root SHALL match the authenticated invocation, the Dagster target SHALL be within the configured environment and code-location/repository scope, and the capability SHALL be enabled. Unauthorized contexts SHALL be refused before reading or changing Dagster data. Being invited to a channel SHALL NOT itself grant that channel access.

#### Scenario: Bot is invited to an unapproved channel
- **WHEN** a person invokes the bot in a channel outside the configured allowlist
- **THEN** the system refuses the operation before accessing Dagster data even if the bot is a channel member

#### Scenario: App or workspace identity differs
- **WHEN** an otherwise valid-looking command identifies another workspace or application
- **THEN** the system refuses it without trusting message-provided identity or target values

#### Scenario: Target escapes the configured scope
- **WHEN** a command or interaction attempts to choose another channel, thread, Dagster environment, endpoint, code location, or repository
- **THEN** the system refuses the target rather than treating parsed input as authorization

### Requirement: Membership determines ordinary channel permissions
The system SHALL give all verified members of an allowed channel the same ordinary operation permissions without an additional hidden engineering role or separate human Teleport login. It SHALL verify the requesting person's membership over the complete paginated membership result rather than treating the bot's membership as proof. The default channel policy SHALL permit only company-controlled, non-Slack-Connect channels; changes to that policy SHALL require explicit configuration. A publicly joinable allowed channel SHALL grant the same rights to newly joined verified members.

#### Scenario: Member appears on a later membership page
- **WHEN** the requester is present only on a later page of the channel's membership result
- **THEN** the system evaluates the complete result and recognizes the requester's membership

#### Scenario: Bot belongs to a channel but the requester does not
- **WHEN** the token holder is a member and the human requester is not
- **THEN** the system refuses the human request

#### Scenario: Slack Connect channel has not been explicitly permitted
- **WHEN** a command originates in a shared channel under the default non-shared-channel policy
- **THEN** the system refuses operational access

### Requirement: Fresh authorization at mutation dispatch
Read authorization SHALL use membership evidence no older than 30 seconds. Each mutation SHALL require a fresh requester-membership and policy check immediately before execution, and final prechecks SHALL be refreshed after a wait longer than five seconds before dispatch authorization. An unavailable or incomplete membership check SHALL deny or defer the action without executing it. Revocation observed before dispatch SHALL invalidate the affected confirmation. These checks SHALL NOT be represented as an atomic guarantee against subsequent changes in Slack membership.

#### Scenario: Requester is removed after confirming
- **WHEN** the requester leaves or is removed from the channel before the mutation's final authorization check
- **THEN** the system denies the action and invalidates its pending execution authority
- **AND** it does not call the Dagster mutation

#### Scenario: Slack membership cannot be verified
- **WHEN** a required membership lookup fails or remains incomplete
- **THEN** the system defers or refuses the mutation without using stale membership as permission

#### Scenario: Execution waits after prechecks
- **WHEN** more than five seconds elapse between final prechecks and dispatch authorization
- **THEN** the system repeats the authorization and required target-state checks before authorizing dispatch

### Requirement: Requester confirmation for each mutation
The system SHALL require explicit confirmation by the original requester for every run launch, re-execution, materialization, exact-run cancellation, schedule/sensor desired-state change, and manual thread binding. Authorized reads SHALL require no confirmation. Each proposal SHALL show its exact target and effect, including future-run effects for enabling schedules or sensors. Another authorized member MAY create an independent request but SHALL NOT confirm, change mode, or cancel the requester's proposal.

#### Scenario: Requester approves a launch preview
- **WHEN** the original requester confirms an unexpired preview for a supported run action
- **THEN** the system authorizes only the exact prepared action subject to dispatch-time revalidation

#### Scenario: Another member clicks the requester's control
- **WHEN** another authorized channel member attempts to confirm or change the requester's proposal
- **THEN** the system gives that person a private denial without mutating the shared card or consuming the requester's approval opportunity

#### Scenario: Person enables an automation definition
- **WHEN** the system prepares a schedule or sensor start action
- **THEN** the preview names the definition and desired state and explains that future runs may be created outside the bot's single-launch slot

### Requirement: Five-minute single-use confirmation binding
Each confirmation SHALL expire five minutes after its preview is prepared and SHALL be bound to its operation, requester, workspace, channel, root thread, saved bot-owned card, chosen mode, immutable request and preview identities, and single-use interaction reference. The system SHALL accept the confirmation only when all bindings, expiry, and current operation state match, and SHALL consume it at most once. Only opaque operation and interaction references SHALL be accepted from controls; client-supplied roles, user IDs, targets, or configuration SHALL NOT override the saved proposal.

#### Scenario: Two deliveries confirm one proposal
- **WHEN** a person double-clicks Confirm or Slack redelivers the same interaction
- **THEN** at most one confirmation transition succeeds and later deliveries resolve to the same operation
- **AND** no additional launch authority is created

#### Scenario: Confirmation expires
- **WHEN** a person attempts to confirm at or after the five-minute expiry
- **THEN** the system refuses the expired proposal and requires preparation of a new preview

#### Scenario: Payload is altered
- **WHEN** an interaction changes any bound actor, context, target, mode, card, or preview identity
- **THEN** the system refuses the interaction without executing the modified action

### Requirement: Immutable approved action and renewed consent
The system SHALL execute only the action described by the approved preview. A material change to target, configuration, policy, source state, selection, or observable code identity SHALL invalidate that approval and require renewed preparation and confirmation. A pre-confirmation mode change SHALL issue a new preview and interaction reference and invalidate earlier ones. Saved actions SHALL NOT be changed by edited Slack text, delayed mode-selection clicks, or a future interpretation interface.

#### Scenario: Current definition makes the preview stale
- **WHEN** dispatch revalidation identifies a material observable definition or configuration change
- **THEN** the system returns to preparation and requires confirmation of the changed preview before executing

#### Scenario: Old controls follow a mode change
- **WHEN** the requester changes the retry mode and later uses the earlier mode's confirmation control
- **THEN** the system rejects the superseded control

### Requirement: Authorization revocation blocks new disclosure
The system SHALL re-evaluate applicable destination policy before delivering operational data. If the channel is removed from the allowlist, the bot loses channel membership, or the destination is otherwise no longer authorized, it SHALL stop new dispatch and new data delivery there, preserve the operation's audit and result, and signal the delivery incident through independent operator monitoring. It SHALL NOT redirect evidence to an arbitrary direct message or different channel. Loss of delivery permission SHALL NOT replay, erase, or falsely report the associated Dagster operation.

#### Scenario: Channel access is revoked after run creation
- **WHEN** a run has been created and the bot is removed from the channel before a result is delivered
- **THEN** the system stops delivery to that channel and preserves the operation's monitoring and audit state
- **AND** it signals the delivery incident without launching a replacement run or rerouting logs

#### Scenario: Channel is removed from the allowlist
- **WHEN** previously permitted notification work is pending for a channel that is no longer allowed
- **THEN** the system blocks its data delivery and subsequent operations even if the bot can still technically post there

### Requirement: Human attribution and service identity remain distinct
The system SHALL use its configured backend service identity for Dagster access and retain the authenticated Slack human, workspace, channel, thread, operation, target, decision, and state changes in protected audit records. It SHALL NOT infer credentials, actor identity, or backend authority from message text. Audit outputs SHALL exclude secrets and raw error bodies and SHALL support investigating a human action separately from the shared service account.

#### Scenario: Two people use the same backend identity
- **WHEN** two authorized people prepare separate actions through the bot
- **THEN** each action remains attributable to its own authenticated Slack requester despite sharing the Dagster service identity

#### Scenario: Command contains claimed credentials or role
- **WHEN** command text claims a privileged role, another user identity, or a different backend credential
- **THEN** the system does not treat those claims as authority or write their secret content into the audit trail

### Requirement: Ordinary controls cannot resolve uncertain submissions
The system SHALL reserve resolution of an uncertain launch for the defined owner/operator recovery procedure. Ordinary Slack channel membership or an old confirmation SHALL NOT authorize resetting a dispatched operation, clearing unresolved execution authority, or sending another automatic launch. A replacement attempt after a resolved incident SHALL require a new explicitly confirmed operation with an auditable recovery decision.

#### Scenario: Channel member retries an unresolved submission
- **WHEN** an ordinary user attempts to reconfirm or reset an operation whose launch outcome remains uncertain
- **THEN** the system reports the unresolved state and does not issue another launch

#### Scenario: Operator resolves an incident and a new launch is desired
- **WHEN** the documented recovery procedure has conclusively resolved the old dispatch and a person wants a new attempt
- **THEN** the system requires a new operation and confirmation rather than reusing the previous dispatch authority

### Requirement: No arbitrary control-plane actions
The system SHALL expose only the named, enabled capability catalog. Phase one SHALL exclude arbitrary GraphQL input, shell commands, direct Dagster storage access, bulk retries, multi-partition backfills, run deletion, asset wiping, code-location reload or shutdown, forced run-status changes, force termination, schedule-definition editing, sensor-cursor editing, dynamic-partition mutation, arbitrary Slack-pasted configuration, and external log-system investigation. Parsing a syntactically valid request SHALL NOT bypass a scope boundary.

#### Scenario: User requests an excluded operation
- **WHEN** an authorized person requests arbitrary GraphQL, a force termination, a bulk rerun, or another excluded action
- **THEN** the system refuses the action and points to the supported catalog without executing an alternative with broader effects

### Requirement: Environment isolation and reusable policy boundary
Each bot deployment SHALL accept only its configured environment, Slack application identity, credentials, allowed channels, and Dagster target scope. DEV and PROD SHALL remain isolated; commands SHALL NOT choose another endpoint or environment. All current and future interfaces SHALL apply the same authorization, target-resolution, preparation, and confirmation guarantees. Phase two interpretation SHALL NOT be able to supply trusted actor context or bypass those controls.

#### Scenario: Production command requests a development endpoint
- **WHEN** a command attempts to override the production bot's configured environment or endpoint
- **THEN** the system refuses the override instead of switching credentials or backend targets

#### Scenario: Future interface supplies a forged actor
- **WHEN** an interpretation interface supplies a user ID or permission claim inconsistent with authenticated transport context
- **THEN** the system rejects that supplied authority and enforces the same operation policy as the phase-one Slack interface
