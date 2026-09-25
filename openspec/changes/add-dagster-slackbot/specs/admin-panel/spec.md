# admin-panel Specification

## Purpose

Provide a small, production-usable administration panel and an independent DEV test interface. Browser and Slack interactions share the same command workflow and guarded Dagster services.

## ADDED Requirements

### Requirement: One deployable application

The application SHALL serve the panel at `/admin`, its static assets, and typed endpoints under `/api/v1`, using the canonical API contract in the design. It SHALL require no separate frontend framework, build pipeline or deployment. Navigation and ordinary forms SHALL use semantic HTML; small browser scripts SHALL add filtering, previews and status refresh. With `SLACK_ENABLED=false`, the application SHALL require no Slack credentials, calls or background tasks. Slack implementation and acceptance SHALL remain a separate workstream.

#### Scenario: Slack integration is not installed
- **WHEN** the panel starts with Slack disabled and valid application configuration
- **THEN** users can inspect Dagster and exercise permitted commands without any Slack dependency.

### Requirement: Explicit environment and mode

`ADMIN_UI_ENABLED` SHALL control panel and browser API registration. Disabled routes and assets SHALL return 404. `APP_ENV` SHALL be `dev` or `prod`; `DAGSTER_MODE` SHALL be `mock` or `live`, fixed at startup. Production mock mode SHALL fail startup. Live mode SHALL use the configured environment's single endpoint and scope. Browser requests SHALL NOT supply endpoints, credentials, environment overrides or arbitrary GraphQL. An always-visible badge SHALL show environment, mode and mutation availability. Production SHALL have a distinct textual `PRODUCTION` badge, not color alone.

#### Scenario: A browser attempts to change backend
- **WHEN** a request supplies another environment, URL, mock actor or unsupported execution parameter
- **THEN** the backend rejects it before contacting Dagster.

### Requirement: Individual identity and least privilege

Production access SHALL require company OIDC authorization-code authentication with PKCE, using a fixed issuer and validated signatures, audience, issuer, expiry, nonce, state and browser binding. Roles SHALL come from configured exact issuer/subject allowlists: `VIEWER` permits scoped reads and `OPERATOR` additionally permits enabled mutation preparation/confirmation; unmatched identities SHALL be denied. Browser-supplied usernames, roles, Slack identities or channel membership SHALL NOT establish authority. Neither role SHALL permit runtime arming, recovery overrides or excluded Dagster operations. Identity, current role and scope SHALL be checked on every request and within five seconds before dispatch. Production SHALL NOT use shared passwords or mock identities. Access SHALL remain private through approved TLS ingress or Teleport access. Missing authentication configuration SHALL block panel access without blocking otherwise valid Slack/core capabilities.

#### Scenario: A viewer calls an action endpoint directly
- **WHEN** a verified viewer submits a launch preparation or confirmation without using the interface
- **THEN** the shared authorization service rejects it; hiding buttons SHALL NOT be the security boundary.

### Requirement: Protected browser sessions

Opaque sessions SHALL be server-held in bounded memory and bound to individual identity and the current boot. They SHALL expire after 60 minutes absolute or 15 minutes idle, capped by the verified ID-token expiry. Cookies SHALL be HttpOnly and SameSite=Lax, with Secure required in production. State-changing requests SHALL require CSRF proof and validated Origin and Host; cross-origin API access SHALL be disabled. Production dispatch SHALL require an OIDC `auth_time` within five minutes; reauthentication SHALL require a new preview and SHALL NOT replay the action. Passwords, session secrets and bearer tokens SHALL NOT appear in URLs, browser storage, rendered configuration or logs. OIDC callback codes SHALL be excluded from logs and promptly removed from the address bar by redirect. Operational responses SHALL disable caching. Expiration SHALL show a sign-in-required state. Restart SHALL invalidate sessions and outstanding controls. DEV-only local login or mock identities SHALL never authenticate a production backend.

#### Scenario: A browser request is forged or stale
- **WHEN** its session, CSRF proof, origin, principal or role is invalid
- **THEN** it cannot prepare or confirm a live operation and receives a clear authorization failure without leaking operational data.

### Requirement: Minimal consistent layout

The panel SHALL have a compact header, navigation for `Runs`, `Jobs`, `Assets`, `Automation`, `Activity` and `System`, and one main content area. `Runs` SHALL be the landing page. The header SHALL show application name, environment/mode, signed-in identity and connection state. Each page SHALL contain its title, a short explanation, relevant filters and a table or detail view. A right-side detail panel on desktop SHALL become a full-width view on small screens. Typography, spacing, borders and status badges SHALL be consistent; decorative charts, marketing content and a separate chat application SHALL NOT be required.

#### Scenario: A user opens the panel on a narrow screen
- **WHEN** the viewport cannot show navigation and a detail panel side by side
- **THEN** navigation collapses to an accessible menu and the same actions remain usable without page-level horizontal scrolling.

### Requirement: Complete read catalog

The following views SHALL expose the approved catalog through shared services. Disabled capabilities SHALL explain their missing prerequisite; an empty collection SHALL be distinguishable from an unavailable dependency.

| View | Required content and controls |
|---|---|
| Runs | Scoped runs; failed-only, status, job and time filters; full-ID lookup; status, steps, timing, retry family, structured errors, redacted configuration, selections and tags |
| Jobs | Jobs and repositories, partitioning, validated preset choices and existing partition keys |
| Assets | Scoped asset list, exact asset details, materialization/check state, existing partitions and approved asset-to-job mapping availability |
| Automation | Separate schedules and sensors tabs; definition, current state, ticks and enabled start/stop controls |
| Activity | Current-session proposals and retained operation observations, plus scoped positively identified Dagster run facts |
| System | Safe health, configured scope/version, capabilities, mutation gate and disabled reasons; concise help and reliability limitations |

Run errors SHALL explicitly describe structured Dagster evidence; the interface SHALL NOT imply persisted stdout/stderr exists. External links SHALL use approved configured human-access conventions; localhost alert URLs SHALL never be fetched by the server.

#### Scenario: A user requests unavailable logs
- **WHEN** the run has structured failure evidence but no persisted compute logs
- **THEN** the panel shows the available errors and stacks with the limitation stated, without querying Kubernetes logs or inventing a download.

### Requirement: Complete approved action catalog

An authorized operator SHALL be able to prepare whole-run re-execution or fresh copy from a selected eligible run; launch one job from an approved preset; request graceful cancellation of one exact run; materialize one mapped asset/partition; and start or stop one schedule or sensor. Required partitions SHALL be selected from verified existing keys. Forms SHALL NOT accept arbitrary configuration, raw tags, arbitrary GraphQL or multi-target operations. Read-only or unavailable actions SHALL remain identifiable with a concise reason. Enabling automation SHALL explicitly warn that it can create future runs beyond the bot's single run-family slot.

#### Scenario: An asset lacks an approved mapping
- **WHEN** a user opens its detail view
- **THEN** inspection remains available and materialization is disabled with the missing-mapping reason; the interface SHALL NOT select a broader job automatically.

### Requirement: Shared commands and trusted context

Commands and action-preparation controls SHALL construct typed turns for the same command workflow, deterministic interpretation/formatting, named tools, policies and redaction used by Slack. Browser components SHALL NOT implement authorization, Dagster semantics or a second execution path. Server-issued conversation IDs SHALL be scoped to the authenticated session and selected target, separate from Slack thread identities. A compact command input and response history in the detail view SHALL exercise supported text commands and future follow-ups; it SHALL not replace forms or required preview fields. Changing targets SHALL create a distinct context, never retarget existing proposals. Confirmation SHALL call the shared confirmation service directly, outside the command workflow.

#### Scenario: A user switches from one run to another
- **WHEN** a preview for the first run remains open
- **THEN** it retains its original target and context; a follow-up in the new context cannot confirm or silently modify it.

### Requirement: Immutable action preview

Every mutation SHALL display an application-generated preview containing action, environment, exact target IDs, selected retry mode, sanitized input/selection summary, current code, retry/queue effects, previous attempts and applicable warnings. The preview SHALL identify its requester and expiration. Confirmation SHALL be bound to the current principal, browser session, boot, context and immutable proposal; it SHALL be single-use and expire after five minutes. Changed inputs or evidence SHALL require a new preview. Model/formatter prose SHALL NOT replace mandatory fields. Buttons SHALL be named `Confirm <action>` and `Discard proposal`; run termination SHALL be named `Cancel run` and require its own preview.

#### Scenario: Two users receive the same preview reference
- **WHEN** another principal or browser session attempts confirmation
- **THEN** the shared service rejects it even if that user is an operator.

### Requirement: Honest status and no automatic replay

The panel SHALL label a DISARMED mutation gate as `Read only` and distinguish busy, queued, running, retry-pending, SUBMISSION_UNKNOWN, expired and observed terminal states, using the design's canonical fields rather than a second state model. Each observation SHALL show its timestamp; refresh failure SHALL mark old data stale instead of presenting it as current. Read-only status polling SHALL pause while the page is hidden and back off after errors. A timeout after command/confirmation submission SHALL trigger only observation of the existing reference, never automatic POST replay. Unknown submission SHALL explain that a run may exist and further mutations are disabled. Lost RAM state SHALL display unavailable history; it SHALL NOT imply that Dagster did nothing.

#### Scenario: The confirmation response is lost
- **WHEN** Dagster may have accepted a mutation but the browser did not receive a result
- **THEN** the panel observes the existing reference, shows uncertainty where necessary and offers no repeat-confirm or force-reset action.

### Requirement: Bounded lists and volatile activity

Lists SHALL use server-validated filters, bounded pages and opaque pagination cursors; initial page size SHALL be 25 and maximum 100. A changed filter SHALL reset pagination. Partial results SHALL be labeled and SHALL NOT establish absence for mutation checks. The Activity view SHALL expose only authorized retained entries and their timestamps, with an explicit current-boot/volatile-history notice. Refresh or reconnect MAY recover verified run facts, but SHALL NOT restore sessions, approval authority or a durable command transcript. The interface SHALL contain no export presented as a complete immutable audit.

#### Scenario: A result collection is incomplete
- **WHEN** pagination, a dependency limit or GraphQL failure prevents full results
- **THEN** the page shows its incomplete state and the execution service cannot use that visible subset to authorize a launch.

### Requirement: Accessible and safe interaction

The interface SHALL target WCAG 2.2 AA: semantic landmarks and tables, persistent form labels, sufficient contrast, visible keyboard focus, keyboard-operable menus/dialogs, meaningful accessible names and status announcements. A modal confirmation dialog SHALL move and contain keyboard focus within it; closing SHALL return focus to its trigger. Escape SHALL close the dialog without executing, discarding the proposal or canceling a run; these effects require their explicit controls. Errors SHALL be associated with affected fields and preserve safe user input. Loading, empty, unavailable and unauthorized states SHALL each have distinct copy and a useful next step. Untrusted messages, stack traces and configuration SHALL render as escaped text; links SHALL be allowlisted and no HTML from Dagster or users SHALL execute.

#### Scenario: A run error contains hostile markup
- **WHEN** its structured error is displayed or copied
- **THEN** it remains inert text and cannot execute script, navigate automatically or overwrite an action preview.

### Requirement: Isolated mock testing and production acceptance

DEV mock mode SHALL use sanitized fixtures, fake Dagster responses and mock actors without live calls or credentials. Fixture/fault controls SHALL be DEV/mock-only; live environments SHALL offer no actor impersonation. The mock operator/test harness SHALL commission the fake runtime independently of the UI. Tests SHALL cover roles, sessions, CSRF, escaped content, keyboard/focus behavior, all catalog mappings, target isolation, confirmation ownership/expiry, duplicate clicks, restart, pagination gaps, retry families, queue delays and accepted-but-timeout outcomes. Live DEV acceptance SHALL prove workload connectivity and controlled actions; production acceptance SHALL additionally verify private authentication, real role enforcement, mock exclusion and initial read-only behavior. Slack scopes, trusted alert extraction, membership, Socket Mode and thread rendering SHALL have separate acceptance tests.

#### Scenario: A mock mutation becomes uncertain
- **WHEN** the fake adapter records acceptance and then raises a timeout
- **THEN** the shared runtime disarms mutations, the panel shows uncertainty and only safe reconciliation proceeds without another launch.
