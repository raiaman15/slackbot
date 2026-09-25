# dev-workbench Specification

## Purpose

Provide a small DEV console for exercising the shared bot application against fixtures or live DEV Dagster before Slack integration, while preserving command safety and production isolation.

## ADDED Requirements

### Requirement: Independent DEV delivery

The application SHALL serve a minimal HTML/CSS/JavaScript console through FastAPI at `/dev`, with typed command and operation endpoints under `/dev/api/`. It SHALL require no separate frontend deployment, frontend framework, Slack installation, or Slack credentials. With `SLACK_ENABLED=false`, Slack transport initialization SHALL be skipped; shared application startup and console tests SHALL remain usable. Slack integration SHALL be a separate implementation and acceptance workstream.

#### Scenario: Slack setup is unavailable
- **WHEN** the application starts in DEV with Slack disabled and the required DEV configuration present
- **THEN** the console and shared command services work without connecting to Slack.

### Requirement: Production exclusion and fixed targets

The console SHALL require both `APP_ENV=dev` and `DEV_UI_ENABLED=true`. Enabling it in any other environment SHALL fail startup; otherwise its routes and assets SHALL be absent outside DEV. Access SHALL default to loopback through a developer port-forward or an approved private DEV network, without public ingress. The backend SHALL use an explicitly configured DEV Dagster target; browser requests SHALL NOT supply endpoints, environment overrides, arbitrary GraphQL, or production targets. The console SHALL expose no recovery, mutation-arming, or force-reset control.

#### Scenario: A request attempts to escape DEV scope
- **WHEN** a client supplies a production target, alternate URL, or unsupported execution operation
- **THEN** the backend rejects it before contacting Dagster.

#### Scenario: The console is requested in production
- **WHEN** production starts with the UI enabled, or receives a DEV route request with it disabled
- **THEN** startup fails in the first case and no DEV route or asset is available in the second.

### Requirement: Explicit mock and live modes

The mode SHALL be fixed at startup and continuously displayed. Mock mode SHALL use sanitized fixtures, a fake Dagster adapter, and mock actors, with no real Dagster or Slack calls or credentials. Live DEV SHALL disable fault simulation and actor impersonation; its banner SHALL state that confirmed actions affect real DEV jobs. A browser control SHALL NOT switch modes or bypass the runtime's initial read-only state. Live mutation enablement SHALL use the execution-runtime operator procedure independently of the UI.

#### Scenario: A developer confirms a mock retry
- **WHEN** the fake backend has been commissioned through the operator/test harness and a retry is confirmed in mock mode
- **THEN** the shared flow exercises the fake adapter and displays simulated results without performing a real mutation.

### Requirement: Authenticated sessions and request protection

Live DEV SHALL require a server-authenticated developer session mapped to a configured principal and DEV scope. Browser-supplied names, roles, channel IDs, and Slack IDs SHALL NOT establish identity or authority. Sessions SHALL be short-lived, held only in server memory, and conveyed by HttpOnly, SameSite=Strict cookies, with Secure cookies when HTTPS is used. Restart SHALL invalidate them. State-changing routes SHALL require CSRF protection and validated Origin and Host values. Credentials SHALL NOT appear in URLs, browser storage, rendered configuration, or logs; responses containing operational data SHALL disable caching.

#### Scenario: A browser forges an identity or confirmation
- **WHEN** a request lacks a valid session or CSRF proof, has a disallowed origin, or asserts another user's identity
- **THEN** it cannot create or confirm a live operation.

### Requirement: Shared commands and policies

The console SHALL call the same compiled LangGraph workflow, deterministic model placeholders, typed tools, authorization policies, previews, execution services, redaction, and Dagster adapter used by Slack. A normalized actor context SHALL contain the transport, authenticated principal, authorized scope, and target context. The DEV adapter SHALL establish DEV authorization; the Slack adapter SHALL separately establish Slack membership and trusted thread identity. Confirmation SHALL bypass interpretation and use the shared confirmation service directly. Neither transport SHALL bypass shared safety checks. The UI SHALL render supported capabilities and explicit disabled reasons instead of implementing a second execution path.

#### Scenario: A capability is disabled
- **WHEN** a developer submits a disabled or out-of-scope command through the UI or its API directly
- **THEN** the shared service rejects it with the same policy outcome as other transports.

### Requirement: Run context and conversation view

The console SHALL accept an exact full DEV run ID or a paginated, scope-filtered DEV run selection and display a conversation-style sequence of commands and responses. A sanitized alert fixture MAY exercise parsing, but SHALL NOT assert a live Slack publisher identity or authorize a mutation. The selected target SHALL remain explicit until the user changes it. The console SHALL support the shared command catalog, including status, structured errors, redacted configuration, rerun versus fresh-run selection, previews, confirmations, and operation observation. It SHALL display Dagster evidence through safe text rendering; untrusted text SHALL NOT execute HTML, scripts, or arbitrary links.

The DEV adapter SHALL use server-issued conversation IDs scoped to its authenticated session, separate from Slack identities. Mock fixtures SHALL exercise the versioned context/interpreter contract with follow-ups, concurrent actors, stale or missing history and hostile text without an actual LLM. Live DEV SHALL retain its existing no-impersonation rules; restarting SHALL not promise restoration of DEV dialogue.

#### Scenario: The selected run changes
- **WHEN** a user selects another run while a preview exists
- **THEN** the existing preview remains bound to its original target and cannot execute against the new selection.

### Requirement: Explicit confirmation and volatile state

Every live mutation SHALL require its own requester-bound preview and one-use confirmation, expiring after five minutes and bound to the authenticated session and current process boot. The UI SHALL distinguish cancelling an unsubmitted proposal from requesting cancellation of a Dagster run. It SHALL display read-only, busy, queued, unknown, expired, and observed terminal states accurately. It SHALL poll read-only status endpoints without automatically replaying command submissions or confirmations after timeouts, reconnects, or reloads. Lost operation state SHALL NOT be represented as proof that Dagster did nothing; run facts MAY be reconstructed through scoped GraphQL reads, while uncertain mutations remain governed by runtime recovery rules.

#### Scenario: A confirmation response is lost
- **WHEN** the browser loses the response after submitting a confirmation
- **THEN** it observes the existing operation if available and never automatically submits another mutation.

#### Scenario: The application restarts
- **WHEN** the developer reconnects after a restart
- **THEN** the session and previous controls are invalid, mutations remain disabled, and the UI explains that prior operation history may be unavailable.

### Requirement: Automated verification and transport boundaries

Automated service and HTTP tests SHALL run without Slack and cover authentication, CSRF, production exclusion, target restrictions, redaction, confirmation ownership/expiry, and restart behavior. Mock fixtures SHALL cover ordinary failures, automatic retry families, queue limits, stale inputs, duplicate submissions, truncated pagination, unavailable GraphQL, and a mutation accepted by Dagster whose response is lost. Live DEV acceptance SHALL verify real workload connectivity, structured evidence, and controlled actions using approved fixtures. The console SHALL NOT substitute for Slack tests of scopes, membership, trusted root extraction, Socket Mode delivery, button identity, or thread rendering.

#### Scenario: A simulated launch becomes uncertain
- **WHEN** the fake adapter records acceptance and then raises a transport timeout
- **THEN** the shared runtime exposes uncertainty, prevents further mutations, and attempts only safe reconciliation without another launch.
