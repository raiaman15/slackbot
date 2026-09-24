## Purpose

Define the private same-cluster Dagster connection, simple resilient FastAPI deployment, supervised asynchronous lifecycle, and operational readiness required for critical Slack control.

## ADDED Requirements

### Requirement: Direct private Dagster service access
The FastAPI application SHALL access the existing Dagster webserver `/graphql` endpoint through its configured private Kubernetes Service in the same cluster. The endpoint SHALL include the actual scheme, Service name, namespace, cluster domain, Service port, and any webserver path prefix. Dagster SHALL retain responsibility for run creation, coordination, execution, and retries. The bot MUST NOT depend on an employee's Teleport session, port forward, localhost, pod IP, public UI URL, local job execution, shared application code, or a new Dagster-side API.

#### Scenario: Applications run in different namespaces
- **WHEN** the FastAPI workload needs to query a run
- **THEN** it SHALL use the verified namespace-qualified private Service endpoint and the actual Service port, without requiring a human tunnel.

#### Scenario: Slack requester launches a run
- **WHEN** the approved operation reaches dispatch
- **THEN** FastAPI SHALL invoke the supported Dagster API and observe the resulting run, while Dagster's existing launcher performs execution.

### Requirement: Fixed destination and explicit internal authentication
The bot SHALL use a fixed deployment-configured Dagster endpoint and the existing supported workload authentication mechanism. Slack text MUST NOT choose hosts, credentials, or backend identity. Same-cluster placement and Kubernetes service-account identity SHALL NOT be assumed to authenticate HTTP automatically. Existing mesh or proxy authentication SHALL be retained; network-only HTTP SHALL require a verified approved network boundary. HTTPS SHALL verify certificates and the adapter SHALL NOT follow redirects to another endpoint.

#### Scenario: Deployment uses an internal authenticated proxy
- **WHEN** the bot connects to Dagster
- **THEN** it SHALL supply the supported workload credentials and preserve the proxy's protections without inventing a separate authentication scheme.

#### Scenario: Slack input contains a different URL
- **WHEN** a command or root alert includes that URL
- **THEN** it MUST NOT override the configured backend destination or cause an arbitrary network request.

### Requirement: Keep Dagster private with verified network paths
Deployment SHALL preserve an internal Dagster Service without a new public ingress, NodePort, or LoadBalancer for bot access. Required paths SHALL cover cluster DNS, bot-to-Dagster traffic, PostgreSQL, outbound Slack HTTPS/WebSocket connectivity or the approved proxy, and required observability/secret systems. The network owner SHALL verify effective source egress and destination ingress using real labels, namespaces, identities, ports, and existing additive policies while preserving required Dagster component access.

#### Scenario: Network policies isolate both applications
- **WHEN** production access is configured
- **THEN** the actual FastAPI workload SHALL be able to reach the selected webserver on its required port and unapproved workloads SHALL remain unable to use a network-only protected endpoint.

#### Scenario: Operator adds a restrictive policy beside a broad allow policy
- **WHEN** validating effective access
- **THEN** the deployment SHALL account for policy additivity and MUST NOT claim that the new policy alone removes existing broad access.

### Requirement: Outbound Slack transport by default
The production default SHALL use Slack Socket Mode with outbound authenticated connectivity, explicit mention and interaction dispatch, and no public FastAPI ingress requirement. HTTP event delivery MAY be enabled as an alternative only with the same acceptance semantics and verified Slack request signatures and timestamps. Both transports SHALL use the same application policy and durable execution services.

#### Scenario: Company permits outbound Slack connections but no inbound webhooks
- **WHEN** the app is deployed using its default transport
- **THEN** mentions and button interactions SHALL work through Socket Mode without exposing `/slack/events` publicly.

### Requirement: One combined FastAPI deployment
Production SHALL run one bot application deployment with two identical replicas, each owning its Slack intake and bounded supervised background work in one application process. It SHALL reuse the company's durable PostgreSQL platform with a dedicated bot database/role. The initial architecture SHALL NOT require a separate worker service, broker, leader-election service, MCP server, or redundant API gateway. Coordination SHALL remain correct when a rolling update briefly adds another replica; DEV MAY use one replica.

#### Scenario: Production rollout overlaps old and new pods
- **WHEN** three bot pods temporarily coexist
- **THEN** shared durable coordination SHALL maintain deduplication and the single active family invariant while intake and safe work continue.

#### Scenario: One production pod restarts
- **WHEN** another healthy replica and dependencies remain available
- **THEN** the surviving replica SHALL continue supported intake and recoverable work without depending on memory in the stopped pod.

### Requirement: Supervised asynchronous startup and work
The FastAPI lifespan SHALL validate configuration and migrated schema, create long-lived asynchronous clients, verify Slack bot identity, recover durable work, and start supervised work before opening intake. Database migrations SHALL run as one controlled deployment step rather than racing per-pod startup. Shared-loop database and HTTP access SHALL be nonblocking, and all accepted work SHALL already have durable state before background execution.

#### Scenario: Durable schema has not been migrated
- **WHEN** a pod starts
- **THEN** it SHALL remain unready and SHALL NOT accept work into an incompatible durable store.

#### Scenario: A background executor receives an accepted operation
- **WHEN** it begins processing
- **THEN** the operation SHALL survive a process crash independently of untracked tasks, in-memory queues, or FastAPI background-task completion.

### Requirement: Bound background load and preserve receipt capacity
The runtime SHALL bound database acquisition, lock/statement duration, external request time, concurrency, response size, and background work. Receipt persistence SHALL have reserved capacity and an initial 500 ms transaction deadline. Reconciliation of active/unknown runs SHALL take priority over ordinary read load. Database transactions MUST NOT be held open while waiting for Slack or Dagster network calls. Pool and concurrency values SHALL be validated for normal replicas and rollout overlap.

#### Scenario: Users request large errors while a run is uncertain
- **WHEN** safe-read work reaches its concurrency bound
- **THEN** acknowledgement persistence and active/unknown-run observation SHALL retain capacity instead of being starved by read work.

#### Scenario: Dagster responds slowly
- **WHEN** a worker waits on the bounded external call
- **THEN** it SHALL NOT hold an open database transaction that blocks receipt or confirmation processing.

### Requirement: Bounded read retries and no launch retries
The Dagster client SHALL reuse asynchronous connections, enforce bounded deadlines and response sizes, and allow only explicitly named read operations to use bounded retries with jitter. Launch and re-execution SHALL have zero automatic transport retries throughout the client, SDK, application, mesh, and proxy. POST method alone MUST NOT determine retryability because reads and writes both use GraphQL POST. Cancellation or timeout after dispatch SHALL follow unknown-outcome reconciliation.

#### Scenario: Read query experiences a transient network failure
- **WHEN** its retry budget and deadline permit another attempt
- **THEN** the adapter MAY repeat that named read safely.

#### Scenario: Launch connection closes unexpectedly
- **WHEN** the request may have reached Dagster
- **THEN** the adapter SHALL report uncertainty without transparent retry at any configured layer.

### Requirement: Critical-loop failure cannot leave a healthy idle server
Liveness SHALL reflect the process, event loop, and critical-loop supervision without downstream network calls. Readiness SHALL require initialization, a writable durable store, running critical loops, and an established Slack session. An unexpected critical-loop exit SHALL close intake, mark unhealthy, and terminate for restart. Dependency outages SHALL use bounded backoff and capability degradation rather than restart storms.

#### Scenario: Background receiver or executor unexpectedly exits
- **WHEN** the HTTP server could otherwise continue responding
- **THEN** the process SHALL stop intake and fail health rather than appear healthy while commands cannot be processed.

#### Scenario: Dagster becomes unavailable
- **WHEN** the bot and durable store remain healthy
- **THEN** compatible help, operation history, and recovery reporting SHALL remain available while Dagster-dependent capabilities report degradation.

#### Scenario: Database is not writable
- **WHEN** receipt durability cannot be established
- **THEN** the application SHALL explicitly pause or disconnect Socket intake and raise independent health signals; a readiness change alone SHALL NOT be treated as stopping the outbound connection.

### Requirement: Graceful draining preserves execution evidence
On shutdown the application SHALL mark unready, stop Socket intake and new work claims, drain bounded in-flight work, preserve dispatch markers and late response evidence, and close clients last. Forced termination SHALL be recoverable through durable state. Stopping a bot pod MUST NOT cancel a Dagster run or authorize a replacement dispatch.

#### Scenario: Pod receives termination during a launch
- **WHEN** a definitive response cannot be recorded before shutdown
- **THEN** the operation SHALL remain discoverable for unknown-submission recovery and the replacement pod MUST NOT resend it.

### Requirement: Minimal Kubernetes and filesystem privileges
The bot SHALL run without Kubernetes resource-mutation, Pod/Job creation, `exec`, or cluster-admin roles, host mounts, or shared Dagster-storage mounts. It SHALL use non-root execution, dropped Linux capabilities, restricted secrets, and a read-only image filesystem where practical. Automatic service-account-token mounting SHALL be disabled unless the actual workload-identity mechanism requires it. Infrastructure changes SHALL be performed by deployment operators, not Slack commands.

#### Scenario: Bot sends a normal Dagster launch request
- **WHEN** execution requires Kubernetes Jobs
- **THEN** Dagster's launcher SHALL use its own existing permissions and the bot SHALL require only its permitted API connectivity.

### Requirement: Isolated configuration and honest human links
DEV and PROD SHALL use distinct Slack apps/tokens, allowed channels, backend endpoints, identities, and durable configuration. Required deployment values SHALL be explicit and secrets SHALL use the platform secret mechanism rather than committed manifests. Internal diagnostic endpoints SHALL be access-restricted and MUST NOT trust a caller-provided Slack user ID. Human run links SHALL use a verified employee-accessible convention or show a full run ID with established access guidance instead of presenting a cluster-only URL as usable from a laptop.

#### Scenario: PROD user asks for a DEV run
- **WHEN** the input contains a DEV selector or endpoint
- **THEN** the PROD app SHALL refuse it rather than switch configurations.

#### Scenario: No stable human-access URL exists
- **WHEN** the bot reports a run
- **THEN** it SHALL provide the full run ID and approved existing access guidance without fabricating a clickable Dagster link.

### Requirement: Availability and independent observability
Production scheduling SHALL support node spreading, at least one available replica during voluntary disruption, and a rolling update with spare capacity. The system SHALL retain independent alarms for zero Slack sessions, durable-store failure, critical schema mismatch, unknown submission, stuck retry-pending state, and stale observations. The bot MUST NOT be its own sole incident-notification channel. Operational documentation SHALL acknowledge the shared cluster failure domain and preserve existing Dagster UI/Teleport fallback.

#### Scenario: Cluster or outbound network fails completely
- **WHEN** both bot replicas lose service
- **THEN** the independent monitoring path SHALL detect the outage, and the system MUST NOT claim that two replicas provide operation across that failure domain.

### Requirement: Measured service objectives
Under normal dependency health the system SHALL be tested against acknowledgement p99 below two seconds and within Slack's three-second deadline, status p95 below five seconds, bounded error response p95 below ten seconds, safe-work recovery below 60 seconds, active observation every 15 seconds with jitter, terminal notification p95 within 30 seconds, and uncertainty alerting within 60 seconds. Monthly command availability SHALL initially target 99.9% end to end, count dependency failures, and use synthetic read-only probes at low traffic. These SHALL be measured acceptance objectives, not unverified guarantees or a promised uncertainty-resolution deadline.

#### Scenario: Command availability is reported
- **WHEN** dependency outages occurred during the measurement interval
- **THEN** the end-to-end metric SHALL include them and SHALL separately expose component causes without excluding them from the overall figure.

### Requirement: Verify deployment facts before enabling writes
Dispatch SHALL default disabled until the actual FastAPI workload context proves private Service DNS/path/port/auth access, a schema-compatible GraphQL read for the expected code location, effective network restrictions, and absence of launch retries. DEV SHALL verify real same-thread logs/status, both retry modes and faithful selections, automatic retry-family behavior, returned run tracking, restart/dispatch failures, and failover recovery. PROD SHALL start read-only, then enable only capabilities that passed their contract gates. Rollback SHALL disable new dispatch while preserving ledger, slot, observation, and notification work where possible.

#### Scenario: A debug pod reaches Dagster but the FastAPI identity is untested
- **WHEN** production launch readiness is evaluated
- **THEN** the debug result SHALL be insufficient and dispatch SHALL remain disabled until the actual workload context is verified.

#### Scenario: Operators roll back the bot release
- **WHEN** rollback is initiated while a Dagster family is active
- **THEN** new dispatch SHALL stop while durable operation/slot state is preserved and existing Dagster runs continue under Dagster control.
