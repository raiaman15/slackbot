# Infrastructure audit — 24 September 2026

## Evidence and scope

Consolidated from the original seven photographs (1719–1725) and the six-photo v2 follow-up (1726–1731) of an internal `environment-audit.md`. The audits report Kubernetes/GraphQL inspection and publisher-source review; v2 also attempted and deleted an ephemeral probe. V2 confirms earlier spec fixes, not completion of their infrastructure tasks. **These are reported observations, not live checks repeated during this documentation update.** Infrastructure may drift; implementation must reconfirm its actual workload path.

This public copy omits company cluster names, namespaces, database/registry/Teleport hosts and Slack channel names. The earlier private companion `slackbot-infrastructure-audit-private.md` retains the v1 exact inventory and historical recommendations; this repository supersedes its bot database recommendations. Deployers supply current values through controlled environment configuration. No photos or private inventory belong in this public repository.

| Photo | Evidence covered |
|---|---|
| 1719.jpg | Date, provenance, version, clusters, Services, endpoints, authentication, deployed scope and replica counts |
| 1720.jpg | Launcher, coordinator, retry/monitoring settings, logs, storage, policy, mesh and mutation surface |
| 1721.jpg | Alert publisher, platform conventions, F1–F2 |
| 1722.jpg | F3–F8: identity, retries, logs, deployed naming, database split and API names |
| 1723.jpg | F8–F11, concurrency, webserver availability and copied-repository hygiene |
| 1724.jpg | Remaining owner evidence and proposed amendments |
| 1725.jpg | Original verification procedure and its limits |
| 1726–1727.jpg | V2 baseline, webserver binding/endpoints, full reported queue limits and admission policies |
| 1728–1729.jpg | Prior findings covered in specs; G1–G6 owner/evidence gaps remain |
| 1730–1731.jpg | Residual risks, negative cross-namespace timeout, unsuccessful ephemeral probe and cleanup |

## Reported environment baseline

| Contract | DEV | PROD |
|---|---|---|
| Dagster version | 1.13.1 | 1.13.1 |
| Webserver | Release-qualified ClusterIP Service; HTTP port 80 → target port 80; `/graphql` | Same shape, separate environment values |
| Endpoint access | No application authentication/proxy; no ingress | Same; network-only boundary |
| Location / repository | `k8s-example-user-code-1` / `__repository__` | Identical |
| Live replicas | Webserver 1, daemon 1, user-code 1 | Webserver 1, daemon 1, user-code 1 |
| Declared/live difference | DEV values declare webserver replicas 2; live deployment has 1 | No equivalent discrepancy reported |
| Launcher | `K8sRunLauncher`, jobs in the Dagster namespace using the existing Dagster service account | Same pattern |
| Coordinator | `QueuedRunCoordinator`, `max_concurrent_runs: -1`, per-tag limits | V2 reports all five limits verified; capture exact deployed YAML for fixtures |
| Automatic run retries | `max_retries: 2`, `retry_on_asset_or_op_failure: false` | `max_retries: 3`, `retry_on_asset_or_op_failure: false` |
| Run monitoring | Enabled; start timeout 600 s; `max_resume_run_attempts: 0`; poll 120 s | Identical |
| Compute logs | `NoOpComputeLogManager`; stdout/stderr not persisted by Dagster | Identical |
| Dagster storage | One in-cluster PostgreSQL pod; database `dagster`, schema `public`; no failover | Shared AWS RDS endpoint; non-default Dagster schema. Aurora engine/failover model inferred, unconfirmed |
| Ingress NetworkPolicy | Selects all namespace pods; allows only same-namespace sources | Identical restriction |
| Mesh evidence | No sidecars or namespace injection/ambient labels observed | Same; ambient enrollment/mTLS still unconfirmed |
| Webserver binding/endpoints | `0.0.0.0:80`; webserver/user-code endpoints reported healthy | Same chart/image; webserver endpoint reported healthy |
| Admission | Kyverno requires compliance labels and approved image registry; attempted probe later failed image pull | V2 reports fleet policy; verify the actual PROD deployment path |
| GraphQL surface | 38 mutations reported by introspection | Identical reported surface |

Port-forward queries establish the server contract, **not** bot-to-Service reachability across namespaces. Service names and cluster DNS must come from each environment's release; PR previews have different names. Configure the complete URL as `http://<webserver-service>.<dagster-namespace>.svc.cluster.local:80/graphql`; the old illustrative port 3000 is inapplicable here.

## Current design decision

The user has chosen **no bot database**, replacing the prior PostgreSQL/two-replica plan. Use one process, bounded memory and supported Dagster GraphQL evidence; a private admin panel and Slack adapter share application services. Every boot disarms all mutations pending operator reconciliation. No SQLite, persistent queue, volume or hidden coordination store is introduced.

Bot database provisioning and its failover qualification (former G4/G6) are removed from the bot release plan. Existing Dagster storage facts remain relevant to availability and loss of run evidence; an outage, restoration or incomplete history must not be interpreted as proof of no previous launch. Extra API checks reduce risk but cannot guarantee exactly-once execution or durable receipt/delivery.

The later UI decision permits a private production admin panel. Company OIDC registration, exact subject-role allowlists and private HTTPS callback routing are new deployment gates, not facts verified in the photographed audit. DEV mock identities remain prohibited in production.

## Findings and adopted changes

| ID | Finding | Plan/spec response |
|---|---|---|
| F1 · High | Same-cluster placement does not overcome the existing same-namespace-only policy. | Keep a separate bot namespace. Platform/Dagster owners must add narrow webserver ingress in the private Dagster deployment repository in DEV and PROD, plus any needed bot egress. Test allowed bot traffic and denied unrelated traffic before enabling access. |
| F2 · Medium | The previous example endpoint did not match real Service name, namespace or port. | Use controlled per-environment release-qualified URLs, HTTP port 80 and explicitly configured network-only access. Confirm ambient mesh separately. |
| F3 · Confirmed | Alert display text contains only the first UUID segment, but the button contains the full ID. | Extract from the trusted `View in Dagster UI` button matching `http://127.0.0.1:8080//runs/<full-uuid>`, including the double slash. Never fetch that URL. No publisher change is needed for identity. |
| F4 · Medium | Instance policy excludes ordinary asset/op failures from automatic run retries; budgets differ by environment. | Verify effective per-run policy, including supported tag overrides. Test both ordinary op/dbt failure without automatic run retry and induced worker-crash recovery, including pending-child gaps. Do not equate the instance defaults with proven exhaustion. |
| F5 · Scope | Persisted raw compute logs are unavailable; run pods are cleaned up. | `logs` returns structured event-log failures only. Explain unavailable stdout/stderr; an authorized human may inspect a surviving pod. Add no bot Kubernetes log permissions or external-log fallback. |
| F6 · Low | The source workspace location differs from the Helm-generated live location. | Target the deployed location/repository above, verified per environment; do not infer scope from the source workspace file. |
| F7 · Medium | DEV single-pod PostgreSQL cannot demonstrate PROD database failover guarantees. | Historical observation retained. The no-database decision removes bot schema/role/provisioning and bot failover tests. Observe Dagster through GraphQL only; disarm/reconcile if its run evidence becomes unavailable or inconsistent. |
| F8 · Low | Required API mutations exist, but schedule stop is `stopRunningSchedule`, not `stopSchedule`; launch has no reported idempotency key. | Pin 1.13.1 documents and result/input contracts. Keep single-attempt dispatch and an explicit allowed mutation set. Schema availability does not enable excluded operations. |
| F9 · Low | Preserved source concurrency tags also constrain bot-created runs. | Retain queue-policy tags, show possible queueing in previews, distinguish run creation from running, and hold the bot family slot while queued. Never strip tags to accelerate a retry. |
| F10 · Availability | Both environments have one webserver replica; DEV values and live count disagree. | Name the webserver as a single point of failure for API reads/dispatch. The bot now also has one instance; neither dependency has an HA guarantee. Reconcile drift with the platform owner and report dependency outages without restart storms. |
| F11 · Local copy | The photographed internal copy used a different OpenSpec change name and lacked the README-referenced config. | This repository already has `add-dagster-slackbot` and `openspec/config.yaml`. Keep its valid names; do not import the copied-repository mismatch. |

### API and alert details

Supported mutation names reported: `launchRun`, `launchRunReexecution`, `terminateRun`, `startSchedule`, `stopRunningSchedule`, `startSensor`, `stopSensor`. Present but excluded: `deleteRun`, `wipeAssets`, `reloadRepositoryLocation`, `shutdownRepositoryLocation`, `launchPartitionBackfill`, `addDynamicPartition`, `setSensorCursor`. One photo groups `setSensorCursor` with present API names; F8 explicitly excludes it, consistent with the existing scope. A mutation-name inventory does not prove input/output behavior or selection fidelity.

The existing failure sensor uses `dagster-slack` 1.13.1 and a custom error section truncated to 3,000 characters. The button URL is the primary identity source; structured Dagster events supply fuller bounded evidence. Publisher bot/app IDs, workspace/channel IDs and channel types still require a real Slack payload. Human access remains approved Teleport port-forwarding; a localhost button is not a generally usable employee link.

### Delivery conventions

The reported platform uses ACD/ArgoCD, environment-specific image repositories/tags, and ExternalSecrets backed by AWS Secrets Manager. V2 adds a concrete deployment gate: satisfy Kyverno labels and registry rules, then prove the chosen image actually pulls and starts. Admission success alone is insufficient. Companion applications already use separate namespaces and private Services. Follow that pattern for the bot; the photographed internal bot project has example pipeline templates, not evidence of a deployed bot. Render and reconcile the real pipeline/manifests before rollout. Secret values must not enter this repository.

## Remaining evidence and release gates

| Owner | Required evidence |
|---|---|
| Platform + Dagster deployment owners | Approved cross-namespace policy change; actual namespace/pod selectors; allowed/denied traffic tests; ambient-mesh status; webserver replica drift resolution |
| Platform/application operators | One active process, audited exec/local recovery CLI, prior-submitter isolation and post-restart reconciliation; no bot database is requested |
| Dagster/job owners | Pinned schema/union fixtures, both retry failure classes, reliable pending/exhausted observations, full selection/partition/current-code behavior, queue-tag preservation |
| Slack owner | Publisher and app/workspace/channel IDs/types, captured alert payload, bot scopes/membership and Socket Mode egress |
| Data/job owners | Redaction/retention approval, launch presets and asset mappings |
| Application/platform owners | Kyverno-compliant/pullable image, delivery manifests, secret rotation, independent alarms, production UI authentication and DEV mock isolation, human access instructions and end-to-end acceptance |

Audit observations close identification questions; they do not complete implementation tasks. The read-only rollout still requires working connectivity, authenticated Slack policy and approved data handling. Mutations additionally require complete GraphQL evidence, validated in-memory dispatch/restart behavior, operator commissioning and explicit enablement. Data-owner approvals and behavioral fixtures remain open unless separately evidenced; the v2 gap list does not itself close them.

## Verification record and implementation trace

V1 reports read-only topology/configuration and GraphQL checks through temporary port-forwards, subsequently closed. V2 additionally reports a cross-namespace query timeout from an existing DEV companion pod without restrictive egress, consistent with the still-unapplied Dagster ingress allowance. This is negative DEV evidence, not successful bot access or a PROD workload test.

The attempted same-namespace ephemeral probe encountered admission requirements, then image-pull failure, and was deleted. Healthy endpoints and port-forward queries do not substitute for its missing positive workload test. The original commands contain placeholders/abbreviated GraphQL and are procedure outlines, not executable fixtures. No cluster operations were performed by this documentation update.

V2 marks F1–F10 addressed in specifications while G1 policy remediation, G2 mesh confirmation, G3 replica drift and G5 Slack identities remain operational work. G4/G6 bot database planning is superseded above. The blanket v2 heading “all confirmed live” does not resolve its explicitly inferred engine/mesh details. Local-copy README hygiene F11 does not justify renaming this repository's valid change/config paths.

Implementation mapping: task groups 1–2 deliver independent core/admin UI; group 3 closes admission/network/GraphQL contracts; group 4 delivers no-database execution/recovery; group 5 owns Slack integration; group 6 verifies PROD. Behavioral requirements are in the four [specs](specs/); architecture is in [design](design.md).

Platform semantics reference: [Kubernetes NetworkPolicy](https://kubernetes.io/docs/concepts/services-networking/network-policies/) and [Dagster run retries](https://docs.dagster.io/deployment/execution/run-retries). Public documentation supports interpretation, not proof of the private deployment or its pinned schema.
