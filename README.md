# Slackbot

Operate self-hosted Dagster through a FastAPI application in the same Kubernetes cluster. Build and test through a small admin panel first; integrate Slack as a separate adapter using the same services.

**Planning complete; implementation pending. No bot database.** One pod/process uses bounded memory and Dagster GraphQL for run evidence. Commands, confirmations and notifications can be lost on restart. Every boot starts read-only until operator reconciliation; uncertain mutations are never automatically resent. This design trades automatic failover and durable delivery for simpler infrastructure.

Slack interaction remains in the existing failure thread:

```text
@bot logs
@bot status
@bot retry
```

The admin panel works without Slack credentials and opens directly in DEV and PROD through the existing Teleport and loopback port-forward access. Both interfaces use the same LangGraph workflow and typed Dagster tools. Phase one uses deterministic parser/formatter implementations behind replaceable model interfaces, without model-provider calls or credentials. DEV mock/live modes and PROD live mode use shared previews, confirmations and execution checks. No application sign-in or user roles are required. Everyone who can reach the panel has its configured capabilities; automatic browser sessions only bind context, CSRF protection and confirmations. Mock identities remain DEV-only.

## OpenSpec plan

Active change: `add-dagster-slackbot`, using the built-in `spec-driven` schema.

| Artifact | Purpose |
|---|---|
| [Audit](openspec/changes/add-dagster-slackbot/environment-audit.md) | Infrastructure evidence, updated decisions and owner gates |
| [Proposal](openspec/changes/add-dagster-slackbot/proposal.md) | Scope and four capabilities |
| [Specs](openspec/changes/add-dagster-slackbot/specs/) | Dagster, runtime, admin panel and Slack requirements |
| [Design](openspec/changes/add-dagster-slackbot/design.md) | Interfaces, in-memory state and conservative recovery |
| [Tasks](openspec/changes/add-dagster-slackbot/tasks.md) | Core/admin UI first; separate Slack workstream; release checks |

With Node.js 20.19 or later, run from the repository root:

```bash
npx --yes --package @fission-ai/openspec@1.13.2 openspec validate --all --strict --no-interactive
npx --yes --package @fission-ai/openspec@1.13.2 openspec instructions apply --change add-dagster-slackbot
```

Requirements remain ADDED deltas until implemented and archived. Read the change artifacts together; company identifiers stay in controlled deployment configuration. Documentation validation does not establish live cluster access or implementation completion.
