# Slackbot

Operate self-hosted Dagster through a FastAPI application in the same Kubernetes cluster. Build and test through a small DEV console first; integrate Slack as a separate adapter using the same services.

**Planning complete; implementation pending. No bot database.** One pod/process uses bounded memory and Dagster GraphQL for run evidence. Commands, confirmations and notifications can be lost on restart. Every boot starts read-only until operator reconciliation; uncertain mutations are never automatically resent. This design trades automatic failover and durable delivery for simpler infrastructure.

Slack interaction remains in the existing failure thread:

```text
@bot logs
@bot status
@bot retry
```

The DEV UI works without Slack credentials. Its mock/live-DEV modes exercise the same previews, confirmations and execution checks; its routes are absent in PROD. Phase one has no LLM.

## OpenSpec plan

Active change: `add-dagster-slackbot`, using the built-in `spec-driven` schema.

| Artifact | Purpose |
|---|---|
| [Audit](openspec/changes/add-dagster-slackbot/environment-audit.md) | Infrastructure evidence, updated decisions and owner gates |
| [Proposal](openspec/changes/add-dagster-slackbot/proposal.md) | Scope and four capabilities |
| [Specs](openspec/changes/add-dagster-slackbot/specs/) | Dagster, runtime, DEV workbench and Slack requirements |
| [Design](openspec/changes/add-dagster-slackbot/design.md) | Interfaces, in-memory state and conservative recovery |
| [Tasks](openspec/changes/add-dagster-slackbot/tasks.md) | Core/UI first; separate Slack workstream; release checks |

With Node.js 20.19 or later, run from the repository root:

```bash
npx --yes --package @fission-ai/openspec@1.13.2 openspec validate --all --strict --no-interactive
npx --yes --package @fission-ai/openspec@1.13.2 openspec instructions apply --change add-dagster-slackbot
```

Requirements remain ADDED deltas until implemented and archived. Read the change artifacts together; company identifiers stay in controlled deployment configuration. Documentation validation does not establish live cluster access or implementation completion.
