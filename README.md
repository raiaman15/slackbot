# Slackbot

A Slack operations bot for self-hosted Dagster, controlled through a FastAPI application in the same Kubernetes cluster.

**Status: OpenSpec planning complete; implementation pending.** The repository contains the proposal, six capability specs, detailed HLD/LLD and 56 unchecked implementation tasks.

## OpenSpec entry points

| Document | Purpose |
|---|---|
| [Proposal](openspec/changes/add-dagster-slackbot/proposal.md) | Why, scope and capability inventory |
| [Capability specs](openspec/changes/add-dagster-slackbot/specs/) | Required behavior with WHEN/THEN scenarios |
| [Design](openspec/changes/add-dagster-slackbot/design.md) | Architecture, decisions, trade-offs and complete technical detail |
| [Tasks](openspec/changes/add-dagster-slackbot/tasks.md) | Implementation checklist and verification outcomes |
| [Project configuration](openspec/config.yaml) | Shared context and artifact rules |

The active change is `add-dagster-slackbot`, using OpenSpec's built-in `spec-driven` schema. Each capability has an ADDED delta at `openspec/changes/add-dagster-slackbot/specs/<capability>/spec.md`. The standard change metadata records its schema and creation date.

There is no implemented capability baseline or archived change yet. `openspec/specs/` and `openspec/changes/archive/` will be created when populated through the normal workflow; empty directories and placeholder files are not tracked.

## Recommended architecture

```mermaid
flowchart TD
  S["Slack failure thread"]
  B["FastAPI bot: one deployment, two PROD replicas"]
  P[("PostgreSQL: durable bot state")]
  D["Existing Dagster webserver Service: /graphql"]
  R["Existing Dagster execution and retries"]
  B -->|"Opens Socket Mode connection"| S
  S -->|"Mentions and buttons over that connection"| B
  B -->|"Web API: same-thread replies"| S
  B <--> P
  B -->|"Private HTTP(S)"| D
  D --> R
```

Each bot pod runs one Uvicorn process with async Slack SDK, HTTPX and Psycopg clients plus supervised durable-work loops. FastAPI calls the existing private Dagster Service; Dagster owns job execution. Use existing PostgreSQL infrastructure with a dedicated bot database/role.

No public ingress or runtime Teleport session is required for this workflow. Existing internal authentication and workload network restrictions remain in force. The actual Service address, deployed GraphQL schema, retry behavior and data policies are implementation verification inputs.

## Slack interaction

Reply in an existing Dagster failure thread:

```text
@bot logs
@bot status
@bot retry
```

The bot resolves the exact source run from the trusted alert. Retry offers whole-run re-execution or a fresh copy, then requester confirmation. Evidence, controls and progress remain in that thread. A bare `@bot` shows a menu.

Durable state prevents duplicate command handling and coordinates one active bot-created run family across replicas. Uncertain launches are reconciled without automatic resubmission. Phase one has no LLM; a later company AI gateway can reuse the same typed services and policies.

## Validate and continue

With Node.js 20.19.0 or later, run from the repository root:

```bash
npx --yes --package @fission-ai/openspec@1.13.2 openspec validate add-dagster-slackbot --strict --no-interactive
npx --yes --package @fission-ai/openspec@1.13.2 openspec status --change add-dagster-slackbot
npx --yes --package @fission-ai/openspec@1.13.2 openspec instructions apply --change add-dagster-slackbot
```

OpenSpec 1.13.2 strict validation passed for this planning change. A complete artifact status means the documents are present; implementation progress remains **0/56 tasks**. Begin with the environment contract tasks, then build and verify in dependency order.

Read the proposal, capability specs, design and tasks before implementation. Each requirement has a `#### Scenario:` with WHEN/THEN behavior; each task states how to verify completion. Update these artifacts together as requirements change.

After implementation and acceptance verification, use OpenSpec's archive workflow to merge the deltas into the main specs and retain the completed change in the archive. Planning completion alone does not mean the bot is delivered.

Tool-specific integrations are optional. Use the pinned CLI's `init --tools <tool-id>` command if you want editor/agent `/opsx:*` commands; this repository has no generated integration files or custom schemas.

Official references: [OpenSpec concepts](https://github.com/Fission-AI/OpenSpec/blob/main/docs/concepts.md), [built-in schema](https://github.com/Fission-AI/OpenSpec/blob/main/schemas/spec-driven/schema.yaml), and [CLI reference](https://github.com/Fission-AI/OpenSpec/blob/main/docs/cli.md).
