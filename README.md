# Slackbot

Control self-hosted Dagster from its existing Slack failure threads. The FastAPI bot calls Dagster's private GraphQL endpoint in the same Kubernetes cluster.

**Planning complete; implementation pending.** Phase one uses deterministic commands without an LLM:

```text
@bot logs
@bot status
@bot retry
```

Replies stay in the alert thread. Mutations require requester confirmation. PostgreSQL coordinates two bot replicas, one active bot-created run family, and recovery from uncertain submissions. Socket Mode requires outbound Slack connectivity without public ingress.

## Implementation plan

The active OpenSpec change is `add-dagster-slackbot`:

| Artifact | Read for |
|---|---|
| [Proposal](openspec/changes/add-dagster-slackbot/proposal.md) | Scope and capabilities |
| [Specs](openspec/changes/add-dagster-slackbot/specs/) | Requirements and acceptance scenarios |
| [Design](openspec/changes/add-dagster-slackbot/design.md) | Architecture, interfaces, storage and recovery |
| [Tasks](openspec/changes/add-dagster-slackbot/tasks.md) | Implementation and verification checklist |

Using Node.js 20.19 or later, run from the repository root:

```bash
npx --yes --package @fission-ai/openspec@1.13.2 openspec validate --all --strict --no-interactive
npx --yes --package @fission-ai/openspec@1.13.2 openspec instructions apply --change add-dagster-slackbot
```

The built-in `spec-driven` schema is configured in [openspec/config.yaml](openspec/config.yaml). Requirements remain ADDED deltas until implemented and archived; baseline specs and archive directories will be created when populated. Read all change artifacts before implementation and keep their task status accurate.
