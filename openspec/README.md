# OpenSpec workflow

This repository uses OpenSpec's built-in `spec-driven` schema. The active change is [add-dagster-slackbot](changes/add-dagster-slackbot/proposal.md).

## Artifact ownership

| Artifact | Meaning |
|---|---|
| `config.yaml` | Project context, default schema and artifact guidance |
| `changes/add-dagster-slackbot/proposal.md` | Motivation, scope and capability inventory |
| `changes/add-dagster-slackbot/specs/*/spec.md` | Proposed normative behavior, including failure scenarios |
| `changes/add-dagster-slackbot/design.md` | Architecture, decisions, trade-offs and detailed HLD/LLD |
| `changes/add-dagster-slackbot/tasks.md` | Unchecked implementation tasks with verification outcomes |
| `specs/` | Completed capability baseline, populated when implementation is delivered and the change is archived/synced |

No baseline capabilities are recorded yet because the bot is not implemented. `openspec status` reports planning artifact presence; it is not a production-readiness or implementation-completion check. Do not mark tasks complete merely because planning documents exist.

Capability specs define required behavior and `design.md` defines the implementation approach. Its detailed annex carries forward the v0.3 document so low-level contracts, open environment facts and references are preserved. Update active specs/design/tasks together when requirements change. The old root document is a compatibility index.

## CLI

Validated tooling version: **OpenSpec 1.13.2**. It requires Node.js **20.19.0 or later**; Node is documentation tooling, not the Python bot runtime.

From the repository root:

```bash
npx --yes --package @fission-ai/openspec@1.13.2 openspec list
npx --yes --package @fission-ai/openspec@1.13.2 openspec status --change add-dagster-slackbot
npx --yes --package @fission-ai/openspec@1.13.2 openspec validate add-dagster-slackbot --strict --no-interactive
npx --yes --package @fission-ai/openspec@1.13.2 openspec instructions apply --change add-dagster-slackbot
```

Read proposal, specs, design and tasks before implementation. Work through tasks in dependency order and record evidence as each completes. Resolve unavailable company-environment facts through task group 1 before enabling writes.

To install editor/agent integrations later, run `openspec init --tools <tool-id>` using the pinned CLI. This repository is initialized with `--tools none`, so it stays independent of a particular editor. `/opsx:*` commands require the corresponding integration; the CLI commands above work without one.

After implementation and acceptance verification, use the normal archive workflow to merge ADDED requirements into `openspec/specs/` and move the completed change into `changes/archive/`. This initial change has deliberately not been archived or synced to an implemented baseline.

## Conventions

- Each capability has `## Purpose` and `## ADDED Requirements` in its initial delta.
- Each requirement uses `### Requirement:` and normative SHALL/MUST wording.
- Each requirement has at least one `#### Scenario:` with explicit WHEN/THEN behavior.
- Tasks use numbered `- [ ] X.Y` entries and include their verification outcome.
- Future changes use new folders and ADDED/MODIFIED/REMOVED deltas as appropriate; archive preserves completed history.

Official references: [OpenSpec repository](https://github.com/Fission-AI/OpenSpec), [concepts](https://github.com/Fission-AI/OpenSpec/blob/main/docs/concepts.md), [CLI](https://github.com/Fission-AI/OpenSpec/blob/main/docs/cli.md), and [spec-driven schema](https://github.com/Fission-AI/OpenSpec/blob/main/schemas/spec-driven/schema.yaml).
