# Slackbot

A detailed specification for operating self-hosted Dagster from Slack.

Users mention the bot inside an existing failure thread to retrieve structured errors, inspect run status, or prepare a retry. Responses, mode selection, confirmation, and progress remain in that thread.

The proposed architecture uses Python, FastAPI, Slack Bolt, PostgreSQL, and Kubernetes, with deterministic commands and no LLM in phase 1. The design supports a later integration with the company AI gateway.

**Status: design specification only.** A runnable bot has not been implemented. Deployment details and API capabilities still require environment verification.

Read [the complete specification: high-level and low-level design](dagster_slackbot_spec.md).
