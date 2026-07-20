# Project rules

Read this before changing anything in this repo.

## Style
- No em dash anywhere, in code, comments, docs, or commit messages. Use commas, colons, or parentheses.
- No long or narrative comments. A comment states why in one short line, or it does not exist. Never restate what the code already says.
- Do not write comments that read as AI filler.

## Code
- Production level. Validate input at trust boundaries. Never swallow failures silently.
- Modular. One job per file and per function. Keep ingestion, processing, orchestration, and schema separate.
- Reuse before adding. No new dependency for what a few lines already cover.
- Configuration comes from .env and arguments, never hard coded. Never commit secrets.
- Pin versions. Local Spark and Dataproc Spark stay on the same version so code that runs locally runs in the cloud.

## Workflow
- Keep local and cloud paths in sync. The same job code runs in both, only endpoints differ.
- Keep scripts minimal and idempotent. local.sh controls local, gcp.sh controls GCP.
- Update README.md when the stack or the run steps change.
