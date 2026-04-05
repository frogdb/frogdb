---
name: runner
description: >
  Manage the self-hosted GitHub Actions runner (rebuild image, restart container, check status/logs, troubleshoot).
  Use this skill whenever runner config files are modified (.github/runner/Dockerfile, docker-compose.yml),
  when the user asks about the CI runner, or when CI jobs fail with runner-related errors.
  Proactively suggest rebuilding when you notice changes to runner configuration during a session.
---

# Self-Hosted GitHub Actions Runner

The project runs a self-hosted GitHub Actions runner via Docker Compose in `.github/runner/`.
The runner image extends `myoung34/github-runner` with project dependencies (Node.js, libclang-dev).

## Commands

All operations use Justfile targets from the repo root:

| Operation | Command |
|-----------|---------|
| Build & start | `just runner` |
| Stop | `just runner-stop` |
| Logs | `just runner-logs` |
| Follow logs | `just runner-logs -f` |

`just runner` runs `docker compose up -d --build`, so it rebuilds the image if the Dockerfile changed.

## When to rebuild

Rebuild (`just runner`) after changes to:
- `.github/runner/Dockerfile` — image dependencies changed
- `.github/runner/docker-compose.yml` — container config changed

If you modify either of these files during a session, remind the user that the runner needs a rebuild and offer to run `just runner`.

## Troubleshooting

**Missing .env file**: The runner needs `.github/runner/.env` with a GitHub PAT. If `just runner` fails with "env file not found", check if `.env` exists. The template is in `.env.example`.

**Runner not appearing in GitHub**: Check logs with `just runner-logs -f`. Common causes:
- Invalid or expired `ACCESS_TOKEN` in `.env`
- Runner already registered with a different name (check GitHub repo Settings > Actions > Runners)

**Image build failure**: Read the build output carefully. The Dockerfile installs system packages — if a package name changes or a mirror is down, the build will fail at the `apt-get install` step.
