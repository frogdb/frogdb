# Self-Hosted GitHub Actions Runner

Runs CI test jobs on a local machine using [myoung34/docker-github-actions-runner](https://github.com/myoung34/docker-github-actions-runner) inside Docker. All test workflow jobs target `self-hosted` — other workflows (build, release, docs) remain on GitHub-hosted runners.

## Setup

1. Create a GitHub [Personal Access Token (classic)](https://github.com/settings/tokens) with `repo` scope.

2. Create a `.env` file from the template:

   ```bash
   cp .github/runner/.env.example .github/runner/.env
   ```

3. Paste your token into `.env`:

   ```
   ACCESS_TOKEN=ghp_...
   ```

4. Start the runner:

   ```bash
   just runner
   ```

5. Verify it appears at **Settings > Actions > Runners** in the GitHub repo.

## Usage

```bash
just runner          # start (rebuilds image if Dockerfile changed)
just runner-stop     # stop
just runner-logs     # view logs
just runner-logs -f  # follow logs
```

## How it works

- `Dockerfile` extends the base runner image with Node.js (required by JavaScript-based GitHub Actions).
- `docker-compose.yml` configures the runner agent with repo URL, labels, and a persistent work volume.
- The base image is pinned by SHA digest for supply-chain security.

## Security notes

- `.env` contains your PAT and is gitignored — never commit it.
- The runner container has access to the Docker socket for jobs that need Docker.
- If the machine is off, CI jobs will queue until it comes back online.
- **Actor gating:** Test workflow jobs only run on `self-hosted` when triggered by a trusted actor (hardcoded in `workflow_gen/workflows/test.py`). All other actors (fork PRs, Dependabot, external contributors) fall back to `ubuntu-latest`. For PRs, the check uses the immutable `pull_request.user.login` field so re-running a fork PR does not bypass the gate.
- For public repos or repos accepting fork PRs, also restrict fork PR workflows in **Settings > Actions > General** to require approval as a backstop.
