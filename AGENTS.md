# Luma project instructions

## Completion workflow

- After every code change, run the relevant checks. At minimum, run `cargo test -q` and `cargo build -q` from `backend`, plus `npm run typecheck` and `npm run build` from `frontend`.
- Do not publish or deploy when a required check fails.
- After all checks pass, commit the intended files and push the verified revision to GitHub.
- After the GitHub push succeeds, deploy that same revision to the NAS and verify the running service.

## NAS deployment

- SSH endpoint: `sunfujun@192.168.5.28:22`.
- Deployment root: `/vol3/1000/docker/`.
- Never store an SSH password, token, private key, or other secret in this repository, Git history, scripts, command output, or documentation. Use an SSH key or a secure credential provider for unattended deployment.
- Before any destructive operation, inspect the NAS and resolve the exact Luma project directory and Docker Compose project. Never recursively replace or delete the deployment root itself.
- Preserve persistent data directories and media/download mounts, including `luma-data` and `downloads`.
- Stop and remove only the Docker container named `luma`, update the Luma application files to the verified Git revision, then rebuild and start it with Docker Compose.
- After startup, verify that the `luma` container is running and that `http://127.0.0.1:3000/api/v1/health` succeeds from the NAS.
