# GitHub Actions Configuration

## Docker Hub Secrets

To make the GitHub Action work for pushing to Docker Hub, you need to configure the following secrets in your GitHub repository:

1. Go to Settings → Secrets and variables → Actions
2. Add the following secrets:
   - `DOCKER_USERNAME`: your Docker Hub username (faxioman)
   - `DOCKER_PASSWORD`: your Docker Hub access token (not the password!)

### How to create a Docker Hub Access Token:
1. Go to https://hub.docker.com/
2. Click on your username → Account Settings
3. Security → New Access Token
4. Give the token a name (e.g. "github-actions-rustka")
5. Copy the token and save it as `DOCKER_PASSWORD` in GitHub secrets

## Workflow

The workflow:
1. Compiles the Rust binary for amd64 and arm64 using musl for static binaries
2. Creates multi-architecture Docker images using Docker buildx
3. Pushes to Docker Hub with:
   - Tag `latest`
   - Tag with commit hash

## Local testing

To test the build locally:
```bash
# Build for amd64
cargo build --release --target x86_64-unknown-linux-musl

# Build Docker image
docker build -t faxioman/rustka:test .
```
