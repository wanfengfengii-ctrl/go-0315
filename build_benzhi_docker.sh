#!/bin/bash
set -euo pipefail

IMAGE_NAME=${1:-archival-replica-integrity-recovery}
DOCKER_PLATFORM=${2:-linux/amd64}
BUILDX_BUILDER=${BUILDX_BUILDER:-benzhi-builder}

docker buildx build --builder "$BUILDX_BUILDER" --load --progress plain --platform "$DOCKER_PLATFORM" -f benzhi.Dockerfile -t "$IMAGE_NAME:latest" .

echo ""
echo "Docker image '$IMAGE_NAME:latest' built successfully."
echo ""
echo "Next step: docker run --rm -it $IMAGE_NAME:latest"
