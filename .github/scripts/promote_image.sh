#!/usr/bin/env bash
set -euo pipefail

: "${IMAGE:?}" "${DIGEST:?}" "${VERSION:?}" "${LATEST_VERSION:?}"
[[ "$DIGEST" =~ ^sha256:[a-f0-9]{64}$ ]]
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$ ]]
existing=$(mktemp)
errors=$(mktemp)
trap 'rm -f "$existing" "$errors"' EXIT
if docker buildx imagetools inspect "$IMAGE:$VERSION" --format '{{json .Manifest}}' > "$existing" 2> "$errors"; then
  previous=$(jq -er .digest "$existing")
  if [ "$previous" != "$DIGEST" ]; then
    echo "::error::Immutable version $IMAGE:$VERSION already points to $previous; refusing to replace it with $DIGEST" >&2
    exit 1
  fi
elif ! grep -Fxq "ERROR: $IMAGE:$VERSION: not found" "$errors"; then
  cat "$errors" >&2
  exit 1
fi

tags=("$IMAGE:$VERSION")
if [ "$VERSION" = "$LATEST_VERSION" ]; then
  tags+=("$IMAGE:latest")
fi
args=()
for tag in "${tags[@]}"; do
  args+=(--tag "$tag")
done
docker buildx imagetools create "${args[@]}" "$IMAGE@$DIGEST"
for tag in "${tags[@]}"; do
  actual=$(docker buildx imagetools inspect "$tag" --format '{{json .Manifest}}' | jq -er .digest)
  test "$actual" = "$DIGEST"
done
