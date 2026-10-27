# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
#
# Canonical multi-stage Dockerfile for subtx-generator.
# Final image: distroless/static:nonroot. Bundles seven binaries:
#
#   - /usr/local/bin/subtx-gen           (continuous BRC-124/BRC-128 frame generator)
#   - /usr/local/bin/send-anchor-frame   (one-shot BRC-134 anchor)
#   - /usr/local/bin/send-block-announce (one-shot BRC-131 announce, multicast ingress)
#   - /usr/local/bin/send-subtree-data   (one-shot BRC-132 subtree-data, multicast ingress)
#   - /usr/local/bin/send-subtree-push   (one-shot BRC-143 subtree push → proxy lane 8726)
#   - /usr/local/bin/send-block-push     (one-shot BRC-144 block push → proxy lane 8727)
#   - /usr/local/bin/tunnel-sink         (consumer tunnel delivery sink + submit relay, diagnostic)
#
# The two push senders target the proxy's current privileged ingest lanes; the
# multicast senders above them exercise the legacy fabric-internal path.
#
# No ENTRYPOINT is set: the consuming workload (Helm chart, docker run --entrypoint)
# selects which binary to invoke.

FROM golang:1.27-alpine@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    mkdir -p /out; \
    for cmd in subtx-gen send-anchor-frame send-block-announce send-subtree-data send-subtree-push send-block-push tunnel-sink; do \
      CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
        go build -trimpath -buildvcs=false \
          -ldflags "-s -w -X main.Version=${VERSION}" \
          -o /out/${cmd} ./cmd/${cmd}/; \
    done

FROM gcr.io/distroless/static:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99
USER nonroot:nonroot
COPY --from=builder /out/ /usr/local/bin/
# No ENTRYPOINT on purpose; consumer selects the binary.
