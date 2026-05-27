# syntax=docker/dockerfile:1.7
#
# Canonical multi-stage Dockerfile for subtx-generator.
# Final image: distroless/static:nonroot. Bundles four binaries:
#
#   - /usr/local/bin/subtx-gen           (continuous BRC-124/BRC-128 frame generator)
#   - /usr/local/bin/send-anchor-frame   (one-shot BRC-134 anchor)
#   - /usr/local/bin/send-block-announce (one-shot BRC-131 announce)
#   - /usr/local/bin/send-subtree-data   (one-shot BRC-127 subtree-data)
#
# No ENTRYPOINT is set: the consuming workload (Helm chart, docker run --entrypoint)
# selects which binary to invoke.

FROM golang:1.25-alpine AS builder
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
    for cmd in subtx-gen send-anchor-frame send-block-announce send-subtree-data; do \
      CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
        go build -trimpath -buildvcs=false \
          -ldflags "-s -w -X main.Version=${VERSION}" \
          -o /out/${cmd} ./cmd/${cmd}/; \
    done

FROM gcr.io/distroless/static:nonroot
USER nonroot:nonroot
COPY --from=builder /out/ /usr/local/bin/
# No ENTRYPOINT on purpose; consumer selects the binary.
