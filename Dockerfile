# syntax=docker/dockerfile:1.29.0@sha256:572cebbc2b15e6f84c077742952674889cb63cd5e9b80abdffcbe587b6f19183
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

FROM golang:1.27-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS builder
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

FROM gcr.io/distroless/static:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931
USER nonroot:nonroot
COPY --from=builder /out/ /usr/local/bin/
# The licenses travel with the image, not only with the source tree. The binaries
# above are statically linked, so they contain the code of every module in
# LICENSE-THIRD-PARTY and none of their license files. Apache-2.0 section 4,
# MIT and BSD-3 all require their text to reach the recipient of a binary.
COPY LICENSE NOTICE LICENSE-THIRD-PARTY /usr/share/doc/subtx-generator/
# No ENTRYPOINT on purpose; consumer selects the binary.
