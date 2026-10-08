# Container images for the Kubernetes deployment entries: deploy/ and
# deploy/kustomize/ (outpostnet-controller, outpostnet-server, outpostnet-sidecar
# injected by the controller, outpostnet-agent in the standalone overlay) and
# hack/e2e-*.sh. One target per image, named like the image:
#
#   for c in outpostnet-controller outpostnet-server outpostnet-sidecar outpostnet-agent; do
#     docker build --target "$c" -t "$c:latest" \
#       --secret id=outpostrange_deploy_key,src=<OutpostRange read-only deploy key file> \
#       [--build-arg GOPROXY=<module proxy>] \
#       [--build-arg VERSION=... --build-arg COMMIT=... --build-arg BUILD_TIME=...] .
#   done
#
# The key may also come from the environment:
# --secret id=outpostrange_deploy_key,env=OUTPOSTRANGE_DEPLOY_KEY (the read-only
# deploy key CI holds as the OUTPOSTRANGE_DEPLOY_KEY secret). The secret is
# required; the build fails without it.
#
# Base images come only from Huawei SWR (risecloud namespace). Do not point FROM
# at Docker Hub or ghcr directly; sync missing upstream images into SWR first.
ARG SWR=swr.cn-east-3.myhuaweicloud.com/risecloud

FROM ${SWR}/golang:1.25-alpine AS builder

# github.com/outpostos/OutpostRange (observation contract) is a private module
# pinned in go.mod. Go fetches it with git over SSH through a host alias bound
# to the read-only deploy key, the same scheme as .github/workflows/ci.yml.
# Nothing in this block is secret.
RUN apk add --no-cache git openssh-client
COPY deploy/docker/github_known_hosts /etc/ssh/ssh_known_hosts
RUN printf '%s\n' \
      'Host github-outpostrange' \
      '  HostName github.com' \
      '  User git' \
      '  IdentityFile /run/secrets/outpostrange_deploy_key' \
      '  IdentitiesOnly yes' \
      '  StrictHostKeyChecking yes' \
      > /etc/ssh/ssh_config.d/outpostos.conf \
 && git config --system url."git@github-outpostrange:outpostos/OutpostRange".insteadOf https://github.com/outpostos/OutpostRange

ENV GOPRIVATE=github.com/outpostos GONOSUMDB=github.com/outpostos
# Public module proxy; override with --build-arg GOPROXY=... where
# proxy.golang.org is unreachable. go.sum still verifies every module.
ARG GOPROXY=https://proxy.golang.org,direct

WORKDIR /src
COPY go.mod go.sum ./
# The deploy key exists only as a BuildKit secret tmpfs mount for the duration
# of this step; it is never written to a layer, the build cache or the image
# history.
RUN --mount=type=secret,id=outpostrange_deploy_key,required=true \
    go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
RUN for c in outpostnet-controller outpostnet-server outpostnet-sidecar outpostnet-agent; do \
      CGO_ENABLED=0 go build -buildvcs=false -trimpath \
        -ldflags "-s -w -X github.com/outpostos/OutpostNet/pkg/version.gitVersion=${VERSION} -X github.com/outpostos/OutpostNet/pkg/version.gitCommit=${COMMIT} -X github.com/outpostos/OutpostNet/pkg/version.buildDate=${BUILD_TIME}" \
        -o /out/$c ./cmd/$c || exit 1; \
    done

FROM ${SWR}/alpine:3.19 AS runtime
RUN apk add --no-cache ca-certificates

FROM runtime AS outpostnet-agent
COPY --from=builder /out/outpostnet-agent /usr/local/bin/outpostnet-agent
ENTRYPOINT ["/usr/local/bin/outpostnet-agent"]

FROM runtime AS outpostnet-sidecar
COPY --from=builder /out/outpostnet-sidecar /usr/local/bin/outpostnet-sidecar
ENTRYPOINT ["/usr/local/bin/outpostnet-sidecar"]

FROM runtime AS outpostnet-server
COPY --from=builder /out/outpostnet-server /usr/local/bin/outpostnet-server
ENTRYPOINT ["/usr/local/bin/outpostnet-server"]

FROM runtime AS outpostnet-controller
COPY --from=builder /out/outpostnet-controller /usr/local/bin/outpostnet-controller
ENTRYPOINT ["/usr/local/bin/outpostnet-controller"]
