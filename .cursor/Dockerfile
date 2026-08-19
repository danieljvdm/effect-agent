# Cloud Agent base image for the effect-agent monorepo.
#
# Bakes the stable toolchain the repository pins in docs/TOOLCHAIN.md:
#   - Node.js 24 (the repo requires ^22.18.0 || >=24.11.0; the default agent
#     image ships an older 22.x that is below that range)
#   - Bun 1.3.14 (the "packageManager" and workspace resolver)
#
# Repository dependencies are NOT installed here. Cursor checks out the
# requested revision separately and the `install` command in environment.json
# runs `bun install` against the committed lockfile after checkout.

FROM node:24-bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

# git + curl for VCS/network, ca-certificates for TLS, unzip for the Bun
# distribution, and python3/build-essential so any optional native postinstall
# (node-gyp) can compile.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     git \
     curl \
     ca-certificates \
     unzip \
     python3 \
     build-essential \
  && rm -rf /var/lib/apt/lists/*

# Pin Bun to the exact version from package.json's "packageManager" field.
# The npm "bun" package resolves the platform binary at install time.
RUN npm install -g bun@1.3.14 \
  && bun --version

# The stock node:24 image already provides a uid/gid 1000 "node" user; run as
# that non-root user so the checked-out workspace is not owned by root.
USER node
