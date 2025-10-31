FROM mcr.microsoft.com/devcontainers/rust:2-1-bullseye

ARG CLAUDE_CODE_VERSION=latest

USER root

# Install Node.js 24.x (latest LTS) from NodeSource
RUN apt-get update && \
    apt-get install -y ca-certificates curl gnupg && \
    mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_24.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list && \
    apt-get update && \
    apt-get install -y nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN npm install -g @anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}
RUN npm install -g @fission-ai/openspec@latest

# Install pre-commit
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install pre-commit && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER vscode