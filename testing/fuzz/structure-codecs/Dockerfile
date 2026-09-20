FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends build-essential git curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo
ENV PATH=/opt/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup-init.sh \
    && sh /tmp/rustup-init.sh -y --profile minimal --default-toolchain 1.91.0 \
    && rm /tmp/rustup-init.sh
