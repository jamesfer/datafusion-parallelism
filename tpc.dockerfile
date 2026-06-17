# Build stage
# Pin to specific nightly version by SHA to avoid downloading new images frequently
FROM rustlang/rust@sha256:b5258a842b01ce7dcba74ab2f5afb001d366ac33ca7209db994d95a657009e2a AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
COPY bin ./bin

# Copy only necessary tpc files for build
COPY tpc/Cargo.toml ./tpc/
COPY tpc/src ./tpc/src
COPY tpc/datafusion-benchmarks ./tpc/datafusion-benchmarks

# Build the tpc binary in release mode
RUN cargo build --release --manifest-path tpc/Cargo.toml

# Runtime stage
FROM debian:trixie-slim

# Install runtime dependencies including gcloud CLI
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && apt-get update && apt-get install -y google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/target/release/tpc /usr/local/bin/tpc

# Copy queries directory (needed for benchmark execution)
COPY tpc/queries ./tpc/queries
COPY tpc/scripts ./tpc/scripts
COPY tpc/datafusion-benchmarks/tpch/queries ./tpc/datafusion-benchmarks/tpch/queries

# Set the entrypoint
ENTRYPOINT ["tpc/scripts/_docker_entrypoint.sh"]
CMD ["--help"]
