#!/usr/bin/env bash
set -euo pipefail

# This script runs on a cloud instance to perform TPC-H benchmarks
# It installs dependencies, clones the repo, downloads data, runs benchmarks, and uploads results

# Set HOME if not set (happens when running as startup script)
export HOME="${HOME:-/root}"

# Configuration via environment variables with defaults
REPO="${REPO:-jamesfer/datafusion-parallelism}"
REPO_BRANCH="${REPO_BRANCH:-master}"
DATA_SOURCE="${DATA_SOURCE:-}"  # S3 or GCS path to download TPC-H data
RESULTS_DEST="${RESULTS_DEST:-}" # S3 or GCS path to upload results
ITERATIONS="${ITERATIONS:-100}"
SCALE_FACTOR="${SCALE_FACTOR:-10}"
QUERY="${QUERY:-}"  # Empty means run all queries
EXCLUDE_QUERIES="${EXCLUDE_QUERIES:-}"
JOIN_VERSION="${JOIN_VERSION:-}"  # Empty means use default DataFusion joins

echo "============================================"
echo "TPC-H Benchmark Runner for Cloud Instances"
echo "============================================"
echo ""
echo "Configuration:"
echo "  Repository: $REPO @ $REPO_BRANCH"
echo "  Data Source: ${DATA_SOURCE:-<none, will generate locally>}"
echo "  Results Destination: ${RESULTS_DEST:-<none, results stay local>}"
echo "  Iterations: $ITERATIONS"
echo "  Scale Factor: $SCALE_FACTOR"
echo "  Query: ${QUERY:-all queries}"
echo "  Join Version: ${JOIN_VERSION:-default}"
echo ""

# Function to detect OS and install packages
install_dependencies() {
    echo "==> Installing system dependencies..."

    if command -v apt-get &> /dev/null; then
        # Debian/Ubuntu
        sudo apt-get update
        sudo apt-get install -y git build-essential curl
    elif command -v yum &> /dev/null; then
        # RHEL/CentOS/Amazon Linux
        sudo yum install -y git gcc gcc-c++ make curl
    else
        echo "Warning: Unknown package manager. Assuming dependencies are installed."
    fi

    echo "==> System dependencies installed"
}

# Install Rust if not present
install_rust() {
    if command -v rustc &> /dev/null; then
        echo "==> Rust already installed: $(rustc --version)"
    else
        echo "==> Installing Rust (nightly)..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly
        source "$HOME/.cargo/env"
        echo "==> Rust installed: $(rustc --version)"
    fi
}

# Install cloud CLI tools if needed
install_cloud_cli() {
    if [[ -n "$DATA_SOURCE" || -n "$RESULTS_DEST" ]]; then
        if [[ "$DATA_SOURCE" == s3://* || "$RESULTS_DEST" == s3://* ]]; then
            if ! command -v aws &> /dev/null; then
                echo "==> Installing AWS CLI..."
                curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
                unzip -q awscliv2.zip
                sudo ./aws/install
                rm -rf aws awscliv2.zip
                echo "==> AWS CLI installed"
            else
                echo "==> AWS CLI already installed"
            fi
        fi

        if [[ "$DATA_SOURCE" == gs://* || "$RESULTS_DEST" == gs://* ]]; then
            if ! command -v gcloud &> /dev/null; then
                echo "==> Installing Google Cloud SDK..."
                curl https://sdk.cloud.google.com | bash
                source "$HOME/google-cloud-sdk/path.bash.inc"
                echo "==> Google Cloud SDK installed"
            else
                echo "==> Google Cloud SDK already installed"
            fi
        fi
    fi
}

# Clone repository
clone_repo() {
    echo "==> Cloning repository..."
    cd "$HOME"
    git clone -b "$REPO_BRANCH" "https://github.com/${REPO}.git" datafusion-parallelism
    cd datafusion-parallelism/tpc
    echo "==> Repository cloned to ~/datafusion-parallelism"
}

# Download or generate TPC-H data
setup_data() {
    if [[ -n "$DATA_SOURCE" ]]; then
        echo "==> Downloading TPC-H data from $DATA_SOURCE..."
        mkdir -p data

        if [[ "$DATA_SOURCE" == s3://* ]]; then
            aws s3 sync "$DATA_SOURCE" ./data
        elif [[ "$DATA_SOURCE" == gs://* ]]; then
            gcloud storage rsync --recursive "$DATA_SOURCE" ./data
        else
            echo "Error: Unsupported data source. Use s3:// or gs:// prefix"
            exit 1
        fi

        echo "==> Data downloaded successfully"
        du -sh data
    else
        echo "==> Generating TPC-H data locally..."
        cargo install tpchgen-cli
        mkdir -p data
        cd data
        tpchgen-cli -s "$SCALE_FACTOR" --format=parquet --parts 32
        cd ..
        echo "==> Data generated successfully"
        du -sh data
    fi
}

# Build the benchmark binary
build_benchmark() {
    echo "==> Building benchmark binary (release mode)..."
    cargo build --release
    echo "==> Build completed"
}

# Run benchmarks
run_benchmark() {
    echo ""
    echo "==> Running TPC-H benchmarks..."
    echo "    Start time: $(date)"
    echo ""

    mkdir -p output

    # Build cargo run command
    # Note: concurrency is determined by the number of CPU cores available
    CMD="cargo run --release --"
    CMD="$CMD --data-path ./data"
    CMD="$CMD --output ./output"
    CMD="$CMD --query-path ./datafusion-benchmarks/tpch/queries"
    CMD="$CMD --iterations $ITERATIONS"

    # Only add join version if specified
    if [[ -n "$JOIN_VERSION" ]]; then
        CMD="$CMD --new-join-replacement $JOIN_VERSION"
    fi

    if [[ -n "$QUERY" ]]; then
        CMD="$CMD --query $QUERY"
    else
        # Run all TPC-H queries (default 22 queries)
        CMD="$CMD --num-queries 22"
        if [[ -n "$EXCLUDE_QUERIES" ]]; then
            for q in ${EXCLUDE_QUERIES//,/ }; do
                CMD="$CMD --exclude $q"
            done
        fi
    fi

    echo "Running: $CMD"
    echo ""

    eval "$CMD"

    echo ""
    echo "==> Benchmarks completed at $(date)"
    echo ""
    echo "==> Results summary:"
    if [[ -f output/results.csv ]]; then
        cat output/results.csv
    fi
}

# Upload results to cloud storage
upload_results() {
    if [[ -n "$RESULTS_DEST" ]]; then
        echo ""
        echo "==> Uploading results to $RESULTS_DEST..."

        if [[ "$RESULTS_DEST" == s3://* ]]; then
            aws s3 sync ./output "$RESULTS_DEST"
        elif [[ "$RESULTS_DEST" == gs://* ]]; then
            gcloud storage rsync --recursive ./output "$RESULTS_DEST"
        else
            echo "Error: Unsupported results destination. Use s3:// or gs:// prefix"
            exit 1
        fi

        echo "==> Results uploaded successfully"
    else
        echo "==> No results destination specified. Results remain in ~/datafusion-parallelism/tpc/output"
    fi
}

# Shutdown instance after completion
shutdown_instance() {
    echo ""
    echo "==> Shutting down instance in 3 seconds..."
    sleep 3
    sudo shutdown -h now
}

# Main execution
main() {
    START_TIME=$(date +%s)

    install_dependencies
    install_rust
    install_cloud_cli
    clone_repo
    setup_data
    build_benchmark
    run_benchmark
    upload_results

    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))

    echo ""
    echo "============================================"
    echo "Benchmark run completed successfully!"
    echo "Total time: ${DURATION}s ($(($DURATION / 60))m $(($DURATION % 60))s)"
    echo "============================================"

    shutdown_instance
}

# Run main function
main
