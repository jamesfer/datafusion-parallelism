#!/usr/bin/env bash
set -euo pipefail

# This script runs on a cloud instance to perform TPC-H benchmarks
# It installs dependencies, clones the repo, downloads data, runs benchmarks, and uploads results

# Set HOME if not set (happens when running as startup script)
export HOME="${HOME:-/root}"

# Track if we had an error
ERROR_OCCURRED=false

# Trap to ensure shutdown on error or completion
trap 'handle_exit' EXIT

# Handle script exit (success or error)
handle_exit() {
    local exit_code=$?

    if [ $exit_code -ne 0 ]; then
        ERROR_OCCURRED=true
        echo ""
        echo "============================================"
        echo "ERROR: Script failed with exit code $exit_code"
        echo "============================================"
        echo ""

        # Create error report
        ERROR_LOG="/tmp/error-report.txt"
        {
            echo "=========================================="
            echo "BENCHMARK RUN FAILED"
            echo "=========================================="
            echo "Exit Code: $exit_code"
            echo "Time: $(date)"
            echo "Failed at line: ${BASH_LINENO[0]}"
            echo ""
            echo "Configuration:"
            echo "  Data Source: ${DATA_SOURCE:-<none>}"
            echo "  Results Dest: ${RESULTS_DEST:-<none>}"
            echo "  Iterations: $ITERATIONS"
            echo "  Scale Factor: $SCALE_FACTOR"
            echo "  Query: ${QUERY:-all queries}"
            echo "  Join Version: ${JOIN_VERSION:-default}"
            echo ""
            echo "=========================================="
            echo "RECENT LOG OUTPUT (last 1000 lines):"
            echo "=========================================="
            tail -1000 /var/log/benchmark.log 2>/dev/null || echo "No benchmark log found"
        } > "$ERROR_LOG"

        # Upload error log if RESULTS_DEST is set
        if [[ -n "$RESULTS_DEST" ]]; then
            echo "==> Uploading error report to ${RESULTS_DEST}/ERROR.txt"
            if [[ "$RESULTS_DEST" == s3://* ]]; then
                aws s3 cp "$ERROR_LOG" "${RESULTS_DEST}/ERROR.txt" || true
            elif [[ "$RESULTS_DEST" == gs://* ]]; then
                gcloud storage cp "$ERROR_LOG" "${RESULTS_DEST}/ERROR.txt" || true
            fi
        fi

        cat "$ERROR_LOG"
    fi
}

# Configuration via environment variables with defaults
DATA_SOURCE="${DATA_SOURCE:-}"  # S3 or GCS path to download TPC-H data
RESULTS_DEST="${RESULTS_DEST:-}" # S3 or GCS path to upload results
ITERATIONS="${ITERATIONS:-100}"
CONCURRENCY="${CONCURRENCY:-}"  # Number of parallel workers (empty = auto-detect)
QUERY="${QUERY:-}"  # Empty means run all queries
EXCLUDE_QUERIES="${EXCLUDE_QUERIES:-}"
JOIN_VERSION="${JOIN_VERSION:-}"  # Empty means use default DataFusion joins

echo "============================================"
echo "TPC-H Benchmark Runner for Cloud Instances"
echo "============================================"
echo ""
echo "Configuration:"
echo "  Data Source: ${DATA_SOURCE:-<none, will generate locally>}"
echo "  Results Destination: ${RESULTS_DEST:-<none, results stay local>}"
echo "  Iterations: $ITERATIONS"
echo "  Concurrency: ${CONCURRENCY:-auto-detect}"
echo "  Query: ${QUERY:-all queries}"
echo "  Join Version: ${JOIN_VERSION:-default}"
echo ""

# Download or setup TPC-H data
setup_data() {
    if [[ -n "$DATA_SOURCE" ]]; then
        if [[ "$DATA_SOURCE" == s3://* ]]; then
            echo "==> Downloading TPC-H data from S3: $DATA_SOURCE..."
            mkdir -p data
            aws s3 sync "$DATA_SOURCE" ./data
            DATA_PATH="./data"
            echo "==> Data downloaded successfully from S3"
            du -sh "$DATA_PATH"
        elif [[ "$DATA_SOURCE" == gs://* ]]; then
            echo "==> Downloading TPC-H data from GCS: $DATA_SOURCE..."
            mkdir -p data
            gcloud storage rsync --recursive "$DATA_SOURCE" ./data
            DATA_PATH="./data"
            echo "==> Data downloaded successfully from GCS"
            du -sh "$DATA_PATH"
        else
            # Treat as local directory path
            if [[ -d "$DATA_SOURCE" ]]; then
                echo "==> Using local data directory: $DATA_SOURCE"
                DATA_PATH="$DATA_SOURCE"
                du -sh "$DATA_PATH"
            else
                echo "Error: Local data directory does not exist: $DATA_SOURCE"
                exit 1
            fi
        fi
    else
        echo "Error: DATA_SOURCE is required. Specify a local directory or cloud storage bucket (s3:// or gs://)"
        exit 1
    fi
}

# Run benchmarks
run_benchmark() {
    echo ""
    echo "==> Running TPC-H benchmarks..."
    echo "    Start time: $(date)"
    echo ""

    mkdir -p output

    # Build cargo run command
    CMD="tpc"
    CMD="$CMD --data-path $DATA_PATH"
    CMD="$CMD --output ./output"
    CMD="$CMD --query-path ./tpc/queries"
    CMD="$CMD --iterations $ITERATIONS"

    # Add parallelism if specified
    if [[ -n "$CONCURRENCY" ]]; then
        CMD="$CMD --concurrency $CONCURRENCY"
    fi

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

# Main execution
main() {
    START_TIME=$(date +%s)

    setup_data
    run_benchmark
    upload_results

    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))

    echo ""
    echo "============================================"
    echo "Benchmark run completed successfully!"
    echo "Total time: ${DURATION}s ($(($DURATION / 60))m $(($DURATION % 60))s)"
    echo "============================================"
}

# Run main function
main
