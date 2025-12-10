#!/usr/bin/env bash
set -euo pipefail

# Script to launch a GCP instance, run benchmarks, and download results

# Default configuration
INSTANCE_NAME="tpch-benchmark-$(date +%s)"
INSTANCE_TYPE="c2-standard-8"
ZONE="us-central1-a"
PROJECT=""  # Will use gcloud default if not specified
IMAGE_FAMILY="ubuntu-minimal-2404-lts"
IMAGE_PROJECT="ubuntu-os-cloud"
DATA_SOURCE=""
RESULTS_DEST=""
ITERATIONS="10"
SCALE_FACTOR="10"
QUERY=""
EXCLUDE_QUERIES=""
JOIN_VERSION=""
LOCAL_RESULTS_DIR="./benchmark-results"
REPO="jamesfer/datafusion-parallelism"
REPO_BRANCH="master"

# Parse command line arguments
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Launch a GCP instance to run TPC-H benchmarks and download results.

OPTIONS:
    -n, --name NAME              Instance name (default: tpch-benchmark-<timestamp>)
    -t, --type TYPE              Instance type (default: c2-standard-8)
    -z, --zone ZONE              GCP zone (default: us-central1-a)
    -p, --project PROJECT        GCP project (default: gcloud config default)
    --image-family FAMILY        Image family (default: ubuntu-minimal-2404-lts)
    -d, --data-source PATH       GCS path to TPC-H data (gs://bucket/path)
    -r, --results-dest PATH      GCS path for results (gs://bucket/path)
    -l, --local-results DIR      Local directory to download results (default: ./benchmark-results)
    -i, --iterations NUM         Number of iterations (default: 10)
    -s, --scale-factor NUM       Data scale factor (default: 10)
    -q, --query NUM              Specific query to run (optional)
    -e, --exclude QUERIES        Comma-separated queries to exclude (optional)
    -j, --join-version VERSION   Join version (e.g., version3) (optional)
    --repo OWNER/REPO            GitHub repository (default: jamesfer/datafusion-parallelism)
    --repo-branch BRANCH         Repository branch (default: master)
    -h, --help                   Show this help message

EXAMPLES:
    # Run benchmark with existing data
    $0 -d gs://my-bucket/tpch-data -r gs://my-bucket/results

    # Run on larger instance with specific query
    $0 -t c2-standard-30 -q 21 -d gs://my-bucket/tpch-data -r gs://my-bucket/results

    # Test specific join version
    $0 -j version3 -d gs://my-bucket/tpch-data -r gs://my-bucket/results

INSTANCE TYPES (examples):
    c2-standard-4    (4 vCPU, 16 GB RAM)
    c2-standard-8    (8 vCPU, 32 GB RAM)   [default]
    c2-standard-16   (16 vCPU, 64 GB RAM)
    c2-standard-30   (30 vCPU, 120 GB RAM)
    c2d-standard-8   (8 vCPU, 32 GB RAM)   [AMD alternative]
    n2-standard-8    (8 vCPU, 32 GB RAM)   [cheaper general purpose]

IMAGE FAMILIES (common):
    ubuntu-minimal-2404-lts  (Ubuntu 24.04 LTS Minimal)  [default]
    ubuntu-2204-lts          (Ubuntu 22.04 LTS)
    ubuntu-2004-lts          (Ubuntu 20.04 LTS)
    debian-12                (Debian 12)
    rocky-linux-9            (Rocky Linux 9)

EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--name)
            INSTANCE_NAME="$2"
            shift 2
            ;;
        -t|--type)
            INSTANCE_TYPE="$2"
            shift 2
            ;;
        -z|--zone)
            ZONE="$2"
            shift 2
            ;;
        -p|--project)
            PROJECT="$2"
            shift 2
            ;;
        --image-family)
            IMAGE_FAMILY="$2"
            shift 2
            ;;
        -d|--data-source)
            DATA_SOURCE="$2"
            shift 2
            ;;
        -r|--results-dest)
            RESULTS_DEST="$2"
            shift 2
            ;;
        -l|--local-results)
            LOCAL_RESULTS_DIR="$2"
            shift 2
            ;;
        -i|--iterations)
            ITERATIONS="$2"
            shift 2
            ;;
        -s|--scale-factor)
            SCALE_FACTOR="$2"
            shift 2
            ;;
        -q|--query)
            QUERY="$2"
            shift 2
            ;;
        -e|--exclude)
            EXCLUDE_QUERIES="$2"
            shift 2
            ;;
        -j|--join-version)
            JOIN_VERSION="$2"
            shift 2
            ;;
        --repo)
            REPO="$2"
            shift 2
            ;;
        --repo-branch)
            REPO_BRANCH="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validation
if [[ -z "$RESULTS_DEST" ]]; then
    echo "Error: --results-dest is required"
    echo ""
    usage
fi

if [[ "$RESULTS_DEST" != gs://* ]]; then
    echo "Error: --results-dest must start with gs://"
    exit 1
fi

if [[ -n "$DATA_SOURCE" && "$DATA_SOURCE" != gs://* ]]; then
    echo "Error: --data-source must start with gs://"
    exit 1
fi

# Display configuration
echo "============================================"
echo "GCP TPC-H Benchmark Runner"
echo "============================================"
echo ""
echo "Instance Configuration:"
echo "  Name: $INSTANCE_NAME"
echo "  Type: $INSTANCE_TYPE (Spot)"
echo "  Zone: $ZONE"
echo "  Image: $IMAGE_FAMILY"
if [[ -n "$PROJECT" ]]; then
    echo "  Project: $PROJECT"
fi
echo "  Boot Disk: 200GB"
echo ""
echo "Benchmark Configuration:"
echo "  Data Source: ${DATA_SOURCE:-<will generate locally>}"
echo "  Results Destination: $RESULTS_DEST"
echo "  Iterations: $ITERATIONS"
echo "  Scale Factor: $SCALE_FACTOR"
echo "  Query: ${QUERY:-all queries}"
echo "  Join Version: ${JOIN_VERSION:-default}"
if [[ -n "$EXCLUDE_QUERIES" ]]; then
    echo "  Exclude Queries: $EXCLUDE_QUERIES"
fi
echo ""
echo "Repository:"
echo "  Repo: $REPO"
echo "  Branch: $REPO_BRANCH"
echo ""

# Create startup script
STARTUP_SCRIPT=$(cat << 'SCRIPT_END'
#!/bin/bash
set -euo pipefail

# Export configuration as environment variables
export REPO="__REPO__"
export REPO_BRANCH="__REPO_BRANCH__"
export DATA_SOURCE="__DATA_SOURCE__"
export RESULTS_DEST="__RESULTS_DEST__"
export ITERATIONS="__ITERATIONS__"
export SCALE_FACTOR="__SCALE_FACTOR__"
export QUERY="__QUERY__"
export EXCLUDE_QUERIES="__EXCLUDE_QUERIES__"
export JOIN_VERSION="__JOIN_VERSION__"

# Log startup
echo "========================================" | tee /var/log/startup-script.log
echo "Startup script beginning at $(date)" | tee -a /var/log/startup-script.log
echo "========================================" | tee -a /var/log/startup-script.log

# Download and run the benchmark script
echo "Downloading benchmark script..." | tee -a /var/log/startup-script.log
SCRIPT_URL="https://raw.githubusercontent.com/${REPO}/refs/heads/${REPO_BRANCH}/tpc/scripts/_run_benchmark.sh"
echo "URL: $SCRIPT_URL" | tee -a /var/log/startup-script.log

curl -fsSL "$SCRIPT_URL" -o /tmp/run_benchmark.sh 2>&1 | tee -a /var/log/startup-script.log
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to download benchmark script" | tee -a /var/log/startup-script.log
    exit 1
fi

echo "Running benchmark script..." | tee -a /var/log/startup-script.log
chmod +x /tmp/run_benchmark.sh
/tmp/run_benchmark.sh 2>&1 | tee -a /var/log/benchmark.log

echo "Startup script completed at $(date)" | tee -a /var/log/startup-script.log
SCRIPT_END
)

# Substitute variables in startup script
STARTUP_SCRIPT="${STARTUP_SCRIPT//__REPO__/$REPO}"
STARTUP_SCRIPT="${STARTUP_SCRIPT//__REPO_BRANCH__/$REPO_BRANCH}"
STARTUP_SCRIPT="${STARTUP_SCRIPT//__DATA_SOURCE__/$DATA_SOURCE}"
STARTUP_SCRIPT="${STARTUP_SCRIPT//__RESULTS_DEST__/$RESULTS_DEST}"
STARTUP_SCRIPT="${STARTUP_SCRIPT//__ITERATIONS__/$ITERATIONS}"
STARTUP_SCRIPT="${STARTUP_SCRIPT//__SCALE_FACTOR__/$SCALE_FACTOR}"
STARTUP_SCRIPT="${STARTUP_SCRIPT//__QUERY__/$QUERY}"
STARTUP_SCRIPT="${STARTUP_SCRIPT//__EXCLUDE_QUERIES__/$EXCLUDE_QUERIES}"
STARTUP_SCRIPT="${STARTUP_SCRIPT//__JOIN_VERSION__/$JOIN_VERSION}"

# Build gcloud command
GCLOUD_CMD="gcloud compute instances create $INSTANCE_NAME"
GCLOUD_CMD="$GCLOUD_CMD --machine-type=$INSTANCE_TYPE"
GCLOUD_CMD="$GCLOUD_CMD --zone=$ZONE"
GCLOUD_CMD="$GCLOUD_CMD --image-family=$IMAGE_FAMILY"
GCLOUD_CMD="$GCLOUD_CMD --image-project=$IMAGE_PROJECT"
GCLOUD_CMD="$GCLOUD_CMD --boot-disk-size=200GB"
#GCLOUD_CMD="$GCLOUD_CMD --boot-disk-type=hyperdisk-balanced"
GCLOUD_CMD="$GCLOUD_CMD --provisioning-model=SPOT"
GCLOUD_CMD="$GCLOUD_CMD --instance-termination-action=DELETE"
GCLOUD_CMD="$GCLOUD_CMD --scopes=storage-rw"
GCLOUD_CMD="$GCLOUD_CMD --metadata=startup-script='$STARTUP_SCRIPT'"

if [[ -n "$PROJECT" ]]; then
    GCLOUD_CMD="$GCLOUD_CMD --project=$PROJECT"
fi

echo "==> Creating GCP instance..."
echo ""

# Create instance
eval "$GCLOUD_CMD"

echo ""
echo "==> Instance created: $INSTANCE_NAME"
echo ""
echo "==> Waiting for instance to boot (30 seconds)..."
sleep 30

echo ""
echo "==> Checking startup script logs..."
gcloud compute ssh "$INSTANCE_NAME" --zone="$ZONE" --command="sudo cat /var/log/startup-script.log 2>/dev/null || echo 'Startup script not started yet'" || echo "Cannot connect yet, instance may still be booting"

echo ""
echo "==> Monitoring instance status..."
echo "    The instance will run benchmarks and automatically shut down when complete."
echo ""
echo "    Monitor commands:"
echo "      gcloud compute ssh $INSTANCE_NAME --zone=$ZONE --command='sudo tail -f /var/log/benchmark.log'"
echo "      gcloud compute instances get-serial-port-output $INSTANCE_NAME --zone=$ZONE"
echo ""

# Wait for instance to stop (indicating completion)
while true; do
    STATUS=$(gcloud compute instances describe "$INSTANCE_NAME" --zone="$ZONE" --format="value(status)" 2>/dev/null || echo "NOTFOUND")

    if [[ "$STATUS" == "TERMINATED" || "$STATUS" == "STOPPING" ]]; then
        echo "==> Instance has shut down (benchmark complete)"
        break
    elif [[ "$STATUS" == "NOTFOUND" ]]; then
        echo "==> Instance not found (may have been deleted)"
        break
    fi

    echo "    Status: $STATUS - $(date '+%H:%M:%S')"
    sleep 30
done

echo ""
echo "==> Downloading results from $RESULTS_DEST to $LOCAL_RESULTS_DIR..."
mkdir -p "$LOCAL_RESULTS_DIR"
gcloud storage rsync --recursive "$RESULTS_DEST" "$LOCAL_RESULTS_DIR"

echo ""
echo "==> Results downloaded successfully"
echo ""

# Display results summary if available
if [[ -f "$LOCAL_RESULTS_DIR/results.csv" ]]; then
    echo "==> Results Summary:"
    cat "$LOCAL_RESULTS_DIR/results.csv"
    echo ""
fi

echo "==> Cleaning up instance..."
gcloud compute instances delete "$INSTANCE_NAME" --zone="$ZONE" --quiet

echo ""
echo "============================================"
echo "Benchmark Complete!"
echo "============================================"
echo "Results are in: $LOCAL_RESULTS_DIR"
echo ""
