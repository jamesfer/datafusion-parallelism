#!/usr/bin/env bash
set -euo pipefail

# Script to deploy TPC-H benchmarks to Google Cloud Batch using Docker

# Default configuration
JOB_NAME=""  # Will be set after parsing arguments
MACHINE_TYPE="c2-standard-8"
CPU="8"  # Number of vCPUs
MEMORY="32"  # Memory in GB
BOOT_DISK_SIZE_GB="100"  # Boot disk size in GB
PROVISIONING_MODEL="SPOT"  # SPOT or STANDARD
REGION="us-central1"
PROJECT=""  # Will use gcloud default if not specified
DOCKER_IMAGE=""
DATA_SOURCE=""
RESULTS_DEST=""
ITERATIONS="100"
SCALE_FACTOR="10"
CONCURRENCY=""
QUERY=""
EXCLUDE_QUERIES=""
JOIN_VERSION=""

# Parse command line arguments
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Deploy TPC-H benchmarks to Google Cloud Batch using a Docker image.

REQUIRED OPTIONS:
    -i, --image IMAGE            Docker image to run (e.g., gcr.io/project/tpc-benchmark:latest)
    -d, --data-source PATH       GCS path to TPC-H data or local path in container (gs://bucket/path)
    -r, --results-dest PATH      GCS path for results (gs://bucket/path)

OPTIONAL OPTIONS:
    -n, --name NAME              Job name (default: tpch-benchmark-<timestamp>)
    -m, --machine-type TYPE      Machine type (default: c2-standard-8)
    --cpu NUM                    Number of vCPUs (default: 8)
    --memory NUM                 Memory in GB (default: 32)
    --disk-size NUM              Boot disk size in GB (default: 100)
    --spot / --no-spot           Use Spot VMs for cost savings (default: --spot)
    -g, --region REGION          GCP region (default: us-central1)
    -p, --project PROJECT        GCP project (default: gcloud config default)
    --iterations NUM             Number of iterations (default: 100)
    -s, --scale-factor NUM       Data scale factor (default: 10)
    --concurrency NUM            Number of parallel workers (default: auto-detect)
    -q, --query NUM              Specific query to run (optional)
    -e, --exclude QUERIES        Comma-separated queries to exclude (optional)
    -j, --join-version VERSION   Join version (e.g., version10) (optional)
    -h, --help                   Show this help message

NOTE:
    Spot VMs are 60-91% cheaper but can be preempted. Use --no-spot for
    guaranteed completion if needed.

EXAMPLES:
    # Run benchmark with cloud data
    $0 -i gcr.io/my-project/tpc:latest \\
       -d gs://my-bucket/tpch-data \\
       -r gs://my-bucket/results

    # Run on larger instance with specific configuration
    $0 -i gcr.io/my-project/tpc:latest \\
       -m c2-standard-30 \\
       --concurrency 30 \\
       -q 21 \\
       -d gs://my-bucket/tpch-data \\
       -r gs://my-bucket/results

    # Test specific join version
    $0 -i gcr.io/my-project/tpc:latest \\
       -j version10 \\
       -d gs://my-bucket/tpch-data \\
       -r gs://my-bucket/results

MACHINE TYPES (examples):
    c2-standard-4    (4 vCPU, 16 GB RAM)
    c2-standard-8    (8 vCPU, 32 GB RAM)   [default]
    c2-standard-16   (16 vCPU, 64 GB RAM)
    c2-standard-30   (30 vCPU, 120 GB RAM)
    c2d-standard-8   (8 vCPU, 32 GB RAM)   [AMD alternative]
    n2-standard-8    (8 vCPU, 32 GB RAM)   [cheaper general purpose]

EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--name)
            JOB_NAME="$2"
            shift 2
            ;;
        -m|--machine-type)
            MACHINE_TYPE="$2"
            shift 2
            ;;
        --cpu)
            CPU="$2"
            shift 2
            ;;
        --memory)
            MEMORY="$2"
            shift 2
            ;;
        --disk-size)
            BOOT_DISK_SIZE_GB="$2"
            shift 2
            ;;
        --spot)
            PROVISIONING_MODEL="SPOT"
            shift 1
            ;;
        --no-spot)
            PROVISIONING_MODEL="STANDARD"
            shift 1
            ;;
        -g|--region)
            REGION="$2"
            shift 2
            ;;
        -p|--project)
            PROJECT="$2"
            shift 2
            ;;
        -i|--image)
            DOCKER_IMAGE="$2"
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
        --iterations)
            ITERATIONS="$2"
            shift 2
            ;;
        -s|--scale-factor)
            SCALE_FACTOR="$2"
            shift 2
            ;;
        --concurrency)
            CONCURRENCY="$2"
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
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Set job name if not provided, including CPU and scale factor
if [[ -z "$JOB_NAME" ]]; then
    JOB_NAME="tpch-sf${SCALE_FACTOR}-cpu${CPU}-$(date +%s)"
fi

# Validation
if [[ -z "$DOCKER_IMAGE" ]]; then
    echo "Error: --image is required"
    echo ""
    usage
fi

if [[ -z "$DATA_SOURCE" ]]; then
    echo "Error: --data-source is required"
    echo ""
    usage
fi

if [[ -z "$RESULTS_DEST" ]]; then
    echo "Error: --results-dest is required"
    echo ""
    usage
fi

if [[ "$RESULTS_DEST" != gs://* ]]; then
    echo "Error: --results-dest must start with gs://"
    exit 1
fi

# Create unique subdirectory for this test run under the results destination
RESULTS_SUBDIR="${RESULTS_DEST%/}/$JOB_NAME"

# Display configuration
echo "============================================"
echo "Google Cloud Batch TPC-H Benchmark Runner"
echo "============================================"
echo ""
echo "Job Configuration:"
echo "  Name: $JOB_NAME"
echo "  Machine Type: $MACHINE_TYPE"
echo "  CPU: ${CPU} vCPUs"
echo "  Memory: ${MEMORY} GB"
echo "  Boot Disk: ${BOOT_DISK_SIZE_GB} GB (pd-balanced)"
echo "  Provisioning: $PROVISIONING_MODEL"
echo "  Region: $REGION"
if [[ -n "$PROJECT" ]]; then
    echo "  Project: $PROJECT"
fi
echo "  Docker Image: $DOCKER_IMAGE"
echo ""
echo "Benchmark Configuration:"
echo "  Data Source: $DATA_SOURCE"
echo "  Results Destination: $RESULTS_SUBDIR"
echo "  Iterations: $ITERATIONS"
echo "  Scale Factor: $SCALE_FACTOR"
echo "  Concurrency: ${CONCURRENCY:-auto-detect}"
echo "  Query: ${QUERY:-all queries}"
echo "  Join Version: ${JOIN_VERSION:-default}"
if [[ -n "$EXCLUDE_QUERIES" ]]; then
    echo "  Exclude Queries: $EXCLUDE_QUERIES"
fi
echo ""

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_FILE="$SCRIPT_DIR/batch-job-template.yaml"

if [[ ! -f "$TEMPLATE_FILE" ]]; then
    echo "Error: Template file not found: $TEMPLATE_FILE"
    exit 1
fi

# Convert CPU and memory to API units
CPU_MILLI=$((CPU * 1000))
MEMORY_MIB=$((MEMORY * 1024))

# Create configuration YAML for this test run
TEST_CONFIG_FILE=$(mktemp)
cat > "$TEST_CONFIG_FILE" << EOF
# TPC-H Benchmark Configuration
# Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")

job:
  name: $JOB_NAME
  region: $REGION
  project: ${PROJECT:-default}

infrastructure:
  machine_type: $MACHINE_TYPE
  cpu_vcpus: $CPU
  memory_gb: $MEMORY
  boot_disk_size_gb: $BOOT_DISK_SIZE_GB
  provisioning_model: $PROVISIONING_MODEL
  docker_image: $DOCKER_IMAGE

benchmark:
  data_source: $DATA_SOURCE
  results_destination: $RESULTS_SUBDIR
  iterations: $ITERATIONS
  scale_factor: $SCALE_FACTOR
  concurrency: ${CONCURRENCY:-auto-detect}
  query: ${QUERY:-all}
  exclude_queries: ${EXCLUDE_QUERIES:-none}
  join_version: ${JOIN_VERSION:-default}
EOF

# Upload configuration to results directory
echo "==> Uploading test configuration..."
gcloud storage cp "$TEST_CONFIG_FILE" "${RESULTS_SUBDIR}/config.yaml"
rm "$TEST_CONFIG_FILE"

# Read template and replace variables
CONFIG_FILE=$(mktemp)
cp "$TEMPLATE_FILE" "$CONFIG_FILE"

# Replace all placeholders using sed
sed -i.bak \
    -e "s|__DOCKER_IMAGE__|${DOCKER_IMAGE}|g" \
    -e "s|__DATA_SOURCE__|${DATA_SOURCE}|g" \
    -e "s|__RESULTS_DEST__|${RESULTS_SUBDIR}|g" \
    -e "s|__ITERATIONS__|${ITERATIONS}|g" \
    -e "s|__SCALE_FACTOR__|${SCALE_FACTOR}|g" \
    -e "s|__CONCURRENCY__|${CONCURRENCY}|g" \
    -e "s|__QUERY__|${QUERY}|g" \
    -e "s|__EXCLUDE_QUERIES__|${EXCLUDE_QUERIES}|g" \
    -e "s|__JOIN_VERSION__|${JOIN_VERSION}|g" \
    -e "s|__CPU_MILLI__|${CPU_MILLI}|g" \
    -e "s|__MEMORY_MIB__|${MEMORY_MIB}|g" \
    -e "s|__MACHINE_TYPE__|${MACHINE_TYPE}|g" \
    -e "s|__BOOT_DISK_SIZE_GB__|${BOOT_DISK_SIZE_GB}|g" \
    -e "s|__PROVISIONING_MODEL__|${PROVISIONING_MODEL}|g" \
    "$CONFIG_FILE"

rm "${CONFIG_FILE}.bak"

echo "==> Creating Google Cloud Batch job..."
echo ""

# Build gcloud command
GCLOUD_CMD="gcloud batch jobs submit $JOB_NAME"
GCLOUD_CMD="$GCLOUD_CMD --location=$REGION"
GCLOUD_CMD="$GCLOUD_CMD --config=$CONFIG_FILE"

if [[ -n "$PROJECT" ]]; then
    GCLOUD_CMD="$GCLOUD_CMD --project=$PROJECT"
fi

# Submit the job
eval "$GCLOUD_CMD"

# Clean up config file
rm "$CONFIG_FILE"

echo ""
echo "==> Job submitted: $JOB_NAME"
echo ""
echo "==> Monitor commands:"
echo "    gcloud batch jobs describe $JOB_NAME --location=$REGION"
echo "    gcloud batch jobs list --location=$REGION"
echo "    gcloud logging read \"resource.type=cloud_batch_job AND resource.labels.job_id=$JOB_NAME\" --limit 50 --format json"
echo ""
echo "==> Wait for job completion:"
echo "    gcloud batch jobs describe $JOB_NAME --location=$REGION --format='value(status.state)'"
echo ""
echo "==> Once complete, download results from: $RESULTS_SUBDIR"
echo ""
