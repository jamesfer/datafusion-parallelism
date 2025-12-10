#!/usr/bin/env bash
set -euo pipefail

# Generates TPC-H dataset using tpchgen-cli (Rust-based generator)
# and optionally uploads to cloud storage

# Default values
SCALE_FACTOR=10
PARTITIONS=32
OUTPUT_DIR="./data"
UPLOAD_DEST=""

# Parse command line arguments
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Generate TPC-H dataset and optionally upload to cloud storage.

OPTIONS:
    -s, --scale-factor NUM    Scale factor for data generation (default: 10)
    -p, --partitions NUM      Number of partitions per table (default: 32)
    -o, --output-dir PATH     Output directory for generated data (default: ./data)
    -u, --upload DEST         Upload destination (s3://bucket/path or gs://bucket/path)
    -h, --help                Show this help message

NOTE:
    Partitions create multiple parquet files per table for parallel processing.
    Higher partition counts improve parallelism but create more files.

EXAMPLES:
    # Generate scale factor 10 with 32 partitions
    $0 -s 10 -p 32

    # Generate and upload to S3
    $0 -s 10 -p 32 -u s3://my-bucket/tpch-data

    # Generate and upload to Google Cloud Storage
    $0 -s 10 -p 32 -u gs://my-bucket/tpch-data

EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--scale-factor)
            SCALE_FACTOR="$2"
            shift 2
            ;;
        -p|--partitions)
            PARTITIONS="$2"
            shift 2
            ;;
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -u|--upload)
            UPLOAD_DEST="$2"
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

echo "==> TPC-H Data Generation Configuration"
echo "    Scale Factor: $SCALE_FACTOR"
echo "    Partitions: $PARTITIONS"
echo "    Output Directory: $OUTPUT_DIR"
if [ -n "$UPLOAD_DEST" ]; then
    echo "    Upload Destination: $UPLOAD_DEST"
fi
echo ""

# Check if tpchgen-cli is installed
if ! command -v tpchgen-cli &> /dev/null; then
    echo "==> tpchgen-cli not found. Installing..."
    cargo install tpchgen-cli
    echo "==> tpchgen-cli installed successfully"
else
    echo "==> tpchgen-cli already installed"
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Navigate to output directory for generation
cd "$OUTPUT_DIR"

echo ""
echo "==> Generating TPC-H data (this may take a few minutes)..."
start_time=$(date +%s)

# Generate data in parquet format with partitions
# --parts creates multiple partition files for each table
tpchgen-cli -s "$SCALE_FACTOR" --format=parquet --parts "$PARTITIONS"

end_time=$(date +%s)
duration=$((end_time - start_time))

echo "==> Data generation completed in ${duration}s"

# Display generated files
echo ""
echo "==> Generated table directories:"
ls -lh

# Upload to cloud storage if destination provided
if [ -n "$UPLOAD_DEST" ]; then
    echo ""
    echo "==> Uploading data to $UPLOAD_DEST..."

    if [[ "$UPLOAD_DEST" == s3://* ]]; then
        # AWS S3 upload (syncs entire directory structure)
        aws s3 sync . "$UPLOAD_DEST"
        echo "==> Upload to S3 completed"
    elif [[ "$UPLOAD_DEST" == gs://* ]]; then
        # Google Cloud Storage upload (syncs entire directory structure)
        gcloud storage rsync --recursive . "$UPLOAD_DEST"
        echo "==> Upload to GCS completed"
    else
        echo "Error: Unsupported upload destination. Use s3:// or gs:// prefix"
        exit 1
    fi
fi

echo ""
echo "==> Done!"
