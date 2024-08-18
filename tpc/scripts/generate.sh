set -eox pipefail

# Generates the data set

dir="$(dirname "$0")"

python "$dir/../datafusion-benchmarks/tpch/tpchgen.py" generate --scale-factor 1 --partitions 32
python "$dir/../datafusion-benchmarks/tpch/tpchgen.py" convert --scale-factor 1 --partitions 32
