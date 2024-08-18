Generate data with
```
poetry run bash scripts/generate.sh
```

Run benchmarks with
```
cargo run -- --concurrency 32 --data-path ./data --iterations 10 --output ./output --query-path ./datafusion-benchmarks/tpch/queries --query 21 --new-join-replacement version3
```
