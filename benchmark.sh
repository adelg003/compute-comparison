#! /bin/sh

# Variables
MEMORY=48G;
DATA_FOLDER=./gitignore/data;
GL_FILES=gl.parquet/gl_*.parquet;
TB_FILES=tb.parquet/tb_*.parquet;
RESULTS_FOLDER=./gitignore/results;


# Prep for tests
echo "Building PySpark";
podman build \
  --tag pyspark:latest \
  pyspark;
echo;

echo "Building Polars-Rust";
podman build \
  --tag polars-rust:latest \
  polars-rust;
echo;

echo "Building DASK";
podman build \
  --tag dask:latest \
  dask;
echo;


# Prep tests
PYSPARK="
  podman run \
    --rm \
    --cpus=\$CPU \
    --memory=$MEMORY \
    --volume $DATA_FOLDER:/opt/pyspark/data:z \
    --volume $RESULTS_FOLDER:/opt/pyspark/results:z \
    pyspark:latest \
    --gl=./data/$GL_FILES \
    --tb=./data/$TB_FILES \
    --output=./results/pyspark";

POLARS="
  podman run \
    --rm \
    --cpus=\$CPU \
    --memory=$MEMORY \
    --volume $DATA_FOLDER:/opt/polars-rust/data:z \
    --volume $RESULTS_FOLDER:/opt/polars-rust/results:z \
    polars-rust:latest \
    --gl=./data/$GL_FILES \
    --tb=./data/$TB_FILES \
    --output=./results/polars-rust";

DASK="
  podman run \
    --rm \
    --cpus=\$CPU \
    --memory=$MEMORY \
    --volume $DATA_FOLDER:/opt/dask/data:z \
    --volume $RESULTS_FOLDER:/opt/dask/results:z \
    --publish 8787:8787 \
    dask:latest \
    --gl=./data/$GL_FILES \
    --tb=./data/$TB_FILES \
    --output=./results/dask";

# Run benchmarks
hyperfine \
  --warmup 0 \
  --runs 1 \
  --export-markdown benchmark_results.md \
  --command-name "dask cpu:32" \
  "CPU=32 && $DASK" \
  --command-name "dask cpu:16" \
  "CPU=16 && $DASK" \
  --command-name "dask cpu:08" \
  "CPU=8 && $DASK" \
  --command-name "dask cpu:04" \
  "CPU=4 && $DASK" \
  --command-name "polars cpu:32" \
  "CPU=32 && $POLARS" \
  --command-name "polars cpu:16" \
  "CPU=16 && $POLARS" \
  --command-name "polars cpu:08" \
  "CPU=8 && $POLARS" \
  --command-name "polars cpu:04" \
  "CPU=4 && $POLARS" \
  --command-name "pyspark cpu:32" \
  "CPU=32 && $PYSPARK" \
  --command-name "pyspark cpu:16" \
  "CPU=16 && $PYSPARK" \
  --command-name "pyspark cpu:08" \
  "CPU=8 && $PYSPARK" \
  --command-name "pyspark cpu:04" \
  "CPU=4 && $PYSPARK";
