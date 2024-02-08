#!/bin/sh
# Build rust image to generate a test data
echo "Building Polars-Rust" &&
podman build \
  --tag polars-rust:latest \
  . &&
echo &&

# Run Data-Generator to stack seed GL / TB data to 1 billion rows
echo "Running Polars-Rust" &&
podman run \
  --rm \
  --cpus=4 \
  --memory=32G \
  --volume ../gitignore/data:/opt/polars-rust/data:z \
  --volume ./gitignore:/opt/polars-rust/gitignore:z \
  polars-rust:latest \
  --gl=./data/gl.parquet/gl_*.parquet \
  --tb=./data/tb.parquet/tb_*.parquet \
  --output=./gitignore/results &&
echo "Done"
