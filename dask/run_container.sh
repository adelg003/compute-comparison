#!/bin/sh
# Build image to generate a test data
echo "Building DASK" &&
podman build \
  --tag dask:latest \
  . &&
echo &&

# Run Data-Generator to stack seed GL / TB data to 1 billion rows
echo "Running DASK" &&
podman run \
  --rm \
  --cpus=4 \
  --memory=32G \
  --volume ../gitignore/data:/opt/dask/data:z \
  --volume ./gitignore:/opt/dask/gitignore:z \
  --publish 8787:8787 \
  dask:latest \
  --gl=./data/gl.parquet/ \
  --tb=./data/tb.parquet/ \
  --output=./gitignore/results/ &&
echo "Done"
