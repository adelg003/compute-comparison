#!/bin/sh
# Build rust image to generate a test data
echo "Building PySpark" &&
podman build \
  --tag pyspark:latest \
  . &&
echo &&

# Run Data-Generator to stack seed GL / TB data to 1 billion rows
echo "Running PySpark" &&
podman run \
  --rm \
  --cpus=4 \
  --memory=32G \
  --volume ../gitignore/data:/opt/pyspark/data:z \
  --volume ./gitignore:/opt/pyspark/gitignore:z \
  pyspark:latest \
  --gl=./data/gl.parquet/ \
  --tb=./data/tb.parquet/ \
  --output=./gitignore/results/ &&
echo "Done"
