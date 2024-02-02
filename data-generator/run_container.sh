#!/bin/sh
# Build rust image to generate a test data
echo "Building Data-Generator"
podman build \
  --tag data-generator:latest \
  .
echo

# Run Data-Generator to stack seed GL / TB data to 1 billion rows
echo "Running Data-Generator"
podman run \
  --rm \
  --volume ./seed_data:/opt/data-generator/seed_data:z \
  --volume ./gitignore:/opt/data-generator/gitignore:z \
  data-generator:latest \
  --gl=seed_data/general_ledger_235_469.parquet \
  --tb=seed_data/trail_balance_13_788.parquet \
  --number=4247 \
  --output=gitignore
