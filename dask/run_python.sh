#!/bin/sh
# Run unbalances and completness test
./venv/bin/python ./src/main.py \
  --gl=../gitignore/data/gl.parquet/ \
  --tb=../gitignore/data/tb.parquet/ \
  --output=gitignore/results
