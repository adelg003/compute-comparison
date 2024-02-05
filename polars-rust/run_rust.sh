#!/bin/sh
# Build and run rust code to generate unbalances and completness test
cargo run --release -- \
  --gl=../gitignore/data/gl.parquet/gl_00*.parquet \
  --tb=../gitignore/data/tb.parquet/tb_*.parquet \
  --output=gitignore/results
# Don't run GL wide open since we will run out of RAM for completness
