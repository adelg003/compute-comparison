#!/bin/sh
# Build and run rust code to generate unbalances and completness test
cargo run --release -- \
  --gl=../gitignore/data/gl.parquet/gl_00*.parquet \
  --tb=../gitignore/data/tb.parquet/tb_00*.parquet \
  --output=results
# Dont run wide open our will run out of RAM
