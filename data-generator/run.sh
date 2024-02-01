#!/bin/sh
cargo run --release -- \
  --gl=seed_data/general_ledger_235_469.parquet \
  --tb=seed_data/trail_balance_13_788.parquet \
  --number=4247 \
  --output=gitignore
