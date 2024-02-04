use polars::prelude::{DataFrame, LazyFrame, ParquetWriter, PolarsError, Schema};
use std::{fs::File, path::Path};

/// Read Parquet data
pub fn read_parquet(path: &Path) -> Result<LazyFrame, PolarsError> {
    LazyFrame::scan_parquet(path, Default::default())
}

/// Write results to Parquet
pub fn write_parquet(df: &mut DataFrame, path: &Path) -> Result<u64, PolarsError> {
    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file).finish(df)
}

/// Validate Parquet Schema
pub fn validate_schema(lf: &LazyFrame, schema: Schema) -> Result<(), PolarsError> {
    match lf.schema()? == schema.into() {
        true => Ok(()),
        false => Err(PolarsError::SchemaMismatch(
            "The expected and provided schemas did not match".into(),
        )),
    }
}
