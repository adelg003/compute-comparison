use polars::prelude::{LazyFrame, PolarsError, Schema};
use std::path::Path;

/// Read Parquet data
pub fn read_parquet(path: &Path) -> Result<LazyFrame, PolarsError> {
    LazyFrame::scan_parquet(path, Default::default())
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
