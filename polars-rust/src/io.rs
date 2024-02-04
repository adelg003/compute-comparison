use crate::utils::{read_parquet, validate_schema};
use polars::prelude::{DataType, Field, LazyFrame, PolarsError, Schema};
use std::{fs, path::Path};

/// Read valid GL data. If not valid GL, return error
pub fn read_gl(path: &Path) -> Result<LazyFrame, PolarsError> {
    // Read GL Data
    let gl = read_parquet(path)?;

    // Validate GL Schema
    let schema = Schema::from_iter([
        Field::new("GL_Business_Unit_Code", DataType::String),
        Field::new("GL_Doc_Number", DataType::String),
        Field::new("GL_Fiscal_Year", DataType::String),
        Field::new("GL_Line_Number", DataType::String),
        Field::new("GL_Effective_Date", DataType::String),
        Field::new("GL_Account_Number", DataType::String),
        Field::new("GL_Local_Amount", DataType::Float64),
        Field::new("GL_Journal_ID", DataType::String),
    ]);
    validate_schema(&gl, schema)?;

    Ok(gl)
}

/// Read valid TB data. If not valid TB, return error
pub fn read_tb(path: &Path) -> Result<LazyFrame, PolarsError> {
    // Read TB Data
    let tb = read_parquet(path)?;

    // Validate TB Schema
    let schema = Schema::from_iter([
        Field::new("TB_Business_Unit_Code", DataType::String),
        Field::new("TB_Fiscal_Year", DataType::String),
        Field::new("TB_Account_Number", DataType::String),
        Field::new("TB_Amount_Opening_Balance", DataType::Float64),
        Field::new("TB_Amount_Ending_Balance", DataType::Float64),
    ]);
    validate_schema(&tb, schema)?;

    Ok(tb)
}

/// Reset the output folder location
pub fn reset_output(path: &Path) -> Result<(), std::io::Error> {
    if path.is_dir() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    Ok(())
}
