mod io;
mod packs;
mod utils;

use crate::{
    io::{read_gl, read_tb, reset_output},
    utils::write_parquet,
};
use clap::Parser;
use color_eyre::eyre;
use packs::{completeness_test, unbalanced_journal_entries_test};
use polars::prelude::{collect_all, DataFrame, LazyFrame, PolarsError};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(version)]
struct CliArgs {
    /// Path to the GL Parquet files. If a directory, use glob format with quotes.
    #[arg(long = "gl")]
    gl_path: PathBuf,
    /// Path to the TB Parquet files. If a directory, use glob format with quotes.
    #[arg(long = "tb")]
    tb_path: PathBuf,
    /// Path to write output to
    #[arg(long = "output")]
    output_path: PathBuf,
}

fn main() -> Result<(), eyre::Error> {
    // Pretty errors
    color_eyre::install()?;

    // Pull in args
    let args = CliArgs::parse();

    // Read GL and TB data
    let gl = read_gl(&args.gl_path)?;
    let tb = read_tb(&args.tb_path)?;

    // Plan how to run the reports
    let unbalanced: LazyFrame = unbalanced_journal_entries_test(&gl);
    let completeness: LazyFrame = completeness_test(&gl, &tb);

    // Run unbalanced and completeness test
    let results: Vec<DataFrame> = collect_all([unbalanced, completeness])?;
    let mut results_iter = results.into_iter();

    let mut unbalanced: DataFrame = results_iter.next().ok_or(PolarsError::NoData(
        "Unbalanced results are missing.".into(),
    ))?;
    let mut completeness: DataFrame = results_iter.next().ok_or(PolarsError::NoData(
        "Completeness results are missing.".into(),
    ))?;

    // Reset the output folder location
    reset_output(&args.output_path)?;

    // Write the results to Parquet
    write_parquet(
        &mut unbalanced,
        &args.output_path.join("unbalanced.parquet"),
    )?;
    write_parquet(
        &mut completeness,
        &args.output_path.join("completeness.parquet"),
    )?;

    Ok(())
}
