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
use polars::prelude::{DataFrame, LazyFrame};
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

    // Reset the output folder location
    reset_output(&args.output_path)?;

    // Run Unbalanced test and write the results to Parquet. Also make sure to remove Dataframe from
    // memory before next test runs.
    {
        println!("Running Unbalanced test");
        let mut unbalanced: DataFrame = unbalanced.collect()?;
        println!("Writing Unbalanced results");
        write_parquet(
            &mut unbalanced,
            &args.output_path.join("unbalanced.parquet"),
        )?;
        println!("Unbalanced results writen");
    }

    // Run Completeness test and write the results to Parquet. Also make sure to remove Dataframe from
    // memory before next test runs.
    {
        println!("Running Completeness test");
        let mut completeness: DataFrame = completeness.collect()?;
        println!("Writing Completeness results");
        write_parquet(
            &mut completeness,
            &args.output_path.join("completeness.parquet"),
        )?;
        println!("Completeness results writen");
    }

    Ok(())
}
