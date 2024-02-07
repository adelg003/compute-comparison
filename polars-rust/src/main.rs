mod io;
mod packs;
mod utils;

use crate::io::{read_gl, read_tb, reset_output};
use clap::Parser;
use color_eyre::eyre;
use packs::{completeness_test, unbalanced_journal_entries_test};
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
    let unbalanced = unbalanced_journal_entries_test(&gl);
    let completeness = completeness_test(&gl, &tb);

    // Reset the output folder location
    reset_output(&args.output_path)?;

    // Run Unbalanced and write results
    println!("Running Unbalanced test");
    unbalanced.sink_parquet(
        args.output_path.join("unbalanced.parquet"),
        Default::default(),
    )?;

    // Run Completeness and write results
    println!("Running Completeness test");
    completeness.sink_parquet(
        args.output_path.join("completeness.parquet"),
        Default::default(),
    )?;

    Ok(())
}
