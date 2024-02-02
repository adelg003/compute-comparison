use clap::Parser;
use color_eyre::eyre;
use polars::prelude::{
    col, concat_str, DataType, Field, GetOutput, LazyFrame, PolarsError, Schema, Series,
};
use std::{
    fs,
    path::{Path, PathBuf},
    usize,
};

#[derive(Debug, Parser)]
#[command(version)]
struct CliArgs {
    /// Path to the GL Parquet files. If a directory, use glob format with quotes.
    #[arg(long = "gl")]
    gl_path: PathBuf,
    /// Path to the TB Parquet files. If a directory, use glob format with quotes.
    #[arg(long = "tb")]
    tb_path: PathBuf,
    /// Number of times to stack the seed data
    #[arg(long = "number")]
    number_of_stacks: usize,
    /// Path to write the stacks TB and GL to
    #[arg(long = "output")]
    output_path: PathBuf,
}

fn main() -> Result<(), eyre::Error> {
    // Pretty errors
    color_eyre::install()?;

    // Pull in args
    let args = CliArgs::parse();

    // Stack TB data
    println!("Generating TB Data");
    let tb_folder: PathBuf =
        generate_tb_data(&args.tb_path, &args.number_of_stacks, &args.output_path)?;
    println!("TB Data is generated at: {}\n", &tb_folder.display());

    // Stack GL data
    println!("Generating GL Data");
    let gl_folder: PathBuf =
        generate_gl_data(&args.gl_path, &args.number_of_stacks, &args.output_path)?;
    println!("GL Data is generated at: {}\n", &gl_folder.display());

    Ok(())
}

/// Lets generate the GL data
fn generate_gl_data(
    input_path: &Path,
    number_of_stacks: &usize,
    output_path: &Path,
) -> Result<PathBuf, PolarsError> {
    // Read GL Seed
    let gl: LazyFrame = LazyFrame::scan_parquet(input_path, Default::default())?;

    // Schema validation
    let gl_schema = gl.schema()?;
    let expected_schema = Schema::from_iter([
        Field::new("GL_Business_Unit_Code", DataType::String),
        Field::new("GL_Doc_Number", DataType::String),
        Field::new("GL_Fiscal_Year", DataType::String),
        Field::new("GL_Line_Number", DataType::String),
        Field::new("GL_Effective_Date", DataType::String),
        Field::new("GL_Account_Number", DataType::String),
        Field::new("GL_Local_Amount", DataType::Float64),
        Field::new("GL_Journal_ID", DataType::String),
    ]);
    assert_eq!(gl_schema, expected_schema.into());

    // Lets build a GL Stack
    let gls: Vec<LazyFrame> = (0..*number_of_stacks)
        .map(|stack| {
            // Current Stack to replace FY in GL with, zero padded.
            let fiscal_year = format!("{:0>4}", &stack);

            // Replace the Fiscal year with stack layer
            let wip_gl: LazyFrame = gl.clone().with_column(
                // Overwrite Fiscal Year
                col("GL_Fiscal_Year")
                    .map(
                        move |series: Series| {
                            let new_series: Series =
                                series.iter().map(|_| fiscal_year.clone()).collect();
                            Ok(Some(new_series))
                        },
                        GetOutput::from_type(DataType::String),
                    )
                    .alias("GL_Fiscal_Year"),
            );

            // Replace JE ID with new fiscal_year.
            let wip_gl: LazyFrame = wip_gl.with_column(
                concat_str(
                    [
                        col("GL_Business_Unit_Code"),
                        col("GL_Doc_Number"),
                        col("GL_Fiscal_Year"),
                    ],
                    "-",
                )
                .alias("GL_Journal_ID"),
            );

            wip_gl
        })
        .collect();

    // Folder we will write generated GL to
    let gl_folder: PathBuf = output_path.join("gl.parquet");
    if gl_folder.is_dir() {
        fs::remove_dir_all(&gl_folder)?;
    }
    fs::create_dir_all(&gl_folder)?;

    // Collect Lazyframes and write to disk
    gls.into_iter()
        .enumerate()
        .try_for_each(|(index, gl): (usize, LazyFrame)| {
            // Path we will write generated GL to
            let file_name = format!("gl_{:0>4}.parquet", index);
            let gl_path: PathBuf = gl_folder.join(file_name);

            // Write GL out.
            println!("GL Data - Generating: {}", &gl_path.display());
            gl.sink_parquet(gl_path.clone(), Default::default())
        })?;

    Ok(gl_folder)
}

/// Lets generate the GL data
fn generate_tb_data(
    input_path: &Path,
    number_of_stacks: &usize,
    output_path: &Path,
) -> Result<PathBuf, PolarsError> {
    // Read TB Seed
    let tb: LazyFrame = LazyFrame::scan_parquet(input_path, Default::default())?;

    // Schema validation
    let tb_schema = tb.schema()?;
    let expected_schema = Schema::from_iter([
        Field::new("TB_Business_Unit_Code", DataType::String),
        Field::new("TB_Fiscal_Year", DataType::String),
        Field::new("TB_Account_Number", DataType::String),
        Field::new("TB_Amount_Opening_Balance", DataType::Float64),
        Field::new("TB_Amount_Ending_Balance", DataType::Float64),
    ]);
    assert_eq!(tb_schema, expected_schema.into());

    // Lets build a TB Stack
    let tbs: Vec<LazyFrame> = (0..*number_of_stacks)
        .map(|stack| {
            // Current Stack to replace FY in TB with, zero padded.
            let fiscal_year = format!("{:0>4}", &stack);

            // Replace the Fiscal year with stack layer
            let wip_tb: LazyFrame = tb.clone().with_column(
                // Overwrite Fiscal Year
                col("TB_Fiscal_Year")
                    .map(
                        move |series: Series| {
                            let new_series: Series =
                                series.iter().map(|_| fiscal_year.clone()).collect();
                            Ok(Some(new_series))
                        },
                        GetOutput::from_type(DataType::String),
                    )
                    .alias("TB_Fiscal_Year"),
            );

            wip_tb
        })
        .collect();

    // Folder we will write generated TB to
    let tb_folder: PathBuf = output_path.join("tb.parquet");
    if tb_folder.is_dir() {
        fs::remove_dir_all(&tb_folder)?;
    }
    fs::create_dir_all(&tb_folder)?;

    // Collect Lazyframes and write to disk
    tbs.into_iter()
        .enumerate()
        .try_for_each(|(index, tb): (usize, LazyFrame)| {
            // Path we will write geneated TB to
            let file_name = format!("tb_{:0>4}.parquet", index);
            let tb_path: PathBuf = tb_folder.join(file_name);

            // Write TB out.
            println!("TB Data - Generating: {}", &tb_path.display());
            tb.sink_parquet(tb_path.clone(), Default::default())
        })?;

    Ok(tb_folder)
}
