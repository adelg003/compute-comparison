use polars::prelude::{col, JoinArgs, JoinType, LazyFrame};

/// Run the Completeness Report
pub fn completeness_test(gl: &LazyFrame, tb: &LazyFrame) -> LazyFrame {
    // Summarize TB by account
    let tb_groupby_columns = [
        col("TB_Fiscal_Year"),
        col("TB_Business_Unit_Code"),
        col("TB_Account_Number"),
    ];
    let tb_total_by_account = tb.clone().group_by(&tb_groupby_columns).agg([
        //TODO Rounding kills streaming
        //col("TB_Amount_Opening_Balance").sum().round(2),
        //col("TB_Amount_Ending_Balance").sum().round(2),
        col("TB_Amount_Opening_Balance").sum(),
        col("TB_Amount_Ending_Balance").sum(),
    ]);

    // Summarize GL by account
    let gl_groupby_columns = [
        col("GL_Fiscal_Year"),
        col("GL_Business_Unit_Code"),
        col("GL_Account_Number"),
    ];
    let gl_total_by_account = gl
        .clone()
        .group_by(&gl_groupby_columns)
        //TODO Rounding kills streaming
        //.agg([col("GL_Local_Amount").sum().round(2)]);
        .agg([col("GL_Local_Amount").sum()]);

    // Combine TB and GL Data
    let gl_tb_total_by_account = tb_total_by_account.join(
        gl_total_by_account,
        &tb_groupby_columns,
        &gl_groupby_columns,
        //TODO Rounding kills streaming, but so do Outer Joins, so just got to live with this one
        //JoinArgs::new(JoinType::Outer { coalesce: true }),
        JoinArgs::new(JoinType::Left),
    );

    // Add the difference between the GL and TB
    let gl_tb_total_by_account = gl_tb_total_by_account.with_column(
        (col("TB_Amount_Opening_Balance").fill_null(0_f64)
            + col("GL_Local_Amount").fill_null(0_f64)
            - col("TB_Amount_Ending_Balance").fill_null(0_f64))
        //TODO Rounding kills streaming
        //.round(2)
        .alias("Difference"),
    );

    // Filer for just columns needed, rename them, and sort to make compressions nice
    gl_tb_total_by_account
        .select([
            col("TB_Business_Unit_Code").alias("Business_Unit_Code"),
            col("TB_Fiscal_Year").alias("Fiscal_Year"),
            col("TB_Account_Number").alias("Account_Number"),
            col("TB_Amount_Opening_Balance").alias("Opening_Balance"),
            col("GL_Local_Amount").alias("Activity"),
            col("TB_Amount_Ending_Balance").alias("Ending_Balance"),
            col("Difference"),
        ])
        .sort_by_exprs(
            [
                col("Business_Unit_Code"),
                col("Fiscal_Year"),
                col("Account_Number"),
            ],
            [false; 3],
            false,
            false,
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{read_gl, read_tb};
    use std::path::Path;

    #[test]
    fn does_completeness_run() {
        let gl = read_gl(Path::new("../gitignore/data/gl.parquet/gl_0000.parquet")).unwrap();
        let tb = read_tb(Path::new("../gitignore/data/tb.parquet/tb_0000.parquet")).unwrap();

        let unbalanced: LazyFrame = completeness_test(&gl, &tb);
        let _ = unbalanced.collect().unwrap();
    }
}
