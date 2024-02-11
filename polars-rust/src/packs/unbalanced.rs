use polars::prelude::{col, JoinArgs, JoinType, LazyFrame};

/// Compute the Unbalanced JE Report
pub fn unbalanced_journal_entries_test(gl: &LazyFrame) -> LazyFrame {
    // Filter out 0 amount lines as they don't mean anything.
    let gl_filtered = gl.clone().filter(col("GL_Local_Amount").abs().gt(0.00001));

    // Get JE totals
    let je_totals = gl_filtered
        .clone()
        .group_by([col("GL_Journal_ID")])
        //TODO Adding the round funtion breaks data streaming, for now.
        //.agg([col("GL_Local_Amount").sum().round(2)]);
        .agg([col("GL_Local_Amount").sum()]);

    // Filter for unbalanced JEs
    let unbalanced_je = je_totals
        //TODO Adding the round funtion breaks data streaming, for now.
        //.filter(col("GL_Local_Amount").abs().gt(0));
        .filter(col("GL_Local_Amount").abs().gt(0.00001));

    // Pull all lines for unbalanced JEs
    let unbalanced_je_lines = gl_filtered.clone().join(
        unbalanced_je,
        [col("GL_Journal_ID")],
        [col("GL_Journal_ID")],
        JoinArgs::new(JoinType::Inner),
    );

    // Filter for just the columns we want on the report and sort them to make compression happier
    unbalanced_je_lines
        .select([
            col("GL_Journal_ID"),
            col("GL_Line_Number"),
            col("GL_Effective_Date"),
            col("GL_Account_Number"),
            col("GL_Local_Amount"),
        ])
        .sort_by_exprs(
            [col("GL_Journal_ID"), col("GL_Line_Number")],
            [false; 2],
            false,
            false,
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::read_gl;
    use std::path::Path;

    #[test]
    fn does_unbalanced_run() {
        let gl = read_gl(Path::new("../gitignore/data/gl.parquet/gl_0000.parquet")).unwrap();

        let unbalanced: LazyFrame = unbalanced_journal_entries_test(&gl);
        let _ = unbalanced.collect().unwrap();
    }
}
