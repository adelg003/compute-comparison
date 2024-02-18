from dask.dataframe.core import DataFrame  # type: ignore
from dask.dataframe.multi import merge  # type: ignore


# Compute the Unbalanced JE Report
def unbalanced_test(gl: DataFrame) -> DataFrame:
    # Filter out $0 lines as they don't really do anything from an accounting standpoint.
    gl_filtered: DataFrame = gl[gl["GL_Local_Amount"] != 0]  # type: ignore

    # Set Index and repartion our GL so all lines for a JE are in the same partion.
    # This is needed for us to avoid Out of Memory issues when grouping our GL by JE ID.
    gl_indexed = (
        gl_filtered.repartition(partition_size="100MB")
        .set_index("GL_Journal_ID")  # type: ignore
        .repartition(partition_size="100MB")
    )

    # Get JE totals withing a partion only (needed to avoid OOM issues)
    gl_je_total: DataFrame = gl_indexed[["GL_Local_Amount"]].map_partitions(  # type: ignore
        func=lambda df: df.groupby(by="GL_Journal_ID")[["GL_Local_Amount"]].sum()
    )

    # Get Unbalanced JEs
    gl_unbalanced_je = gl_je_total[gl_je_total["GL_Local_Amount"].abs().round(2) > 0]  # type: ignore

    # Get Unbalanced Lines
    gl_unbalanced_lines = merge(
        left=gl_indexed,
        right=gl_unbalanced_je,
        how="inner",
        left_index=True,
        right_index=True,
        suffixes=(None, "_y"),
    )

    # Sort the results to make parquet compression happy
    gl_unbalanced_lines_sorted = gl_unbalanced_lines.map_partitions(
        func=lambda df: df.sort_values(
            by=[
                "GL_Journal_ID",
                "GL_Line_Number",
            ]
        )
    )

    # Filter just for the columns we want.
    unbalanced_lines_report = gl_unbalanced_lines_sorted.reset_index()[
        [
            "GL_Journal_ID",
            "GL_Line_Number",
            "GL_Effective_Date",
            "GL_Account_Number",
            "GL_Local_Amount",
        ]
    ]

    return unbalanced_lines_report
