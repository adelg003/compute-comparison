from pyspark.sql.dataframe import DataFrame  # type: ignore
from pyspark.sql.functions import abs, round, sum  # type: ignore


# Compute the Unbalanced JE Report
def unbalanced_test(gl: DataFrame) -> DataFrame:
    # Push PySpark Dataframes to SQL like objects
    gl_filtered = gl.filter(gl["GL_Local_Amount"] != 0)

    # Get JE totals
    gl_je_total = gl_filtered.groupby("GL_Journal_ID").agg(
        sum("GL_Local_Amount").alias("GL_Local_Amount")
    )

    # Get Magnitude of all unbalanced JEs
    gl_je_magnitude = (
        gl_je_total.withColumn("GL_Local_Amount", abs("GL_Local_Amount"))
        .withColumn("GL_Local_Amount", round("GL_Local_Amount", 2))
        .withColumnRenamed("GL_Local_Amount", "GL_Magnitude")
    )

    # Get Unbalanced JEs
    gl_unbalanced_je = gl_je_magnitude.filter(gl_je_magnitude["GL_Magnitude"] > 0)

    # Get Unbalanced Lines
    gl_unbalanced_lines = gl_filtered.join(
        gl_unbalanced_je,
        on=["GL_Journal_ID"],
        how="inner",
    )

    # Filter just for the columns we want and sort so results compress well
    unbalanced_lines_report = gl_unbalanced_lines[
        [
            "GL_Journal_ID",
            "GL_Line_Number",
            "GL_Effective_Date",
            "GL_Account_Number",
            "GL_Local_Amount",
        ]
    ].sort(
        [
            "GL_Journal_ID",
            "GL_Line_Number",
        ]
    )
    return unbalanced_lines_report
