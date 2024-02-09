from pyspark.sql.dataframe import DataFrame  # type: ignore
from pyspark.sql.functions import sum  # type: ignore


# Compute the Unbalanced JE Report
def completeness_test(gl: DataFrame, tb: DataFrame) -> DataFrame:

    # Summaraize GL by account number
    gl_groupby_columns = [
        "GL_Fiscal_Year",
        "GL_Business_Unit_Code",
        "GL_Account_Number",
    ]
    gl_by_account = gl.groupby(gl_groupby_columns).agg(
        sum("GL_Local_Amount").alias("GL_Local_Amount")
    )

    # Summaraize TB by account number
    tb_groupby_columns = [
        "TB_Fiscal_Year",
        "TB_Business_Unit_Code",
        "TB_Account_Number",
    ]
    tb_by_account = tb.groupby(tb_groupby_columns).agg(
        sum("TB_Amount_Opening_Balance").alias("TB_Amount_Opening_Balance"),
        sum("TB_Amount_Ending_Balance").alias("TB_Amount_Ending_Balance"),
    )

    # Combine GL and TB data
    gl_tb_by_account = gl_by_account.join(
        tb_by_account,
        on=[
            gl_by_account["GL_Fiscal_Year"] == tb_by_account["TB_Fiscal_Year"],
            gl_by_account["GL_Business_Unit_Code"]
            == tb_by_account["TB_Business_Unit_Code"],
            gl_by_account["GL_Account_Number"] == tb_by_account["TB_Account_Number"],
        ],
        how="outer",
    )

    # Fill in Null columns from Outer Join
    gl_tb_by_account = gl_tb_by_account.na.fill(0)

    # Compute any completness differances
    gl_tb_by_account = gl_tb_by_account.withColumn(
        "Differance",
        (
            gl_tb_by_account["TB_Amount_Opening_Balance"]
            + gl_tb_by_account["GL_Local_Amount"]
            - gl_tb_by_account["TB_Amount_Ending_Balance"]
        ),
    )

    # Filter just for the columns we want and cleanup column names
    column_list = [
        "TB_Fiscal_Year",
        "TB_Business_Unit_Code",
        "TB_Account_Number",
        "TB_Amount_Opening_Balance",
        "GL_Local_Amount",
        "TB_Amount_Ending_Balance",
        "Differance",
    ]
    completness_report = (
        gl_tb_by_account[column_list]
        .withColumnRenamed("TB_Fiscal_Year", "Fiscal_Year")
        .withColumnRenamed("TB_Business_Unit_Code", "Business_Unit_Code")
        .withColumnRenamed("TB_Account_Number", "Account_Number")
        .withColumnRenamed("TB_Amount_Opening_Balance", "Opening_Balance")
        .withColumnRenamed("GL_Local_Amount", "Activity")
        .withColumnRenamed("TB_Amount_Ending_Balance", "Ending_Balance")
    )
    return completness_report
