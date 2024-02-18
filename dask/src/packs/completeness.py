from dask.dataframe.core import DataFrame  # type: ignore
from dask.dataframe.multi import merge  # type: ignore


# Compute the Unbalanced JE Report
def completeness_test(gl: DataFrame, tb: DataFrame) -> DataFrame:
    # Summaraize GL by account number
    gl_groupby_columns = [
        "GL_Fiscal_Year",
        "GL_Business_Unit_Code",
        "GL_Account_Number",
    ]
    gl_by_account = (
        gl.groupby(by=gl_groupby_columns)[["GL_Local_Amount"]].sum().reset_index()
    )

    # Summaraize TB by account number
    tb_groupby_columns = [
        "TB_Fiscal_Year",
        "TB_Business_Unit_Code",
        "TB_Account_Number",
    ]
    tb_by_account = (
        tb.groupby(by=tb_groupby_columns)[
            ["TB_Amount_Opening_Balance", "TB_Amount_Ending_Balance"]
        ]
        .sum()
        .reset_index()
    )

    # Combine GL and TB data
    gl_tb_by_account = merge(
        left=gl_by_account,
        right=tb_by_account,
        how="outer",
        left_on=gl_groupby_columns,
        right_on=tb_groupby_columns,
    )

    # Compute any completness differances
    gl_tb_by_account["Differance"] = (
        gl_tb_by_account["TB_Amount_Opening_Balance"]
        + gl_tb_by_account["GL_Local_Amount"]
        - gl_tb_by_account["TB_Amount_Ending_Balance"]
    )

    # Filter just for the columns we want and cleanup column names
    column_map = {
        "TB_Fiscal_Year": "Fiscal_Year",
        "TB_Business_Unit_Code": "Business_Unit_Code",
        "TB_Account_Number": "Account_Number",
        "TB_Amount_Opening_Balance": "Opening_Balance",
        "GL_Local_Amount": "Activity",
        "TB_Amount_Ending_Balance": "Ending_Balance",
        "Differance": "Differance",
    }
    completness_report = (
        gl_tb_by_account[list(column_map.keys())]
        .rename(columns=column_map)
        .sort_values(
            [
                "Business_Unit_Code",
                "Fiscal_Year",
                "Account_Number",
            ]
        )
    )

    return completness_report
