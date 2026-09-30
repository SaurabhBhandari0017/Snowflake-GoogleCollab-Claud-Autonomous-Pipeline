/* ============================================================
   AUTOMATED DAILY DATA CLEANING & REPORTING PIPELINE
   Snowflake-native: cleaning, summary, and email all run inside
   Snowflake, triggered automatically every day by a native Task.
   ============================================================ */


-- ============================================================
-- SETUP (one-time)
-- ============================================================
CREATE WAREHOUSE IF NOT EXISTS REPORT_WH WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60;
CREATE DATABASE IF NOT EXISTS MY_DATA;
CREATE SCHEMA IF NOT EXISTS MY_DATA.PUBLIC;
USE WAREHOUSE REPORT_WH;
USE DATABASE MY_DATA;
USE SCHEMA PUBLIC;

-- One-time email integration (requires ACCOUNTADMIN)
USE ROLE ACCOUNTADMIN;
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS report_email_int
  TYPE = EMAIL
  ENABLED = TRUE;


-- ============================================================
-- STEP 1: Cleaning procedure
-- Reads a source table, fixes nulls/duplicates/discrepancies,
-- writes the cleaned result to a target table.
-- ============================================================
CREATE OR REPLACE PROCEDURE clean_and_report(source_table STRING, target_table STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'pyarrow')
HANDLER = 'run'
AS
$$
import pandas as pd
from snowflake.snowpark import Session

def run(session: Session, source_table: str, target_table: str):
    df = session.table(source_table).to_pandas()
    rows_before = len(df)

    text_cols = df.select_dtypes(include="object").columns.tolist()

    # Trim whitespace + fix inconsistent casing on text columns
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip().str.title()

    # Detect and convert date-like text columns to real dates
    date_cols = []
    for col in df.select_dtypes(include="object").columns:
        sample = df[col].dropna().astype(str).head(20)
        parsed = pd.to_datetime(sample, errors='coerce')
        if len(sample) > 0 and parsed.notna().mean() > 0.7:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            date_cols.append(col)

    # Remove exact duplicate rows
    dupes = df.duplicated().sum()
    df = df.drop_duplicates()

    # Fill nulls, column by column, based on type
    for col in df.columns:
        if df[col].isna().sum() == 0:
            continue
        if col in date_cols:
            df[col] = df[col].ffill().bfill()
        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].median())
        else:
            mode_val = df[col].mode(dropna=True)
            fill_val = mode_val.iloc[0] if not mode_val.empty else "Unknown"
            df[col] = df[col].fillna(fill_val)

    rows_after = len(df)
    session.write_pandas(df, target_table, auto_create_table=True, overwrite=True)

    return {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "duplicates_removed": int(dupes),
        "date_columns_filled": date_cols,
        "source_table": source_table,
        "target_table": target_table,
    }
$$;


-- ============================================================
-- STEP 2: Summary procedure
-- Turns the cleaning stats into a plain-English report.
-- (Uses a Python template rather than Cortex/Claude, since
--  AI functions are unavailable on Snowflake trial accounts.
--  Swap in SNOWFLAKE.CORTEX.COMPLETE() here once available.)
-- ============================================================
CREATE OR REPLACE PROCEDURE generate_summary(cleaning_stats VARIANT)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session, cleaning_stats: dict):
    date_cols = ", ".join(cleaning_stats.get("date_columns_filled", [])) or "none"
    return (
        f"Daily data quality report: processed {cleaning_stats['rows_before']} rows from "
        f"{cleaning_stats['source_table']}, resulting in {cleaning_stats['rows_after']} clean rows "
        f"after removing {cleaning_stats['duplicates_removed']} duplicate entries. "
        f"Missing dates were filled in these columns: {date_cols}. "
        f"The cleaned data is available in {cleaning_stats['target_table']}."
    )
$$;


-- ============================================================
-- STEP 3: Master procedure
-- Runs cleaning, generates the summary, and emails it — one call.
-- ============================================================
CREATE OR REPLACE PROCEDURE run_daily_report(manager_email STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark import Session

def run(session: Session, manager_email: str):
    stats = session.call("clean_and_report", "RAW_DATA", "CLEANED_DATA")
    summary_text = session.call("generate_summary", stats)
    session.sql(
        "CALL SYSTEM$SEND_EMAIL(?, ?, ?, ?, ?)",
        params=["report_email_int", manager_email, "Daily Data Quality Report", summary_text, "text/plain"],
    ).collect()
    return summary_text
$$;


-- ============================================================
-- STEP 4: Schedule — runs automatically every day at 9:00 AM IST
-- ============================================================
CREATE OR REPLACE TASK daily_report_task
  WAREHOUSE = REPORT_WH
  SCHEDULE = 'USING CRON 30 3 * * * UTC'
AS
  CALL run_daily_report('manager@yourcompany.com');   -- replace with the real recipient

ALTER TASK daily_report_task RESUME;

-- Verify it's active:
-- SHOW TASKS;  (state should read 'started')
