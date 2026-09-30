# Automated Daily Data Cleaning & Reporting Pipeline

An end-to-end, fully automated data pipeline built on Snowflake — cleans raw customer/sales data every morning, fixes nulls, duplicates, and data discrepancies, and emails a data quality report to a manager automatically, with no manual steps.

## What it does

Every morning at 9:00 AM IST, the pipeline automatically:
1. Reads raw data from `RAW_DATA`
2. Cleans it — fixes inconsistent text casing, detects and converts date columns (filling gaps with forward/backward-fill), removes exact duplicate rows, and fills missing numeric/text values (median and mode respectively)
3. Writes the cleaned result to `CLEANED_DATA`, keeping the original raw data untouched for auditing
4. Generates a plain-English summary of what was cleaned
5. Emails that summary automatically to a manager

## Tech stack & what each tool did

**Snowflake** — the production engine. Hosts the data warehouse (raw and cleaned tables), runs the cleaning logic as Python stored procedures (Snowpark), sends the report natively via `SYSTEM$SEND_EMAIL`, and schedules the entire pipeline with a native `TASK` on a cron schedule. Everything in production runs inside Snowflake — no external server required.

**Google Colab** — the development and testing environment. Cleaning rules (null handling, duplicate detection, casing/date normalization) were prototyped and validated interactively here, connecting directly to Snowflake via `snowflake-connector-python`, before being converted into permanent Snowflake stored procedures.

## Architecture

```
Daily Task (Snowflake, 9 AM cron trigger)
        │
        ▼
run_daily_report()  — master procedure
        │
        ├──▶ clean_and_report()  — cleans RAW_DATA → writes CLEANED_DATA
        │
        ├──▶ generate_summary()  — turns cleaning stats into a readable report
        │
        └──▶ SYSTEM$SEND_EMAIL()  — emails the summary to the manager
```

## Build journey

1. Prepared a raw CSV with real-world nulls, duplicates, and discrepancies
2. Set up a dedicated Snowflake database, schema, and warehouse
3. Connected to Snowflake from Google Colab and loaded the CSV
4. Prototyped and validated cleaning logic interactively in Colab (pandas)
5. Rebuilt the proven cleaning logic as a permanent Snowflake stored procedure
6. Added a summary-generation procedure and native email sending
7. Combined cleaning, summarizing, and emailing into one master procedure
8. Scheduled it with a Snowflake `TASK`, tested end-to-end, and activated it

## Key engineering decisions

- **Raw data is never overwritten** — cleaning always writes to a separate table, preserving the original for auditing or reprocessing.
- **Cleaning logic is type-based, not column-name-based** — it inspects each column's data type rather than hardcoding column names, so it generalizes across different datasets.
- **Fully serverless automation** — Snowflake's native `TASK` scheduler removes the need for any external cron job or third-party scheduler.

## Planned extensions

- Real-time source refresh (auto-loading a daily-updating source into `RAW_DATA` before cleaning runs)
- AI-generated summaries via Snowflake Cortex (Claude), currently blocked on trial accounts — falls back to a deterministic Python-generated summary
- BI dashboard integration (Power BI / Tableau) connecting directly to `CLEANED_DATA`

## Skills demonstrated

- SQL & Python (Snowpark) development inside Snowflake
- Data cleaning: null handling, deduplication, type coercion, and date imputation strategies
- Prototyping with `pandas` in Google Colab before productionizing
- Workflow automation and scheduling (Snowflake Tasks)
- Automated email reporting pipelines
