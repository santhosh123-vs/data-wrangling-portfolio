# Log File Cleaner & Classifier

## Overview
A complete data wrangling pipeline that transforms messy, inconsistent server log data into clean, structured, analysis-ready datasets.

## Problem Statement
Raw server logs contain multiple data quality issues:
- 4+ different timestamp formats plus Unix timestamps
- 12 severity level variations for just 4 categories
- 9 environment name variations for just 3 categories
- Invalid IP addresses (INVALID_IP, 0.0.0.0)
- Mixed user ID formats (USR-XXXX vs raw numbers vs UNKNOWN)
- Negative and outlier response times (-1, 0, 99999)
- 250 duplicate records
- Up to 41% missing data in some columns

## Cleaning Pipeline

| Step | Action | Rows Affected |
|------|--------|---------------|
| 1 | Remove duplicate rows | 250 |
| 2 | Standardize severity levels (12 to 4) | 4,622 |
| 3 | Standardize environment names (9 to 3) | 4,541 |
| 4 | Unify timestamp formats to datetime | 5,000 |
| 5 | Remove invalid response times | 2,223 |
| 6 | Clean invalid IP addresses | 1,998 |
| 7 | Standardize user IDs to USR-XXXX | 965 |
| 8 | Fill missing categorical values | 1,470 |

## Results
- Original rows: 5,250
- Cleaned rows: 5,000
- Duplicates removed: 250
- All timestamps unified to ISO format
- All severity levels standardized to Critical/High/Medium/Low
- All environments standardized to production/staging/development
- Response time range: 10.75ms to 4,996ms (mean: 2,480.81ms)

## Tech Stack
- Python 3.14
- Pandas 3.0.1
- NumPy 2.4.2
- Matplotlib 3.10.8
- Seaborn 0.13.2
- PyArrow 23.0.1

## Output Formats
- CSV
- JSON
- Parquet
- Excel

## Project Structure
01-log-file-cleaner/
├── raw_data/
│ ├── messy_server_logs.csv
│ └── messy_api_logs.json
├── cleaned_data/
│ ├── clean_server_logs.csv
│ ├── clean_server_logs.json
│ ├── clean_server_logs.parquet
│ └── clean_server_logs.xlsx
├── notebooks/
│ └── log_cleaning_EDA.ipynb
├── src/
│ └── generate_raw_logs.py
├── metadata/
│ ├── data_lineage.json
│ ├── missing_values_chart.png
│ ├── cleaning_results.png
│ ├── before_after_missing.png
│ └── errors_by_month.png
└── README.md

text

## Key Visualizations

### Missing Values Before vs After
Shows dramatic reduction in missing categorical values after cleaning.

### Error Types by Month
Stacked bar chart showing distribution of 10 error types across 12 months.

### Cleaning Results Dashboard
4-panel visualization showing severity distribution, environment distribution, error type breakdown, and response time histogram.

## Author
Kethavath Santhosh
February 2026
