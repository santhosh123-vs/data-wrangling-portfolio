# Log File Cleaner and Classifier

## Overview
A complete, modular, and tested data wrangling pipeline that transforms messy, inconsistent server log data into clean, structured, analysis-ready datasets.

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

## Architecture

### Modular Design
The pipeline is built as a reusable LogCleaner class that can be imported into any project:

    from src.log_cleaner import LogCleaner
    
    cleaner = LogCleaner()
    cleaner.load_csv("raw_data/messy_server_logs.csv")
    cleaner.run_full_pipeline()
    cleaner.export_data()

### Test Suite
8 automated tests verify every cleaning step:

    python3 src/test_log_cleaner.py
    
    RESULTS: 8 passed, 0 failed out of 8

## Cleaning Pipeline

| Step | Action | Rows Affected | Reason |
|------|--------|---------------|--------|
| 1 | Remove duplicates | 250 | Duplicates inflate error counts and skew analysis |
| 2 | Standardize severity (12 to 4) | 4622 | Inconsistent labels prevent priority analysis |
| 3 | Standardize environment (9 to 3) | 4541 | Needed for isolating production vs dev issues |
| 4 | Unify timestamps | 5000 | Multiple formats prevent time-series analysis |
| 5 | Clean response times | 2223 | Negative/zero/outlier values corrupt statistics |
| 6 | Clean IP addresses | 1998 | INVALID_IP and 0.0.0.0 are not real addresses |
| 7 | Standardize user IDs | 965 | Mixed formats prevent per-user analysis |
| 8 | Fill missing categories | 1470 | Preserves records while flagging incomplete data |

## Decision Log

### Why fill missing values instead of dropping rows?
Dropping rows with missing categories would lose valuable timing and error data. Filling with Unknown preserves records while clearly marking them as incomplete.

### Why remove response times over 10000ms?
Values above 10000ms likely represent timeout events rather than actual response times. Including them would inflate mean response time and skew distribution analysis.

### Why standardize user IDs to USR-XXXX?
Raw data contains bare numbers (5678), prefixed IDs (USR-1234), and placeholders (UNKNOWN). Standardizing enables accurate per-user error frequency analysis.

## Results
- Original rows: 5250
- Cleaned rows: 5000
- Duplicates removed: 250
- All timestamps unified to ISO format
- All severity levels standardized to Critical/High/Medium/Low
- All environments standardized to production/staging/development
- Response time range: 10.75ms to 4996ms (mean: 2480.81ms)

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
      raw_data/
        messy_server_logs.csv
        messy_api_logs.json
      cleaned_data/
        clean_server_logs.csv
        clean_server_logs.json
        clean_server_logs.parquet
        clean_server_logs.xlsx
      notebooks/
        log_cleaning_EDA.ipynb
      src/
        generate_raw_logs.py
        log_cleaner.py
        test_log_cleaner.py
      metadata/
        data_lineage.json
        missing_values_chart.png
        cleaning_results.png
        before_after_missing.png
        errors_by_month.png
      README.md

## How to Reproduce

    git clone https://github.com/santhosh123-vs/data-wrangling-portfolio.git
    cd data-wrangling-portfolio/01-log-file-cleaner
    python3 -m venv venv
    source venv/bin/activate
    pip3 install pandas numpy matplotlib seaborn pyarrow openpyxl
    python3 src/generate_raw_logs.py
    python3 src/log_cleaner.py
    python3 src/test_log_cleaner.py

## Author
Kethavath Santhosh - February 2025
