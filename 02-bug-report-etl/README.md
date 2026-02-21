# Bug Report ETL Pipeline

## Overview
A complete ETL pipeline that extracts bug reports from 3 different sources (JIRA, GitHub Issues, Excel), transforms them into a unified schema, classifies bugs by type and SDLC phase, and exports clean data.

## Problem Statement
Organizations track bugs across multiple tools with inconsistent formats:
- JIRA uses P0/P1/P2/P3 priority and custom statuses
- GitHub uses labels and open/closed states
- Excel trackers use free-text severity and informal status notes

## ETL Pipeline (15 Steps)

| Step | Action |
|------|--------|
| 1-3 | Extract and map 3 sources to unified schema |
| 4 | Combine 4620 records from all sources |
| 5 | Remove 120 duplicate records |
| 6 | Standardize 18 priority variations to 4 levels |
| 7 | Standardize 20+ status variations to 4 statuses |
| 8-9 | Standardize components and environments |
| 10 | Classify by SDLC phase |
| 11-14 | Clean dates, time, reporters, browsers |
| 15 | Add bug type classification and resolution days |

## Bug Type Classification (QA Intuition)

- Crash/Fatal: System crashes, null pointer exceptions
- UI/Visual: CSS issues, button alignment, layout problems
- Performance: Slow queries, memory leaks, timeouts
- Security: XSS vulnerabilities, permission issues
- API/Integration: 404/500 errors, gateway failures
- Data Integrity: Wrong calculations, incorrect results
- Functional: General software defects

## Results
- Sources: 3 (JIRA: 2000, GitHub: 1500, Excel: 1000)
- Raw Records: 4620
- Cleaned Records: 4500
- Duplicates Removed: 120
- Automated Tests: 7

## Tech Stack
- Python 3.14, Pandas, NumPy, Matplotlib, Seaborn, PyArrow

## How to Reproduce

    cd 02-bug-report-etl
    python3 -m venv venv
    source venv/bin/activate
    pip3 install pandas numpy matplotlib seaborn pyarrow openpyxl
    python3 src/generate_bug_reports.py
    python3 src/bug_report_etl.py
    python3 src/test_bug_etl.py

## Author
Kethavath Santhosh - June 2025
