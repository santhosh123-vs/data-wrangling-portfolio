# Data Wrangling Portfolio

A collection of data wrangling and ETL projects demonstrating data cleaning, quality assurance, and pipeline engineering skills.

## Projects

### Project 1: Log File Cleaner and Classifier
Transforms messy server logs into clean, structured data. Handles multiple timestamp formats, inconsistent severity levels, invalid IPs, and duplicate records.
- 5250 raw records cleaned to 5000
- 8 automated tests
- Polars vs Pandas benchmark (2.44x faster)
- Exports to CSV, JSON, Parquet, Excel

### Project 2: Bug Report ETL Pipeline
Extracts bug reports from 3 sources (JIRA, GitHub, Excel), transforms them into a unified schema, and classifies bugs by type and SDLC phase.
- 4620 raw records from 3 sources cleaned to 4500
- 15-step ETL pipeline
- 7 automated tests
- QA bug classification (Crash vs UI vs Performance vs Security)

## Tech Stack
- Python 3.14
- Pandas, NumPy, Polars
- Matplotlib, Seaborn
- PyArrow (Parquet)
- SQL (upcoming)

## Author
Kethavath Saidamma - June 2025
