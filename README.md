# Data Wrangling Portfolio

A collection of data wrangling and ETL projects demonstrating data cleaning, quality assurance, and pipeline engineering skills.

## Projects

### Project 1: Log File Cleaner and Classifier
Transforms messy server logs into clean, structured data. Handles multiple timestamp formats, inconsistent severity levels, invalid IPs, and duplicate records.
- 5,250 raw records cleaned to 5,000
- 8-step cleaning pipeline with modular LogCleaner class
- 8 automated tests
- Polars vs Pandas benchmark (2.44x faster)
- Exports to CSV, JSON, Parquet, Excel
- Complete data lineage documentation

### Project 2: Bug Report ETL Pipeline
Extracts bug reports from 3 sources (JIRA, GitHub, Excel), transforms them into a unified schema, and classifies bugs by type and SDLC phase.
- 4,620 raw records from 3 sources cleaned to 4,500
- 15-step ETL pipeline with modular BugReportETL class
- 7 automated tests
- QA bug classification (Crash vs UI vs Performance vs Security)
- SDLC phase analysis (Requirements/Design/Development/Testing)

### Project 3: SQL Database Cleaning and EDA
Cleans a messy SQLite e-commerce database with 4 tables and 13,690 records using SQL queries and Python.
- 4 tables: customers, orders, products, support_tickets
- 13,690 raw records across all tables
- 19-step cleaning pipeline with modular SQLCleaner class
- 5 automated tests
- SQL quality check queries for duplicate detection, invalid values, and missing data
- Exports to CSV and Parquet

## Portfolio Summary

| Metric | Project 1 | Project 2 | Project 3 | Total |
|--------|-----------|-----------|-----------|-------|
| Raw Records | 5,250 | 4,620 | 13,690 | 23,560 |
| Cleaning Steps | 8 | 15 | 19 | 42 |
| Automated Tests | 8 | 7 | 5 | 20 |
| Data Sources | 2 | 3 | 4 tables | 9 |

## Skills Demonstrated
- Data Wrangling (CSV, JSON, Parquet, Excel, SQLite)
- ETL Pipeline Design
- QA Bug Classification (system crash vs UI glitch vs performance)
- SDLC Phase Analysis
- SQL Data Quality Checks
- Automated Testing
- Data Lineage Documentation
- Polars vs Pandas Performance Benchmarking

## Tech Stack
- Python 3.14
- Pandas, NumPy, Polars
- Matplotlib, Seaborn
- PyArrow (Parquet)
- SQLite3
- Git / GitHub

## Author
Kethavath Santhosh - December 2025
