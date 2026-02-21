# SQL Database Cleaning and EDA

## Overview
Cleans a messy SQLite e-commerce database with 4 tables and 13,690 records using SQL queries and Python. Demonstrates SQL proficiency, data quality assessment, and exploratory data analysis.

## Problem Statement
E-commerce database contains:
- 147 duplicate customer records
- 1,732 invalid email addresses
- 2,227 invalid age values
- 7,503 invalid order records
- 17 different order status variations for 6 categories
- Inconsistent naming across all tables

## Database Schema
- customers: 3,150 records (name, email, phone, city, state, membership)
- orders: 8,000 records (product, quantity, price, status, payment)
- products: 540 records (name, category, price, rating, stock)
- support_tickets: 2,000 records (issue type, priority, status, agent)

## Cleaning Pipeline (19 Steps)

| Steps | Table | Actions |
|-------|-------|---------|
| 1-8 | Customers | Dedup, names, emails, country, city, state, membership, ages |
| 9-12 | Orders | Status, payment, amounts, dates |
| 13-16 | Products | Dedup, names, categories, prices/ratings |
| 17-19 | Support | Issue types, priority/status, agents |

## SQL Quality Check Queries
Located in queries/data_quality_checks.sql:
- Duplicate detection
- Invalid email finder
- Age range validation
- Missing values summary
- Status inconsistency check
- Price/rating outlier detection

## Results
- Customers: 3,150 -> 3,000 (150 duplicates removed)
- Products: 540 -> 500 (40 duplicates removed)
- 19 cleaning steps documented with reasons
- 5 automated tests passing

## Tech Stack
- Python 3.14, SQLite3, Pandas, Matplotlib, Seaborn

## How to Reproduce

    cd 03-sql-cleaning-eda
    python3 -m venv venv
    source venv/bin/activate
    pip3 install pandas numpy matplotlib seaborn pyarrow openpyxl
    python3 src/create_database.py
    python3 src/sql_cleaner.py
    python3 src/test_sql_cleaner.py

## Author
Kethavath Saidamma - December 2025
