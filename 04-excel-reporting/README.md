# 04 - Excel Reporting & Dashboard

## Overview
Advanced Excel reporting with formulas, conditional formatting, and lookup tables generated from cleaned data across all 3 portfolio projects.

## Excel File: portfolio_report.xlsx

### Sheets Included
| Sheet | Records | Description |
|-------|---------|-------------|
| Dashboard | - | Summary with formulas & conditional formatting |
| Server_Logs | 5,000 | Cleaned server log data |
| Bug_Reports | 4,500 | Unified bug reports from 3 sources |
| Orders | 8,000 | Cleaned e-commerce orders |
| Customers | 3,000 | Cleaned customer data |

### Excel Formulas Used
| Formula | Purpose | Example |
|---------|---------|---------|
| SUM | Total records | =SUM(B5:B7) |
| AVERAGE | Average retention % | =AVERAGE(D5:D7) |
| Percentage | Calculate % | =C5/B5*100 |
| VLOOKUP Reference | Severity code lookup table | Severity -> Description -> SLA |

### Conditional Formatting
| Color | Meaning | Used For |
|-------|---------|----------|
| Red | High Risk / Critical | CRITICAL severity |
| Yellow | Medium / Warning | ERROR severity |
| Green | Low Risk / OK | WARNING, INFO severity |

### Lookup Table (VLOOKUP Reference)
| Severity | Description | Action | SLA (hrs) |
|----------|-------------|--------|-----------|
| CRITICAL | System down | Immediate | 1 |
| ERROR | Feature broken | Fix within SLA | 4 |
| WARNING | Degraded performance | Monitor | 24 |
| INFO | Informational | Log and review | 72 |

## Portfolio Summary
| Metric | Value |
|--------|-------|
| Total Raw Records | 23,560 |
| Total Clean Records | 23,000 |
| Cleaning Steps | 42 |
| Automated Tests | 20 |
| Data Retention | 97-100% |

## Tech Stack
- Python (Pandas, openpyxl)
- Excel Formulas (SUM, AVERAGE, %)
- Conditional Formatting
- Multi-sheet workbooks

## Author
Kethavath Santhosh - github.com/santhosh123-vs
