"""
Log File Cleaner & Classifier
=============================
A modular data wrangling pipeline for cleaning messy server logs.

Author: Kethavath Saidamma
Date: February 2025

This module provides a reusable LogCleaner class that can:
- Load data from multiple formats (CSV, JSON)
- Remove duplicates
- Standardize categorical fields
- Parse and unify timestamps
- Clean and validate numeric fields
- Clean and validate IP addresses
- Standardize user IDs
- Track all transformations (data lineage)
- Export cleaned data in multiple formats
"""

import pandas as pd
import numpy as np
import re
import json
from datetime import datetime


class LogCleaner:
    """
    A class to clean and standardize messy server log data.
    
    Attributes:
        raw_data (pd.DataFrame): The original messy data
        clean_data (pd.DataFrame): The cleaned data
        cleaning_log (list): Record of all cleaning steps
    """

    # Mapping dictionaries for standardization
    SEVERITY_MAP = {
        "Critical": "Critical",
        "CRITICAL": "Critical",
        "P1": "Critical",
        "High": "High",
        "high": "High",
        "P2": "High",
        "Medium": "Medium",
        "med": "Medium",
        "P3": "Medium",
        "Low": "Low",
        "LOW": "Low",
        "P4": "Low"
    }

    ENVIRONMENT_MAP = {
        "production": "production",
        "PRODUCTION": "production",
        "prod": "production",
        "staging": "staging",
        "Staging": "staging",
        "stg": "staging",
        "development": "development",
        "dev": "development",
        "DEV": "development"
    }

    def __init__(self, data=None):
        """
        Initialize the LogCleaner.
        
        Args:
            data (pd.DataFrame, optional): Raw data to clean
        """
        self.raw_data = data
        self.clean_data = None
        self.cleaning_log = []

    def load_csv(self, filepath):
        """
        Load data from a CSV file.
        
        Args:
            filepath (str): Path to the CSV file
            
        Returns:
            pd.DataFrame: Loaded data
            
        Decision: Using pandas read_csv with default settings
        because the log data uses comma separation.
        """
        self.raw_data = pd.read_csv(filepath)
        print("Loaded {} rows from {}".format(
            len(self.raw_data), filepath
        ))
        return self.raw_data

    def load_json(self, filepath):
        """
        Load data from a JSON file.
        
        Args:
            filepath (str): Path to the JSON file
            
        Returns:
            pd.DataFrame: Loaded data
        """
        with open(filepath, "r") as f:
            data = json.load(f)
        self.raw_data = pd.DataFrame(data)
        print("Loaded {} rows from {}".format(
            len(self.raw_data), filepath
        ))
        return self.raw_data

    def _log_step(self, step, description, rows_before,
                  rows_after, rows_affected, reason):
        """
        Record a cleaning step in the log.
        
        Args:
            step (int): Step number
            description (str): What was done
            rows_before (int): Row count before
            rows_after (int): Row count after
            rows_affected (int): How many rows changed
            reason (str): WHY this cleaning was necessary
        """
        self.cleaning_log.append({
            "step": int(step),
            "description": description,
            "rows_before": int(rows_before),
            "rows_after": int(rows_after),
            "rows_affected": int(rows_affected),
            "reason": reason,
            "timestamp": datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        })

    def remove_duplicates(self):
        """
        Remove duplicate rows from the dataset.
        
        Decision: Exact duplicates are removed because they 
        represent redundant log entries that would skew analysis.
        We keep the first occurrence and remove subsequent copies.
        """
        rows_before = len(self.clean_data)
        self.clean_data = self.clean_data.drop_duplicates()
        rows_after = len(self.clean_data)

        self._log_step(
            step=1,
            description="Removed duplicate rows",
            rows_before=rows_before,
            rows_after=rows_after,
            rows_affected=rows_before - rows_after,
            reason="Duplicate log entries would inflate error "
                   "counts and skew statistical analysis. "
                   "Keeping first occurrence only."
        )
        return self

    def standardize_severity(self):
        """
        Map all severity variations to: 
        Critical, High, Medium, Low.
        
        Decision: The raw data contains 12 different severity 
        representations (e.g., 'CRITICAL', 'P1', 'high', 'med').
        These are mapped to 4 standard levels for consistent 
        analysis. Unmapped values become NaN to flag data issues.
        """
        rows_before = len(self.clean_data)
        affected = int(self.clean_data['severity'].notna().sum())

        self.clean_data['severity'] = (
            self.clean_data['severity'].map(self.SEVERITY_MAP)
        )

        self._log_step(
            step=2,
            description="Standardized severity levels "
                        "(12 variations to 4 standard levels)",
            rows_before=rows_before,
            rows_after=len(self.clean_data),
            rows_affected=affected,
            reason="Inconsistent severity labels prevent "
                   "accurate grouping and priority analysis. "
                   "P1/CRITICAL/Critical all mean the same thing."
        )
        return self

    def standardize_environment(self):
        """
        Map all environment variations to: 
        production, staging, development.
        
        Decision: 9 different representations exist for 3 
        environments. Standardizing enables accurate filtering 
        by deployment environment.
        """
        rows_before = len(self.clean_data)
        affected = int(
            self.clean_data['environment'].notna().sum()
        )

        self.clean_data['environment'] = (
            self.clean_data['environment'].map(
                self.ENVIRONMENT_MAP
            )
        )

        self._log_step(
            step=3,
            description="Standardized environment names "
                        "(9 variations to 3 standard names)",
            rows_before=rows_before,
            rows_after=len(self.clean_data),
            rows_affected=affected,
            reason="Environment filtering is critical for "
                   "isolating production issues from dev/staging. "
                   "Inconsistent names prevent this analysis."
        )
        return self

    @staticmethod
    def _parse_single_timestamp(ts):
        """
        Parse a single timestamp value into datetime.
        
        Handles: ISO format, US format, European format, 
        and Unix timestamps.
        
        Args:
            ts: Raw timestamp value
            
        Returns:
            pd.Timestamp or pd.NaT
        """
        if pd.isna(ts):
            return pd.NaT
        ts = str(ts).strip()
        if ts == "INVALID_TIME":
            return pd.NaT
        try:
            return pd.to_datetime(float(ts), unit='s')
        except (ValueError, OverflowError):
            pass
        try:
            return pd.to_datetime(ts)
        except Exception:
            return pd.NaT

    def standardize_timestamps(self):
        """
        Convert all timestamp formats to unified datetime.
        
        Decision: Raw data contains 4+ timestamp formats 
        including Unix timestamps. All are converted to pandas 
        datetime for consistent time-series analysis.
        Unparseable values become NaT (Not a Time).
        """
        rows_before = len(self.clean_data)
        before_missing = int(
            self.clean_data['timestamp'].isna().sum()
        )

        self.clean_data['timestamp'] = (
            self.clean_data['timestamp'].apply(
                self._parse_single_timestamp
            )
        )

        after_missing = int(
            self.clean_data['timestamp'].isna().sum()
        )

        self._log_step(
            step=4,
            description="Standardized timestamps to "
                        "datetime format",
            rows_before=rows_before,
            rows_after=len(self.clean_data),
            rows_affected=rows_before,
            reason="Multiple timestamp formats prevent "
                   "time-series analysis and sorting. "
                   "Unified format enables trend detection."
        )
        return self

    def clean_response_time(self, min_valid=1, max_valid=10000):
        """
        Clean response time values.
        
        Decision: Values less than 1ms are likely errors.
        Values over 10000ms are outliers (10 second timeout).
        'N/A' strings are converted to NaN.
        
        Args:
            min_valid (int): Minimum valid response time in ms
            max_valid (int): Maximum valid response time in ms
        """
        self.clean_data['response_time_ms'] = pd.to_numeric(
            self.clean_data['response_time_ms'], errors='coerce'
        )

        invalid_mask = (
            (self.clean_data['response_time_ms'] < min_valid) |
            (self.clean_data['response_time_ms'] > max_valid)
        )
        invalid_count = int(invalid_mask.sum())

        self.clean_data.loc[
            invalid_mask, 'response_time_ms'
        ] = np.nan

        self._log_step(
            step=5,
            description="Cleaned response_time_ms: removed "
                        "values outside {}-{}ms range".format(
                            min_valid, max_valid
                        ),
            rows_before=len(self.clean_data),
            rows_after=len(self.clean_data),
            rows_affected=invalid_count,
            reason="Negative values (-1) indicate logging "
                   "errors. Zero values are impossible for real "
                   "requests. Values >10000ms suggest timeouts, "
                   "not actual response times."
        )
        return self

    @staticmethod
    def _clean_single_ip(ip):
        """
        Validate and clean a single IP address.
        
        Args:
            ip: Raw IP address value
            
        Returns:
            str or np.nan
        """
        if pd.isna(ip):
            return np.nan
        ip = str(ip).strip()
        if ip in ["INVALID_IP", "0.0.0.0", ""]:
            return np.nan
        ip_pattern = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
        if re.match(ip_pattern, ip):
            return ip
        return np.nan

    def clean_ip_addresses(self):
        """
        Clean and validate IP addresses.
        
        Decision: 'INVALID_IP' is a placeholder from failed 
        logging. '0.0.0.0' is a non-routable address that 
        indicates missing source information. Both are replaced 
        with NaN.
        """
        before_missing = int(
            self.clean_data['ip_address'].isna().sum()
        )

        self.clean_data['ip_address'] = (
            self.clean_data['ip_address'].apply(
                self._clean_single_ip
            )
        )

        after_missing = int(
            self.clean_data['ip_address'].isna().sum()
        )

        self._log_step(
            step=6,
            description="Cleaned IP addresses",
            rows_before=len(self.clean_data),
            rows_after=len(self.clean_data),
            rows_affected=after_missing - before_missing,
            reason="INVALID_IP is a system placeholder, not a "
                   "real address. 0.0.0.0 is non-routable. Both "
                   "would corrupt network analysis."
        )
        return self

    @staticmethod
    def _clean_single_user_id(uid):
        """
        Standardize a single user ID to USR-XXXX format.
        
        Args:
            uid: Raw user ID value
            
        Returns:
            str or np.nan
        """
        if pd.isna(uid):
            return np.nan
        uid = str(uid).strip()
        if uid in ["UNKNOWN", "", "nan"]:
            return np.nan
        if uid.startswith("USR-"):
            return uid
        if uid.isdigit():
            return "USR-{}".format(uid)
        return np.nan

    def clean_user_ids(self):
        """
        Standardize user IDs to USR-XXXX format.
        
        Decision: Raw user IDs are inconsistent - some have 
        the USR- prefix, some are bare numbers, some are 
        'UNKNOWN'. Standardizing to USR-XXXX enables accurate 
        per-user analysis.
        """
        before_missing = int(
            self.clean_data['user_id'].isna().sum()
        )

        self.clean_data['user_id'] = (
            self.clean_data['user_id'].apply(
                self._clean_single_user_id
            )
        )

        after_missing = int(
            self.clean_data['user_id'].isna().sum()
        )

        self._log_step(
            step=7,
            description="Standardized user IDs to "
                        "USR-XXXX format",
            rows_before=len(self.clean_data),
            rows_after=len(self.clean_data),
            rows_affected=after_missing - before_missing,
            reason="Mixed ID formats prevent user-level "
                   "analysis. UNKNOWN provides no useful "
                   "information and is treated as missing."
        )
        return self

    def fill_missing_categories(self):
        """
        Fill missing categorical values with defaults.
        
        Decision: Rather than dropping rows with missing 
        categories (which would lose valuable data), we fill 
        them with 'Unknown' to preserve the records while 
        clearly marking them as incomplete.
        """
        self.clean_data['severity'] = (
            self.clean_data['severity'].fillna("Unknown")
        )
        self.clean_data['module'] = (
            self.clean_data['module'].fillna("unknown-module")
        )
        self.clean_data['environment'] = (
            self.clean_data['environment'].fillna("unknown")
        )

        self._log_step(
            step=8,
            description="Filled missing categorical values",
            rows_before=len(self.clean_data),
            rows_after=len(self.clean_data),
            rows_affected=int(
                self.clean_data['severity'].eq("Unknown").sum() +
                self.clean_data['module'].eq(
                    "unknown-module"
                ).sum() +
                self.clean_data['environment'].eq("unknown").sum()
            ),
            reason="Dropping rows with missing categories would "
                   "lose valuable timing and error data. "
                   "Filling with 'Unknown' preserves records "
                   "while flagging incomplete data."
        )
        return self

    def run_full_pipeline(self):
        """
        Execute the complete cleaning pipeline.
        
        Returns:
            pd.DataFrame: Cleaned data
        """
        print("Starting cleaning pipeline...")
        print("Original shape: {}".format(self.raw_data.shape))
        print("")

        self.clean_data = self.raw_data.copy()

        self.remove_duplicates()
        self.standardize_severity()
        self.standardize_environment()
        self.standardize_timestamps()
        self.clean_response_time()
        self.clean_ip_addresses()
        self.clean_user_ids()
        self.fill_missing_categories()

        print("Pipeline complete!")
        print("Final shape: {}".format(self.clean_data.shape))
        return self.clean_data

    def get_quality_report(self):
        """
        Generate a data quality report.
        
        Returns:
            dict: Quality metrics
        """
        report = {
            "total_rows": int(len(self.clean_data)),
            "total_columns": int(len(self.clean_data.columns)),
            "missing_values": {
                col: int(self.clean_data[col].isna().sum())
                for col in self.clean_data.columns
            },
            "missing_percentages": {
                col: round(
                    self.clean_data[col].isna().sum() /
                    len(self.clean_data) * 100, 2
                )
                for col in self.clean_data.columns
            }
        }
        return report

    def export_data(self, output_dir="cleaned_data"):
        """
        Export cleaned data in multiple formats.
        
        Args:
            output_dir (str): Output directory path
        """
        self.clean_data.to_csv(
            "{}/clean_server_logs.csv".format(output_dir),
            index=False
        )
        self.clean_data.to_json(
            "{}/clean_server_logs.json".format(output_dir),
            orient="records", indent=2, date_format="iso"
        )
        self.clean_data.to_parquet(
            "{}/clean_server_logs.parquet".format(output_dir),
            index=False
        )
        self.clean_data.to_excel(
            "{}/clean_server_logs.xlsx".format(output_dir),
            index=False
        )
        print("Exported to 4 formats in {}/".format(output_dir))

    def export_lineage(self, output_path="metadata/data_lineage.json"):
        """
        Export the complete data lineage documentation.
        
        Args:
            output_path (str): Path for lineage JSON file
        """
        lineage = {
            "project": "Log File Cleaner & Classifier",
            "author": "Kethavath Saidamma",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "cleaning_steps": self.cleaning_log,
            "final_stats": {
                "original_rows": int(len(self.raw_data)),
                "cleaned_rows": int(len(self.clean_data)),
                "rows_removed": int(
                    len(self.raw_data) - len(self.clean_data)
                ),
                "remaining_nulls": int(
                    self.clean_data.isnull().sum().sum()
                )
            }
        }

        with open(output_path, "w") as f:
            json.dump(lineage, f, indent=2)

        print("Lineage saved to {}".format(output_path))


if __name__ == "__main__":
    cleaner = LogCleaner()
    cleaner.load_csv("raw_data/messy_server_logs.csv")
    cleaner.run_full_pipeline()
    cleaner.export_data()
    cleaner.export_lineage()

    print("")
    print("Quality Report:")
    report = cleaner.get_quality_report()
    print(json.dumps(report, indent=2))
