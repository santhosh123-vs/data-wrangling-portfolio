import pandas as pd
import numpy as np
import json
from datetime import datetime


class BugReportETL:

    PRIORITY_MAP = {
        "Critical": "Critical",
        "CRITICAL": "Critical",
        "P0": "Critical",
        "Blocker": "Critical",
        "Very High": "Critical",
        "very high": "Critical",
        "V. High": "Critical",
        "High": "High",
        "HIGH": "High",
        "P1": "High",
        "Major": "High",
        "high": "High",
        "Hi": "High",
        "Medium": "Medium",
        "MEDIUM": "Medium",
        "P2": "Medium",
        "Normal": "Medium",
        "medium": "Medium",
        "Med": "Medium",
        "med": "Medium",
        "Low": "Low",
        "LOW": "Low",
        "P3": "Low",
        "Minor": "Low",
        "Trivial": "Low",
        "low": "Low",
        "Lo": "Low",
        "Not Important": "Low",
        "N/A": "Low"
    }

    STATUS_MAP = {
        "Open": "Open",
        "OPEN": "Open",
        "New": "Open",
        "TODO": "Open",
        "open": "Open",
        "Reopened": "Open",
        "REOPENED": "Open",
        "Re-opened": "Open",
        "In Progress": "In Progress",
        "IN_PROGRESS": "In Progress",
        "InProgress": "In Progress",
        "Working": "In Progress",
        "Still working on it": "In Progress",
        "need more info": "In Progress",
        "waiting for deploy": "In Progress",
        "Resolved": "Resolved",
        "RESOLVED": "Resolved",
        "Fixed": "Resolved",
        "Done": "Resolved",
        "DONE": "Resolved",
        "Fixed by John on Monday": "Resolved",
        "Closed": "Closed",
        "CLOSED": "Closed",
        "Verified": "Closed",
        "Complete": "Closed",
        "closed": "Closed",
        "closed - duplicate": "Closed",
        "wont fix": "Closed",
        "Cant reproduce": "Closed"
    }

    COMPONENT_MAP = {
        "auth-service": "Authentication",
        "Authentication": "Authentication",
        "AUTH": "Authentication",
        "Auth": "Authentication",
        "Login": "Authentication",
        "payment-gateway": "Payment",
        "Payment": "Payment",
        "PAYMENT": "Payment",
        "Payments": "Payment",
        "user-dashboard": "Dashboard",
        "Dashboard": "Dashboard",
        "UI": "Dashboard",
        "api-gateway": "API",
        "API": "API",
        "Backend": "API",
        "database": "Database",
        "Database": "Database",
        "DB": "Database",
        "notification-service": "Notifications",
        "Notifications": "Notifications",
        "EMAIL": "Notifications",
        "Reports": "Reports"
    }

    SDLC_MAP = {
        "Requirements": "Requirements",
        "requirements": "Requirements",
        "Design": "Design",
        "DESIGN": "Design",
        "Development": "Development",
        "Dev": "Development",
        "Testing": "Testing",
        "QA": "Testing",
        "Deployment": "Deployment",
        "Maintenance": "Maintenance"
    }

    ENVIRONMENT_MAP = {
        "Production": "Production",
        "PROD": "Production",
        "production": "Production",
        "prod": "Production",
        "Staging": "Staging",
        "STAGE": "Staging",
        "staging": "Staging",
        "stg": "Staging",
        "Development": "Development",
        "DEV": "Development",
        "development": "Development",
        "dev": "Development",
        "QA": "Testing",
        "Testing": "Testing",
        "UAT": "Testing"
    }

    def __init__(self):
        self.jira_data = None
        self.github_data = None
        self.excel_data = None
        self.unified_data = None
        self.cleaning_log = []

    def _log_step(self, step, description, details, reason):
        self.cleaning_log.append({
            "step": int(step),
            "description": description,
            "details": details,
            "reason": reason,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        print("STEP {}: {}".format(step, description))
        print("   {}".format(details))
        print("")

    def extract_jira(self, filepath):
        self.jira_data = pd.read_csv(filepath)
        print("Extracted {} JIRA records".format(len(self.jira_data)))
        return self

    def extract_github(self, filepath):
        with open(filepath, "r") as f:
            data = json.load(f)
        self.github_data = pd.DataFrame(data)
        print("Extracted {} GitHub records".format(len(self.github_data)))
        return self

    def extract_excel(self, filepath):
        if filepath.endswith(".xlsx"):
            self.excel_data = pd.read_excel(filepath)
        else:
            self.excel_data = pd.read_csv(filepath)
        print("Extracted {} Excel records".format(len(self.excel_data)))
        return self

    def _transform_jira(self):
        df = self.jira_data.copy()
        transformed = pd.DataFrame({
            "source": "JIRA",
            "ticket_id": df["ticket_id"],
            "title": df["title"],
            "description": df["description"],
            "priority": df["priority"],
            "status": df["status"],
            "component": df["component"],
            "reporter": df["reporter"],
            "assignee": df["assignee"],
            "environment": df["environment"],
            "sdlc_phase": df["sdlc_phase"],
            "created_date": df["created_date"],
            "resolved_date": df["resolved_date"],
            "time_spent_minutes": df["time_spent_minutes"],
            "browser": df["browser"],
            "os": df["os"]
        })
        return transformed

    def _transform_github(self):
        df = self.github_data.copy()

        def extract_priority_from_labels(labels):
            if labels is None or not isinstance(labels, list):
                return None
            for label in labels:
                if "critical" in str(label).lower():
                    return "Critical"
                if "high" in str(label).lower():
                    return "High"
                if "low" in str(label).lower():
                    return "Low"
            if "bug" in [str(l).lower() for l in labels]:
                return "Medium"
            return None

        def extract_component_from_title(title):
            title_lower = str(title).lower()
            if any(w in title_lower for w in ["login", "auth"]):
                return "Authentication"
            if any(w in title_lower for w in ["payment"]):
                return "Payment"
            if any(w in title_lower for w in ["dashboard", "ui", "button"]):
                return "Dashboard"
            if any(w in title_lower for w in ["api", "backend"]):
                return "API"
            if any(w in title_lower for w in ["database", "db"]):
                return "Database"
            if any(w in title_lower for w in ["email", "notification"]):
                return "Notifications"
            if any(w in title_lower for w in ["security", "xss"]):
                return "Security"
            if any(w in title_lower for w in ["performance", "slow", "timeout"]):
                return "Performance"
            return None

        transformed = pd.DataFrame({
            "source": "GitHub",
            "ticket_id": df["issue_number"].apply(lambda x: "GH-{}".format(x)),
            "title": df["title"],
            "description": df["body"],
            "priority": df["labels"].apply(extract_priority_from_labels),
            "status": df["state"].map({
                "open": "Open", "closed": "Closed",
                "OPEN": "Open", "CLOSED": "Closed"
            }),
            "component": df["title"].apply(extract_component_from_title),
            "reporter": df["user"],
            "assignee": df["assignee"],
            "environment": None,
            "sdlc_phase": None,
            "created_date": df["created_at"],
            "resolved_date": df["closed_at"],
            "time_spent_minutes": None,
            "browser": None,
            "os": None
        })
        return transformed

    def _transform_excel(self):
        df = self.excel_data.copy()
        transformed = pd.DataFrame({
            "source": "Excel",
            "ticket_id": df["Bug #"].apply(lambda x: "XL-{}".format(x)),
            "title": df["Bug Title"],
            "description": df["Notes"],
            "priority": df["Severity"],
            "status": df["Status Notes"],
            "component": df["Module"],
            "reporter": df["Found By"],
            "assignee": None,
            "environment": None,
            "sdlc_phase": None,
            "created_date": df["Date Found"],
            "resolved_date": None,
            "time_spent_minutes": None,
            "browser": None,
            "os": None
        })
        return transformed

    def transform_and_unify(self):
        print("Transforming data from all sources...")
        print("")
        jira_transformed = self._transform_jira()
        self._log_step(1, "Transformed JIRA data",
            "{} records mapped to unified schema".format(len(jira_transformed)),
            "JIRA has 15 columns that need mapping to our 16-column unified schema")
        github_transformed = self._transform_github()
        self._log_step(2, "Transformed GitHub data",
            "{} records mapped to unified schema".format(len(github_transformed)),
            "GitHub uses labels instead of priority and state instead of status")
        excel_transformed = self._transform_excel()
        self._log_step(3, "Transformed Excel data",
            "{} records mapped to unified schema".format(len(excel_transformed)),
            "Excel data has informal column names and free-text fields")
        self.unified_data = pd.concat(
            [jira_transformed, github_transformed, excel_transformed],
            ignore_index=True
        )
        self._log_step(4, "Combined all sources",
            "Total: {} records from 3 sources".format(len(self.unified_data)),
            "Unified schema enables cross-source analysis")
        return self

    def clean_duplicates(self):
        before = len(self.unified_data)
        self.unified_data = self.unified_data.drop_duplicates(
            subset=["ticket_id", "title", "source"], keep="first"
        )
        after = len(self.unified_data)
        self._log_step(5, "Removed duplicates",
            "Removed {} duplicate records".format(before - after),
            "Duplicates from same source with same ticket_id and title are redundant")
        return self

    def clean_priority(self):
        before_null = int(self.unified_data["priority"].isna().sum())
        self.unified_data["priority"] = self.unified_data["priority"].map(self.PRIORITY_MAP)
        after_null = int(self.unified_data["priority"].isna().sum())
        self.unified_data["priority"] = self.unified_data["priority"].fillna("Unknown")
        self._log_step(6, "Standardized priority levels",
            "Mapped to Critical/High/Medium/Low. {} unmapped values set to Unknown".format(after_null - before_null),
            "18 different priority representations consolidated to 4 standard levels")
        return self

    def clean_status(self):
        self.unified_data["status"] = self.unified_data["status"].map(self.STATUS_MAP)
        self.unified_data["status"] = self.unified_data["status"].fillna("Unknown")
        self._log_step(7, "Standardized status values",
            "Mapped to Open/In Progress/Resolved/Closed",
            "20+ status variations consolidated to 4 standard statuses")
        return self

    def clean_component(self):
        self.unified_data["component"] = self.unified_data["component"].map(self.COMPONENT_MAP)
        self.unified_data["component"] = self.unified_data["component"].fillna("Unknown")
        self._log_step(8, "Standardized component names",
            "Mapped to unified component names",
            "Multiple naming conventions consolidated for cross-source analysis")
        return self

    def clean_environment(self):
        self.unified_data["environment"] = self.unified_data["environment"].map(self.ENVIRONMENT_MAP)
        self.unified_data["environment"] = self.unified_data["environment"].fillna("Unknown")
        self._log_step(9, "Standardized environment names",
            "Mapped to Production/Staging/Development/Testing",
            "15 environment variations consolidated to 4 standard names")
        return self

    def clean_sdlc_phase(self):
        self.unified_data["sdlc_phase"] = self.unified_data["sdlc_phase"].map(self.SDLC_MAP)
        self.unified_data["sdlc_phase"] = self.unified_data["sdlc_phase"].fillna("Unknown")
        self._log_step(10, "Standardized SDLC phases",
            "Mapped to Requirements/Design/Development/Testing/Deployment/Maintenance",
            "SDLC classification enables phase-based defect analysis")
        return self

    @staticmethod
    def _parse_date(date_val):
        if pd.isna(date_val):
            return pd.NaT
        date_str = str(date_val).strip()
        if date_str in ["INVALID_DATE", "N/A", "Not Yet", "", "last week", "yesterday"]:
            return pd.NaT
        try:
            return pd.to_datetime(float(date_str), unit="s")
        except (ValueError, OverflowError):
            pass
        try:
            return pd.to_datetime(date_str)
        except Exception:
            return pd.NaT

    def clean_dates(self):
        self.unified_data["created_date"] = self.unified_data["created_date"].apply(self._parse_date)
        self.unified_data["resolved_date"] = self.unified_data["resolved_date"].apply(self._parse_date)
        valid_created = int(self.unified_data["created_date"].notna().sum())
        valid_resolved = int(self.unified_data["resolved_date"].notna().sum())
        self._log_step(11, "Standardized date formats",
            "Valid created dates: {}, Valid resolved dates: {}".format(valid_created, valid_resolved),
            "Multiple date formats unified to datetime for time-series analysis")
        return self

    def clean_time_spent(self):
        def parse_time(val):
            if pd.isna(val):
                return np.nan
            val_str = str(val).strip()
            if val_str in ["N/A", "", "None"]:
                return np.nan
            if "hour" in val_str:
                try:
                    hours = float(val_str.split()[0])
                    return hours * 60
                except Exception:
                    return np.nan
            if "day" in val_str:
                try:
                    days = float(val_str.split()[0])
                    return days * 480
                except Exception:
                    return np.nan
            try:
                num = float(val_str)
                if num <= 0:
                    return np.nan
                return num
            except ValueError:
                return np.nan
        self.unified_data["time_spent_minutes"] = self.unified_data["time_spent_minutes"].apply(parse_time)
        self._log_step(12, "Cleaned time spent values",
            "Converted text values to minutes, removed invalid entries",
            "Negative and zero values indicate logging errors, text values converted to numeric")
        return self

    def clean_reporters(self):
        def clean_reporter(val):
            if pd.isna(val):
                return "Unknown"
            val = str(val).strip()
            if val in ["UNKNOWN", "", "None", "unassigned"]:
                return "Unknown"
            return val
        self.unified_data["reporter"] = self.unified_data["reporter"].apply(clean_reporter)
        self.unified_data["assignee"] = self.unified_data["assignee"].apply(clean_reporter)
        self._log_step(13, "Cleaned reporter and assignee fields",
            "Standardized empty and placeholder values",
            "UNKNOWN, empty strings, and None replaced with Unknown for consistency")
        return self

    def clean_browsers(self):
        browser_map = {
            "Chrome": "Chrome", "chrome": "Chrome",
            "Firefox": "Firefox", "FIREFOX": "Firefox",
            "Safari": "Safari", "Edge": "Edge",
            "IE": "Internet Explorer"
        }
        self.unified_data["browser"] = self.unified_data["browser"].map(browser_map)
        os_map = {
            "Windows 10": "Windows", "Win10": "Windows",
            "macOS": "macOS", "Mac": "macOS",
            "Linux": "Linux", "ubuntu": "Linux",
            "iOS": "iOS", "Android": "Android"
        }
        self.unified_data["os"] = self.unified_data["os"].map(os_map)
        self._log_step(14, "Standardized browser and OS names",
            "Unified browser and OS naming conventions",
            "Inconsistent casing and abbreviations prevented accurate platform analysis")
        return self

    def add_computed_fields(self):
        self.unified_data["created_date"] = pd.to_datetime(
            self.unified_data["created_date"], errors="coerce"
        )
        self.unified_data["resolved_date"] = pd.to_datetime(
            self.unified_data["resolved_date"], errors="coerce"
        )
        mask = (
            self.unified_data["created_date"].notna() &
            self.unified_data["resolved_date"].notna()
        )
        self.unified_data["resolution_days"] = np.nan
        self.unified_data.loc[mask, "resolution_days"] = (
            (self.unified_data.loc[mask, "resolved_date"] -
             self.unified_data.loc[mask, "created_date"]).dt.days
        )
        self.unified_data.loc[
            self.unified_data["resolution_days"] < 0,
            "resolution_days"
        ] = np.nan

        def classify_bug_type(title):
            title_lower = str(title).lower()
            if any(w in title_lower for w in ["crash", "exception", "null pointer"]):
                return "Crash/Fatal"
            if any(w in title_lower for w in ["ui", "css", "button", "overlapping", "alignment", "looks weird"]):
                return "UI/Visual"
            if any(w in title_lower for w in ["slow", "timeout", "performance", "memory leak"]):
                return "Performance"
            if any(w in title_lower for w in ["security", "xss", "vulnerability", "permission"]):
                return "Security"
            if any(w in title_lower for w in ["api", "500", "404", "gateway"]):
                return "API/Integration"
            if any(w in title_lower for w in ["data", "wrong", "calculation", "search results"]):
                return "Data Integrity"
            return "Functional"

        self.unified_data["bug_type"] = self.unified_data["title"].apply(classify_bug_type)
        self._log_step(15, "Added computed fields",
            "Added resolution_days and bug_type classification",
            "Bug type classification enables pattern analysis across crash vs UI vs performance issues")
        return self

    def run_full_pipeline(self):
        print("=" * 60)
        print("RUNNING BUG REPORT ETL PIPELINE")
        print("=" * 60)
        print("")
        self.transform_and_unify()
        self.clean_duplicates()
        self.clean_priority()
        self.clean_status()
        self.clean_component()
        self.clean_environment()
        self.clean_sdlc_phase()
        self.clean_dates()
        self.clean_time_spent()
        self.clean_reporters()
        self.clean_browsers()
        self.add_computed_fields()
        print("=" * 60)
        print("PIPELINE COMPLETE")
        print("Final shape: {}".format(self.unified_data.shape))
        print("=" * 60)
        return self.unified_data

    def export_data(self, output_dir="cleaned_data"):
        self.unified_data.to_csv(
            "{}/unified_bug_reports.csv".format(output_dir), index=False
        )
        self.unified_data.to_json(
            "{}/unified_bug_reports.json".format(output_dir),
            orient="records", indent=2, date_format="iso"
        )
        self.unified_data.to_parquet(
            "{}/unified_bug_reports.parquet".format(output_dir), index=False
        )
        print("Exported to 3 formats in {}/".format(output_dir))

    def export_lineage(self, output_path="metadata/data_lineage.json"):
        lineage = {
            "project": "Bug Report ETL Pipeline",
            "author": "Kethavath Saidamma",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "sources": {
                "jira": int(len(self.jira_data)) if self.jira_data is not None else 0,
                "github": int(len(self.github_data)) if self.github_data is not None else 0,
                "excel": int(len(self.excel_data)) if self.excel_data is not None else 0
            },
            "unified_records": int(len(self.unified_data)),
            "cleaning_steps": self.cleaning_log
        }
        with open(output_path, "w") as f:
            json.dump(lineage, f, indent=2)
        print("Lineage saved to {}".format(output_path))

    def get_summary(self):
        print("")
        print("=" * 60)
        print("BUG REPORT SUMMARY")
        print("=" * 60)
        print("")
        print("Total Records: {}".format(len(self.unified_data)))
        print("")
        print("By Source:")
        print(self.unified_data["source"].value_counts())
        print("")
        print("By Priority:")
        print(self.unified_data["priority"].value_counts())
        print("")
        print("By Status:")
        print(self.unified_data["status"].value_counts())
        print("")
        print("By Bug Type:")
        print(self.unified_data["bug_type"].value_counts())
        print("")
        print("By Component:")
        print(self.unified_data["component"].value_counts())
        print("")
        print("By SDLC Phase:")
        print(self.unified_data["sdlc_phase"].value_counts())


if __name__ == "__main__":
    etl = BugReportETL()
    etl.extract_jira("raw_data/jira_bugs_export.csv")
    etl.extract_github("raw_data/github_issues.json")
    etl.extract_excel("raw_data/manual_bug_tracker.csv")
    etl.run_full_pipeline()
    etl.export_data()
    etl.export_lineage()
    etl.get_summary()
