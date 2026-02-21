"""
Tests for Log File Cleaner & Classifier
========================================
These tests verify that each cleaning step works correctly.

Run with: python3 -m pytest src/test_log_cleaner.py -v
Or simply: python3 src/test_log_cleaner.py
"""

import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from log_cleaner import LogCleaner


def create_test_data():
    """Create a small messy dataset for testing."""
    data = {
        "log_id": [1, 2, 3, 4, 5, 5],
        "timestamp": [
            "2024-01-15 10:30:00",
            "01/15/2024 10:30",
            "15-Jan-2024 10:30:00",
            "1705312200.0",
            "INVALID_TIME",
            "INVALID_TIME"
        ],
        "error_type": [
            "System Crash",
            "UI Glitch",
            "API Timeout",
            "Memory Leak",
            "System Crash",
            "System Crash"
        ],
        "severity": [
            "Critical",
            "CRITICAL",
            "P1",
            "high",
            "med",
            "med"
        ],
        "module": [
            "auth-service",
            "api-gateway",
            None,
            "user-dashboard",
            "auth-service",
            "auth-service"
        ],
        "environment": [
            "production",
            "PRODUCTION",
            "prod",
            "dev",
            "DEV",
            "DEV"
        ],
        "user_id": [
            "USR-1234",
            "5678",
            "UNKNOWN",
            None,
            "",
            ""
        ],
        "response_time_ms": [
            150.5,
            -1,
            0,
            99999,
            "N/A",
            "N/A"
        ],
        "message": [
            "Error at line 10",
            "Error at line 20",
            "Error at line 30",
            "Error at line 40",
            "Error at line 50",
            "Error at line 50"
        ],
        "ip_address": [
            "192.168.1.1",
            "INVALID_IP",
            "0.0.0.0",
            None,
            "10.0.1.1",
            "10.0.1.1"
        ]
    }
    return pd.DataFrame(data)


class TestLogCleaner:
    """Test suite for LogCleaner class."""

    def setup(self):
        """Set up test fixtures."""
        self.cleaner = LogCleaner(data=create_test_data())
        self.cleaner.clean_data = self.cleaner.raw_data.copy()

    def test_remove_duplicates(self):
        """Test that duplicate rows are removed."""
        self.setup()
        before = len(self.cleaner.clean_data)
        self.cleaner.remove_duplicates()
        after = len(self.cleaner.clean_data)
        assert after < before, "Duplicates were not removed"
        assert after == 5, "Expected 5 rows, got {}".format(after)
        print("PASS: test_remove_duplicates")

    def test_standardize_severity(self):
        """Test severity standardization."""
        self.setup()
        self.cleaner.standardize_severity()
        unique = set(
            self.cleaner.clean_data['severity'].dropna().unique()
        )
        valid = {"Critical", "High", "Medium", "Low"}
        assert unique.issubset(valid), (
            "Invalid severity values found: {}".format(
                unique - valid
            )
        )
        print("PASS: test_standardize_severity")

    def test_standardize_environment(self):
        """Test environment standardization."""
        self.setup()
        self.cleaner.standardize_environment()
        unique = set(
            self.cleaner.clean_data['environment'].dropna().unique()
        )
        valid = {"production", "staging", "development"}
        assert unique.issubset(valid), (
            "Invalid environment values: {}".format(unique - valid)
        )
        print("PASS: test_standardize_environment")

    def test_standardize_timestamps(self):
        """Test timestamp parsing."""
        self.setup()
        self.cleaner.standardize_timestamps()
        valid_timestamps = (
            self.cleaner.clean_data['timestamp'].dropna()
        )
        assert len(valid_timestamps) > 0, "No valid timestamps"
        assert all(
            isinstance(t, pd.Timestamp) for t in valid_timestamps
        ), "Not all timestamps are datetime type"
        print("PASS: test_standardize_timestamps")

    def test_clean_response_time(self):
        """Test response time cleaning."""
        self.setup()
        self.cleaner.clean_response_time()
        valid = self.cleaner.clean_data[
            'response_time_ms'
        ].dropna()
        assert all(valid > 0), "Negative values still exist"
        assert all(valid <= 10000), "Outliers still exist"
        print("PASS: test_clean_response_time")

    def test_clean_ip_addresses(self):
        """Test IP address cleaning."""
        self.setup()
        self.cleaner.clean_ip_addresses()
        valid = self.cleaner.clean_data[
            'ip_address'
        ].dropna()
        assert "INVALID_IP" not in valid.values, (
            "INVALID_IP still exists"
        )
        assert "0.0.0.0" not in valid.values, (
            "0.0.0.0 still exists"
        )
        print("PASS: test_clean_ip_addresses")

    def test_clean_user_ids(self):
        """Test user ID standardization."""
        self.setup()
        self.cleaner.clean_user_ids()
        valid = self.cleaner.clean_data['user_id'].dropna()
        assert all(
            uid.startswith("USR-") for uid in valid
        ), "Not all user IDs have USR- prefix"
        assert "UNKNOWN" not in valid.values, (
            "UNKNOWN still exists"
        )
        print("PASS: test_clean_user_ids")

    def test_full_pipeline(self):
        """Test the complete pipeline runs without error."""
        self.setup()
        self.cleaner.clean_data = None
        result = self.cleaner.run_full_pipeline()
        assert result is not None, "Pipeline returned None"
        assert len(result) > 0, "Pipeline returned empty data"
        print("PASS: test_full_pipeline")


def run_all_tests():
    """Run all tests and report results."""
    print("=" * 60)
    print("RUNNING LOG CLEANER TESTS")
    print("=" * 60)
    print("")

    tester = TestLogCleaner()
    tests = [
        tester.test_remove_duplicates,
        tester.test_standardize_severity,
        tester.test_standardize_environment,
        tester.test_standardize_timestamps,
        tester.test_clean_response_time,
        tester.test_clean_ip_addresses,
        tester.test_clean_user_ids,
        tester.test_full_pipeline
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print("FAIL: {} - {}".format(test.__name__, e))
            failed += 1
        except Exception as e:
            print("ERROR: {} - {}".format(test.__name__, e))
            failed += 1

    print("")
    print("=" * 60)
    print("RESULTS: {} passed, {} failed out of {}".format(
        passed, failed, len(tests)
    ))
    print("=" * 60)

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    if success:
        print("ALL TESTS PASSED!")
    else:
        print("SOME TESTS FAILED!")
        sys.exit(1)
