import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from bug_report_etl import BugReportETL


def create_test_data():
    jira_data = pd.DataFrame({
        "ticket_id": ["JIRA-1", "JIRA-2", "JIRA-2"],
        "title": ["Login crash", "UI broken", "UI broken"],
        "description": ["Bug in auth", "CSS issue", "CSS issue"],
        "priority": ["CRITICAL", "med", "med"],
        "status": ["OPEN", "IN_PROGRESS", "IN_PROGRESS"],
        "component": ["AUTH", "UI", "UI"],
        "reporter": ["john@test.com", "UNKNOWN", "UNKNOWN"],
        "assignee": ["jane@test.com", None, None],
        "environment": ["PROD", "dev", "dev"],
        "sdlc_phase": ["Testing", "Dev", "Dev"],
        "created_date": ["2024-01-15 10:00:00", "01/20/2024 14:30", "01/20/2024 14:30"],
        "resolved_date": ["2024-01-16 10:00:00", "N/A", "N/A"],
        "time_spent_minutes": [120, "2 hours", "2 hours"],
        "browser": ["chrome", "FIREFOX", "FIREFOX"],
        "os": ["Win10", "Mac", "Mac"]
    })
    return jira_data


class TestBugReportETL:

    def setup(self):
        self.etl = BugReportETL()
        self.etl.jira_data = create_test_data()
        self.etl.github_data = pd.DataFrame({
            "issue_number": [1, 2],
            "title": ["Bug: API returns 404", "UI: Button broken"],
            "body": ["API issue", "Button issue"],
            "labels": [["bug", "critical"], ["bug", "low-priority"]],
            "state": ["open", "closed"],
            "user": ["dev1", "qa1"],
            "assignee": ["dev2", None],
            "created_at": ["2024-01-15T10:00:00Z", "2024-02-20T14:00:00Z"],
            "closed_at": [None, "2024-02-25T14:00:00Z"],
            "comments": [5, 2],
            "milestone": ["v1.0", None]
        })
        self.excel_data = pd.DataFrame({
            "Bug #": [1, 2],
            "Bug Title": ["login broken", "slow performance"],
            "Severity": ["Very High", "Med"],
            "Status Notes": ["DONE", "Still working on it"],
            "Found By": ["QA Team", "Developer"],
            "Date Found": ["2024-02-20", "Jan 15, 2024"],
            "Module": ["Login", "Dashboard"],
            "Notes": ["Critical", "Low priority"]
        })
        self.etl.excel_data = self.excel_data

    def test_transform_unify(self):
        self.setup()
        self.etl.transform_and_unify()
        assert self.etl.unified_data is not None
        assert len(self.etl.unified_data) == 7
        assert "source" in self.etl.unified_data.columns
        print("PASS: test_transform_unify")

    def test_clean_duplicates(self):
        self.setup()
        self.etl.transform_and_unify()
        before = len(self.etl.unified_data)
        self.etl.clean_duplicates()
        after = len(self.etl.unified_data)
        assert after <= before
        print("PASS: test_clean_duplicates")

    def test_clean_priority(self):
        self.setup()
        self.etl.transform_and_unify()
        self.etl.clean_priority()
        valid = {"Critical", "High", "Medium", "Low", "Unknown"}
        actual = set(self.etl.unified_data["priority"].unique())
        assert actual.issubset(valid)
        print("PASS: test_clean_priority")

    def test_clean_status(self):
        self.setup()
        self.etl.transform_and_unify()
        self.etl.clean_status()
        valid = {"Open", "In Progress", "Resolved", "Closed", "Unknown"}
        actual = set(self.etl.unified_data["status"].unique())
        assert actual.issubset(valid)
        print("PASS: test_clean_status")

    def test_clean_component(self):
        self.setup()
        self.etl.transform_and_unify()
        self.etl.clean_component()
        assert "AUTH" not in self.etl.unified_data["component"].values
        assert "UI" not in self.etl.unified_data["component"].values
        print("PASS: test_clean_component")

    def test_bug_type_classification(self):
        self.setup()
        self.etl.transform_and_unify()
        self.etl.add_computed_fields()
        assert "bug_type" in self.etl.unified_data.columns
        assert "Crash/Fatal" in self.etl.unified_data["bug_type"].values
        print("PASS: test_bug_type_classification")

    def test_full_pipeline(self):
        self.setup()
        result = self.etl.run_full_pipeline()
        assert result is not None
        assert len(result) > 0
        assert "bug_type" in result.columns
        assert "resolution_days" in result.columns
        print("PASS: test_full_pipeline")


def run_all_tests():
    print("=" * 60)
    print("RUNNING BUG REPORT ETL TESTS")
    print("=" * 60)
    print("")

    tester = TestBugReportETL()
    tests = [
        tester.test_transform_unify,
        tester.test_clean_duplicates,
        tester.test_clean_priority,
        tester.test_clean_status,
        tester.test_clean_component,
        tester.test_bug_type_classification,
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
