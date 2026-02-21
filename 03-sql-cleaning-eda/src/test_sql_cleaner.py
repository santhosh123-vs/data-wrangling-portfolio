import sqlite3
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from sql_cleaner import SQLCleaner


class TestSQLCleaner:

    def setup(self):
        self.cleaner = SQLCleaner("raw_data/messy_ecommerce.db")

    def test_clean_customers(self):
        self.setup()
        df = self.cleaner.clean_customers()
        assert len(df) > 0
        assert "JOHN SMITH" not in df["name"].values
        assert "Unknown" in df["membership"].values
        names = df["name"].dropna()
        assert all(n == n.strip().title() or n == "Unknown" for n in names)
        print("PASS: test_clean_customers")
        self.cleaner.close()

    def test_clean_orders(self):
        self.setup()
        df = self.cleaner.clean_orders()
        assert len(df) > 0
        valid_statuses = {"Delivered", "Shipped", "Processing", "Cancelled", "Returned", "Pending", "Unknown"}
        actual = set(df["status"].unique())
        assert actual.issubset(valid_statuses)
        print("PASS: test_clean_orders")
        self.cleaner.close()

    def test_clean_products(self):
        self.setup()
        df = self.cleaner.clean_products()
        assert len(df) > 0
        assert "ELECTRONICS" not in df["category"].values
        assert "electronics" not in df["category"].values
        valid_ratings = df["rating"].dropna()
        assert all(valid_ratings >= 0)
        assert all(valid_ratings <= 5)
        print("PASS: test_clean_products")
        self.cleaner.close()

    def test_clean_support_tickets(self):
        self.setup()
        df = self.cleaner.clean_support_tickets()
        assert len(df) > 0
        assert "DELIVERY" not in df["issue_type"].values
        assert "delivery issue" not in df["issue_type"].values
        print("PASS: test_clean_support_tickets")
        self.cleaner.close()

    def test_full_pipeline(self):
        self.setup()
        self.cleaner.run_full_pipeline()
        assert self.cleaner.clean_customers_df is not None
        assert self.cleaner.clean_orders_df is not None
        assert self.cleaner.clean_products_df is not None
        assert self.cleaner.clean_tickets_df is not None
        assert len(self.cleaner.clean_customers_df) > 0
        assert len(self.cleaner.clean_orders_df) > 0
        print("PASS: test_full_pipeline")
        self.cleaner.close()


def run_all_tests():
    print("=" * 60)
    print("RUNNING SQL CLEANER TESTS")
    print("=" * 60)
    print("")

    tester = TestSQLCleaner()
    tests = [
        tester.test_clean_customers,
        tester.test_clean_orders,
        tester.test_clean_products,
        tester.test_clean_support_tickets,
        tester.test_full_pipeline
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print("FAIL: {} - {}".format(test.__name__, e))
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
