"""
SQL Database Cleaner and EDA
==============================
Cleans messy e-commerce SQLite database using SQL queries
and Python, then performs exploratory data analysis.

Author: Kethavath Saidamma
Date: February 2025
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
from datetime import datetime

sns.set_style("whitegrid")


class SQLCleaner:

    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cleaning_log = []

    def _log_step(self, step, description, details, reason):
        self.cleaning_log.append({
            "step": int(step),
            "description": description,
            "details": details,
            "reason": reason
        })
        print("STEP {}: {}".format(step, description))
        print("   {}".format(details))
        print("")

    def run_quality_checks(self):
        print("=" * 60)
        print("DATA QUALITY CHECKS")
        print("=" * 60)
        print("")

        for table in ["customers", "orders", "products", "support_tickets"]:
            df = pd.read_sql("SELECT COUNT(*) as cnt FROM {}".format(table), self.conn)
            print("{}: {} records".format(table, df['cnt'].values[0]))
        print("")

        print("--- Duplicate Customers ---")
        query = """
            SELECT customer_id, COUNT(*) as cnt
            FROM customers
            GROUP BY customer_id, name, email
            HAVING COUNT(*) > 1
        """
        df = pd.read_sql(query, self.conn)
        print("Found {} duplicate groups".format(len(df)))
        print("")

        print("--- Invalid Emails ---")
        query = """
            SELECT COUNT(*) as cnt FROM customers
            WHERE email NOT LIKE '%@%.%'
               OR email IS NULL OR email = '' OR email = 'N/A'
               OR email LIKE '%@@%'
        """
        df = pd.read_sql(query, self.conn)
        print("Found {} invalid emails".format(df['cnt'].values[0]))
        print("")

        print("--- Invalid Ages ---")
        query = """
            SELECT COUNT(*) as cnt FROM customers
            WHERE age < 0 OR age > 120 OR age IS NULL OR age = 0
        """
        df = pd.read_sql(query, self.conn)
        print("Found {} invalid ages".format(df['cnt'].values[0]))
        print("")

        print("--- Invalid Orders ---")
        query = """
            SELECT COUNT(*) as cnt FROM orders
            WHERE quantity <= 0 OR unit_price <= 0
               OR quantity IS NULL OR unit_price IS NULL
        """
        df = pd.read_sql(query, self.conn)
        print("Found {} invalid orders".format(df['cnt'].values[0]))
        print("")

        print("--- Inconsistent Statuses ---")
        query = "SELECT status, COUNT(*) as cnt FROM orders GROUP BY status ORDER BY cnt DESC"
        df = pd.read_sql(query, self.conn)
        print(df.to_string(index=False))
        print("")

        print("--- Missing Values Summary ---")
        for table in ["customers", "orders", "products", "support_tickets"]:
            df = pd.read_sql("SELECT * FROM {}".format(table), self.conn)
            missing = df.isnull().sum()
            total_missing = missing.sum()
            print("{}: {} total missing values".format(table, total_missing))
        print("")

    def clean_customers(self):
        print("=" * 60)
        print("CLEANING CUSTOMERS TABLE")
        print("=" * 60)
        print("")

        df = pd.read_sql("SELECT * FROM customers", self.conn)
        original_count = len(df)

        df = df.drop_duplicates(subset=["customer_id"], keep="first")
        self._log_step(1, "Removed duplicate customers",
            "Removed {} duplicates".format(original_count - len(df)),
            "Duplicate customer_ids cause incorrect join results")

        def clean_name(name):
            if pd.isna(name) or str(name).strip() in ["", "UNKNOWN", "None"]:
                return "Unknown"
            return str(name).strip().title()

        df["name"] = df["name"].apply(clean_name)
        self._log_step(2, "Standardized customer names",
            "Converted to Title Case, replaced empty values",
            "Inconsistent casing prevents accurate customer matching")

        def clean_email(email):
            if pd.isna(email):
                return None
            email = str(email).strip().lower()
            if email in ["", "n/a", "none"]:
                return None
            if "@@" in email:
                return None
            if "@" not in email or "." not in email.split("@")[-1]:
                return None
            return email

        df["email"] = df["email"].apply(clean_email)
        self._log_step(3, "Cleaned email addresses",
            "Lowercased, removed invalid formats",
            "Invalid emails like jane@@test.com and john@email cannot be used for communication")

        country_map = {
            "USA": "USA", "usa": "USA", "US": "USA",
            "United States": "USA", "U.S.A": "USA"
        }
        df["country"] = df["country"].map(country_map).fillna("Unknown")
        self._log_step(4, "Standardized country names",
            "Mapped 5 variations to USA",
            "Multiple country name formats prevent geographic analysis")

        city_map = {
            "New York": "New York", "new york": "New York",
            "NEW YORK": "New York", "NY": "New York",
            "Los Angeles": "Los Angeles", "los angeles": "Los Angeles",
            "LA": "Los Angeles",
            "Chicago": "Chicago", "chicago": "Chicago",
            "CHICAGO": "Chicago",
            "Houston": "Houston",
            "San Francisco": "San Francisco",
            "Seattle": "Seattle"
        }
        df["city"] = df["city"].map(city_map).fillna("Unknown")
        self._log_step(5, "Standardized city names",
            "Mapped abbreviations and case variations",
            "NYC, New York, and new york should all be the same city")

        state_map = {
            "NY": "NY", "New York": "NY", "new york": "NY",
            "CA": "CA", "California": "CA", "california": "CA", "calif": "CA",
            "IL": "IL", "Illinois": "IL",
            "TX": "TX", "Texas": "TX",
            "WA": "WA", "Washington": "WA"
        }
        df["state"] = df["state"].map(state_map).fillna("Unknown")
        self._log_step(6, "Standardized state names to abbreviations",
            "California -> CA, New York -> NY etc.",
            "Mix of full names and abbreviations prevents geographic grouping")

        membership_map = {
            "Gold": "Gold", "GOLD": "Gold", "gold": "Gold",
            "Silver": "Silver", "SILVER": "Silver", "silver": "Silver",
            "Bronze": "Bronze", "BRONZE": "Bronze", "bronze": "Bronze",
            "Premium": "Premium", "PREMIUM": "Premium",
            "Free": "Free", "FREE": "Free", "free": "Free",
            "N/A": "Free"
        }
        df["membership"] = df["membership"].map(membership_map).fillna("Unknown")
        self._log_step(7, "Standardized membership levels",
            "Unified casing variations",
            "GOLD, Gold, and gold should be the same membership tier")

        df.loc[(df["age"] < 13) | (df["age"] > 120), "age"] = None
        self._log_step(8, "Cleaned invalid ages",
            "Removed ages below 13 and above 120",
            "Ages like -1, 0, 150, 999 are clearly data entry errors")

        return df

    def clean_orders(self):
        print("=" * 60)
        print("CLEANING ORDERS TABLE")
        print("=" * 60)
        print("")

        df = pd.read_sql("SELECT * FROM orders", self.conn)

        status_map = {
            "Delivered": "Delivered", "DELIVERED": "Delivered", "delivered": "Delivered",
            "Shipped": "Shipped", "SHIPPED": "Shipped", "shipped": "Shipped",
            "Processing": "Processing", "PROCESSING": "Processing", "processing": "Processing",
            "Cancelled": "Cancelled", "CANCELLED": "Cancelled", "cancelled": "Cancelled", "Canceled": "Cancelled",
            "Returned": "Returned", "RETURNED": "Returned",
            "Pending": "Pending", "PENDING": "Pending"
        }
        df["status"] = df["status"].map(status_map).fillna("Unknown")
        self._log_step(9, "Standardized order statuses",
            "17 variations mapped to 6 standard statuses",
            "Cancelled vs CANCELLED vs Canceled should be the same")

        payment_map = {
            "Credit Card": "Credit Card", "credit card": "Credit Card", "CC": "Credit Card",
            "Debit Card": "Debit Card", "debit card": "Debit Card", "DC": "Debit Card",
            "PayPal": "PayPal", "paypal": "PayPal", "PAYPAL": "PayPal",
            "Cash": "Cash", "CASH": "Cash",
            "Bitcoin": "Crypto", "BTC": "Crypto",
            "N/A": None
        }
        df["payment_method"] = df["payment_method"].map(payment_map)
        self._log_step(10, "Standardized payment methods",
            "Unified naming conventions",
            "CC, Credit Card, and credit card are the same method")

        df.loc[df["quantity"] <= 0, "quantity"] = None
        df.loc[df["unit_price"] <= 0, "unit_price"] = None
        df.loc[df["total_amount"] <= 0, "total_amount"] = None

        mask = df["quantity"].notna() & df["unit_price"].notna()
        df.loc[mask, "total_amount"] = round(df.loc[mask, "quantity"] * df.loc[mask, "unit_price"], 2)

        self._log_step(11, "Cleaned order amounts",
            "Removed negative/zero values, recalculated totals",
            "Negative quantities and prices are data entry errors")

        def parse_date(val):
            if pd.isna(val):
                return pd.NaT
            try:
                return pd.to_datetime(val)
            except Exception:
                return pd.NaT

        df["order_date"] = df["order_date"].apply(parse_date)
        self._log_step(12, "Standardized order dates",
            "Multiple formats unified to datetime",
            "Consistent dates needed for time-series sales analysis")

        return df

    def clean_products(self):
        print("=" * 60)
        print("CLEANING PRODUCTS TABLE")
        print("=" * 60)
        print("")

        df = pd.read_sql("SELECT * FROM products", self.conn)
        original = len(df)

        df = df.drop_duplicates(subset=["product_id"], keep="first")
        self._log_step(13, "Removed duplicate products",
            "Removed {} duplicates".format(original - len(df)),
            "Duplicate product_ids cause incorrect inventory counts")

        def clean_product_name(name):
            if pd.isna(name):
                return "Unknown Product"
            return str(name).strip().title()

        df["product_name"] = df["product_name"].apply(clean_product_name)
        self._log_step(14, "Standardized product names",
            "Converted to Title Case",
            "wireless mouse and WIRELESS MOUSE should be the same product")

        category_map = {
            "Electronics": "Electronics", "ELECTRONICS": "Electronics", "electronics": "Electronics",
            "Clothing": "Clothing", "CLOTHING": "Clothing", "clothing": "Clothing",
            "Books": "Books", "BOOKS": "Books", "books": "Books",
            "Home": "Home & Garden", "HOME": "Home & Garden", "home & garden": "Home & Garden",
            "Sports": "Sports", "SPORTS": "Sports", "sports": "Sports"
        }
        df["category"] = df["category"].map(category_map).fillna("Unknown")
        self._log_step(15, "Standardized product categories",
            "Unified casing and naming",
            "ELECTRONICS and electronics should be the same category")

        df.loc[df["price"] <= 0, "price"] = None
        df.loc[df["price"] > 10000, "price"] = None
        df.loc[df["rating"] < 0, "rating"] = None
        df.loc[df["rating"] > 5, "rating"] = None
        df.loc[df["stock_quantity"] < 0, "stock_quantity"] = None
        self._log_step(16, "Cleaned invalid product values",
            "Removed negative prices, invalid ratings, negative stock",
            "Rating of 10 or -1 is impossible on a 1-5 scale")

        return df

    def clean_support_tickets(self):
        print("=" * 60)
        print("CLEANING SUPPORT TICKETS TABLE")
        print("=" * 60)
        print("")

        df = pd.read_sql("SELECT * FROM support_tickets", self.conn)

        issue_map = {
            "Delivery Issue": "Delivery",
            "delivery issue": "Delivery",
            "DELIVERY": "Delivery",
            "Product Defect": "Product Defect",
            "product defect": "Product Defect",
            "DEFECT": "Product Defect",
            "Billing Error": "Billing",
            "billing error": "Billing",
            "BILLING": "Billing",
            "Account Problem": "Account",
            "account problem": "Account",
            "Return Request": "Return",
            "return request": "Return",
            "Technical Issue": "Technical",
            "technical issue": "Technical"
        }
        df["issue_type"] = df["issue_type"].map(issue_map).fillna("Unknown")
        self._log_step(17, "Standardized issue types",
            "16 variations mapped to 7 categories",
            "delivery issue and DELIVERY should be the same category")

        priority_map = {
            "High": "High", "HIGH": "High", "high": "High", "P1": "High",
            "Medium": "Medium", "MEDIUM": "Medium", "medium": "Medium", "P2": "Medium",
            "Low": "Low", "LOW": "Low", "low": "Low", "P3": "Low"
        }
        df["priority"] = df["priority"].map(priority_map).fillna("Unknown")

        status_map = {
            "Open": "Open", "OPEN": "Open", "open": "Open",
            "In Progress": "In Progress", "IN_PROGRESS": "In Progress",
            "Resolved": "Resolved", "RESOLVED": "Resolved",
            "Closed": "Closed", "CLOSED": "Closed"
        }
        df["status"] = df["status"].map(status_map).fillna("Unknown")
        self._log_step(18, "Standardized ticket priority and status",
            "Unified naming conventions",
            "Consistent values needed for support performance analysis")

        def clean_agent(val):
            if pd.isna(val) or str(val).strip() in ["", "UNASSIGNED", "None"]:
                return "Unassigned"
            return str(val).strip().title()

        df["agent"] = df["agent"].apply(clean_agent)
        self._log_step(19, "Cleaned agent names",
            "Standardized to Title Case, replaced empty values",
            "Consistent agent names needed for workload analysis")

        return df

    def run_full_pipeline(self):
        print("=" * 60)
        print("RUNNING SQL CLEANING PIPELINE")
        print("=" * 60)
        print("")

        self.run_quality_checks()

        self.clean_customers_df = self.clean_customers()
        self.clean_orders_df = self.clean_orders()
        self.clean_products_df = self.clean_products()
        self.clean_tickets_df = self.clean_support_tickets()

        print("=" * 60)
        print("PIPELINE COMPLETE")
        print("=" * 60)

        return self

    def create_visualizations(self):
        print("Creating visualizations...")

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle("E-Commerce Data Analysis Dashboard", fontsize=16, fontweight="bold")

        self.clean_orders_df["status"].value_counts().plot(
            kind="bar", ax=axes[0][0], color="steelblue"
        )
        axes[0][0].set_title("Orders by Status")
        axes[0][0].set_xlabel("")
        axes[0][0].tick_params(axis="x", rotation=45)

        self.clean_customers_df["membership"].value_counts().plot(
            kind="bar", ax=axes[0][1],
            color=["gold", "silver", "orange", "green", "gray", "purple"]
        )
        axes[0][1].set_title("Customers by Membership")
        axes[0][1].set_xlabel("")
        axes[0][1].tick_params(axis="x", rotation=0)

        self.clean_products_df["category"].value_counts().plot(
            kind="barh", ax=axes[1][0], color="coral"
        )
        axes[1][0].set_title("Products by Category")

        self.clean_tickets_df["issue_type"].value_counts().plot(
            kind="barh", ax=axes[1][1], color="mediumpurple"
        )
        axes[1][1].set_title("Support Tickets by Issue Type")

        plt.tight_layout()
        plt.savefig("metadata/ecommerce_dashboard.png", dpi=150)
        plt.close()
        print("Saved: metadata/ecommerce_dashboard.png")

        fig, axes = plt.subplots(1, 2, figsize=(16, 6))

        valid_ages = self.clean_customers_df["age"].dropna()
        valid_ages.plot(kind="hist", bins=30, ax=axes[0], color="steelblue", edgecolor="black")
        axes[0].set_title("Customer Age Distribution (After Cleaning)")
        axes[0].set_xlabel("Age")

        valid_prices = self.clean_orders_df["unit_price"].dropna()
        valid_prices[valid_prices < 500].plot(
            kind="hist", bins=30, ax=axes[1], color="coral", edgecolor="black"
        )
        axes[1].set_title("Order Price Distribution (After Cleaning)")
        axes[1].set_xlabel("Price ($)")

        plt.tight_layout()
        plt.savefig("metadata/distribution_analysis.png", dpi=150)
        plt.close()
        print("Saved: metadata/distribution_analysis.png")

    def export_data(self):
        self.clean_customers_df.to_csv("cleaned_data/clean_customers.csv", index=False)
        self.clean_orders_df.to_csv("cleaned_data/clean_orders.csv", index=False)
        self.clean_products_df.to_csv("cleaned_data/clean_products.csv", index=False)
        self.clean_tickets_df.to_csv("cleaned_data/clean_support_tickets.csv", index=False)

        self.clean_customers_df.to_parquet("cleaned_data/clean_customers.parquet", index=False)
        self.clean_orders_df.to_parquet("cleaned_data/clean_orders.parquet", index=False)

        print("Exported all cleaned tables to cleaned_data/")

    def export_lineage(self):
        lineage = {
            "project": "SQL Cleaning and EDA",
            "author": "Kethavath Saidamma",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "database": self.db_path,
            "tables_cleaned": {
                "customers": int(len(self.clean_customers_df)),
                "orders": int(len(self.clean_orders_df)),
                "products": int(len(self.clean_products_df)),
                "support_tickets": int(len(self.clean_tickets_df))
            },
            "cleaning_steps": self.cleaning_log
        }
        with open("metadata/data_lineage.json", "w") as f:
            json.dump(lineage, f, indent=2)
        print("Lineage saved to metadata/data_lineage.json")

    def print_summary(self):
        print("")
        print("=" * 60)
        print("CLEANED DATA SUMMARY")
        print("=" * 60)
        print("")
        print("Customers: {} records".format(len(self.clean_customers_df)))
        print("  Membership: {}".format(dict(self.clean_customers_df["membership"].value_counts())))
        print("")
        print("Orders: {} records".format(len(self.clean_orders_df)))
        print("  Status: {}".format(dict(self.clean_orders_df["status"].value_counts())))
        print("")
        print("Products: {} records".format(len(self.clean_products_df)))
        print("  Categories: {}".format(dict(self.clean_products_df["category"].value_counts())))
        print("")
        print("Support Tickets: {} records".format(len(self.clean_tickets_df)))
        print("  Issue Types: {}".format(dict(self.clean_tickets_df["issue_type"].value_counts())))

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    cleaner = SQLCleaner("raw_data/messy_ecommerce.db")
    cleaner.run_full_pipeline()
    cleaner.create_visualizations()
    cleaner.export_data()
    cleaner.export_lineage()
    cleaner.print_summary()
    cleaner.close()
