"""
SQL Database Creator
=====================
Creates a SQLite database with messy e-commerce data
for cleaning and analysis.

Tables:
1. customers - Customer information with data quality issues
2. orders - Order records with inconsistencies
3. products - Product catalog with duplicates and errors
4. support_tickets - Customer support data with messy text

Author: Kethavath Saidamma
Date: February 2025
"""

import sqlite3
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def create_messy_database(db_path="raw_data/messy_ecommerce.db"):

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS customers")
    cursor.execute("DROP TABLE IF EXISTS orders")
    cursor.execute("DROP TABLE IF EXISTS products")
    cursor.execute("DROP TABLE IF EXISTS support_tickets")

    cursor.execute("""
        CREATE TABLE customers (
            customer_id INTEGER,
            name TEXT,
            email TEXT,
            phone TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            signup_date TEXT,
            age INTEGER,
            membership TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            product_id INTEGER,
            order_date TEXT,
            quantity INTEGER,
            unit_price REAL,
            total_amount REAL,
            status TEXT,
            payment_method TEXT,
            shipping_address TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE products (
            product_id INTEGER,
            product_name TEXT,
            category TEXT,
            price REAL,
            stock_quantity INTEGER,
            supplier TEXT,
            rating REAL,
            created_date TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE support_tickets (
            ticket_id INTEGER,
            customer_id INTEGER,
            order_id INTEGER,
            issue_type TEXT,
            description TEXT,
            priority TEXT,
            status TEXT,
            created_date TEXT,
            resolved_date TEXT,
            agent TEXT
        )
    """)

    names = [
        "John Smith", "JOHN SMITH", "john smith",
        "Jane Doe", "jane doe", "JANE DOE",
        "Bob Wilson", "Alice Brown", "Charlie Davis",
        "Emma Johnson", "Michael Lee", "Sarah Miller",
        "David Garcia", "Lisa Anderson", "James Taylor",
        None, "", "UNKNOWN"
    ]

    emails = [
        "john@email.com", "JOHN@EMAIL.COM", "john@email",
        "jane@test.com", "jane@@test.com", "invalid-email",
        "bob@company.com", "alice@gmail.com",
        None, "", "N/A"
    ]

    phones = [
        "555-123-4567", "(555) 123-4567", "5551234567",
        "+1-555-123-4567", "555.123.4567",
        "000-000-0000", "INVALID", None, "", "N/A"
    ]

    cities = [
        "New York", "new york", "NEW YORK", "NY",
        "Los Angeles", "los angeles", "LA",
        "Chicago", "chicago", "CHICAGO",
        "Houston", "San Francisco", "Seattle",
        None, "", "UNKNOWN"
    ]

    states = [
        "NY", "New York", "new york",
        "CA", "California", "california", "calif",
        "IL", "Illinois",
        "TX", "Texas",
        "WA", "Washington",
        None, ""
    ]

    memberships = [
        "Gold", "GOLD", "gold",
        "Silver", "SILVER", "silver",
        "Bronze", "BRONZE", "bronze",
        "Premium", "PREMIUM",
        "Free", "FREE", "free",
        None, "", "N/A"
    ]

    base_time = datetime(2023, 1, 1)
    customers = []

    for i in range(3000):
        signup = base_time + timedelta(days=random.randint(0, 730))
        date_formats = [
            signup.strftime("%Y-%m-%d"),
            signup.strftime("%m/%d/%Y"),
            signup.strftime("%d-%b-%Y"),
            str(signup.timestamp()),
            "INVALID_DATE" if random.random() < 0.02 else signup.strftime("%Y-%m-%d"),
            None
        ]
        age_options = [
            random.randint(18, 80),
            random.randint(18, 80),
            -1, 0, 150, 999,
            None
        ]
        customers.append((
            i + 1,
            random.choice(names),
            random.choice(emails),
            random.choice(phones),
            random.choice(cities),
            random.choice(states),
            random.choice(["USA", "usa", "US", "United States", "U.S.A", None]),
            random.choice(date_formats),
            random.choice(age_options),
            random.choice(memberships)
        ))

    num_dup = int(len(customers) * 0.05)
    duplicates = random.choices(customers, k=num_dup)
    customers.extend(duplicates)
    random.shuffle(customers)

    cursor.executemany(
        "INSERT INTO customers VALUES (?,?,?,?,?,?,?,?,?,?)",
        customers
    )

    categories = [
        "Electronics", "ELECTRONICS", "electronics",
        "Clothing", "CLOTHING", "clothing",
        "Books", "BOOKS", "books",
        "Home", "HOME", "home & garden",
        "Sports", "SPORTS", "sports",
        None
    ]

    products = []
    for i in range(500):
        price_options = [
            round(random.uniform(5, 500), 2),
            -1.0, 0.0, 99999.99,
            None
        ]
        rating_options = [
            round(random.uniform(1, 5), 1),
            0.0, -1.0, 6.0, 10.0,
            None
        ]
        products.append((
            i + 1,
            random.choice([
                "Wireless Mouse", "wireless mouse", "WIRELESS MOUSE",
                "Laptop Stand", "laptop stand",
                "USB Cable", "usb cable", "USB-C Cable",
                "Headphones", "HEADPHONES",
                "Keyboard", "keyboard",
                "Monitor", "Phone Case", "Tablet",
                "Backpack", "Water Bottle",
                None
            ]),
            random.choice(categories),
            random.choice(price_options),
            random.choice([
                random.randint(0, 1000),
                -1, None
            ]),
            random.choice([
                "TechCorp", "techcorp", "TECHCORP",
                "StyleHub", "stylehub",
                "BookWorld", "HomeGoods",
                None, "", "UNKNOWN"
            ]),
            random.choice(rating_options),
            random.choice([
                datetime(2023, random.randint(1, 12), random.randint(1, 28)).strftime("%Y-%m-%d"),
                None
            ])
        ))

    num_dup = int(len(products) * 0.08)
    duplicates = random.choices(products, k=num_dup)
    products.extend(duplicates)

    cursor.executemany(
        "INSERT INTO products VALUES (?,?,?,?,?,?,?,?)",
        products
    )

    order_statuses = [
        "Delivered", "DELIVERED", "delivered",
        "Shipped", "SHIPPED", "shipped",
        "Processing", "PROCESSING", "processing",
        "Cancelled", "CANCELLED", "cancelled", "Canceled",
        "Returned", "RETURNED",
        "Pending", "PENDING",
        None
    ]

    payment_methods = [
        "Credit Card", "credit card", "CC",
        "Debit Card", "debit card", "DC",
        "PayPal", "paypal", "PAYPAL",
        "Cash", "CASH",
        "Bitcoin", "BTC",
        None, "", "N/A"
    ]

    orders = []
    for i in range(8000):
        order_date = base_time + timedelta(days=random.randint(0, 730))
        date_formats = [
            order_date.strftime("%Y-%m-%d %H:%M:%S"),
            order_date.strftime("%m/%d/%Y"),
            order_date.strftime("%d-%b-%Y"),
            None
        ]
        qty = random.choice([
            random.randint(1, 10),
            0, -1, None
        ])
        price = random.choice([
            round(random.uniform(5, 500), 2),
            -1.0, 0.0, None
        ])
        if qty and price and qty > 0 and price > 0:
            total = round(qty * price, 2)
        else:
            total = random.choice([None, 0, -1])

        orders.append((
            i + 1,
            random.randint(1, 3000),
            random.randint(1, 500),
            random.choice(date_formats),
            qty,
            price,
            total,
            random.choice(order_statuses),
            random.choice(payment_methods),
            random.choice([
                "123 Main St, New York",
                "456 Oak Ave, Los Angeles",
                "789 Pine Rd, Chicago",
                None, "", "UNKNOWN"
            ])
        ))

    cursor.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?)",
        orders
    )

    issue_types = [
        "Delivery Issue", "delivery issue", "DELIVERY",
        "Product Defect", "product defect", "DEFECT",
        "Billing Error", "billing error", "BILLING",
        "Account Problem", "account problem",
        "Return Request", "return request",
        "Technical Issue", "technical issue",
        None
    ]

    priorities_list = [
        "High", "HIGH", "high", "P1",
        "Medium", "MEDIUM", "medium", "P2",
        "Low", "LOW", "low", "P3",
        None
    ]

    ticket_statuses = [
        "Open", "OPEN", "open",
        "In Progress", "IN_PROGRESS",
        "Resolved", "RESOLVED",
        "Closed", "CLOSED",
        None
    ]

    tickets = []
    for i in range(2000):
        created = base_time + timedelta(days=random.randint(0, 730))
        resolved = created + timedelta(days=random.randint(0, 14))
        tickets.append((
            i + 1,
            random.randint(1, 3000),
            random.randint(1, 8000),
            random.choice(issue_types),
            random.choice([
                "Product arrived damaged",
                "Wrong item received",
                "Charged twice for order",
                "Cannot login to account",
                "Want to return product",
                "App crashing on checkout",
                None, ""
            ]),
            random.choice(priorities_list),
            random.choice(ticket_statuses),
            created.strftime("%Y-%m-%d %H:%M:%S"),
            resolved.strftime("%Y-%m-%d %H:%M:%S") if random.random() > 0.3 else None,
            random.choice([
                "Agent_Smith", "agent_smith",
                "Agent_Jones", "Agent_Brown",
                None, "", "UNASSIGNED"
            ])
        ))

    cursor.executemany(
        "INSERT INTO support_tickets VALUES (?,?,?,?,?,?,?,?,?,?)",
        tickets
    )

    conn.commit()

    print("Database created: {}".format(db_path))
    print("")

    for table in ["customers", "orders", "products", "support_tickets"]:
        cursor.execute("SELECT COUNT(*) FROM {}".format(table))
        count = cursor.fetchone()[0]
        print("{}: {} records".format(table, count))

    print("")

    print("Sample customers:")
    df = pd.read_sql("SELECT * FROM customers LIMIT 5", conn)
    print(df)
    print("")

    print("Sample orders:")
    df = pd.read_sql("SELECT * FROM orders LIMIT 5", conn)
    print(df)

    conn.close()


if __name__ == "__main__":
    create_messy_database()
