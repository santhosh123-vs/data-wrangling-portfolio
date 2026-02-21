import pandas as pd
import numpy as np
import random
import json
from datetime import datetime, timedelta

def generate_messy_logs(num_records=5000):

    error_types = [
        "System Crash",
        "UI Glitch",
        "API Timeout",
        "Authentication Failure",
        "Performance Degradation",
        "Database Connection Error",
        "Memory Leak",
        "File Not Found",
        "Permission Denied",
        "Network Timeout"
    ]

    severity_levels = [
        "Critical", "High", "Medium", "Low",
        "CRITICAL", "high", "med", "LOW",
        "P1", "P2", "P3", "P4",
        None
    ]

    modules = [
        "auth-service",
        "payment-gateway",
        "user-dashboard",
        "api-gateway",
        "database-manager",
        "file-storage",
        "notification-service",
        None
    ]

    environments = [
        "production", "staging", "development",
        "PRODUCTION", "Staging", "dev",
        "prod", "stg", "DEV",
        None
    ]

    base_time = datetime(2024, 1, 1)

    logs = []

    for i in range(num_records):
        timestamp = base_time + timedelta(
            minutes=random.randint(0, 525600)
        )

        time_formats = [
            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            timestamp.strftime("%m/%d/%Y %H:%M"),
            timestamp.strftime("%d-%b-%Y %H:%M:%S"),
            str(timestamp.timestamp()),
            None
        ]

        if random.random() < 0.02:
            chosen_time = "INVALID_TIME"
        else:
            chosen_time = random.choice(time_formats)

        response_options = [
            random.randint(10, 5000),
            round(random.uniform(10, 5000), 2),
            -1,
            0,
            99999,
            None,
            "N/A"
        ]

        user_options = [
            "USR-{}".format(random.randint(1000, 9999)),
            str(random.randint(1000, 9999)),
            None,
            "UNKNOWN",
            ""
        ]

        ip_options = [
            "192.168.{}.{}".format(
                random.randint(0, 255),
                random.randint(0, 255)
            ),
            "INVALID_IP",
            None,
            "0.0.0.0",
            "10.0.{}.{}".format(
                random.randint(0, 255),
                random.randint(0, 255)
            )
        ]

        log_entry = {
            "log_id": i + 1,
            "timestamp": chosen_time,
            "error_type": random.choice(error_types),
            "severity": random.choice(severity_levels),
            "module": random.choice(modules),
            "environment": random.choice(environments),
            "user_id": random.choice(user_options),
            "response_time_ms": random.choice(response_options),
            "message": "Error in module at line {}: {}".format(
                random.randint(1, 500),
                random.choice(error_types)
            ),
            "ip_address": random.choice(ip_options)
        }

        logs.append(log_entry)

    num_duplicates = int(num_records * 0.05)
    duplicates = random.choices(logs, k=num_duplicates)
    logs.extend(duplicates)

    random.shuffle(logs)

    return pd.DataFrame(logs)


if __name__ == "__main__":
    print("Generating messy log data...")
    df = generate_messy_logs(5000)

    df.to_csv("raw_data/messy_server_logs.csv", index=False)

    json_subset = df.head(1000).to_dict(orient="records")
    with open("raw_data/messy_api_logs.json", "w") as f:
        json.dump(json_subset, f, indent=2, default=str)

    print("Generated {} log entries".format(len(df)))
    print("Saved to raw_data/messy_server_logs.csv")
    print("Saved to raw_data/messy_api_logs.json")
    print("")
    print("Data Shape: {}".format(df.shape))
    print("")
    print("Sample of messy data:")
    print(df.head(10))
    print("")
    print("Null counts:")
    print(df.isnull().sum())
