"""
Pandas vs Polars Performance Comparison
========================================
Demonstrates knowledge of modern data tools by comparing
Pandas and Polars for common data wrangling operations.
"""

import pandas as pd
import polars as pl
import numpy as np
import time
import json

def benchmark_pandas(filepath):
    """Run cleaning operations using Pandas."""
    start = time.time()

    df = pd.read_csv(filepath)

    df = df.drop_duplicates()

    df['severity'] = df['severity'].map({
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
    })

    df['environment'] = df['environment'].map({
        "production": "production",
        "PRODUCTION": "production",
        "prod": "production",
        "staging": "staging",
        "Staging": "staging",
        "stg": "staging",
        "development": "development",
        "dev": "development",
        "DEV": "development"
    })

    df['response_time_ms'] = pd.to_numeric(
        df['response_time_ms'], errors='coerce'
    )

    severity_counts = df['severity'].value_counts()
    env_counts = df['environment'].value_counts()
    mean_response = df['response_time_ms'].mean()

    end = time.time()
    return round(end - start, 4)


def benchmark_polars(filepath):
    """Run same cleaning operations using Polars."""
    start = time.time()

    df = pl.read_csv(filepath)

    df = df.unique()

    severity_map = {
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

    env_map = {
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

    df = df.with_columns(
        pl.col("severity").replace_strict(
            severity_map, default=None
        ).alias("severity")
    )

    df = df.with_columns(
        pl.col("environment").replace_strict(
            env_map, default=None
        ).alias("environment")
    )

    severity_counts = df.group_by("severity").len()
    env_counts = df.group_by("environment").len()
    mean_response = df.select(
        pl.col("response_time_ms").cast(pl.Float64, strict=False).mean()
    )

    end = time.time()
    return round(end - start, 4)


if __name__ == "__main__":
    filepath = "raw_data/messy_server_logs.csv"

    print("=" * 60)
    print("PANDAS vs POLARS PERFORMANCE COMPARISON")
    print("=" * 60)
    print("")

    num_runs = 5
    pandas_times = []
    polars_times = []

    for i in range(num_runs):
        pt = benchmark_pandas(filepath)
        pandas_times.append(pt)
        plt_time = benchmark_polars(filepath)
        polars_times.append(plt_time)

    pandas_avg = round(sum(pandas_times) / num_runs, 4)
    polars_avg = round(sum(polars_times) / num_runs, 4)

    if polars_avg > 0:
        speedup = round(pandas_avg / polars_avg, 2)
    else:
        speedup = 0

    print("Pandas average time: {} seconds".format(pandas_avg))
    print("Polars average time: {} seconds".format(polars_avg))
    print("")
    print("Polars is {}x faster than Pandas".format(speedup))

    results = {
        "benchmark": "Data cleaning pipeline on 5250 rows",
        "num_runs": num_runs,
        "pandas": {
            "avg_time_seconds": pandas_avg,
            "all_runs": pandas_times
        },
        "polars": {
            "avg_time_seconds": polars_avg,
            "all_runs": polars_times
        },
        "speedup": "{}x".format(speedup),
        "conclusion": "Polars is {}x faster than Pandas for this workload. For larger datasets (millions of rows), the difference would be even more significant.".format(speedup)
    }

    with open("metadata/polars_benchmark.json", "w") as f:
        json.dump(results, f, indent=2)

    print("")
    print("Results saved to metadata/polars_benchmark.json")
    print("")
    print(json.dumps(results, indent=2))
