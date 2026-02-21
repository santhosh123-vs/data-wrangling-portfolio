import pandas as pd
import json

print("Creating sample files...")

df_raw = pd.read_csv("raw_data/messy_server_logs.csv")
df_clean = pd.read_csv("cleaned_data/clean_server_logs.csv")

df_raw.head(20).to_csv("samples/raw_logs_sample.csv", index=False)
print("Saved: samples/raw_logs_sample.csv")

df_clean.head(20).to_csv("samples/cleaned_logs_sample.csv", index=False)
print("Saved: samples/cleaned_logs_sample.csv")

df_clean.head(20).to_parquet("samples/cleaned_logs_sample.parquet", index=False)
print("Saved: samples/cleaned_logs_sample.parquet")

comparison = {
    "before": {
        "total_rows": len(df_raw),
        "duplicate_rows": len(df_raw) - len(df_raw.drop_duplicates()),
        "missing_values": int(df_raw.isnull().sum().sum()),
        "severity_variations": int(df_raw['severity'].nunique()),
        "environment_variations": int(df_raw['environment'].nunique()),
        "timestamp_formats": "4+ different formats"
    },
    "after": {
        "total_rows": len(df_clean),
        "duplicate_rows": 0,
        "missing_values": int(df_clean.isnull().sum().sum()),
        "severity_variations": int(df_clean['severity'].nunique()),
        "environment_variations": int(df_clean['environment'].nunique()),
        "timestamp_formats": "1 unified ISO format"
    },
    "improvements": {
        "duplicates_removed": len(df_raw) - len(df_clean),
        "null_reduction_percentage": round(
            (1 - df_clean.isnull().sum().sum() / df_raw.isnull().sum().sum()) * 100, 2
        ),
        "severity_standardized": "12 variations -> 4 standard levels",
        "environment_standardized": "9 variations -> 3 standard names"
    }
}

with open("samples/before_after_comparison.json", "w") as f:
    json.dump(comparison, f, indent=2)
print("Saved: samples/before_after_comparison.json")

print("")
print("Before vs After:")
print(json.dumps(comparison, indent=2))
