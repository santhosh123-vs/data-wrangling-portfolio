import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid")

df = pd.read_csv("cleaned_data/unified_bug_reports.csv")

fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle("Bug Report ETL - Analysis Dashboard", fontsize=16, fontweight="bold")

df["bug_type"].value_counts().plot(
    kind="barh", ax=axes[0][0], color="steelblue"
)
axes[0][0].set_title("Bugs by Type (QA Classification)")
axes[0][0].set_xlabel("Count")

df["priority"].value_counts().plot(
    kind="bar", ax=axes[0][1],
    color=["red", "orange", "gold", "green", "gray"]
)
axes[0][1].set_title("Bugs by Priority")
axes[0][1].set_xlabel("")
axes[0][1].tick_params(axis="x", rotation=0)

df["source"].value_counts().plot(
    kind="pie", ax=axes[1][0], autopct="%1.1f%%",
    colors=["#3498db", "#2ecc71", "#e74c3c"]
)
axes[1][0].set_title("Bugs by Source")
axes[1][0].set_ylabel("")

df["status"].value_counts().plot(
    kind="bar", ax=axes[1][1],
    color=["#e74c3c", "#f39c12", "#2ecc71", "#3498db", "#95a5a6"]
)
axes[1][1].set_title("Bugs by Status")
axes[1][1].set_xlabel("")
axes[1][1].tick_params(axis="x", rotation=0)

plt.tight_layout()
plt.savefig("metadata/bug_report_dashboard.png", dpi=150)
plt.show()
print("Saved: metadata/bug_report_dashboard.png")

fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle("SDLC and Component Analysis", fontsize=14, fontweight="bold")

df["sdlc_phase"].value_counts().plot(
    kind="barh", ax=axes[0], color="coral"
)
axes[0].set_title("Bugs by SDLC Phase")
axes[0].set_xlabel("Count")

df["component"].value_counts().plot(
    kind="barh", ax=axes[1], color="mediumpurple"
)
axes[1].set_title("Bugs by Component")
axes[1].set_xlabel("Count")

plt.tight_layout()
plt.savefig("metadata/sdlc_component_analysis.png", dpi=150)
plt.show()
print("Saved: metadata/sdlc_component_analysis.png")
