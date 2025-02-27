"""
Script to inspect 2024 data and create plots.
"""

import os
import pprint
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

os.environ["POLARS_MAX_THREADS"] = "4"  # Useful on personal laptop

import polars as pl
import seaborn as sns

if len(sys.argv) != 2:
    raise ValueError("Please provide the path to the data file")

_, file = sys.argv


file = Path(file)
if not file.exists():
    raise FileNotFoundError(f"File {file} not found")

print(f"Reading file {file}")
suffix = "-".join(file.stem.split("-")[-2:])  # year-frequency

df = pl.read_parquet(file)

print("Schema:")
pprint.pprint(df.schema)
print(f"Shape: {df.shape}")

gp = (
    df.sort("# Timestamp")
    .group_by_dynamic(
        "# Timestamp",
        every="1d",
        closed="left",
        include_boundaries=False,
    )
    .agg(pl.len())
)


gp_tom = (
    df.sort("# Timestamp")
    .group_by_dynamic(
        "# Timestamp",
        every="1w",
        closed="left",
        include_boundaries=False,
        group_by="Type of mobile",
    )
    .agg(pl.len())
).sort("# Timestamp", "Type of mobile")


# Assert that we have all the dates
data = gp.with_columns(pl.col("# Timestamp").dt.date().alias("date"))
expected_dates = pd.date_range(start="2024-01-01", end="2024-12-31", freq="D").date
missing_dates = set(expected_dates) - set(data["date"].dt.date())
assert len(missing_dates) == 0


# Create the plot
fig, ax = plt.subplots(figsize=(10, 6))
sns.lineplot(
    x="date",
    y="len",
    ax=ax,
    data=data.to_pandas(),
    color="black",
)
ax.set(
    xlabel="Date",
    ylabel="Count",
    title="Count of records by Date",
)
sns.despine()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
fig.savefig(f"figs/aisdk-plot-{suffix}.png")


# Create the plot
fig, ax = plt.subplots(figsize=(10, 6))
sns.lineplot(
    x="# Timestamp",
    y="len",
    hue="Type of mobile",
    ax=ax,
    data=gp_tom.to_pandas(),
)
ax.set(
    xlabel="Date",
    ylabel="Count",
    title="Counts of mobile unit types",
)
sns.despine()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
fig.savefig(f"figs/aisdk-{suffix}-plot-mobile-type.png")
