"""
Script to inspect 2021-2025 data and create count plot.
"""

import os

import matplotlib.pyplot as plt
import pandas as pd

os.environ["POLARS_MAX_THREADS"] = "4"  # Useful on personal laptop

import polars as pl
import seaborn as sns

df = (
    pl.scan_parquet(
        [
            "data/aisdk-2021-1h.parquet",
            "data/aisdk-2022-1h.parquet",
            "data/aisdk-2023-1h.parquet",
            "data/aisdk-2024-1h.parquet",
            "data/aisdk-2025-1h.parquet",
        ]
    )
    .sort("# Timestamp")
    .group_by_dynamic(
        "# Timestamp",
        every="1d",
        closed="left",
        include_boundaries=False,
    )
    .agg(pl.len())
    .collect()
)

# Assert that we have all the dates
data = df.with_columns(pl.col("# Timestamp").dt.date().alias("date"))
expected_dates = pd.date_range(start="2024-01-01", end="2025-02-14", freq="D").date
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
fig.savefig("figs/aisdk-plot-all.png")
