"""
Find 🦅 in our datasets.
"""

import polars as pl

eagle_mmsi = "518998865"
eagle_imo = "9329760"


df = (
    pl.scan_parquet(
        [
            # Polars can read list of files lazily 🫶 and find 🦅
            "data/aisdk-2021-1h.parquet",
            "data/aisdk-2022-1h.parquet",
            "data/aisdk-2023-1h.parquet",
            "data/aisdk-2024-1h.parquet",
            "data/aisdk-2025-1h.parquet",
        ]
    )
    .filter(pl.col("MMSI").eq(eagle_mmsi).or_(pl.col("IMO").eq(eagle_imo)))
    .collect()
    .select(
        "MMSI",
        "# Timestamp",
        "Latitude",
        "Longitude",
    )
)

yms = (
    df.select(
        pl.col("# Timestamp").dt.strftime("%Y-%m").alias("year-month").unique().sort()
    )
    .to_series()
    .to_list()
)
print("Eagle found in the given month years:")
for ym in yms:
    print(f"  - {ym}")
