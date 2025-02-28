"""
Plot all activity 24 daily obs.
"""

import geopandas as gpd
import polars as pl

from sdsprint import utils

ships = (
    utils.read_ships(
        [
            "data/aisdk-2024-1h.parquet",
        ]
    )
    .group_by_dynamic(
        "# Timestamp",
        every="1d",
        closed="both",
        group_by="MMSI",
        include_boundaries=False,
    )
    .agg(pl.all().first())
)

danish_waters = gpd.read_file("data/geom/dk-shape2/dk.shp")
danish_waters = danish_waters.to_crs("EPSG:25832")

print("Plotting all activity 24h")
fig, ax = utils.plot_activity(
    ships,
)
ax.set(title="All activity 24h")
fig.savefig("figs/all_activity_24.png", bbox_inches="tight")
