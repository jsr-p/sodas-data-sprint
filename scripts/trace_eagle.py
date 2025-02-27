"""
Trace 🦅 position in Danish waters
"""

import geopandas as gpd
import polars as pl
from shapely.geometry import Point

eagle_mmsi = "518998865"
eagle_imo = "9329760"


def read_eagle(file: str):
    return (
        pl.scan_parquet(file)
        .filter(pl.col("MMSI").eq(eagle_mmsi).or_(pl.col("IMO").eq(eagle_imo)))
        .collect()
        .select(
            "MMSI",
            "# Timestamp",
            "Latitude",
            "Longitude",
        )
    )


def plot(df: pl.DataFrame, suffix: str):
    # Convert to GeoDataFrame
    geometry = [Point(xy) for xy in zip(df["Longitude"], df["Latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)

    gdf = gdf.set_crs("EPSG:4326")  # WGS84 first
    gdf = gdf.to_crs("EPSG:25832")  # Convert to Danish projection

    # Downloaded from: https://simplemaps.com/gis/country/dk#all
    danish_waters = gpd.read_file("data/geom/dk-shape2/dk.shp")
    danish_waters = danish_waters.to_crs("EPSG:25832")

    # Plot geometry
    ax = danish_waters.plot(figsize=(8, 8), color="lightblue")
    gdf.plot(ax=ax, color="red", markersize=1)
    ax.legend(["Eagle"], frameon=False)
    ax.set_title(f"Trace of Eagle; {suffix}")
    ax.set(xlim=(2.6e5, 11.0e5), ylim=(6.0e6, 6.45e6))
    # Despine plot
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    fig = ax.get_figure()
    fig.tight_layout(pad=1.0)
    fig.savefig(f"figs/trace_eagle-{suffix}.png", bbox_inches="tight")


for file, suffix in zip(
    [
        "data/aisdk-2024-15m.parquet",
        "data/aisdk-2024-1h.parquet",
        "data/aisdk-2024-30m.parquet",
        "data/aisdk-2023-1h.parquet",
    ],
    ["2024-15m", "2024-1h", "2024-30m", "2023-1h"],
):
    plot(read_eagle(file), suffix=suffix)
    print(f"Generated figs/trace_eagle-{suffix}.png")
