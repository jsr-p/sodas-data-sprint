import json
from pathlib import Path
from typing import Literal

import geopandas as gpd
import polars as pl
from shapely.geometry import Point

eagle_mmsi = "518998865"
eagle_imo = "9329760"


def read_ships(file: str | list[str]):
    return (
        pl.scan_parquet(file)
        .collect()
        .select(
            "MMSI",
            "# Timestamp",
            "Latitude",
            "Longitude",
        )
    )


def read_eagle(file: str | list[str]):
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


dk_csrs = Literal["EPSG:25832", "EPSG:25833"]


def to_gdf(df: pl.DataFrame, csr: dk_csrs = "EPSG:25832"):
    # Convert to GeoDataFrame
    geometry = [Point(xy) for xy in zip(df["Longitude"], df["Latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    gdf = gdf.set_crs("EPSG:4326")  # WGS84 first
    gdf = gdf.to_crs(csr)  # Convert to Danish projection
    if not isinstance(gdf, gpd.GeoDataFrame):
        raise ValueError("Conversion to GeoDataFrame failed")
    return gdf


def resample(
    df: pl.DataFrame,
    index_col: str,
    every: str,
    group_by: str,
):
    return df.group_by_dynamic(
        index_col,
        every=every,
        closed="left",
        group_by=group_by,
        include_boundaries=False,
    ).agg(pl.all().first(), pl.len().alias("numobs"))


def despine(ax):
    # Despine plot
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)


def get_cables():
    # https://www.submarinecablemap.com/api/v3/cable/cable-geo.json
    csr = "EPSG:25832"
    cables = gpd.read_file("data/geom/cable-geo.json")
    cables = cables.to_crs(csr)
    cables = cables[cables.geometry.is_valid]
    danish_waters = gpd.read_file("data/geom/dk-shape2/dk.shp")
    danish_waters = danish_waters.to_crs(csr)

    # spatial join cables + Danish waters
    return gpd.sjoin(cables, danish_waters, how="inner", predicate="intersects")


def plot_trace(
    df: pl.DataFrame,
    suffix: str,
    cables: gpd.GeoDataFrame | None = None,
    save: bool = True,
    title: str | None = None,
):
    gdf = to_gdf(df)

    # Downloaded from: https://simplemaps.com/gis/country/dk#all
    danish_waters = gpd.read_file("data/geom/dk-shape2/dk.shp")
    danish_waters = danish_waters.to_crs("EPSG:25832")

    # Plot geometry
    ax = danish_waters.plot(figsize=(8, 8), color="lightblue")
    gdf.plot(ax=ax, color="red", markersize=1)

    if isinstance(cables, gpd.GeoDataFrame):
        cables.plot(
            ax=ax,
            column="id_left",
            linewidth=0.5,
            legend=True,
        )
        ax.legend(["Eagle", "Cables"], frameon=False, loc=(0.7, 0.7))
    else:
        ax.legend(["Eagle"], frameon=False)

    if title:
        ax.set_title(title)
    else:
        ax.set_title(f"Trace of Eagle; {suffix}")
    ax.set(xlim=(2.6e5, 11.0e5), ylim=(6.0e6, 6.45e6))

    despine(ax)

    fig = ax.get_figure()
    fig.tight_layout(pad=1.0)
    if not save:
        return fig, ax
    else:
        fig.savefig(f"figs/trace_eagle-{suffix}.png", bbox_inches="tight")
        return fig, ax


def plot_activity(
    df: pl.DataFrame,
    **kwargs,
):
    gdf = to_gdf(df)

    # Downloaded from: https://simplemaps.com/gis/country/dk#all
    danish_waters = gpd.read_file("data/geom/dk-shape2/dk.shp")
    danish_waters = danish_waters.to_crs("EPSG:25832")

    # Plot geometry
    ax = danish_waters.plot(figsize=(8, 8), color="lightblue")
    gdf.plot(
        ax=ax,
        color="red",
        markersize=1,
        **kwargs,
    )
    ax.set(xlim=(2.6e5, 11.0e5), ylim=(6.0e6, 6.45e6))
    despine(ax)
    fig = ax.get_figure()
    fig.tight_layout(pad=1.0)
    return fig, ax
