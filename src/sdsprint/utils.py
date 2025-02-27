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


def to_gdf(df: pl.DataFrame):
    # Convert to GeoDataFrame
    geometry = [Point(xy) for xy in zip(df["Longitude"], df["Latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    gdf = gdf.set_crs("EPSG:4326")  # WGS84 first
    gdf = gdf.to_crs("EPSG:25832")  # Convert to Danish projection

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
