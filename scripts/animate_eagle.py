"""
Animate 🦅 position in Danish waters.
"""

from datetime import datetime, timedelta
from pathlib import Path

import geopandas as gpd
import imageio
import matplotlib.pyplot as plt
import pandas as pd
import polars as pl

from sdsprint import utils

eagle = utils.read_eagle(
    [
        "data/aisdk-2021-1h.parquet",
        "data/aisdk-2022-1h.parquet",
        "data/aisdk-2023-1h.parquet",
        "data/aisdk-2024-1h.parquet",
        "data/aisdk-2025-1h.parquet",
    ]
)
gdf = eagle.pipe(utils.to_gdf)


def get_chunks():
    indices = (
        gdf[[0, 1]]
        .pipe(pl.from_pandas)
        .with_columns(
            pl.int_range(pl.len(), dtype=pl.UInt32).alias("index"),
        )
        .select(
            pl.when(pl.col("1").diff().gt(timedelta(hours=1)))
            .then(pl.col("index"))
            .otherwise(None),
        )
        .drop_nulls()
        .to_numpy()
        .flatten()
        .tolist()
    )
    periods = [0] + indices
    pairs = list(zip(periods[:-1], periods[1:]))
    chunks = [gdf[p[0] : p[1]] for p in pairs]
    return chunks


def get_dt_chunk(subset):
    """Datetime column of gdf assumed to be 1 xD"""
    return str(subset.reset_index(drop=True)[1][subset.shape[0] - 1])


def plot_chunk(
    subset,
    danish_waters,
    name_out: str | None = None,
):
    ax = danish_waters.plot(figsize=(8, 8), color="lightblue")
    fp_gif = Path.cwd().joinpath("figs/gif")
    fp_gif.mkdir(parents=True, exist_ok=True)
    fp_gif_eagle = fp_gif / "eagle"
    fp_gif_eagle.mkdir(parents=True, exist_ok=True)

    # Remove old files
    if fp_gif_eagle.exists():
        for f in fp_gif_eagle.glob("*.png"):
            f.unlink()

    # Create gif by plotting incrementally plotting eagle positions
    # and then using stored png files with imageio
    utils.despine(ax)
    ax.set(xlim=(2.6e5, 11.0e5), ylim=(6.0e6, 6.45e6))
    dts = []
    for i in range(subset.shape[0]):
        _subset = subset[: i + 1].reset_index(drop=True)
        fig = ax.get_figure()
        _subset.plot(ax=ax, color="red", markersize=1)
        dt = get_dt_chunk(_subset)
        ax.set(title=f"Trace of Eagle; {dt}")
        print(f"Animating eagle: {i}; {dt=}")
        utils.despine(ax)
        fig.savefig(fp_gif_eagle / f"animeagle_{i}.png", bbox_inches="tight")
        dts.append(dt)
        plt.close(fig)

    first_dt = dts[0]
    files = sorted(
        fp_gif_eagle.glob("*.png"),
        key=lambda x: int(x.stem.split("_")[-1]),
    )
    print("Animating...")
    ims = [imageio.v2.imread(f) for f in files]
    if name_out:
        f_out = fp_gif.joinpath(name_out).with_suffix(".gif")
    else:
        f_out = fp_gif.joinpath(first_dt).with_suffix(".gif")
    imageio.mimwrite(f_out, ims)
    print(f"Generated: {f_out}")


danish_waters = gpd.read_file("data/geom/dk-shape2/dk.shp")
danish_waters = danish_waters.to_crs("EPSG:25832")

chunks = get_chunks()
print(f"Found {len(chunks)} chunks of sizes {[c.shape[0] for c in chunks]}")

for i, subset in enumerate(chunks):
    print(f"Animating chunk: {i}")
    plot_chunk(subset, danish_waters)


# Interesting to compare two chunks
subset = eagle.filter(
    pl.col("# Timestamp").is_between(
        datetime(2023, 11, 25),
        datetime(2023, 12, 4),
    )
)

dt1 = chunks[0][1].iloc[0]  # Initial path
dt2 = chunks[1][1].iloc[0]  # After turning off
dt3 = chunks[1][1].iloc[-1]  # Last timestamp
cables = utils.get_cables()
fig, ax = utils.plot_trace(
    subset,
    cables=cables,
    suffix="extra",
    save=False,
    title=f"Eagle activity from\n{dt1} to {dt3}",
)
ax.set(xlim=(2.6e5, 9.3e5), ylim=(6.0e6, 6.45e6))
ax.annotate(
    f"Initial path:\n{dt1}",
    xy=(2.7e5, 6.110e6),
    xytext=(2.9e5, 6.050e6),
    fontsize=11,
    ha="center",
    va="center",
    color="black",
    backgroundcolor="white",
    arrowprops=dict(
        arrowstyle="->",
        linewidth=1,
        color="black",
    ),
)
ax.annotate(
    f"Second path:\n{dt2}",
    xy=(3.6e5, 6.31e6),
    xytext=(3.4e5, 6.430e6),
    fontsize=11,
    ha="center",
    va="center",
    color="black",
    backgroundcolor="white",
    arrowprops=dict(
        arrowstyle="->",
        linewidth=1,
        color="black",
    ),
)
ax.annotate(
    "Eagle turning off\nhere",
    xy=(2.95e5, 6.20e6),
    xytext=(2.9e5, 6.35e6),
    fontsize=11,
    ha="center",
    va="center",
    color="black",
    backgroundcolor="white",
    arrowprops=dict(
        arrowstyle="->",
        linewidth=1,
        color="black",
    ),
)
ax.annotate(
    "Eagle turning off again here\nand then not seen again",
    xy=(8.05e5, 6.10e6),
    xytext=(8.2e5, 6.25e6),
    fontsize=11,
    ha="center",
    va="center",
    color="black",
    backgroundcolor="white",
    arrowprops=dict(
        arrowstyle="->",
        linewidth=1,
        color="black",
    ),
)
fig.savefig("figs/trace_eagle-annotated-path.png", bbox_inches="tight")
