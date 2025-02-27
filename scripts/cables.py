"""
🔌 + 🦅
"""

import json

import geopandas as gpd

from sdsprint import utils

# https://www.submarinecablemap.com/api/v3/cable/cable-geo.json
with open("data/geom/cable-geo.json") as f:
    cable_geo = json.load(f)
cables = gpd.read_file("data/geom/cable-geo.json")
cables = cables.to_crs("EPSG:25832")
cables = cables[cables.geometry.is_valid]

eagle = utils.read_eagle("data/aisdk-2024-1h.parquet").pipe(utils.to_gdf)

danish_waters = gpd.read_file("data/geom/dk-shape2/dk.shp")
danish_waters = danish_waters.to_crs("EPSG:25832")

# spatial join cables + Danish waters
joined = gpd.sjoin(cables, danish_waters, how="inner", predicate="intersects")

# Plot
ax = danish_waters.plot(figsize=(8, 8), color="lightblue")
joined_plot = joined.plot(
    ax=ax,
    column="id_left",
    linewidth=0.5,
    legend=True,
)
eagle_plot = eagle.plot(ax=ax, color="red", markersize=1, legend=True, label="Eagle")
ax.legend(["Cables", "Eagle"], frameon=False, loc=(0.7, 0.7))
ax.set(xlim=(2.6e5, 11.0e5), ylim=(6.0e6, 6.45e6))
ax.set(title="Cables in Danish waters + Eagle")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["left"].set_visible(False)
ax.spines["bottom"].set_visible(False)
ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
fig = ax.get_figure()
fig.tight_layout(pad=1.0)
fig.savefig("figs/cables.png", bbox_inches="tight")
