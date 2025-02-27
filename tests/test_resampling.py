"""
Test for backwards filling nans from AIS data.
"""

from datetime import datetime

import polars as pl

from sdsprint import utils

dtr = pl.datetime_range(
    start=datetime(year=2021, month=12, day=15, hour=3, minute=0),
    end=datetime(year=2021, month=12, day=16, hour=3, minute=0),
    interval="15m",
    eager=True,
    closed="left",
)
n = int(dtr.shape[0] / 2)
dfn = pl.DataFrame(
    #  NOTE: Data is by 15m
    {
        "dt": dtr,
        "y": ([None, None] * 3 + [None, "y"]) * (n // 4),
        "y2": [2, None] * n,
        "y3": [None, None, None, 4] * (n // 2),
        "x1": [None, None, "y", None] * (n // 2),
        "x2": [None, "z", "y", None, None, "y", "z", None] * (n // 4),
        "id": ["a"] * n * 2,  # Same ship
    }
)

filled = dfn.pipe(
    utils.resample,
    index_col="dt",
    every="1h",
    group_by="id",
).sort("dt")

"""
- y_mod2: every odd hour has a nan when backwards filling 
- y_mod2_r: every equal hour equals y when backwards filling
- y3: all values equal 4
- x1: all values equal y
- x2_mod2: every odd hour has a nan when backwards filling
- x2_mod2_r: every equal hour equals y when backwards filling
"""

res = filled.select(
    y_mod2=pl.col("y").filter(pl.col("dt").dt.hour().mod(2).eq(1)).is_null().all(),
    y_mod2_r=pl.col("y").filter(pl.col("dt").dt.hour().mod(2).eq(0)).eq("y").all(),
    y3=pl.col("y3").eq(4).all(),
    x1=pl.col("x1").eq("y").all(),
    x2_mod2=pl.col("x2").filter(pl.col("dt").dt.hour().mod(2).eq(1)).eq("z").all(),
    x2_mod2_r=pl.col("x2").filter(pl.col("dt").dt.hour().mod(2).eq(0)).eq("y").all(),
)

assert res.to_numpy().flatten().all()  # All True
