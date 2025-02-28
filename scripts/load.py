import polars as pl

# Read 10k rows
df = pl.read_parquet("data/aisdk-2024-1h.parquet", n_rows=10_000)
print(df.head())
