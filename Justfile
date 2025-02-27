inspect24:
    python scripts/queries.py data/aisdk-2024-1h.parquet

inspectall:
    python scripts/queries-all.py

eagle:
    python scripts/find_eagle.py
    python scripts/trace_eagle.py
    python scripts/cables.py
