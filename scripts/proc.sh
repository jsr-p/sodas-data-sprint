#!/usr/bin/env bash

# Script run on server to process the data

for year in {2021..2025} ; do
	python scripts/zip_proc.py proc-year $year
	python scripts/zip_proc.py resample-year $year
	python scripts/zip_proc.py resample-final $year
done
