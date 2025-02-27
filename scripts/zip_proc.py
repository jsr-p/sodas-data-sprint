"""
Script to process AIS data from http://web.ais.dk/aisdata/.

We extract csv files from zip files, convert csv files to parquet and then
resample them by 15m, 30m and 1h.
We resample by the `# Timestamp` and `MMSI` columns (time & unique id).

Final data is stored on tresorit; see link in e-mail.

The script is run on the SODAS server.
"""

import datetime
import os
import sys
import warnings
import zipfile
from pathlib import Path

import click
import polars as pl
import tabulate
from loguru import logger

KU_ID = os.getenv("KUID")

fp_ais = Path(f"/home/{KU_ID}/main-compute/ais-proc")
fp_sdir = Path(f"/home/{KU_ID}/ukraine-sdir/AIS_DK")
fp_exp = Path(f"/home/{KU_ID}/ukraine-sdir/explore-ais")
fp_test = fp_exp.joinpath("tests")

if not Path.home().name == "jsr-p":  # Only log on server
    logger.remove()
    logger.add(
        sys.stdout,
        level="INFO",
    )
    logger.add(
        sink=fp_ais / "log.log",
        rotation="50 MB",
        backtrace=True,
        diagnose=True,
        serialize=False,
        level="INFO",
    )


# Suppress specific DeprecationWarning from polars
warnings.filterwarnings(
    "ignore", category=DeprecationWarning, message=".*old streaming engine.*"
)

years = [
    "2021",
    "2022",
    "2023",
    "2024",
    "2025",
]

cats = [
    "IMO",  # IMO number of the vessel
    "Callsign",  # Callsign of the vessel
    "Name",  # Name of the vessel
    "Destination",  # Destination of the vessel
]
nums = [
    "ROT",
    "Heading",
    "SOG",
    "COG",
    "A",
    "B",
    "C",
    "D",
    "Width",
    "Length",
    "Draught",
]


def proc_ais(df: pl.DataFrame):
    return df.with_columns(
        pl.col("# Timestamp").str.to_datetime(
            # format="%Y-%m-%d %H:%M:%S"
            format="%d/%m/%Y %H:%M:%S"
        ),
        pl.col("ETA").str.to_datetime(format="%d/%m/%Y %H:%M:%S"),
    ).with_columns(
        pl.col(nums).cast(pl.Float64),
        #  NOTE: If categorical we'll get some problems when concatenating
        pl.col(cats).cast(pl.Utf8),
        pl.col("MMSI").cast(pl.Utf8),
    )


def sink_csv(csv_path: Path, pq_path: Path):
    pl.scan_csv(csv_path).sink_parquet(pq_path)
    logger.info(f"Sinked {csv_path} to {pq_path}")


def proc_zip(zip_path: Path, output_dir: Path):
    """
    Extracts the CSV files from a ZIP archive and sinks them as Parquet files.
    """
    with zipfile.ZipFile(zip_path, "r") as z:
        names = z.namelist()
        logger.info(f"Extracting {len(names)} files from {zip_path}")
        for filename in names:
            if filename.endswith(".csv"):
                csv_out = Path.joinpath(output_dir, filename)
                pqfile = csv_out.with_suffix(".parquet")
                if pqfile.exists():
                    logger.info(f"File {pqfile} already exists")
                    continue
                logger.info(f"Extracting {filename}...")
                z.extract(filename, output_dir)
                logger.info(f"Extracted {filename} to {output_dir}")
                sink_csv(csv_out, pqfile)
                csv_out.unlink()
                logger.info(f"Deleted {filename}")


def extract_and_sink(zip_path: Path, output_dir: Path):
    try:
        proc_zip(zip_path, output_dir)
    except zipfile.BadZipfile as ex:
        logger.info(f"Error: {ex}")
        error_file = fp_ais / "errors.csv"
        with open(error_file, "a") as f:
            f.write(f"{zip_path.name}\n")
    else:
        logger.info(f"Done processing {zip_path}")


def file_in_mb(f: Path) -> float:
    return os.path.getsize(f) / (1024 * 1024)


def file_in_gb(f: Path) -> float:
    return os.path.getsize(f) / (1024 * 1024 * 1024)


def get_zip_files(year: int | str) -> list[Path]:
    return sorted(fp_sdir.glob(f"{year}/*.zip"), key=lambda f: f.name)


def current_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def proc_zip_files(zip_files: list[Path], out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    for i, file in enumerate(zip_files):
        logger.info(f": Processing zip-file: {file.name}")
        extract_and_sink(file, out_dir)
        logger.info(f"Done processing {file.name} ({i + 1}/{len(zip_files)})")


def proc_zip_files_year(year: int):
    out_dir = fp_ais.joinpath("data", f"{year}")
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_files = get_zip_files(f"{year}")
    print(f"Processing files for {year=}")
    proc_zip_files(zip_files, out_dir)
    logger.info(f"Done processing all files for {year}")


@click.group()
def cli():
    pass


@cli.command()
def list_zips():
    for year in years:
        print(f"Year: {year}")
        zip_files = get_zip_files(year)
        for f in zip_files:
            print(f"  {f.name} - {file_in_mb(f):.2f} MB")


@cli.command()
def inspect_sizes():
    tots = []
    for year in years:
        print(f"Year: {year}")
        fp_pq = fp_exp.joinpath(year)
        files = sorted(fp_pq.glob("*.parquet"), key=lambda f: f.name)
        sizes = []
        for f in files:
            size = file_in_mb(f)
            print(f"  {f.name} - {size:.2f} MB")
            sizes.append(size)
        zip_size = sum(file_in_mb(f) for f in get_zip_files(year))
        tots.append((year, sum(sizes) / 1024, zip_size / 1024))

    print(" Total sizes ".center(40, "="))
    print(tabulate.tabulate(tots, headers=["Year", "Size (GB)", "Zip size (GB)"]))


@cli.command()
@click.argument("year", type=int)
def proc_year(year: int):
    """
    Process all zip files for a given year.
    """
    proc_zip_files_year(year)


@cli.command()
def proc_errd():
    """Proc zip files that err'd before"""
    fp_extra = Path(f"/home/{KU_ID}/ukraine-sdir/extra-zips")

    # 2024
    proc_zip_files(
        [
            # fp_extra / "aisdk-2024-01.zip",
            fp_extra
            / "2024-05-26.zip",
        ],
        out_dir=fp_ais.joinpath("data", "2024"),
    )

    # 2023
    proc_zip_files(
        [
            fp_extra / "2023-02.zip",
        ],
        out_dir=fp_ais.joinpath("data", "2023"),
    )

    # 2021
    proc_zip_files(
        [
            fp_sdir.joinpath("2021") / "aisdk-2021-08.zip",
            fp_extra / "2021-09.zip",
            fp_extra / "2021-10.zip",
            fp_extra / "2021-12.zip",
            fp_extra / "aisdk-2021-11.zip",
        ],
        out_dir=fp_ais.joinpath("data", "2021"),
    )


@cli.command()
def proc_errdcsvs():
    """Proc faulty csv files"""
    fp_csvs = Path(f"/home/{KU_ID}/ukraine-sdir/extra-csvs")

    # 2024 files
    files = [Path("aisdk-2024-02-03.csv")]
    for file in files:
        f_csv = fp_csvs / file
        f_pq = fp_ais.joinpath("data", "2024") / file.with_suffix(".parquet")
        logger.info(f"Processing errd csv `{f_csv}`")
        sink_csv(csv_path=f_csv, pq_path=f_pq)


def resample_df(df: pl.DataFrame, every: str) -> pl.DataFrame:
    return df.group_by_dynamic(
        "# Timestamp",
        every=every,
        closed="left",
        group_by="MMSI",
        include_boundaries=False,
    ).agg(pl.all().backward_fill().first())


def rs_df(file: Path, every: str) -> pl.DataFrame:
    """Resample dataframe by backwards-filling nans.

    The first observation is kept for each group (id, time).
    """
    return (
        pl.scan_parquet(file)
        .pipe(proc_ais)
        .pipe(resample_df, every=every)
        # collect
        .collect()
    )


def resample_files(files: list[Path], every: str, fp_out: Path):
    for f in files:
        print(f"Processing {f}")
        stem = f.stem
        file_out = fp_out / f"{stem}-{every}.parquet"
        try:  # Just write it directly to avoid memory issues
            if not file_out.exists():
                rs_df(f, every=every).write_parquet(file_out)
            else:
                print(f"File {file_out} already exists")
        except Exception as ex:
            logger.info(f"Failed processing {f} {every=}")
            logger.exception(ex)
        else:
            logger.info(f"Done processing {f} {every=} to {file_out}")


@cli.command()
@click.argument("year", type=str)
def resample_year(year: str):
    """
    Resample data to 15m intervals for a given year.
    Later on we resample to 30m and 1h.
    """
    every = "15m"  # Can resample to 30m and 1h from 15 m then
    year = f"{year}"

    fp_pq = fp_ais.joinpath("data", year)
    fp_out = fp_ais.joinpath("data", "proc", year)
    fp_out.mkdir(parents=True, exist_ok=True)

    files = sorted(fp_pq.glob("aisdk*.parquet"), key=lambda f: f.name)
    logger.info(f"Processing {len(files)} files from {fp_pq}; {year=}")
    resample_files(files, every="15m", fp_out=fp_out)
    logger.info(f"Done processing all files for {year=} for {every=}")


@cli.command()
def rs_extra():
    """Resample missing ones"""
    # One missing for 2024
    fp_data = fp_ais.joinpath("data", "2024")
    fp_out = fp_ais.joinpath("data", "proc", "2024")
    resample_files([fp_data / "aisdk-2024-05-26.parquet"], every="15m", fp_out=fp_out)


@cli.command()
@click.argument("year", type=str)
def resample_final(year: str):
    """
    Resample all data for a given year into 15m, 30m and 1h intervals.
    These are the datasets provided for the data sprint.
    """
    fp_dsprint = fp_ais.joinpath("data", "proc", "data-sprint")
    fp_dsprint.mkdir(parents=True, exist_ok=True)
    fp = fp_ais.joinpath("data", "proc", year)

    logger.info(f"Loading all files for {year}")
    df = pl.read_parquet([f for f in fp.glob("*.parquet")])
    logger.info(f"Loaded all files; {df.shape[0]} rows in total.")

    df30m = df.sort("# Timestamp", "MMSI").pipe(resample_df, every="30m")
    df1h = df30m.pipe(resample_df, every="1h")

    df.write_parquet(fp_dsprint.joinpath(f"aisdk-{year}-15m.parquet"))
    df30m.write_parquet(fp_dsprint.joinpath(f"aisdk-{year}-30m.parquet"))
    df1h.write_parquet(fp_dsprint.joinpath(f"aisdk-{year}-1h.parquet"))

    print(df.shape, df30m.shape, df1h.shape, sep="\n")
    logger.info(f"Done resampling {year}")


@cli.command()
def inspect_final():
    fp_pq = fp_ais.joinpath("data", "proc", "data-sprint")
    files = sorted(fp_pq.glob("*.parquet"), key=lambda f: f.name)
    sizes = []
    for f in files:
        size = file_in_mb(f)
        shape = pl.read_parquet(f).shape
        print(f"  {f.name} - {size:.2f} MB; {shape}")
        sizes.append(size)
    print(f"  Total: {sum(sizes):.2f} MB")


if __name__ == "__main__":
    cli()
