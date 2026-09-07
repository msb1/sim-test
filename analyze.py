"""Analyze iot-sim sensor datasets and anomaly-detect results from RustFS.
"""
import os

import pandas as pd
import s3fs

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://192.168.1.50:9000")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "iotsim")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "access")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "secret")
S3_DATASET_KEY = "dataset/municipal_water_system_01-2026-08-30T00:00:00Z.parquet"
S3_PROCESSED_KEY = "processed/water_hammer-2026-08-30T00:00:00Z.parquet"
S3_RESULTS_KEY = "results/"


def rustfs() -> s3fs.S3FileSystem:
    """Create a path-style RustFS client using iot-sim/anomaly-detect defaults."""
    return s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={"endpoint_url": S3_ENDPOINT_URL},
    )


def read_parquet(filesystem: s3fs.S3FileSystem, key: str) -> pd.DataFrame:
    with filesystem.open(f"{S3_BUCKET_NAME}/{key}", "rb") as source:
        return pd.read_parquet(source, engine="pyarrow")


def write_parquet(df: pd.DataFrame, filesystem: s3fs.S3FileSystem, key: str) -> None:
    """Writes a pandas DataFrame to an S3-compatible path as a Parquet file."""
    with filesystem.open(f"{S3_BUCKET_NAME}/{key}", "wb") as target:
        df.to_parquet(
            target,
            engine="pyarrow",
            compression="snappy",
            index=False
        )


def main() -> None:
    filesystem = rustfs()
    df = read_parquet(filesystem, S3_DATASET_KEY)
    print("***** Original Dataset *****")
    print(df.head(20))
    print(f"number of rows: {len(df)}")
    print(df.dtypes)

    keep_parameters = ['line_pressure_psi']

    df_wide = (
        df[df['metric'].isin(keep_parameters)].copy()
        .assign(timestamp=lambda x: pd.to_datetime(x['timestamp_ms'], unit='ms'))
        .pivot(index='timestamp', columns='metric', values='value')
        .reset_index()
    )

    # Rename the columns index to clean up visual layout
    df_wide.columns.name = None

    print("***** Wide Dataset After ETL *****")
    print(df_wide.head(20))
    print(f"number of rows: {len(df_wide)}")
    print(df_wide.dtypes)

    write_parquet(df_wide, filesystem, S3_PROCESSED_KEY)


if __name__ == "__main__":
    main()
