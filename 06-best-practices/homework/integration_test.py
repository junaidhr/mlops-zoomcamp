import os
import pandas as pd
from datetime import datetime


def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


def main():
    # Same data as Q3
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
    ]

    columns = ['PULocationID', 'DOLocationID',
               'tpep_pickup_datetime', 'tpep_dropoff_datetime']

    df_input = pd.DataFrame(data, columns=columns)

    # Read env vars
    input_file_pattern = os.getenv("INPUT_FILE_PATTERN")
    s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")

    if not input_file_pattern:
        raise ValueError("INPUT_FILE_PATTERN is not set")

    # Pretend it's January 2023
    input_file = input_file_pattern.format(year=2023, month=1)

    # Options for Localstack
    options = {
        "client_kwargs": {
            "endpoint_url": s3_endpoint_url
        }
    }

    # Save to Localstack S3 (no compression!)
    df_input.to_parquet(
        input_file,
        engine="pyarrow",
        compression=None,
        index=False,
        storage_options=options
    )

    print(f"Saved test input to: {input_file}")


if __name__ == "__main__":
    main()
