#!/usr/bin/env python
# coding: utf-8

import os
import sys
import pickle
import pandas as pd


# ------------------------------
# 1. Data transformation
# ------------------------------
def prepare_data(df, categorical):
    df["duration"] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    df[categorical] = df[categorical].fillna(-1).astype(int).astype(str)

    return df


# ------------------------------
# 2. Input / Output Path Helpers
# ------------------------------
def get_input_path(year, month):
    default = (
        "https://d37ci6vzurychx.cloudfront.net/"
        "trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
    )
    pattern = os.getenv("INPUT_FILE_PATTERN", default)
    return pattern.format(year=year, month=month)


def get_output_path(year, month):
    default = (
        "s3://nyc-duration-prediction-alexey/"
        "taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet"
    )
    pattern = os.getenv("OUTPUT_FILE_PATTERN", default)
    return pattern.format(year=year, month=month)


# ------------------------------
# 3. Reading parquet with Localstack
# ------------------------------
def read_data(filename, categorical):
    s3_endpoint = os.getenv("S3_ENDPOINT_URL")

    if filename.startswith("s3://") and s3_endpoint:
        opts = {"client_kwargs": {"endpoint_url": s3_endpoint}}
        df = pd.read_parquet(filename, storage_options=opts)
    else:
        df = pd.read_parquet(filename)

    return prepare_data(df, categorical)


# ------------------------------
# 4. Saving to S3 with Localstack (Q6 requirement)
# ------------------------------
def save_data(df, output_file):
    s3_endpoint = os.getenv("S3_ENDPOINT_URL")
    opts = None

    if output_file.startswith("s3://") and s3_endpoint:
        opts = {"client_kwargs": {"endpoint_url": s3_endpoint}}

    df.to_parquet(
        output_file,
        engine="pyarrow",
        compression=None,
        index=False,
        storage_options=opts
    )


# ------------------------------
# 5. Main pipeline
# ------------------------------
def main(year, month):
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    categorical = ["PULocationID", "DOLocationID"]

    with open("model.bin", "rb") as f_in:
        dv, lr = pickle.load(f_in)

    df = read_data(input_file, categorical)
    df["ride_id"] = f"{year:04d}/{month:02d}_" + df.index.astype(str)

    dicts = df[categorical].to_dict(orient="records")
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print("Predicted mean duration:", y_pred.mean())

    df_result = pd.DataFrame({
        "ride_id": df["ride_id"],
        "predicted_duration": y_pred
    })

    save_data(df_result, output_file)


# ------------------------------
# 6. Entry point
# ------------------------------
if __name__ == "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year, month)
