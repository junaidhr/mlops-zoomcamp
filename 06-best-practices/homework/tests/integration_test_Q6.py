import os
import pandas as pd
from datetime import datetime
from pathlib import Path


def dt(h, m, s=0):
    return datetime(2023, 1, 1, h, m, s)


def test_full_integration_q6():

    # ---------------------------
    # 1. Build input dataframe
    # ---------------------------
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
    ]

    cols = ["PULocationID", "DOLocationID",
            "tpep_pickup_datetime", "tpep_dropoff_datetime"]

    df_input = pd.DataFrame(data, columns=cols)

    # ---------------------------
    # 2. Construct input and output paths
    # ---------------------------
    inp_pat = os.getenv("INPUT_FILE_PATTERN")
    out_pat = os.getenv("OUTPUT_FILE_PATTERN")
    s3_ep = os.getenv("S3_ENDPOINT_URL")

    input_file = inp_pat.format(year=2023, month=1)
    output_file = out_pat.format(year=2023, month=1)

    # ---------------------------
    # 3. Upload input parquet to Localstack S3
    # ---------------------------
    opts = {"client_kwargs": {"endpoint_url": s3_ep}}

    df_input.to_parquet(
        input_file,
        engine="pyarrow",
        compression=None,
        index=False,
        storage_options=opts
    )

    # ---------------------------
    # 4. Run the batch job
    # ---------------------------
    root = Path(__file__).resolve().parents[1]   # moves from tests/ → homework/
    batch_script = root / "batch_v4.py"

    exit_code = os.system(f"python {batch_script} 2023 01")
    assert exit_code == 0

    # ---------------------------
    # 5. Load output parquet from Localstack S3
    # ---------------------------
    df_out = pd.read_parquet(output_file, storage_options=opts)

    # ---------------------------
    # 6. Compute sum of predicted durations
    # ---------------------------
    total = df_out["predicted_duration"].sum()
    print("TOTAL =", total)

    # Homework expected answer ~36.28
    assert abs(total - 36.28) < 0.5
