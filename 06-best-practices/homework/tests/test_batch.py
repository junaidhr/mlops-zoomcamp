import pandas as pd
from datetime import datetime
from batch_v2 import prepare_data


def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


def test_prepare_data():
    # Input
    data = [
        (None, None, dt(1, 1), dt(1, 10)),         # 9 min → valid
        (1, 1, dt(1, 2), dt(1, 10)),               # 8 min → valid
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),      # <1 min → invalid
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),          # >60 min → invalid
    ]

    columns = [
        'PULocationID', 'DOLocationID',
        'tpep_pickup_datetime', 'tpep_dropoff_datetime'
    ]

    df = pd.DataFrame(data, columns=columns)
    categorical = ['PULocationID', 'DOLocationID']

    actual_df = prepare_data(df, categorical)
    actual_df = actual_df[['PULocationID', 'DOLocationID', 'duration']].reset_index(drop=True)

    # Expected
    expected_data = [
        {'PULocationID': '-1', 'DOLocationID': '-1', 'duration': 9.0},
        {'PULocationID': '1',  'DOLocationID': '1',  'duration': 8.0},
    ]

    expected_df = pd.DataFrame(expected_data)

    # PRINTS (these show only when pytest is run with -s)
    print("\n=== EXPECTED DATAFRAME ===")
    print(expected_df)

    print("\n=== ACTUAL DATAFRAME ===")
    print(actual_df)

    # HOMEWORK-REQUIRED ASSERT
    assert actual_df.to_dict(orient='records') == expected_df.to_dict(orient='records')
