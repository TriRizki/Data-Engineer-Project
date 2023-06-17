import pandas as pd

if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, location, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    location = location.dropna()
    df = df.dropna()
    df = df[df.RatecodeID != 99.0]

    df = df.drop_duplicates().reset_index(drop=True)

    df = df.merge(location, left_on="PULocationID", right_on="LocationID")
    df.rename(
        columns={
            "Borough": "PU_Borough",
            "Zone": "PU_Zone",
            "service_zone": "PU_service_zone",
        },
        inplace=True,
    )
    df = df.merge(location, left_on="DOLocationID", right_on="LocationID")
    df.rename(
        columns={
            "Borough": "DO_Borough",
            "Zone": "DO_Zone",
            "service_zone": "DO_service_zone",
        },
        inplace=True,
    )

    df = df.drop(["LocationID_x", "LocationID_y"], axis=1)

    df["trip_id"] = df.index

    rate_code_type = {
        1.0: "Standard rate",
        2.0: "JFK",
        3.0: "Newark",
        4.0: "Nassau or Westchester",
        5.0: "Negotiated fare",
        6.0: "Group ride",
    }

    df["rate_code_name"] = df["RatecodeID"].map(rate_code_type)

    payment_type_name = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip",
    }

    df["payment_type_name"] = df["payment_type"].map(payment_type_name)

    df = df[
        [
            "trip_id",
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "payment_type_name",
            "rate_code_name",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "airport_fee",
            "PU_Borough",
            "PU_Zone",
            "DO_Borough",
            "DO_Zone",
        ]
    ]

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, "The output is undefined"
