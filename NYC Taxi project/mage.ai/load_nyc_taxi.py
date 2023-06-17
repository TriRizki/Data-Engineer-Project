import io
import pandas as pd
import requests
from fastparquet import ParquetFile

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = "https://storage.googleapis.com/nyc-taxi-project/yellow_tripdata_2023-01.parquet"
    response = requests.get(url)

    pf = ParquetFile(io.BytesIO(response.content))
    return pf.to_pandas()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, "The output is undefined"
