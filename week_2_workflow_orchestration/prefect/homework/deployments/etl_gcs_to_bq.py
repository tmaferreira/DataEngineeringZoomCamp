from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(taxi_color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{taxi_color}/{taxi_color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-connector")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")

    return Path(f"../data/{gcs_path}")


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""
    gcp_credentials_block = GcpCredentials.load("gcs-connector-creds")

    df.to_gbq(
        destination_table="dezoomcamp_dataset.yellow_taxi_trips",
        project_id="dezoomcamp-375623",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int, taxi_color: str) -> None:
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(taxi_color, year, month)

    # Read parquet file and load to a Dataframe
    df = pd.read_parquet(path)
    print(f"Total number of rows: {df.shape[0]} (month {month})")
    write_bq(df)

@flow()
def etl_parent_flow(
    months: list[int] = [2, 3], year: int = 2019, taxi_color: str = "yellow"
):
    for month in months:
        etl_gcs_to_bq(year, month, taxi_color)

if __name__ == "__main__":
    taxi_color = "yellow"
    months = [2, 3]
    year = 2019
    etl_parent_flow(months, year, taxi_color)
