from airflow.decorators import dag, task
from pendulum import datetime


@dag(schedule=None,
     start_date=datetime(2024, 1, 1, tz="UTC"),
     description="Train the taxi trip times prediction model using all the available data in the last 6 months.",
     catchup=False)
def ny_yellow_taxi_trip_train():
    @task.virtualenv(system_site_packages=True,
                     use_dill=True,
                     requirements=[
                         "aiobotocore",
                         "apache-airflow[amazon]",
                         "apache-airflow-providers-amazon[s3fs]",
                         "requests",
                     ])
    def train():
        import logging
        import pendulum
        import pandas as pd

        from airflow.io.path import ObjectStoragePath
        from airflow.models import Variable

        now = pendulum.now("UTC")

        # This variable should contain the S3 path to store NYC taxi trip downloads; it should include the connection ID like:
        # "s3://<connection_id>@example-bucket/<path>/"
        bucket_root = ObjectStoragePath(Variable.get("s3_ny_taxi_trip_root"))

        with (bucket_root / "current").open("r") as file:
            training_root = bucket_root / "training" / file.read()

        [logging.info(f) for f in training_root.iterdir() if f.is_file()]

        with (training_root / "train.parquet").open("rb") as file:
            df_train = pd.read_parquet(file)
        with (training_root / "test.parquet").open("rb") as file:
            df_test = pd.read_parquet(file)

    train()


ny_yellow_taxi_trip_train()
