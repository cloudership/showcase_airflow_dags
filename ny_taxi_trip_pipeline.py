from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime

TRIP_DATA_ROOT = "https://d37ci6vzurychx.cloudfront.net/trip-data"


@dag(schedule=None,
     start_date=datetime(2024, 1, 1, tz="UTC"),
     description=("Fetch all available data for the last 6 months from the NY taxi trip dataset. "
                  "If new data is available, trigger the training DAG"),
     catchup=False)
def ny_yellow_taxi_trip_fetch():
    @task.virtualenv(system_site_packages=True,
                     use_dill=True,
                     requirements=[
                         "aiobotocore",
                         "apache-airflow[amazon]",
                         "apache-airflow-providers-amazon[s3fs]",
                         "requests",
                     ])
    def fetch():
        import io
        import logging

        import pandas
        import pendulum
        import requests

        from airflow.exceptions import AirflowSkipException
        from airflow.io.path import ObjectStoragePath
        from airflow.models import Variable

        # This variable should contain the S3 path to store NYC taxi trip downloads; it should include the connection ID like:
        # "s3://<connection_id>@example-bucket/<path>/"
        bucket_root = ObjectStoragePath(Variable.get("s3_ny_taxi_trip_root"))
        new_data_available = False
        for i in range(6):
            date_to_check = pendulum.now("UTC").start_of("month").subtract(months=i)
            endpoint = f"{TRIP_DATA_ROOT}/yellow_tripdata_{date_to_check.year}-{date_to_check.month:02}.parquet"

            path = bucket_root / f"{date_to_check.year}/{date_to_check.month:02}/yellow.parquet"
            if path.is_file():
                logging.info(f"{path} exists")
            else:
                logging.info(f"{path} missing - attempting fetch of {endpoint}")
                with requests.get(endpoint) as response:
                    if response.status_code == 403:
                        logging.info(f"{endpoint} absent; skipping")
                    else:
                        # If there's some other error then raise an exception, otherwise download the file
                        response.raise_for_status()
                        # Check it
                        pandas.read_parquet(io.BytesIO(response.content))
                        # Store it to S3
                        with path.open("wb") as file:
                            file.write(response.content)
                        new_data_available = True
                        logging.info(f"{path} saved")

        if not new_data_available:
            raise AirflowSkipException("No new data available - not triggering retraining task")

    trigger_retrain = TriggerDagRunOperator(
        task_id="trigger_retrain",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        trigger_dag_id="ny_yellow_taxi_trip_train",
    )

    fetch() >> trigger_retrain


ny_yellow_taxi_trip_fetch()


@dag(schedule=None,
     start_date=datetime(2024, 1, 1, tz="UTC"),
     description=("Using all the available data in the last 6 months, train the taxi trip times prediction model. "
                  "If its loss is less than the previous version of the model, deploy it (TODO). "
                  "If loss is greater, trigger an alert so the model can be checked (TODO)"),
     catchup=False)
def ny_yellow_taxi_trip_train():
    @task
    def train():
        import logging

        logging.info("Training model - TODO")

    train()


ny_yellow_taxi_trip_train()
