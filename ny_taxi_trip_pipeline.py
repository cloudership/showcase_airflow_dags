from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime


@dag(schedule=None,
     start_date=datetime(2024, 1, 1, tz="UTC"),
     description=("Fetch all available data for the last 6 months from the NY taxi trip dataset. "
                  "If new data is available, trigger the training DAG"),
     catchup=False)
def ny_yellow_taxi_trip_fetch():
    @task.branch_virtualenv(system_site_packages=True,
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

        from airflow.io.path import ObjectStoragePath
        from airflow.models import Variable

        trip_data_root = "https://d37ci6vzurychx.cloudfront.net/trip-data"

        # This variable should contain the S3 path to store NYC taxi trip downloads; it should include the connection ID like:
        # "s3://<connection_id>@example-bucket/<path>/"
        bucket_root = ObjectStoragePath(Variable.get("s3_ny_taxi_trip_root"))
        new_data_available = False
        now = pendulum.now("UTC")
        for i in range(6):
            date_to_check = now.start_of("month").subtract(months=i)
            endpoint = f"{trip_data_root}/yellow_tripdata_{date_to_check.year}-{date_to_check.month:02}.parquet"

            path = bucket_root / f"raw/{date_to_check.year}/{date_to_check.month:02}/yellow.parquet"
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
            logging.info("No new data available - not triggering rest of training pipeline")
            return "skip_retrain"
        else:
            logging.info("New data is available! Triggering rest of training pipeline")
            return "trigger_retrain"

    trigger_retrain = TriggerDagRunOperator(
        task_id="trigger_retrain",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        trigger_dag_id="ny_yellow_taxi_trip_prepare",
    )

    skip_retrain = EmptyOperator(task_id="skip_retrain")

    fetch() >> [skip_retrain, trigger_retrain]


ny_yellow_taxi_trip_fetch()


@dag(schedule=None,
     start_date=datetime(2024, 1, 1, tz="UTC"),
     catchup=False)
def ny_yellow_taxi_trip_prepare():
    @task.virtualenv(system_site_packages=True,
                     use_dill=True,
                     requirements=[
                         "aiobotocore",
                         "apache-airflow[amazon]",
                         "apache-airflow-providers-amazon[s3fs]",
                         "requests",
                     ])
    def prepare():
        """
        Take all available data from the last 6 months and create training and test datasets.
        Store by timestamp so they can be sorted.
        """
        import logging

        import pandas as pd
        import pendulum

        from airflow.io.path import ObjectStoragePath
        from airflow.models import Variable

        # This variable should contain the S3 path to store NYC taxi trip downloads; it should include the connection ID like:
        # "s3://<connection_id>@example-bucket/<path>/"
        bucket_root = ObjectStoragePath(Variable.get("s3_ny_taxi_trip_root"))
        now = pendulum.now("UTC")
        df_train_list = []
        df_test = None
        for i in range(6):
            date_to_check = now.start_of("month").subtract(months=i)
            path = bucket_root / f"raw/{date_to_check.year}/{date_to_check.month:02}/yellow.parquet"
            if path.is_file():
                with path.open("rb") as file:
                    if df_test is None:
                        logging.info(f"Test set: {path}")
                        df_test = pd.read_parquet(file)
                    else:
                        logging.info(f"Training subset: {path}")
                        df_train_list.append(pd.read_parquet(file))

        if not df_train_list:
            raise "Training data not set"

        if df_test.empty:
            raise "Test data not set"

        df_train = pd.concat(df_train_list)

        training_root = bucket_root.joinpath(f"training/{now.isoformat()}")

        train_path = training_root / "train.parquet"
        test_path = training_root / "test.parquet"
        with train_path.open("wb") as file:
            logging.info(f"Writing training dataset to {train_path}")
            df_train.to_parquet(file)
        with test_path.open("wb") as file:
            logging.info(f"Writing test dataset to {test_path}")
            df_test.to_parquet(file)
        with (bucket_root / "current").open("w") as file:
            logging.info(f"Storing current training path as {training_root.name}")
            file.write(str(training_root.name))

    trigger_train = TriggerDagRunOperator(
        task_id="trigger_train",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        trigger_dag_id="ny_yellow_taxi_trip_train",
    )

    prepare() >> trigger_train


ny_yellow_taxi_trip_prepare()


@dag(schedule=None,
     start_date=datetime(2024, 1, 1, tz="UTC"),
     description=("Using all the available data in the last 6 months, train the taxi trip times prediction model. "
                  "If its loss is less than the previous version of the model, deploy it (TODO). "
                  "If loss is greater, trigger an alert so the model can be checked (TODO)"),
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
