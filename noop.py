import logging

from airflow.decorators import dag, task
from pendulum import datetime


@dag(schedule="@once",
     start_date=datetime(2024, 1, 1, tz="UTC"),
     description="Do nothing - used for testing Airflow is running",
     catchup=False)
def noop():
    @task
    def nothing():
        logging.info("Que? I know nothing.")

    nothing()


noop()
