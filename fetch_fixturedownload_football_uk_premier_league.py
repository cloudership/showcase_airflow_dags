import logging
from datetime import datetime, UTC

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from airflow.models import Variable


@dag(schedule="@daily",
     start_date=datetime(2024, 1, 1, tzinfo=UTC),
     description="Download UK Premier League football fixtures data",
     catchup=False)
def fetch_fixturedownload_football_uk_premier_league():
    @task
    def fetch(logical_date: pendulum.DateTime) -> ObjectStoragePath:
        endpoint = f"https://fixturedownload.com/feed/json/epl-{logical_date.year}"
        # This variable should contain the S3 path to store NYC taxi trip downloads; it should include the connection ID like:
        # "s3://<connection_id>@example-bucket/<path>/"
        bucket_root = ObjectStoragePath(Variable.get("s3_donotdrivenow_root"))
        path = bucket_root / f"{logical_date.year}/football/uk/premier_league-{logical_date.to_iso8601_string()}.json"
        with requests.get(endpoint) as response:
            response.raise_for_status()
            with path.open("wb") as file:
                file.write(response.content)
        return path

    @task
    def transform(path: ObjectStoragePath):
        logging.info(path.read_text(encoding="utf-8"))

    transform(fetch())


fetch_fixturedownload_football_uk_premier_league()
