import pendulum
from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath


@dag(schedule=None,
     start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
     description="Download UK Premier League football fixtures data",
     catchup=False)
def fetch_fixturedownload_football_uk_premier_league():
    @task.virtualenv(system_site_packages=True,
                     use_dill=True,
                     requirements=[
                         "aiobotocore",
                         "apache-airflow[amazon]",
                         "apache-airflow-providers-amazon[s3fs]",
                         "requests",
                     ])
    def fetch(logical_date: pendulum.DateTime) -> ObjectStoragePath:
        import logging
        import requests
        from airflow.io.path import ObjectStoragePath
        from airflow.models import Variable

        endpoint = f"https://fixturedownload.com/feed/json/epl-{logical_date.year}"
        # This variable should contain the S3 path to store NYC taxi trip downloads; it should include the connection ID like:
        # "s3://<connection_id>@example-bucket/<path>/"
        bucket_root = ObjectStoragePath(Variable.get("s3_donotdrivenow_root"))
        path = bucket_root / f"{logical_date.year}/football/uk/premier_league-{logical_date.to_iso8601_string()}.json"
        logging.info(f"Fetching {endpoint}")
        with requests.get(endpoint) as response:
            response.raise_for_status()
            logging.info(f"Writing {endpoint} to {path}")
            with path.open("wb") as file:
                file.write(response.content)
        return path

    @task.virtualenv(system_site_packages=True,
                     use_dill=True,
                     requirements=[
                         "aiobotocore",
                         "apache-airflow[amazon]",
                         "apache-airflow-providers-amazon[s3fs]",
                         "requests",
                     ])
    def transform(path: ObjectStoragePath):
        import logging

        logging.info(path.read_text(encoding="utf-8"))

    transform(fetch())


fetch_fixturedownload_football_uk_premier_league()
