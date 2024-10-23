import logging

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime


@dag(schedule=None,
     start_date=datetime(2024, 1, 1, tz="UTC"),
     description="Do nothing - used for testing Airflow is running",
     catchup=False)
def noop():
    @task
    def nothing():
        logging.info("Que? I know nothing.")

    nothing()


noop()


@dag(schedule=None,
     start_date=datetime(2024, 1, 1, tz="UTC"),
     description="Trigger another DAG",
     catchup=False)
def trigger_other_dag():
    TriggerDagRunOperator(
        task_id="trigger_other_dag",
        trigger_dag_id="noop",
        logical_date="{{ logical_date.subtract(months=2).start_of('month') }}",
        reset_dag_run=True,
    )


trigger_other_dag()
