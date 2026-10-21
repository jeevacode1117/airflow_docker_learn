from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="first_dag",
    start_date=datetime(2026, 10, 14),
    schedule=None,
    catchup=False
)
def first_dag():

    @task
    def first_task():
        print("Hai! This is my first task.")

    @task
    def second_task():
        print("Hai! This is my second task.")

    @task
    def third_task():
        print("Hai! This is my third task.")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


first_dag()