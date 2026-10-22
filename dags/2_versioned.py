from datetime import datetime
from airflow.sdk import dag, task

@dag(
    dag_id="versioned_dag",
    start_date=datetime(2026, 10, 14),
    schedule=None,
    catchup=False
)
def versioned_dag():
    @task.python
    def version():
        print('This is first Version')
    
    @task.python
    def version_2():
        print('This is Second version')
    
    @task.python
    def first_task():
        print("Hai! This is my first task.")

    @task.python
    def second_task():
        print("Hai! This is my second task.")

    @task.python
    def third_task():
        print("Hai! This is my third task.")

    zero = version()
    zero_1 = version_2()
    first = first_task()
    second = second_task()
    third = third_task()

    zero >> zero_1 >> first >> second >> third
    
versioned_dag()