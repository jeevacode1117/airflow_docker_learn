from datetime import datetime
from airflow.sdk import dag, task

@dag(
    dag_id="operators",
    start_date=datetime(2026, 10, 14),
    schedule=None,
    catchup=False
)
def operators():

    @task.python
    def first_task():
       numbers = list(range(1,10))
       return numbers
   
    @task.python
    def second_task(data):
        transform_data = data
        transform_data = [x for x in transform_data if x % 2 == 0]
        return transform_data
    
    @task.python
    def third_task(data):
        print("data")
    
    numbers = first_task()
    transform_data = second_task(numbers)
    third_task(transform_data)
    
operators()