from datetime import datetime
from airflow.sdk import dag, task

@dag(
    dag_id="kwargs",
    start_date=datetime(2026, 10, 14),
    schedule=None,
    catchup=False
)
def kwargs_dag():

    @task.python
    def first_task(**kwargs):
        numbers = list(range(1, 10))
        ti = kwargs['ti']
        ti.xcom_push(key='first_task_key', value=numbers)

    @task.python
    def second_task(**kwargs): # even data 
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='first_task', key='first_task_key')
        transform_data = [x for x in data if x % 2 == 0]
        ti.xcom_push(key='second_task_key', value=transform_data)

    @task.python
    def third_task(**kwargs): # odd datà
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='first_task', key='first_task_key')
        transform_data = [x for x in data if x % 2 !=0]
        ti.xcom_push(key='third_task_key', value=transform_data)
    
    @task.python
    def fourth_task(**kwargs):
        ti = kwargs['ti']
        n = ti.xcom_pull(task_ids='first_task', key='first_task_key')
        even = ti.xcom_pull(task_ids='second_task', key='second_task_key')
        odd = ti.xcom_pull(task_ids='third_task', key='third_task_key')
        print(f'Original : {n}')
        print(f'Even : {even}')
        print(f'Odd : {odd}')

    t1 = first_task()
    t2 = second_task()
    t3 = third_task()
    t4 = fourth_task()
    
    t1 >> [t2,t3]  >> t4

kwargs_dag()