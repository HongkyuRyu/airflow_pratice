from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id='dags_python_operator',
    schedule="30 6 * * *",  # 06시 30분 daily
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # Python은 함수를 정의해야한다.
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0, 3)
        print(fruit[rand_int])
    # task 작성
    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=select_fruit  # 어떤 함수를 실행할 것인가?
    )

    py_t1
