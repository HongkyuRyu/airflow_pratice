from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    # airflow화면에서 보이는 dag이름 (일반적으로 python파일명과 일치시킨다.)
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",  # cron 스케줄 (매일마다 0시 0분)
    start_date=pendulum.datetime(
        2023, 11, 1, tz="Asia/Seoul"),  # 해당 DAG이 언제부터 돌건지?
    catchup=False,  # 현재가 11월 26일이니까, catchup=True면, 누락된 구간까지 모두 돌린다는 뜻
    # 그런데, 일괄적으로 한번에 돈다.
)as dag:
    # task 객체명
    bash_t1 = BashOperator(
        task_id="bash_t1",  # task객체명과 task_id일치시킨다.
        bash_command="echo whoami",
    )
    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    # task 수행 순서결정

    bash_t1 >> bash_t2
