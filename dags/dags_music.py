from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from common import genie_rank
import pendulum
from datetime import datetime


def upload_to_s3(file_name_prefix, key_prefix, bucket_name):
    df = genie_rank()
    current_date_str = datetime.now().strftime('%Y%m%d')
    filename = f'{file_name_prefix}_{current_date_str}.csv'
    key = f'{key_prefix}/{filename}'
    hook = S3Hook(aws_conn_id='aws_conn')  # Specify aws_conn_id for S3Hook
    hook.load_string(df.to_csv(index=False), key, bucket_name, replace=True)


with DAG(
    dag_id='dags_music',
    schedule_interval="0 9 * * *",  # Corrected schedule_interval
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    upload_task = PythonOperator(
        task_id='upload_task',
        python_callable=upload_to_s3,
        op_args=['genie_music', 'rank', 'daily-music-rank'],
    )
    upload_done_email = EmailOperator(
        task_id='upload_done_email',
        to='hongkyu08@gmail.com',
        subject='S3 데이터 적재가 완료되었습니다.',
        html_content='성공',
    )

upload_task >> upload_done_email
