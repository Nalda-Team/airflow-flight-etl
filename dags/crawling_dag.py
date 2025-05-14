from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from custom_sensor.redis_key_value_sensor import RedisKeyValueSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pendulum import timezone
from date_utils.logical_date_func import xcom_push_actual_execution_date, get_actual_execution_date
from airflow.operators.python import PythonOperator
from crawling_dag_helpers.gcs_util import create_placeholder_dir
import os
from airflow.utils.log.logging_mixin import LoggingMixin
logger = LoggingMixin().log
# 기본 인자 설정
default_args = {
    'owner': 'gunu',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def init_stream_folder(bucket_name, **context):
    actual_execution_date = get_actual_execution_date(context['execution_date'])
    execution_date_in_seoul = actual_execution_date.astimezone(timezone('Asia/Seoul'))
    date_str = execution_date_in_seoul.strftime('%Y-%m-%d')
    domestic_dir=os.path.join(date_str, 'domestic')
    international_dir=os.path.join(date_str, 'international')
    create_placeholder_dir(bucket_name, domestic_dir)
    create_placeholder_dir(bucket_name, international_dir)
    logger.info(f'{domestic_dir} 경로를 생성했습니다.')
    logger.info(f'{international_dir} 경로를 생성했습니다.')

    
# DAG 정의
with DAG(
    'crawling__dag',
    default_args=default_args,
    description='내일 출발하는 항공권 일정부터 크롤링',
    schedule_interval='@daily',
    start_date=datetime(2025, 3, 26, tzinfo=timezone('Asia/Seoul')),
    catchup=False,
) as dag:
    
    # Task 1-1: 이전에 실행했던 크롤링 서비스 제거
    kill_prev_crawlers_task = SSHOperator(
        task_id='kill_prev_crawlers',
        ssh_conn_id='host_machine',
        command='cd /home/gunu/ && docker service rm multi_naver_flights || true',
        cmd_timeout=300,
    )

    # Task 1-2: 스파크 스트리밍 시 추적할 폴더 생성 (이곳에 항공권 데이터가 쌓일 예정)
    init_stream_folder_task = PythonOperator(
        task_id='init_stream_folder',
        python_callable=init_stream_folder,
        op_kwargs={'bucket_name': 'fetched_flight_data_bucket'},
    )

    # Task 2: Redis 초기화 Python 스크립트 실행
    init_schedules_to_redis_task = SSHOperator(
        task_id='init_schedules_to_redis',
        ssh_conn_id='host_machine',
        command='python3 /home/gunu/init_schedules_to_redis.py',
        cmd_timeout=60,
    )

    # Task3 : 크롤러 실행날짜 xcom 변수화 ('actual_execution_date')
    get_actual_execution_date_task = PythonOperator(
        task_id='get_actual_execution_date',
        python_callable=xcom_push_actual_execution_date,
    )

    # Task 4-1: Docker 실행 Bash 스크립트 실행
    run_crawler_task = SSHOperator(
        task_id='run_crawler',
        ssh_conn_id='host_machine',
        command='cd /home/gunu/ && docker service create --name multi_naver_flights \
                        --config source=gcp_key,target=/app/gcp_key.json \
                        --config source=env_config,target=/app/.env \
                        -e TOMORROW_FLAG=Y \
                        --network flights_network \
                        --replicas 6 \
                        --constraint node.labels.service_type==crawling \
                        --restart-condition=none \
                        vimcat23/naver_flights:gcs',
        cmd_timeout=300,
    )

    # Task 4-2 : 스파크 스트리밍 작업 시작
    submit_spark_stream_job_task =  EmptyOperator(task_id='submit_spark_stream_job')
    
    # Task 5: Redis 상태 확인 (크롤링 작업 완료 대기)
    wait_for_crawling_completion_task = RedisKeyValueSensor(
        task_id='wait_for_crawling_completion',
        redis_conn_id='host_machine_redis_server',  # Airflow UI에 설정된 Redis 연결 ID
        key='crawling:status',
        expected_value='completed',
        poke_interval=60,  # 60초마다 확인
        timeout=timedelta(hours=23),      # 12시간 타임아웃
        mode='poke',       # 계속해서 확인
        soft_fail=True
    )

    # Task 6: crawling_remaining_schedule_dag 트리거
    trigger_update_crawled_flight_data_dag_task = TriggerDagRunOperator(
        task_id='trigger_update_crawled_flight_data_dag',
        trigger_dag_id='update_crawled_flight_data_dag',
        wait_for_completion=False,
        reset_dag_run=False,
        conf={'parent_dag': 'crawling_remaining_schedules_dag',
            'actual_execution_date': '{{ ti.xcom_pull(task_ids="get_actual_execution_date", key="actual_execution_date") }}'}
    )

    
    [kill_prev_crawlers_task, init_stream_folder_task, get_actual_execution_date_task] \
    >> init_schedules_to_redis_task \
    >> [run_crawler_task, submit_spark_stream_job_task] \
    >> wait_for_crawling_completion_task >> trigger_update_crawled_flight_data_dag_task