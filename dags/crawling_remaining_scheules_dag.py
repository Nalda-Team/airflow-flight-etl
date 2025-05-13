from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from custom_sensor.redis_key_value_sensor import RedisKeyValueSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pendulum import timezone
from date_utils.logical_date_func import get_parent_execution_date_and_push_again
from airflow.operators.python import PythonOperator

# 기본 인자 설정
default_args = {
    'owner': 'gunu',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# DAG 정의 (다이어그램과 일치하도록 수정)
with DAG(
    'crawling_remaining_schedules_dag',
    default_args=default_args,
    description='남은 항공권 일정들 수집 (내일 모레 ~ 300일 후)',
    schedule_interval=None,  # 트리거에 의해 실행되므로 None으로 설정
    start_date=datetime(2025, 3, 26, tzinfo=timezone('Asia/Seoul')),
    catchup=False,
) as dag:

    kill_prev_crawlers = SSHOperator(
        task_id='kill_prev_crawlers',
        ssh_conn_id='host_machine',
        command='cd /home/gunu/ && docker service rm multi_naver_flights || true',
        cmd_timeout=300,
    )
    # Task 1: Redis 초기화 Python 스크립트 실행 (다이어그램과 일치)
    redis_init_remaining_schedules = SSHOperator(
        task_id='redis_init_remaining_schedules',
        ssh_conn_id='host_machine',
        command='python3 /home/gunu/redis_init_remaining_schedules.py',
        cmd_timeout=60,
    )
    # Task2 : 전달받은 크롤러 실행날짜 다시 xcom에 저장
    get_actual_execution_date = PythonOperator(
        task_id='get_actual_execution_date',
        python_callable=get_parent_execution_date_and_push_again,
    )
    
    # Task 2: Docker 실행 Bash 스크립트 실행
    run_crawler_second = SSHOperator(
        task_id='run_crawler_second',
        ssh_conn_id='host_machine',
        command='cd /home/gunu/ && docker service create --name multi_naver_flights \
                            --config source=gcp_key,target=/app/gcp_key.json \
                            --config source=env_config,target=/app/.env \
                            -e TOMORROW_FLAG=N\
                            --network flights_network \
                            --replicas 6 \
                            --constraint node.labels.service_type==crawling \
                            --restart-condition=none \
                            vimcat23/naver_flights:gcs',
        cmd_timeout=300,
    )
    
    # Task 3: Redis 상태 확인 (크롤링 작업 완료 대기) - task_id 수정
    wait_for_second_crawling_completion = RedisKeyValueSensor(
        task_id='wait_for_second_crawling_completion',
        redis_conn_id='host_machine_redis_server',
        key='crawling:status',
        expected_value='completed',
        poke_interval=120,  # 120초마다 확인
        timeout=timedelta(hours=23),
        mode='poke'
    )
    
    # # Task 4: flight_data_finalization_dag 트리거
    trigger_update_crawled_flight_data_dag = TriggerDagRunOperator(
        task_id='trigger_update_crawled_flight_data_dag',
        trigger_dag_id='update_crawled_flight_data_dag',
        wait_for_completion=False,
        reset_dag_run=False,
        conf={'parent_dag': 'crawling_remaining_schedules_dag',
            'actual_execution_date': '{{ ti.xcom_pull(task_ids="get_actual_execution_date", key="actual_execution_date") }}'}
    )
    # 작업 순서 정의 
    kill_prev_crawlers >> redis_init_remaining_schedules >> get_actual_execution_date >> run_crawler_second >> wait_for_second_crawling_completion >> trigger_update_crawled_flight_data_dag