from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from custom_sensor.redis_key_value_sensor import RedisKeyValueSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pendulum import timezone
from date_utils.logical_date_func import xcom_push_actual_execution_date
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

# DAG 정의
with DAG(
    'crawling_only_tomorrow_dag',
    default_args=default_args,
    description='내일 출발하는 항공권 일정부터 크롤링',
    schedule_interval='@daily',
    start_date=datetime(2025, 3, 26, tzinfo=timezone('Asia/Seoul')),
    catchup=False,
) as dag:
    
    kill_prev_crawlers = SSHOperator(
        task_id='kill_prev_crawlers',
        ssh_conn_id='host_machine',
        command='cd /home/gunu/ && docker service rm multi_naver_flights || true',
        cmd_timeout=300,
    )

    # Task 1: Redis 초기화 Python 스크립트 실행
    redis_init_only_tomorrow = SSHOperator(
        task_id='redis_init_only_tomorrow',
        ssh_conn_id='host_machine',
        command='python3 /home/gunu/redis_init_only_tomorrow.py',
        cmd_timeout=60,
    )

    # Task2 : 크롤러 실행날짜 xcom 변수화 ('actual_execution_date')
    get_actual_execution_date = PythonOperator(
        task_id='get_actual_execution_date',
        python_callable=xcom_push_actual_execution_date,
    )

    # Task 2: Docker 실행 Bash 스크립트 실행
    run_crawler_first = SSHOperator(
        task_id='run_crawler_first',
        ssh_conn_id='host_machine',
        command='cd /home/gunu/ && docker service create --name multi_naver_flights \
                        --config source=gcp_key,target=/app/gcp_key.json \
                        --config source=env_config,target=/app/.env \
                        -e TOMORROW_FLAG=Y \
                        --network flights_network \
                        --replicas 6 \
                        --constraint node.labels.service_type==crawling \
                        --restart-condition=none \
                        vimcat23/naver_flights:direct',
        cmd_timeout=300,
    )
    
    # Task 3: Redis 상태 확인 (크롤링 작업 완료 대기)
    wait_for_first_crawling_completion = RedisKeyValueSensor(
        task_id='wait_for_first_crawling_completion',
        redis_conn_id='host_machine_redis_server',  # Airflow UI에 설정된 Redis 연결 ID
        key='crawling:status',
        expected_value='completed',
        poke_interval=60,  # 60초마다 확인
        timeout=timedelta(hours=12),      # 12시간 타임아웃
        mode='poke',       # 계속해서 확인
        soft_fail=True
    )
    
    # Task 4: ML_ETL_dag 트리거 (아직 구현 전이라 더미로 사용)
    trigger_ML_ETL_dag = EmptyOperator(
        task_id='trigger_ML_ETL_dag',
        # conf={
        #     'parent_dag': 'crawling_only_tomorrow_dag',
        #     'actual_execution_date': '{{ ti.xcom_pull(task_ids="get_actual_execution_date", key="actual_execution_date") }}'}
    )

    # Task 5: crawling_remaining_schedule_dag 트리거
    trigger_crawling_remaining_schedules_dag = TriggerDagRunOperator(
        task_id='trigger_crawling_remaining_schedules_dag',
        trigger_dag_id='crawling_remaining_schedules_dag',
        wait_for_completion=False,
        reset_dag_run=True,
        conf={
            'parent_dag': 'crawling_only_tomorrow_dag',
            'actual_execution_date': '{{ ti.xcom_pull(task_ids="get_actual_execution_date", key="actual_execution_date") }}'}
    )

    
    kill_prev_crawlers>>get_actual_execution_date >> redis_init_only_tomorrow >> run_crawler_first >> wait_for_first_crawling_completion >> [trigger_ML_ETL_dag, trigger_crawling_remaining_schedules_dag]