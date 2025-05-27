from airflow.models.baseoperator import chain
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from custom_sensor.redis_key_value_sensor import RedisKeyValueSensor
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
from pendulum import timezone
from date_utils.logical_date_func import xcom_push_actual_execution_date, get_actual_execution_date
from crawling_dag_helpers.gcs_util import create_placeholder_dir
import os

# PostgreSQL connection
# pg_conn = BaseHook.get_connection("NAVER_DB")
logger = LoggingMixin().log

def init_stream_folder(bucket_name, **context):
    actual_execution_date = get_actual_execution_date(context['execution_date'])
    execution_date_in_seoul = actual_execution_date.astimezone(timezone('Asia/Seoul'))
    date_str = execution_date_in_seoul.strftime('%Y-%m-%d')
    create_placeholder_dir(bucket_name, os.path.join(date_str, 'domestic'))
    print('경로를 생성했습니다.', os.path.join(date_str, 'domestic'))
    create_placeholder_dir(bucket_name, os.path.join(date_str, 'international'))
    print('경로를 생성했습니다.', os.path.join(date_str, 'international'))

# DAG definition
with DAG(
    'crawling__dag',
    default_args={
        'owner': 'gunu',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    description='내일 출발하는 항공권 일정부터 크롤링',
    schedule_interval='@daily',
    start_date=datetime(2025, 3, 26, tzinfo=timezone('Asia/Seoul')),
    catchup=False
) as dag:

    kill_prev_crawlers_task = SSHOperator(
        task_id='kill_prev_crawlers',
        ssh_conn_id='host_machine',
        command='cd /home/gunu/ && docker service rm multi_naver_flights || true',
        cmd_timeout=300,
    )

    init_stream_folder_task = PythonOperator(
        task_id='init_stream_folder',
        python_callable=init_stream_folder,
        op_kwargs={'bucket_name': 'flight-data-staging-bucket'},
    )

    init_schedules_to_redis_task = SSHOperator(
        task_id='init_schedules_to_redis',
        ssh_conn_id='host_machine',
        command='python3 /home/gunu/init_schedules_to_redis.py',
        cmd_timeout=60,
    )

    get_actual_execution_date_task = PythonOperator(
        task_id='get_actual_execution_date',
        python_callable=xcom_push_actual_execution_date,
    )

    run_crawler_task = SSHOperator(
        task_id='run_crawler',
        ssh_conn_id='host_machine',
        command='cd /home/gunu/ && docker service create --name multi_naver_flights '
                '--config source=gcp-key,target=/app/gcp-key.json '
                '--config source=env_config,target=/app/.env '
                '-e TOMORROW_FLAG=Y '
                '--network flights_network '
                '--replicas 6 '
                '--constraint node.labels.service_type==crawling '
                '--restart-condition=none '
                '9unu/nalda-crawling:gcs',
        cmd_timeout=300,
    )


    submit_spark_stream_task = SSHOperator(
        task_id="submit_spark_stream",
        ssh_conn_id="spark-master-node-ssh",
        command="""
            CONTAINER_ID=$(docker ps --format "{{ '{{' }}.ID{{ '}}' }} {{ '{{' }}.Names{{ '}}' }}" \
            | grep "spark_spark-master" | awk '{print $1}') && \
            echo "Found container: $CONTAINER_ID" && \
            docker exec -i $CONTAINER_ID /opt/spark/bin/spark-submit \
            --master spark://34.64.144.154:7077 \
            --deploy-mode client \
            --name international_flight_stream \
            --conf spark.executorEnv.LOCAL_FLAG=N \
            --conf spark.executorEnv.DB_HOST={{ conn.NAVER_DB.host }} \
            --conf spark.executorEnv.DB_USER={{ conn.NAVER_DB.login }} \
            --conf spark.executorEnv.DB_PASSWORD={{ conn.NAVER_DB.password }} \
            --conf spark.executorEnv.DB_NAME={{ conn.NAVER_DB.schema }} \
            --py-files /opt/spark/scripts/spark_json_parser.zip \
            /opt/spark/scripts/spark_json_parser/stream_processor_international_with_stop.py \
            --LOCAL_FLAG N \
            --DB_HOST {{ conn.NAVER_DB.host }} \
            --DB_USER {{ conn.NAVER_DB.login }} \
            --DB_PASSWORD {{ conn.NAVER_DB.password }} \
            --DB_NAME {{ conn.NAVER_DB.schema }} \
            --bucket flight-data-staging-bucket \
            --folder "{{ ti.xcom_pull(task_ids='get_actual_execution_date', key='actual_execution_date_str') }}/international" \
            --checkpoint-dir "nald-spark-streaming-checkpoint-bucket/flight-data-streaming/international/{{ ti.xcom_pull(task_ids='get_actual_execution_date', key='actual_execution_date_str') }}" \
            --max-files 60 \
            --processing-interval "1 minute" \
            --timeout 30
        """,
        cmd_timeout=82800,
    )

    wait_for_crawling_completion_task = RedisKeyValueSensor(
        task_id='wait_for_crawling_completion',
        redis_conn_id='host_machine_redis_server',
        key='crawling:status',
        expected_value='completed',
        poke_interval=60,
        timeout=timedelta(hours=23),
        mode='poke',
        soft_fail=True
    )

    trigger_update_crawled_flight_data_dag_task = TriggerDagRunOperator(
        task_id='trigger_update_crawled_flight_data_dag',
        trigger_dag_id='update_crawled_flight_data_dag',
        wait_for_completion=False,
        reset_dag_run=False,
        conf={
            'parent_dag': 'crawling_remaining_schedules_dag',
            'actual_execution_date': '{{ ti.xcom_pull(task_ids="get_actual_execution_date", key="actual_execution_date") }}'
        }
    )

    chain(
        [kill_prev_crawlers_task, init_stream_folder_task, get_actual_execution_date_task],
        init_schedules_to_redis_task,
        [run_crawler_task, submit_spark_stream_task],
        wait_for_crawling_completion_task,
        trigger_update_crawled_flight_data_dag_task
    )
