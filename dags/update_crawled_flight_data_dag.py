from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pendulum import timezone
from update_crawled_flight_data_dag_helpers.migrate_functions import run_migration, run_delete_migrate_records, run_aggregate
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
    'update_crawled_flight_data_dag',
    default_args=default_args,
    description='서비스 쿼리용 레디스 서버에 오늘자 항공권 데이터 추가 -> fare_info 테이블 통합 + temp_fare_info 테이블 비우기',
    schedule_interval=None,  # 트리거에 의해 실행되므로 None으로 설정
    start_date=datetime(2025, 3, 26, tzinfo=timezone('Asia/Seoul')),
    catchup=False,
) as dag:
    
    # 오늘 수집한 데이터로 노선별 시간대별 항공권 정보 집계 테이블 연산
    aggregate_temp_fare_info_to_agg_flight_info=PythonOperator(
        task_id='aggregate_temp_fare_info_to_agg_flight_info',
        python_callable=run_aggregate
    )

    # 오늘 수집한 데이터 전체 팩트 테이블로 마이그레이션 (삽입 속도 최적화를 위해 인조키로 temp_fare_info에 삽입 후 팩트 테이블로 마이그레이션함)
    migrate_temp_fare_info_to_origin_fare_info = PythonOperator(
        task_id='migrate_temp_fare_info_to_origin_fare_info',
        python_callable=run_migration
    )

    # 마이그레이션 한 뒤 temp_fare_info에서 delete
    delete_migrated_records = PythonOperator(
        task_id='delete_migrated_records',
        python_callable=run_delete_migrate_records
    )

    # temp_fare_info 중복값 제거-> 중간 집계 테이블 연산 -> 전체 fare_info테이블로 마이그레이션 -> 마이그레이션 후 temp_fare_info에서 delete
    aggregate_temp_fare_info_to_agg_flight_info>>migrate_temp_fare_info_to_origin_fare_info >> delete_migrated_records