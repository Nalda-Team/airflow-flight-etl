from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import timezone
from datetime import datetime, timedelta
import os
from expired_flights_etl_dag_helpers.extract_gone_data import extract_expired_flights, extract_layover_infos, delete_extracted_flights
from expired_flights_etl_dag_helpers.data_formatter import FlightDataFormatter
from expired_flights_etl_dag_helpers.upload_to_bucket import (
    dump_in_json, 
    upload_json_to_gcp, 
    remove_uploaded_file
)
from expired_flights_etl_dag_helpers.utils import  get_airport_lists
from database_helpers.utils import get_db_conn
from date_utils.logical_date_func import get_actual_execution_date

from airflow.utils.log.logging_mixin import LoggingMixin
logger = LoggingMixin().log
conn, engine=get_db_conn()
# 상수 정의
BUCKET_NAME = 'nalda_expired_flight_bucket'
DATA_DIR = '/opt/airflow/data'
TIMEZONE = 'Asia/Seoul'


def process_single_route(execution_date_in_seoul, depart_airport, arrival_airport, formatter:FlightDataFormatter):
    """단일 경로(출발지->도착지)에 대한 데이터 처리"""
    # 데이터 추출 (직항, 경유 포함)
    df = extract_expired_flights(execution_date_in_seoul, depart_airport, arrival_airport, engine)
    if len(df) == 0:
        logger.warning(f'{execution_date_in_seoul}에 {depart_airport}에서 {arrival_airport}로 가는 항공권이 없습니다!!')
        return False
    
    # 데이터 처리
    groups, max_fetched_date = formatter.process_dataframe(df)
    scenarios = formatter.get_scenarios(groups, max_fetched_date)
    
    
    # 경유 항공편 정보만 따로 추가 해주기
    layover_flight_ids = df[df['is_layover'] == True]['air_id'].tolist()
    if len(layover_flight_ids)!=0:
        layover_ids_str = ','.join(f"'{id}'" for id in layover_flight_ids)
        layover_df=extract_layover_infos(layover_ids_str, engine)
        scenarios["layover_info"] = formatter.formatting_layover_infos(layover_df)
    else:
        scenarios["layover_info"]={}

    # 파일 경로 설정
    file_name = f'{execution_date_in_seoul.strftime("%Y-%m-%d")}.json'
    upload_dir = f'{depart_airport}_{arrival_airport}'
    upload_path = os.path.join(upload_dir, file_name)
    save_path = os.path.join(DATA_DIR, upload_path)
    
    # 파일 저장 및 업로드
    dump_in_json(scenarios, save_path)
    upload_json_to_gcp(BUCKET_NAME, source_file_path=save_path, destination_blob=upload_path)
    remove_uploaded_file(save_path)
    
    logger.info(f'{upload_path}에 저장되었습니다')

    return True


def expired_flights_etl(**context):
    """만료된 항공권 ETL 메인 함수"""
    # 실행 날짜 설정
    execution_date = context['logical_date']
    execution_date_in_seoul = execution_date.astimezone(timezone(TIMEZONE))
    
    logger.info(f'실행 날짜 (UTC) : {execution_date}')
    logger.info(f'실행 날짜 (Asia/Seoul) : {execution_date_in_seoul}')
    
    # 공항 리스트 가져오기
    depart_airports, arrival_airports = get_airport_lists()
    logger.info('항공권 조합 지역 : 대한민국 (ICN, CMP, CJU), 일본, 동남아, 중국, 유럽, 미주, 대양주, 남미, 중동')
    
    # 데이터 포맷터 초기화
    formatter = FlightDataFormatter()
    
    # 모든 경로 조합 처리
    for depart_airport in depart_airports:
        for arrival_airport in arrival_airports:
            # 출발 -> 도착 방향 처리
            process_single_route(execution_date_in_seoul, depart_airport, arrival_airport, formatter)
            
            # 도착 -> 출발 방향 처리 (왕복)
            process_single_route(execution_date_in_seoul, arrival_airport, depart_airport, formatter)
    logger.info('데이터 추출 및 업로드 완료')
    return True

def delete_expired_flights_from_db(**context):
    execution_date = get_actual_execution_date(context['logical_date'])
    execution_date_in_seoul = execution_date.astimezone(timezone(TIMEZONE))
    logger.info('데이터 마이그레이션이 끝난 항공권들을 DB에서 제거합니다!')
    deleted_info=delete_extracted_flights(execution_date_in_seoul, conn)
    logger.info(f"{execution_date_in_seoul}에 출발한 항공편 {deleted_info['flight_info']}개 레코드가 삭제되었습니다.")

# DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['vimcat23@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'expired_flights_etl_dag',
    default_args=default_args,
    description='출발일이 지난 항공권들 추출 -> 클라우드 스토리지로 마이그레이션(json)',
    schedule='@daily',
    start_date=datetime(2024, 12, 31, tzinfo=timezone(TIMEZONE)),
    catchup=True,
    max_active_runs=1,  # 동시 실행 가능한 DAG 최대 개수 지정
) as dag:
    extract_expired_flights_and_upload_to_GCS = PythonOperator(
        task_id='extract_expired_flights_and_upload_to_GCS',
        python_callable=expired_flights_etl,
    )
    
    DELETE_expired_flights_from_db = PythonOperator(
        task_id='DELETE_expired_flights_from_db',
        python_callable=delete_expired_flights_from_db,
    )

    extract_expired_flights_and_upload_to_GCS >> DELETE_expired_flights_from_db