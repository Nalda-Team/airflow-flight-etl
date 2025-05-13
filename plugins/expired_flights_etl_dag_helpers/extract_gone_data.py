from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook  # 수정된 import
from datetime import datetime,timedelta
import pandas as pd
import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas.io.sql")
from airflow.utils.log.logging_mixin import LoggingMixin
logger = LoggingMixin().log

def extract_expired_flights(target_timestamp, depart_airport, arrival_airport, engine):
    start_point = target_timestamp
    end_point = start_point+timedelta(days=1)

    query = f"""
            WITH filtered_flight_info AS (
                SELECT
                    air_id,
                    is_layover,
                    depart_airport,
                    arrival_airport,
                    airline,
                    depart_timestamp,
                    arrival_timestamp,
                    journey_time
                FROM 
                    flight_info
                WHERE 
                    (depart_timestamp >= '{start_point}' AND depart_timestamp < '{end_point}')
                    AND (depart_airport='{depart_airport}' AND arrival_airport='{arrival_airport}')
            )
            SELECT
                fi.air_id,
                fi.is_layover,
                dep.airport_code,
                dep.name AS departure_airport,
                dep.country AS departure_country,    
                arr.airport_code AS airport_code_1,
                arr.name AS arrival_airport,
                arr.country AS arrival_country,
                fi.airline,
                timezone('UTC', fi.depart_timestamp) AS depart_timestamp,
                timezone('UTC', fi.arrival_timestamp) AS arrival_timestamp,
                timezone(dep.time_zone, fi.depart_timestamp) AS depart_time_in_dep,
                timezone(dep.time_zone, fi.arrival_timestamp) AS arrival_time_in_dep,
                timezone(arr.time_zone, fi.depart_timestamp) AS depart_time_in_arr,
                timezone(arr.time_zone, fi.arrival_timestamp) AS arrival_time_in_arr,
                fare.seat_class AS seat_class,
                fare.agt_code,
                fare.adult_fare,
                fare.fare_class,
                fi.journey_time,
                fare.fetched_date
            FROM 
                filtered_flight_info fi
            JOIN 
                airport_info AS dep ON fi.depart_airport = dep.airport_code
            JOIN 
                airport_info AS arr ON fi.arrival_airport = arr.airport_code
            JOIN 
                fare_info fare ON fi.air_id = fare.air_id;
    """
   
    try:
       df = pd.read_sql(query, engine)
       return df
           
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

def extract_layover_infos(air_ids, engine):
    query = f"""
            SELECT 
                l.air_id,
                l.segment_id,
                l.layover_order,
                l.connect_time,
                s.airline AS segment_airline,
                s.depart_airport AS segment_depart,
                s.arrival_airport AS segment_arrival,
                s.depart_timestamp AS segment_depart_time_utc,
                s.arrival_timestamp AS segment_arrival_time_utc,
                
                -- 출발 공항 시간대 기준 시간
                timezone(dep.time_zone, s.depart_timestamp) AS segment_depart_time_in_dep,
                timezone(dep.time_zone, s.arrival_timestamp) AS segment_arrival_time_in_dep,
                
                -- 도착 공항 시간대 기준 시간
                timezone(arr.time_zone, s.depart_timestamp) AS segment_depart_time_in_arr,
                timezone(arr.time_zone, s.arrival_timestamp) AS segment_arrival_time_in_arr,
                
                s.journey_time AS segment_duration
            FROM 
                layover_info l
            JOIN
                flight_info s ON l.segment_id = s.air_id
            JOIN 
                airport_info dep ON s.depart_airport = dep.airport_code
            JOIN 
                airport_info arr ON s.arrival_airport = arr.airport_code
            WHERE
                l.air_id IN ({air_ids})
            ORDER BY
                l.air_id, l.layover_order;
            """
    try:
       df = pd.read_sql(query, engine)
       return df
           
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

def delete_extracted_flights(start_point, conn):
    end_point = start_point+timedelta(days=1)
    
    try:
        with conn.cursor() as cur:
            query = f"""
            DELETE FROM flight_info 
            WHERE depart_timestamp >= '{start_point}' 
            AND depart_timestamp < '{end_point}'
            """
            cur.execute(query)
            deleted = cur.rowcount
            conn.commit()
            
            logger.info(f"flight_info에서 {deleted}개의 레코드가 삭제되었습니다.")
            return {'flight_info': deleted}
    
    except Exception as e:
        conn.rollback()
        logger.error(f"삭제 중 오류 발생: {str(e)}")
        raise