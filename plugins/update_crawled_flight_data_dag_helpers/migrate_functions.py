from airflow.utils.log.logging_mixin import LoggingMixin
from pendulum import timezone
from database_helpers.utils import get_db_conn
from date_utils.logical_date_func import get_execution_date_with_timezone

TIMEZONE = 'Asia/Seoul'
conn, engine = get_db_conn()
logger = LoggingMixin().log

def run_aggregate(**context):
    """특정 날짜의 temp_fare_info 데이터를 집계하여 agg_flights_info 테이블에 삽입
       또한 집계 후 date_str보다 이틀 전 데이터를 삭제함"""
    
    # 실행 날짜 획득
    actual_execution_date = context['dag_run'].conf.get('actual_execution_date')
    execution_date_in_seoul = get_execution_date_with_timezone(actual_execution_date, TIMEZONE)
    date_str = execution_date_in_seoul.strftime('%Y-%m-%d')
    
    logger.info(f"{date_str} 날짜에 수집된 항공권 데이터 집계를 시작합니다.")
    
    # 집계 쿼리 실행
    cur = conn.cursor()
    aggregate_query = f"""
            WITH aggregated_data AS (
                SELECT 
                    f.depart_airport || '_' || f.arrival_airport AS route_key,
                    EXTRACT(YEAR FROM timezone(dep.time_zone, f.depart_timestamp)) AS depart_year,
                    EXTRACT(MONTH FROM timezone(dep.time_zone, f.depart_timestamp)) AS depart_month,
                    EXTRACT(DAY FROM timezone(dep.time_zone, f.depart_timestamp)) AS depart_day,
                    f.is_layover,
                    COALESCE(f.airline, 'airline 여러개') AS airline,
                    fa.seat_class,
                    fa.fare_class,
                    fa.fetched_date,
                    AVG(fa.adult_fare) AS mean_price,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fa.adult_fare) AS median_price,
                    COUNT(*) AS ticket_count
                FROM (
                    SELECT *
                    FROM temp_fare_info
                    WHERE fetched_date = '{date_str}'
                ) fa
                JOIN flight_info f ON f.air_id = fa.air_id
                JOIN airport_info dep ON f.depart_airport = dep.airport_code
                JOIN airport_info arr ON f.arrival_airport = arr.airport_code
                GROUP BY 
                    f.depart_airport || '_' || f.arrival_airport,
                    EXTRACT(YEAR FROM timezone(dep.time_zone, f.depart_timestamp)),
                    EXTRACT(MONTH FROM timezone(dep.time_zone, f.depart_timestamp)),
                    EXTRACT(DAY FROM timezone(dep.time_zone, f.depart_timestamp)),
                    f.is_layover,
                    COALESCE(f.airline, 'airline 여러개'),
                    fa.seat_class,
                    fa.fare_class,
                    fa.fetched_date
            )
            INSERT INTO agg_flights_info (
                route_key,
                depart_year,
                depart_month,
                depart_day,
                is_layover,
                airline,
                seat_class,
                fare_class,
                fetched_date,
                mean_price,
                median_price,
                ticket_count
            )
            SELECT
                route_key,
                depart_year,
                depart_month,
                depart_day,
                is_layover,
                airline,
                seat_class,
                fare_class,
                fetched_date,
                mean_price,
                median_price,
                ticket_count
            FROM aggregated_data
            ON CONFLICT (route_key, depart_year, depart_month, depart_day, is_layover, seat_class, airline, fare_class, fetched_date)
            DO UPDATE SET
                mean_price = EXCLUDED.mean_price,
                median_price = EXCLUDED.median_price,
                ticket_count = EXCLUDED.ticket_count;
    """
    
    cur.execute(aggregate_query)
    affected_rows = cur.rowcount
    conn.commit()
    
    logger.info(f"{date_str} 날짜 데이터 집계 완료: {affected_rows}개 레코드가 처리되었습니다.")
    
    # date_str보다 이틀 전 데이터 삭제
    delete_query = f"""
        DELETE FROM agg_flights_info
        WHERE fetched_date <= ('{date_str}'::date - INTERVAL '2 days')::date;
    """
    
    cur.execute(delete_query)
    deleted_rows = cur.rowcount
    conn.commit()
    
    logger.info(f"오래된 데이터 정리 완료: {deleted_rows}개의 레코드({date_str}에서 2일 이전)가 삭제되었습니다.")
    
    return True

def run_migration(**context):
    actual_execution_date = context['dag_run'].conf.get('actual_execution_date')
    print(actual_execution_date)
    min_batch_id, max_batch_id = get_batch_idx_range(actual_execution_date)
    logger.info(f"batch_id {min_batch_id}부터 {max_batch_id}까지 마이그레이션을 시작합니다.")
    if min_batch_id is not None and max_batch_id is not None:
        migrate_to_origin_fare_info(min_batch_id, max_batch_id)
        logger.info("마이그레이션이 완료되었습니다!!")
        return True
    return True

def get_batch_idx_range(execution_date):
    execution_date_in_seoul = get_execution_date_with_timezone(execution_date, TIMEZONE)
    date_str = execution_date_in_seoul.strftime('%Y-%m-%d')
    cur = conn.cursor()
    query = f"SELECT MIN(batch_id), MAX(batch_id) FROM temp_fare_info WHERE fetched_date='{date_str}'"
    logger.info(query)
    cur.execute(query)
    result = cur.fetchone()
    min_batch_id, max_batch_id = result
    return min_batch_id, max_batch_id

def migrate_to_origin_fare_info(min_batch_id, max_batch_id, batch_size=30000):
    for idx in range(min_batch_id, max_batch_id, batch_size):
        batch_id_start = idx
        batch_id_end = idx + batch_size
        query = f"""
                INSERT INTO fare_info (air_id, seat_class, agt_code, adult_fare, fetched_date, fare_class)
                SELECT air_id, seat_class, agt_code, adult_fare, fetched_date, fare_class 
                FROM temp_fare_info 
                WHERE batch_id >={batch_id_start} AND batch_id <={batch_id_end}
                ORDER BY air_id, seat_class, agt_code, fetched_date, fare_class, batch_id DESC
                ON CONFLICT (air_id, seat_class, agt_code, fetched_date, fare_class) 
                DO UPDATE SET adult_fare = EXCLUDED.adult_fare;
            """
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        logger.info(f"batch_id {batch_id_start}부터 {batch_id_end}까지 마이그레이션을 되었습니다.")
    return True



def run_delete_migrate_records(**context):
    actual_execution_date = context['dag_run'].conf.get('actual_execution_date')
    execution_date_in_seoul = get_execution_date_with_timezone(actual_execution_date, TIMEZONE)
    date_str = execution_date_in_seoul.strftime('%Y-%m-%d')
    cur = conn.cursor()
    logger.info(f"{execution_date_in_seoul}에 수집된 데이터를 제거합니다.")
    logger.info("중간 집계 + 마이그레이션 완료된 레코드를 temp_fare_info에서 제거합니다.")
    query = f"DELETE FROM temp_fare_info WHERE fetched_date='{date_str}'"
    cur.execute(query)
    deleted = cur.rowcount
    logger.info(f"temp_fare_info에서 {deleted}개의 레코드가 삭제되었습니다.")
    conn.commit()
    return True