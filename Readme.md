# 1. Dag 플로우 차트 및 개요

항공권 데이터 크롤링, 만료 항공편 ETL, 크롤링 데이터 중간 집계 및 마이그레이션을 Airflow DAG로 자동화합니다.

![Dag Flow Chart](https://github.com/user-attachments/assets/90aef51d-1b6e-4761-bb52-9387aabe8773)

# 2. DAG 목록 및 역할

| DAG 이름                     | 실행 시점     | 주요 태스크 및 설명 | 후속 DAG 트리거 |
|-----------------------------|----------------|----------------------|------------------|
| **crawler_dag**             | 매일 자정      | - 이전 크롤러 종료 (`kill_prev_crawlers`)<br> - GCS 스트림 폴더 초기화 (`init_stream_folder_to_GCS`)<br> - Redis 수집 큐 초기화 (`init_schedules_to_redis`)<br> - 크롤러 및 Spark 스트리밍 잡 실행 (`run_crawler`, `submit_spark_stream_job`)<br> - Redis Sensor로 완료 대기 (`wait_for_crawling_completion`) | `update_crawled_flight_data_dag` |
| **expired_flights_etl_dag** | 매일 자정      | - 만료 항공편 GCS 업로드 (`extract_yesterday_expired_flights_and_upload_to_GCS`)<br> - DB에서 해당 레코드 삭제 (`DELETE_yesterday_expired_flight_from_db`) | `ML_ops_dag` |
| **update_crawled_flight_data_dag** | 필요 시 트리거 | - 오늘 항공권 가격 중간 집계 (`init_today_agg_flight_info`)<br> - 원본 `fare_info`로 마이그레이션 (`migrate_temp_fare_info_to_origin_fare_info`)<br> - 임시 테이블 비우기 (`truncate_temp_fare_info`) | 없음 |

# 3. 추가 DAG
- **ML_ops_dag** : 모델 학습·배포 등 ML 파이프라인 처리 (추가 구현 필요)


**실제 배포는 docker image로 배포 후 docker compose로 배포되어있습니다. (Docker file 참고)**