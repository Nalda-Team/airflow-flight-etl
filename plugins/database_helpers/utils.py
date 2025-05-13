from airflow.providers.postgres.hooks.postgres import PostgresHook  # 수정된 import
def get_db_conn():
    """네이버 항공권 데이터베이스 연결"""
    pg_hook = PostgresHook(postgres_conn_id='NAVER_DB')
    engine = pg_hook.get_sqlalchemy_engine()
    conn = pg_hook.get_conn()
    return conn, engine