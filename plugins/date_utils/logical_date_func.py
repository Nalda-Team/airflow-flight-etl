from datetime import timedelta, datetime
from pendulum import timezone
def get_actual_execution_date(logical_date):
    actual_date = logical_date + timedelta(days=1)
    return actual_date


def xcom_push_actual_execution_date(**context):
    logical_date = context['logical_date']
    print(f"Logical Date: {logical_date}")
    actual_execution_date = get_actual_execution_date(context['execution_date'])
    print(f"actual_execution_date: {actual_execution_date}")
    execution_date_in_seoul = actual_execution_date.astimezone(timezone('Asia/Seoul'))
    print(f"actual_execution_date: {execution_date_in_seoul}")

    context['ti'].xcom_push(key='actual_execution_date', value=execution_date_in_seoul)

    actual_execution_date_str = execution_date_in_seoul.strftime('%Y-%m-%d')  # <-- 문자열로 변환
    print('actual_execution_date_str:', actual_execution_date_str)
    context['ti'].xcom_push(key='actual_execution_date_str', value=actual_execution_date_str)
    # 필요한 처리 수행
    return actual_execution_date


def get_parent_execution_date_and_push_again(**context):
    # 부모 DAG에서 전달된 값 추출
    actual_execution_date = context['dag_run'].conf.get('actual_execution_date')
    print(f"부모 DAG에서 전달받은 actual_execution_date: {actual_execution_date}")
    
    # 다시 XCom에 저장
    context['ti'].xcom_push(key='actual_execution_date', value=actual_execution_date)
    
    return actual_execution_date

def get_execution_date_with_timezone(execution_date, time_zone='Asia/Seoul'):
    if isinstance(execution_date, str):
        execution_date = datetime.fromisoformat(execution_date.replace('Z', '+00:00'))
    return execution_date.astimezone(timezone(time_zone))