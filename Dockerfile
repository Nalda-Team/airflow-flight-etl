FROM apache/airflow:2.7.1

USER root
RUN apt-get update && \
    apt-get install -y vim

# requirements.txt만 먼저 복사
COPY requirements.txt /opt/airflow/requirements.txt

# Python 패키지 설치 (변경되지 않으면 캐시 재사용)
USER airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

# 나머지 파일 복사
COPY . /opt/airflow/

# 권한 설정
USER root
RUN chown -R 50000:0 /opt/airflow