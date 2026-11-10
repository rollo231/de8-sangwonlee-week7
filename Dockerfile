# Q5 — 공식 Airflow 이미지 + JDK + AWS/Spark provider + pyspark
FROM apache/airflow:3.4.0

# 1) OS 패키지(apt)는 root 권한이 필요하다
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/java-21 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# 아키텍처(arm64/amd64)마다 다른 JDK 경로를 위 심볼릭 링크로 통일
ENV JAVA_HOME=/usr/lib/jvm/java-21

# 2) 파이썬 패키지는 airflow 사용자로 설치한다 (공식 이미지 규칙)
USER airflow
COPY requirements.txt /requirements.txt
# apache-airflow 를 함께 고정해 provider 설치가 Airflow 본체를 올려 버리지 않게 한다
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
