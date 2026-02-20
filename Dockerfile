# ----------------------------------------------------------------
# 1. Builder Stage: 의존성 다운로드 및 빌드
# ----------------------------------------------------------------
FROM apache/airflow:3.1.5-python3.12 AS build
ARG SPARK_VERSION
USER root
WORKDIR /opt

COPY requirements.txt /opt
COPY download/spark-$SPARK_VERSION-bin-hadoop3.tgz /opt

# Apache Spark with hadoop
# 2026-02-10 최종 확인 => 반드시 with-hadoop3 버전 사용해야 함, without-hadoop 버전은 따로 설정할 jar 의존성이 너무 복잡함
# SPARK_VERSION-bin-hadoop3.tgz 내 hadoop-client 버전이 Yarn 클러스터에서 쓰는 버전과 다를 수 있으므로 명시적으로 제거한 후 파일 복사 진행
RUN umask 0002 &&\
    tar -zxvf spark-$SPARK_VERSION-bin-hadoop3.tgz &&\
    ln -s spark-$SPARK_VERSION-bin-hadoop3 spark &&\
    rm -rf spark-$SPARK_VERSION-bin-hadoop3.tgz &&\
    rm -rf /opt/spark/jars/hadoop-client-api*.jar /opt/spark/jars/hadoop-client-runtime*.jar
COPY download/*.jar /opt/spark/jars
COPY config/spark /opt/spark/conf
COPY config/hadoop /opt/hadoop/etc/hadoop

# ----------------------------------------------------------------
# 2. Final Stage: 최종 이미지 생성
# ----------------------------------------------------------------
FROM apache/airflow:3.1.5-python3.12
COPY --from=build /opt/hadoop /opt/hadoop
COPY --from=build /opt/spark /opt/spark
COPY --from=build /opt/requirements.txt /opt/requirements.txt

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV SPARK_HOME=/opt/spark

USER root
WORKDIR /opt
RUN umask 0002 &&\
    apt-get update &&\
    apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless procps curl unzip wget git vim &&\
    apt-get autoremove -yqq --purge &&\
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/* &&\
    echo "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))" >> /root/.bashrc &&\
    chown -R airflow:root /opt

USER airflow
WORKDIR /opt
RUN umask 0002 &&\
    pip install -U pip setuptools wheel &&\
    pip install --no-cache-dir -r /opt/requirements.txt &&\
    echo "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))" >> /home/airflow/.bashrc
