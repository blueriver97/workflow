#!/bin/bash

# .env 파일 읽기
while IFS='=' read -r key value; do
    if [[ $key == "SPARK_VERSION" ]]; then
        SPARK_VERSION=$value
        echo "$key=$value"
    elif [[ $key == "HADOOP_VERSION" ]]; then
        HADOOP_VERSION=$value
        echo "$key=$value"
    fi
done < ../.env

# apache-airflow-providers-apache-spark 설치 시 pyspark도 같이 설치되는데, 설치된 pyspark 버전은 Yarn 클러스터의 의존성과는 관계 없음 확인.
# (2026-02-10) Yarn 클러스터를 구성하는 Hadoop 버전와 /opt/spark/jars 내 hadoop 버전이 맞아야 함.
# spark-without-hadoop 버전의 경우, 따로 설치할 jar 의존성이 너무 복잡해서 반드시 spark-with-hadoop3 버전을 사용해야 함.
# -----------------------------
# hadoop-client-api-x.y.z.jar
# hadoop-client-runtime-x.y.z.jar
# hadoop-aws-x.y.z.jar
# -----------------------------
if [[ $HADOOP_VERSION == "3.4.2" ]]; then
    AWS_SDK_VERSION="2.29.52"
fi
declare -a package_urls=(
    "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz"
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/$AWS_SDK_VERSION/bundle-$AWS_SDK_VERSION.jar"
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_VERSION/hadoop-aws-$HADOOP_VERSION.jar"
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/$HADOOP_VERSION/hadoop-client-api-$HADOOP_VERSION.jar"
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/$HADOOP_VERSION/hadoop-client-runtime-$HADOOP_VERSION.jar"
)

declare -a packages=()

for url in "${package_urls[@]}"; do
    filename=$(basename "$url")
    packages+=("$filename")
done

for (( i=0; i<${#package_urls[@]}; i++ )); do
    if [ -f "${packages[$i]}" ]; then
        echo "${packages[$i]} ... existed"
    else
        wget "${package_urls[$i]}" -O "${packages[$i]}"
    fi
done
