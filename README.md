# Airflow Workflow Project

이 프로젝트는 Apache Airflow를 기반으로 데이터 파이프라인을 구축하고 관리하기 위한 워크플로우 저장소입니다. Docker Compose를 사용하여 로컬 개발 환경을 구성하고, Spark, Hadoop(YARN), Glue, Polaris, DataHub(OpenLineage) 등 다양한 데이터 에코시스템과 연동하여 배치 및 스트리밍 작업을 수행합니다.

## 프로젝트 구조

주요 디렉토리 및 파일 구성은 다음과 같습니다.

- **compose.yaml**: Airflow 및 관련 서비스 실행을 위한 Docker Compose 설정
- **Dockerfile**: Airflow 커스텀 이미지 빌드 설정
- **config/**: Airflow, Hadoop, Spark 관련 설정 파일
- **dags/**: Airflow DAG 파일 (Glue, Polaris, Lineage 등)
- **src/**: Spark 애플리케이션 소스 코드 및 유틸리티
- **plugins/**: Airflow 플러그인
- **scripts/**: 개발 및 유지보수 스크립트

## 시작하기

### 1. 사전 요구 사항

- Docker & Docker Compose
- Java 17 (로컬 개발 시)
- Python 3.12 (로컬 개발 시)

### 2. 환경 설정

프로젝트 루트에 환경 변수 파일(`.env`)을 생성하고 필요한 설정을 진행합니다. 또한 `download/` 디렉토리에 필요한 Spark 및 Hadoop 바이너리 파일이 준비되어 있어야 합니다.

### 3. 실행 방법

Docker Compose를 사용하여 이미지를 빌드하고 컨테이너를 실행합니다. 실행 후 웹 브라우저를 통해 Airflow Webserver에 접속할 수 있습니다.

## 주요 기능 및 DAG

### 1. Glue Catalog 연동

Kafka 토픽 및 MySQL 테이블 데이터를 읽어 AWS Glue Catalog 기반의 Iceberg 테이블로 적재하는 배치 및 스트리밍 작업을 수행합니다.

### 2. Polaris Catalog 연동

Kafka 토픽 및 MySQL 테이블 데이터를 읽어 Apache Polaris Catalog 기반의 Iceberg 테이블로 적재하는 작업을 수행합니다.

### 3. Data Lineage

Airflow에서 실행되는 Spark 작업의 입출력 데이터셋 정보를 OpenLineage를 통해 수집하고, 이를 DataHub로 전송하여 데이터 리니지를 시각화합니다.

## 기술 스택

- **Orchestration**: Apache Airflow
- **Compute**: Apache Spark (on YARN)
- **Storage Format**: Apache Iceberg
- **Catalog**: AWS Glue, Apache Polaris
- **Message Queue**: Apache Kafka
- **Metadata & Lineage**: DataHub, OpenLineage
- **Secret Management**: HashiCorp Vault
- **Database**: MySQL, PostgreSQL

## 개발 가이드

### Spark 애플리케이션 개발

`src/` 디렉토리 내에서 Spark 작업을 개발하며, 공통 유틸리티 모듈을 활용합니다. 배포 시에는 유틸리티 모듈을 패키징하여 함께 제출합니다.

### Airflow DAG 개발

`dags/` 디렉토리 내에서 DAG를 정의하며, `SparkSubmitOperator`를 사용하여 Spark 작업을 실행하도록 구성합니다.

## 기여

이 프로젝트에 기여하고 싶으신 분들은 Pull Request를 보내주세요. 버그 리포트나 기능 제안은 Issue를 통해 가능합니다.

## 라이선스

이 프로젝트는 Apache License 2.0을 따릅니다.
