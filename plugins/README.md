### StreamingSparkSubmitOperator

`SparkSubmitOperator`를 확장하여 S3 시그널 기반 graceful shutdown을 지원합니다.

- `execute()`: 잔여 시그널 파일 정리 후 spark-submit 실행
- `on_kill()`: S3에 시그널 파일 생성 → Spark 앱이 감지하여 현재 배치 완료 후 종료

### SlackNotifier

Redis 기반 Slack 스레드 알림 시스템.

- 실패/재시도/복구 상태별 Slack 스레드 메시지
- Redis SET NX로 중복 알림 방지
- 월별 실패 횟수 집계 (`airflow:alert:{YYYYMM}`)
- DAG 파싱 시 연결 비용 제거를 위한 lazy callback 패턴
