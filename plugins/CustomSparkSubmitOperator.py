from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


class CustomSparkSubmitOperator(SparkSubmitOperator):
    def execute(self, context):
        # 1. 부모의 execute를 실행하여 Spark 작업 수행
        super().execute(context)

        # 2. 작업 완료 후 Hook에 저장된 application_id 추출
        # Note. SparkSubmitHook은 실행 과정에서 찾은 ID를 내부 변수에 보관합니다.
        app_id = self._hook.get_application_id()

        if app_id:
            # 3. XCom에 명시적으로 Push (Korean: XCom에 application_id를 명시적으로 저장)
            self.xcom_push(context, key="application_id", value=app_id)
            self.log.info(f"Pushed Spark Application ID to XCom: {app_id}")  # English logging
        else:
            self.log.warning("Spark Application ID could not be found in logs.")  # English logging


# English: Manually pushing the application ID to XCom after hook execution
