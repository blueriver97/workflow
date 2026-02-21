import os
import tempfile

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


class SecureYamlSparkSubmitOperator(SparkSubmitOperator):
    """
    YAML 템플릿을 읽어 Airflow Variables로 렌더링한 후, 임시 파일로 만들어
    --files 옵션으로 Spark 클러스터에 안전하게 전달하는 오퍼레이터.
    """

    # 템플릿 필드에 yaml_template_path 추가
    template_fields = list(SparkSubmitOperator.template_fields) + ["yaml_template_path"]

    def __init__(self, yaml_template_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.yaml_template_path = yaml_template_path
        self._temp_file_path = None

    def execute(self, context):
        # 1. 로컬의 YAML 템플릿 파일 읽기
        with open(self.yaml_template_path, encoding="utf-8") as f:
            yaml_content = f.read()

        # 2. Airflow 내장 엔진으로 {{ var.value.XXX }} 치환 (렌더링)
        rendered_content = self.render_template(yaml_content, context)

        # 3. 렌더링된 내용을 담은 로컬 임시 파일 생성
        fd, self._temp_file_path = tempfile.mkstemp(suffix=".yml", prefix="settings_")
        with os.fdopen(fd, "w") as f:
            f.write(rendered_content)

        # 4. spark-submit 명령어의 --files 파라미터에 동적으로 임시 파일 경로 주입
        if self._files:
            self._files = f"{self._files},{self._temp_file_path}"
        else:
            self._files = self._temp_file_path

        try:
            # 5. 부모 클래스(SparkSubmitOperator)의 실제 실행 로직 호출
            super().execute(context)
        finally:
            # 6. 작업 종료 즉시 임시 파일 삭제 (인증 정보 디스크 잔류 방지)
            if os.path.exists(self._temp_file_path):
                os.remove(self._temp_file_path)
