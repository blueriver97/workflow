from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


class CustomSparkSubmitOperator(SparkSubmitOperator):
    # 기존 template_fields에 inlets와 outlets를 추가하여 확장
    # Extend template_fields to support dynamic lineage mapping
    template_fields = SparkSubmitOperator.template_fields + ("mapped_inlets", "mapped_outlets")

    def __init__(self, **kwargs):
        self.mapped_inlets = kwargs.pop("mapped_inlets", None)
        self.mapped_outlets = kwargs.pop("mapped_outlets", None)
        super().__init__(**kwargs)

    def execute(self, context):
        # 실행 직전, 템플릿이 완료된 값을 실제 inlets/outlets에 할당
        # Assign rendered template values to self.inlets/outlets before execution
        if self.mapped_inlets:
            self.inlets = self.mapped_inlets
        if self.mapped_outlets:
            self.outlets = self.mapped_outlets

        print(f"Executing task with inlets: {self.inlets}")
        return super().execute(context)
