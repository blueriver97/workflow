from cosmos.operators.local import DbtCompileLocalOperator


class DbtCompileLocalOperatorNoUpload(DbtCompileLocalOperator):
    """remote_target_path 미설정 환경에서 compiled SQL 업로드를 건너뛰는 변형.

    DbtCompileLocalOperator가 should_upload_compiled_sql=True를 하드코딩하므로,
    __init__ 이후 강제로 비활성화한다.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.should_upload_compiled_sql = False
