import pyspark.sql.functions as F  # noqa
from abc import ABC, abstractmethod
from textwrap import dedent
from collections import OrderedDict
from pyspark.sql import DataFrame, SparkSession


class BaseDatabaseManager(ABC):
    """
    데이터베이스 관리를 위한 추상 베이스 클래스
    """

    def __init__(self, config):
        # 설정 객체 초기화
        self.config = config

    def _execute_jdbc_query(self, spark: SparkSession, options: dict[str, str], query: str) -> DataFrame:
        # JDBC 쿼리 실행 및 데이터프레임 반환
        print(f"Executing JDBC query on {self.config.database.type}")
        return spark.read.format("jdbc").options(**options).option("query", query).load()

    @abstractmethod
    def get_jdbc_options(self, database: str = "") -> dict[str, str]:
        # DB별 JDBC 옵션 생성
        pass

    @abstractmethod
    def get_primary_key(self, spark: SparkSession, table_name: str) -> list[str]:
        # PK 정보 조회
        pass

    @abstractmethod
    def get_partition_key(self, spark: SparkSession, table_name: str) -> str | None:
        # 파티션 키 후보 조회
        pass

    @abstractmethod
    def get_schema(self, spark: SparkSession, table_name: str) -> dict[str, str]:
        # 테이블 스키마 정보 조회
        pass


class MySQLManager(BaseDatabaseManager):
    """
    MySQL 전용 관리 클래스
    """

    def get_jdbc_options(self, database: str = "") -> dict[str, str]:
        # MySQL 연결 옵션 생성
        db = self.config.database
        options = {
            "url": f"jdbc:mysql://{db.host}:{db.port}/{database}?zeroDateTimeBehavior=convertToNull",
            "driver": "com.mysql.cj.jdbc.Driver",
            "user": db.user,
            "password": db.password.get_secret_value(),
        }
        return options

    def get_primary_key(self, spark: SparkSession, table_name: str) -> list[str]:
        options = self.get_jdbc_options()
        query = dedent(f"""
            SELECT COLUMN_NAME
            FROM information_schema.STATISTICS
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{table_name}'
              AND INDEX_NAME = 'PRIMARY'
            ORDER BY SEQ_IN_INDEX
        """)
        df = self._execute_jdbc_query(spark, options, query)
        return [row.COLUMN_NAME for row in df.collect()]

    def get_partition_key(self, spark: SparkSession, table_name: str) -> str | None:
        # 분산 처리를 위한 파티션 키 자동 탐색 (Auto_increment 또는 숫자형 우선)
        options = self.get_jdbc_options()
        query = dedent(f"""
            SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS AS c
                 JOIN (SELECT TABLE_SCHEMA
                            , TABLE_NAME
                            , MIN(ORDINAL_POSITION)                                                       AS min_ordinal
                            , MIN(CASE WHEN EXTRA = 'auto_increment' THEN ORDINAL_POSITION ELSE NULL END) AS extra_ordinal
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{table_name}'
                              AND (DATA_TYPE IN ('int', 'bigint', 'date', 'datetime', 'timestamp') OR EXTRA LIKE 'auto_increment')
                       GROUP BY TABLE_SCHEMA, TABLE_NAME) AS t
                      ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
                          AND c.TABLE_NAME = t.TABLE_NAME
                          AND (c.ORDINAL_POSITION = COALESCE(t.extra_ordinal, t.min_ordinal))
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
        """)
        df = self._execute_jdbc_query(spark, options, query)
        row = df.first()
        return row.COLUMN_NAME if row else None

    def get_schema(self, spark: SparkSession, table_name: str) -> dict[str, str]:
        # Spark 데이터 타입 매핑을 포함한 스키마 조회
        options = self.get_jdbc_options()
        query = dedent(f"""
            SELECT COLUMN_NAME, COLUMN_TYPE
            FROM information_schema.COLUMNS
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        df = self._execute_jdbc_query(spark, options, query)
        return OrderedDict([(row.COLUMN_NAME, row.COLUMN_TYPE) for row in df.collect()])


class SQLServerManager(BaseDatabaseManager):
    """
    SQL Server 전용 관리 클래스
    """

    def get_jdbc_options(self, database: str | None = None) -> dict[str, str]:
        # MSSQL 연결 옵션 생성 (encrypt=false 기본값 설정)
        db = self.config.database
        db_prop = f";databaseName={database}" if database else ""
        options = {
            "url": f"jdbc:sqlserver://{db.host}:{db.port}{db_prop};encrypt=false;",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "user": db.user,
            "password": db.password.get_secret_value(),
        }
        return options

    def get_primary_key(self, spark: SparkSession, table_name: str) -> list[str]:
        # MSSQL PK 조회 (Catalog.Schema.Table 형식 대응)
        db_name = table_name.split(".")[0]
        options = self.get_jdbc_options(db_name)
        query = dedent(f"""
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c ON c.CONSTRAINT_NAME = t.CONSTRAINT_NAME
            WHERE t.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND CONCAT(t.TABLE_CATALOG, '.dbo.', t.TABLE_NAME) = '{table_name}'
            ORDER BY c.ORDINAL_POSITION
        """)
        df = self._execute_jdbc_query(spark, options, query)
        return [row.COLUMN_NAME for row in df.collect()]

    def get_partition_key(self, spark: SparkSession, table_name: str) -> str | None:
        # MSSQL IsIdentity 또는 날짜/숫자형 기반 파티션 키 탐색
        db_name = table_name.split(".")[0]
        options = self.get_jdbc_options(db_name)
        query = dedent(f"""
            SELECT TOP 1 c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS AS c
            WHERE CONCAT(TABLE_CATALOG, '.dbo.', TABLE_NAME) = '{table_name}'
              AND (DATA_TYPE IN ('date', 'datetime', 'datetime2', 'timestamp') OR
                   COLUMNPROPERTY(OBJECT_ID(CONCAT(TABLE_SCHEMA, '.', TABLE_NAME)), COLUMN_NAME, 'IsIdentity') = 1)
            ORDER BY (CASE WHEN COLUMNPROPERTY(OBJECT_ID(CONCAT(TABLE_SCHEMA, '.', TABLE_NAME)), COLUMN_NAME, 'IsIdentity') = 1 THEN 0 ELSE 1 END),
                     ORDINAL_POSITION
        """)
        df = self._execute_jdbc_query(spark, options, query)
        row = df.first()
        return row.COLUMN_NAME if row else None

    def get_schema(self, spark: SparkSession, table_name: str) -> dict[str, str]:
        # MSSQL 데이터 타입 조회 및 정렬
        db_name = table_name.split(".")[0]
        options = self.get_jdbc_options(db_name)
        query = dedent(f"""
            SELECT COLUMN_NAME, DATA_TYPE AS COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE CONCAT(TABLE_CATALOG, '.dbo.', TABLE_NAME) = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        df = self._execute_jdbc_query(spark, options, query)
        return OrderedDict([(row.COLUMN_NAME, row.COLUMN_TYPE) for row in df.collect()])
