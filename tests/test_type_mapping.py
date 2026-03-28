"""convert_db_type_to_spark 타입 매핑 파싱 검증 테스트"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pyspark.sql.types as T

from utils.database import convert_db_type_to_spark


def test_mysql_basic_types():
    assert isinstance(convert_db_type_to_spark("int", "mysql"), T.IntegerType)
    assert isinstance(convert_db_type_to_spark("bigint", "mysql"), T.LongType)
    assert isinstance(convert_db_type_to_spark("smallint", "mysql"), T.IntegerType)
    assert isinstance(convert_db_type_to_spark("tinyint", "mysql"), T.IntegerType)
    assert isinstance(convert_db_type_to_spark("float", "mysql"), T.FloatType)
    assert isinstance(convert_db_type_to_spark("double", "mysql"), T.DoubleType)
    assert isinstance(convert_db_type_to_spark("boolean", "mysql"), T.BooleanType)


def test_mysql_string_types():
    assert isinstance(convert_db_type_to_spark("varchar(255)", "mysql"), T.StringType)
    assert isinstance(convert_db_type_to_spark("char(10)", "mysql"), T.StringType)
    assert isinstance(convert_db_type_to_spark("text", "mysql"), T.StringType)
    assert isinstance(convert_db_type_to_spark("tinytext", "mysql"), T.StringType)
    assert isinstance(convert_db_type_to_spark("mediumtext", "mysql"), T.StringType)
    assert isinstance(convert_db_type_to_spark("longtext", "mysql"), T.StringType)
    assert isinstance(convert_db_type_to_spark("enum('a','b')", "mysql"), T.StringType)
    assert isinstance(convert_db_type_to_spark("set('x','y')", "mysql"), T.StringType)
    assert isinstance(convert_db_type_to_spark("json", "mysql"), T.StringType)


def test_mysql_temporal_types():
    assert isinstance(convert_db_type_to_spark("date", "mysql"), T.DateType)
    assert isinstance(convert_db_type_to_spark("datetime", "mysql"), T.TimestampType)
    assert isinstance(convert_db_type_to_spark("timestamp", "mysql"), T.TimestampType)
    assert isinstance(convert_db_type_to_spark("time", "mysql"), T.TimestampType)


def test_mysql_tinyint1_boolean():
    assert isinstance(convert_db_type_to_spark("tinyint(1)", "mysql"), T.BooleanType)
    assert isinstance(convert_db_type_to_spark("tinyint", "mysql"), T.IntegerType)
    assert isinstance(convert_db_type_to_spark("tinyint(4)", "mysql"), T.IntegerType)


def test_mysql_decimal():
    result = convert_db_type_to_spark("decimal(10,2)", "mysql")
    assert isinstance(result, T.DecimalType)
    assert result.precision == 10
    assert result.scale == 2

    result2 = convert_db_type_to_spark("decimal(5)", "mysql")
    assert isinstance(result2, T.DecimalType)
    assert result2.precision == 5
    assert result2.scale == 0


def test_mysql_int_unsigned():
    result = convert_db_type_to_spark("int unsigned", "mysql")
    assert isinstance(result, T.LongType)


def test_mysql_binary_types():
    assert isinstance(convert_db_type_to_spark("blob", "mysql"), T.BinaryType)
    assert isinstance(convert_db_type_to_spark("tinyblob", "mysql"), T.BinaryType)
    assert isinstance(convert_db_type_to_spark("mediumblob", "mysql"), T.BinaryType)
    assert isinstance(convert_db_type_to_spark("longblob", "mysql"), T.BinaryType)


def test_mssql_basic_types():
    assert isinstance(convert_db_type_to_spark("int", "sqlserver"), T.IntegerType)
    assert isinstance(convert_db_type_to_spark("bigint", "sqlserver"), T.LongType)
    assert isinstance(convert_db_type_to_spark("smallint", "sqlserver"), T.ShortType)
    assert isinstance(convert_db_type_to_spark("tinyint", "sqlserver"), T.ByteType)
    assert isinstance(convert_db_type_to_spark("bit", "sqlserver"), T.BooleanType)
    assert isinstance(convert_db_type_to_spark("float", "sqlserver"), T.DoubleType)
    assert isinstance(convert_db_type_to_spark("real", "sqlserver"), T.FloatType)


def test_mssql_string_types():
    assert isinstance(convert_db_type_to_spark("varchar", "sqlserver"), T.StringType)
    assert isinstance(convert_db_type_to_spark("nvarchar", "sqlserver"), T.StringType)
    assert isinstance(convert_db_type_to_spark("char", "sqlserver"), T.StringType)
    assert isinstance(convert_db_type_to_spark("nchar", "sqlserver"), T.StringType)
    assert isinstance(convert_db_type_to_spark("text", "sqlserver"), T.StringType)
    assert isinstance(convert_db_type_to_spark("ntext", "sqlserver"), T.StringType)
    assert isinstance(convert_db_type_to_spark("uniqueidentifier", "sqlserver"), T.StringType)
    assert isinstance(convert_db_type_to_spark("xml", "sqlserver"), T.StringType)
    assert isinstance(convert_db_type_to_spark("sql_variant", "sqlserver"), T.StringType)


def test_mssql_temporal_types():
    assert isinstance(convert_db_type_to_spark("date", "sqlserver"), T.DateType)
    assert isinstance(convert_db_type_to_spark("datetime", "sqlserver"), T.TimestampType)
    assert isinstance(convert_db_type_to_spark("datetime2", "sqlserver"), T.TimestampType)
    assert isinstance(convert_db_type_to_spark("smalldatetime", "sqlserver"), T.TimestampType)
    assert isinstance(convert_db_type_to_spark("time", "sqlserver"), T.StringType)


def test_mssql_decimal_types():
    result = convert_db_type_to_spark("decimal", "sqlserver")
    assert isinstance(result, T.DecimalType)
    assert result.precision == 38
    assert result.scale == 10

    result2 = convert_db_type_to_spark("numeric", "sqlserver")
    assert isinstance(result2, T.DecimalType)

    result3 = convert_db_type_to_spark("money", "sqlserver")
    assert isinstance(result3, T.DecimalType)
    assert result3.precision == 19
    assert result3.scale == 4

    result4 = convert_db_type_to_spark("smallmoney", "sqlserver")
    assert isinstance(result4, T.DecimalType)
    assert result4.precision == 10
    assert result4.scale == 4


def test_mssql_binary_types():
    assert isinstance(convert_db_type_to_spark("binary", "sqlserver"), T.BinaryType)
    assert isinstance(convert_db_type_to_spark("varbinary", "sqlserver"), T.BinaryType)
    assert isinstance(convert_db_type_to_spark("image", "sqlserver"), T.BinaryType)


def test_unknown_type_defaults_to_string():
    assert isinstance(convert_db_type_to_spark("geometry", "mysql"), T.StringType)
    assert isinstance(convert_db_type_to_spark("hierarchyid", "sqlserver"), T.StringType)


def test_whitespace_handling():
    assert isinstance(convert_db_type_to_spark("  varchar(255)  ", "mysql"), T.StringType)
    assert isinstance(convert_db_type_to_spark("int", "mysql"), T.IntegerType)


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-v"])
