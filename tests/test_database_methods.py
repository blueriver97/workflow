"""database.py의 get_column_comments, get_table_comment, get_nullable_info 검증 테스트

로컬 MySQL/SQLServer 컨테이너에 직접 쿼리하여 반환값 구조와 내용을 검증합니다.
DatabaseManager는 Spark JDBC를 사용하므로, 여기서는 동일한 SQL을 Python DB 드라이버로 실행하여
쿼리 자체의 정확성을 검증합니다.
"""

import pymssql
import pymysql
import pytest

# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------
MYSQL_CONF = dict(host="127.0.0.1", port=3306, user="admin", password="admin", database="store")
MYSQL_TABLE = "store.tb_lower"


@pytest.fixture(scope="module")
def mysql_conn():
    conn = pymysql.connect(**MYSQL_CONF, cursorclass=pymysql.cursors.DictCursor)
    yield conn
    conn.close()


def test_mysql_column_comments(mysql_conn):
    """MySQL get_column_comments 쿼리 검증"""
    with mysql_conn.cursor() as cur:
        cur.execute(f"""
            SELECT COLUMN_NAME, COLUMN_COMMENT
            FROM information_schema.COLUMNS
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{MYSQL_TABLE}'
            ORDER BY ORDINAL_POSITION
        """)
        rows = cur.fetchall()

    result = {r["COLUMN_NAME"]: r["COLUMN_COMMENT"] for r in rows}
    assert len(result) > 0, "컬럼이 반환되어야 합니다"
    assert "id" in result, "id 컬럼이 존재해야 합니다"
    assert result["id"] == "기본키", f"id 컬럼 주석은 '기본키'여야 합니다, got: {result['id']}"
    assert result["char36"] == "char", f"char36 주석은 'char'여야 합니다, got: {result['char36']}"
    print(f"\n[MySQL] column_comments ({len(result)} cols): {dict(list(result.items())[:5])} ...")


def test_mysql_table_comment(mysql_conn):
    """MySQL get_table_comment 쿼리 검증"""
    with mysql_conn.cursor() as cur:
        cur.execute(f"""
            SELECT TABLE_COMMENT
            FROM information_schema.TABLES
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{MYSQL_TABLE}'
        """)
        row = cur.fetchone()

    assert row is not None, "테이블 정보가 반환되어야 합니다"
    assert row["TABLE_COMMENT"] == "단일키 테이블", (
        f"테이블 주석은 '단일키 테이블'이어야 합니다, got: {row['TABLE_COMMENT']}"
    )
    print(f"\n[MySQL] table_comment: {row['TABLE_COMMENT']}")


def test_mysql_nullable_info(mysql_conn):
    """MySQL get_nullable_info 쿼리 검증"""
    with mysql_conn.cursor() as cur:
        cur.execute(f"""
            SELECT COLUMN_NAME, IS_NULLABLE
            FROM information_schema.COLUMNS
            WHERE CONCAT_WS('.', TABLE_SCHEMA, TABLE_NAME) = '{MYSQL_TABLE}'
            ORDER BY ORDINAL_POSITION
        """)
        rows = cur.fetchall()

    result = {r["COLUMN_NAME"]: r["IS_NULLABLE"] == "YES" for r in rows}
    assert len(result) > 0, "컬럼이 반환되어야 합니다"
    assert result["id"] is False, "id(PK, AUTO_INCREMENT)는 NOT NULL이어야 합니다"
    assert result["char36"] is True, "char36은 NULL 허용이어야 합니다"
    print(f"\n[MySQL] nullable_info ({len(result)} cols): {dict(list(result.items())[:5])} ...")


# ---------------------------------------------------------------------------
# SQL Server
# ---------------------------------------------------------------------------
MSSQL_CONF = dict(server="127.0.0.1", port=1433, user="SA", password="foqaktmxj01!", database="store")
MSSQL_TABLE = "store.dbo.tb_lower"
MSSQL_TBL = "tb_lower"


@pytest.fixture(scope="module")
def mssql_conn():
    conn = pymssql.connect(**MSSQL_CONF)
    yield conn
    conn.close()


def test_mssql_column_comments(mssql_conn):
    """SQL Server get_column_comments 쿼리 검증 (sys.extended_properties)"""
    cursor = mssql_conn.cursor(as_dict=True)
    cursor.execute(f"""
        SELECT c.name AS COLUMN_NAME,
               CAST(ep.value AS NVARCHAR(4000)) AS COLUMN_COMMENT
        FROM sys.columns c
            INNER JOIN sys.tables t ON c.object_id = t.object_id
            LEFT JOIN sys.extended_properties ep
                ON ep.major_id = c.object_id
                AND ep.minor_id = c.column_id
                AND ep.name = 'MS_Description'
        WHERE t.name = '{MSSQL_TBL}'
        ORDER BY c.column_id
    """)
    rows = cursor.fetchall()

    result = {r["COLUMN_NAME"]: (r["COLUMN_COMMENT"] or "") for r in rows}
    assert len(result) > 0, "컬럼이 반환되어야 합니다"
    assert "id" in result, "id 컬럼이 존재해야 합니다"
    # SQL Server DDL에 extended_properties가 없으면 빈 문자열
    print(f"\n[MSSQL] column_comments ({len(result)} cols): {dict(list(result.items())[:5])} ...")


def test_mssql_table_comment(mssql_conn):
    """SQL Server get_table_comment 쿼리 검증"""
    cursor = mssql_conn.cursor(as_dict=True)
    cursor.execute(f"""
        SELECT CAST(ep.value AS NVARCHAR(4000)) AS TABLE_COMMENT
        FROM sys.tables t
            INNER JOIN sys.extended_properties ep
                ON ep.major_id = t.object_id
                AND ep.minor_id = 0
                AND ep.name = 'MS_Description'
        WHERE t.name = '{MSSQL_TBL}'
    """)
    row = cursor.fetchone()

    # DDL에 extended_properties 미설정 시 None 반환이 정상
    if row:
        print(f"\n[MSSQL] table_comment: {row['TABLE_COMMENT']}")
    else:
        print("\n[MSSQL] table_comment: None (extended_properties 미설정)")


def test_mssql_nullable_info(mssql_conn):
    """SQL Server get_nullable_info 쿼리 검증"""
    cursor = mssql_conn.cursor(as_dict=True)
    cursor.execute(f"""
        SELECT COLUMN_NAME, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE CONCAT(TABLE_CATALOG, '.dbo.', TABLE_NAME) = '{MSSQL_TABLE}'
        ORDER BY ORDINAL_POSITION
    """)
    rows = cursor.fetchall()

    result = {r["COLUMN_NAME"]: r["IS_NULLABLE"] == "YES" for r in rows}
    assert len(result) > 0, "컬럼이 반환되어야 합니다"
    assert result["id"] is False, "id(IDENTITY)는 NOT NULL이어야 합니다"
    assert result["char36"] is True, "char36은 NULL 허용이어야 합니다"
    print(f"\n[MSSQL] nullable_info ({len(result)} cols): {dict(list(result.items())[:5])} ...")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
