import re
import os
import pendulum

from hooks.clickhouse_hook import ClickHouseHook
from utils import cleaner


def get_sql_file(sql_dir):
    result = {}

    for filename in sorted(os.listdir(sql_dir)):
        if filename.endswith(".sql"):
            with open(os.path.join(sql_dir, filename), "r") as f:
                name = filename.replace(".sql", "")
                result[name] = f.read()

    return result


def find_upstream_task_from_sql(sql, current_table):
    # Normalize whitespace and case
    sql = re.sub(r'\s+', ' ', sql.strip()).lower()
    result = set()

    # Match FROM or JOIN followed by table name (ignore alias)
    pattern = r'(?:from|join)\s+([a-zA-Z0-9_.]+)'
    matches = re.findall(pattern, sql)

    # Filter and normalize
    for match in matches:
        cleaned = match.replace('_cold', '')
        if cleaned != current_table:
            result.add(cleaned)

    return sorted(result)


def store_to_bronze_cold(table, **kwargs):
    return True
    execution_date = kwargs["ds"]
    partition_date = \
        pendulum.parse(execution_date).format("YYYYMMDD")
    ch_hook = ClickHouseHook()

    # DELETE data in cold table if exist based on partition_date
    ch_hook.execute(f"""
        ALTER TABLE bronze_cold_{table} DROP PARTITION '{partition_date}';
    """)

    query = f"""
        INSERT INTO bronze_cold_{table}
        SELECT
            *
        FROM bronze_hot_{table}
        WHERE DATE(updated_at) = DATE('{execution_date}')
    """
    print(query)
    ch_hook.execute(query)

    # DELETE data in hot table based on partition_date
    ch_hook.execute(f"""
        ALTER TABLE bronze_hot_{table} DROP PARTITION '{partition_date}';
    """)

    return "Successfully processed Bronze table."


def transform_bronze_to_silver(table, **kwargs):
    return True
    execution_date = kwargs["ds"]
    ch_hook = ClickHouseHook()

    query = f"""
        SELECT name, type
        FROM system.columns
        WHERE table = 'bronze_cold_{table}'
        """
    get_column = ch_hook.get_records(query)
    column_names = cleaner.apply_cleaner(get_column)

    query = f"""
        INSERT INTO silver_{table}
        SELECT
            {', '.join(column_names)}
        FROM bronze_cold_{table}
        WHERE DATE(updated_at) = DATE('{execution_date}')
    """
    print(query)
    ch_hook.execute(query)

    return "Successfully processed Silver table."


def transform_gold_dim(table, query, **kwargs):
    execution_date = kwargs["ds"]
    ch_hook = ClickHouseHook()

    query_last_date = f"""
        SELECT MAX(start_date) last_date
        FROM {table}
        """
    get_data = ch_hook.get_records(query_last_date)
    if get_data:
        last_date = get_data[0][0]

        # Raise error if Last Date >= Execution Date
        if str(last_date) >= execution_date:
            raise Exception("Last Date >= Execution Date")

    # Execute SQL
    query = query.replace('{execution_date}', execution_date)
    print(query)
    ch_hook.execute(query)

    # Deduplication
    ch_hook.execute(f"OPTIMIZE TABLE {table} FINAL;")

    return "Successfully processed Dim table."


def transform_gold_fact(table, query, **kwargs):
    execution_date = kwargs["ds"]
    ch_hook = ClickHouseHook()

    query_last_date = f"""
        SELECT date_key AS last_date
        FROM {table}
        WHERE date_key = toDate('{execution_date}')
        """
    get_data = ch_hook.get_records(query_last_date)
    if get_data:
        last_date = get_data[0][0]

        # Raise error if Last Date >= Execution Date
        if str(last_date) == execution_date:
            raise Exception("This date already processed.")

    # Execute SQL
    query = query.replace('{execution_date}', execution_date)
    print(query)
    ch_hook.execute(query)

    return "Successfully processed Fact table."
