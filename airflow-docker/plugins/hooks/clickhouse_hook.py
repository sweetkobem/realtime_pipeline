from airflow.hooks.base import BaseHook
import clickhouse_connect


class ClickHouseHook(BaseHook):
    def __init__(self, conn_id='clickhouse_default'):
        self.conn = BaseHook.get_connection(conn_id)
        self.client = clickhouse_connect.get_client(
            host=self.conn.host,
            port=self.conn.port or 8123,
            username=self.conn.login,
            password=self.conn.password,
            database=self.conn.schema or 'default'
        )

    def get_records(self, sql):
        """For SELECT"""
        try:
            result = self.client.query(sql)
            return result.result_rows
        except Exception as e:
            raise Exception(f"ClickHouse query failed: {str(e)}")

    def execute(self, sql: str):
        """For INSERT, CREATE, DROP, etc."""
        try:
            self.client.command(sql)
        except Exception as e:
            raise Exception(f"ClickHouse execute command failed: {str(e)}")
