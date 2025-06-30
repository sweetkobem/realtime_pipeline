from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
import os

from utils import general

# List of source tables
SOURCE_TABLES = [
    "ecommerce_category", "ecommerce_product", "ecommerce_user",
    "ecommerce_order", "ecommerce_order_item", "ecommerce_payment",
    "ecommerce_shipping", "ecommerce_product_review"
]
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
DAG_ID = 'ecommerce_pipeline'
SQL_PATH = f"{AIRFLOW_HOME}/config/sql_script/{DAG_ID}"

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule='0 0 * * *',
    catchup=True,
    start_date=pendulum.datetime(2025, 6, 12, 17, 30, 00, tz="UTC"),
    tags=["bronze", "silver", "gold"],
) as dag:

    terminal_tasks = []

    # Generate tasks Bronze and Silver per table
    for table in SOURCE_TABLES:
        extract_task = PythonOperator(
            task_id=f"bronze_{table}",
            python_callable=general.store_to_bronze_cold,
            op_kwargs={"table": table},
        )

        silver_task = PythonOperator(
            task_id=f"silver_{table}",
            python_callable=general.transform_bronze_to_silver,
            op_kwargs={"table": table},
        )

        extract_task >> silver_task
        terminal_tasks.append(silver_task)

    # Generate tasks Dim table
    sql_file = general.get_sql_file(f"{SQL_PATH}/gold_dim")
    for name, query in sql_file.items():
        upstreams = general.find_upstream_task_from_sql(query, name)
        dim_task = PythonOperator(
            task_id=name,
            python_callable=general.transform_gold_dim,
            op_kwargs={"table": name, "query": query},
            depends_on_past=True,
        )

        if upstreams:
            for task_id in upstreams:
                dag.get_task(task_id) >> dim_task
        else:
            dim_task

        terminal_tasks.append(dim_task)

    # Generate tasks fact table
    sql_file = general.get_sql_file(f"{SQL_PATH}/gold_fact")
    for name, query in sql_file.items():
        upstreams = general.find_upstream_task_from_sql(query, name)
        fact_task = PythonOperator(
            task_id=name,
            python_callable=general.transform_gold_fact,
            op_kwargs={"table": name, "query": query},
            depends_on_past=True,
        )

        if upstreams:
            for task_id in upstreams:
                dag.get_task(task_id) >> fact_task
        else:
            fact_task

        terminal_tasks.append(fact_task)

    # Generate tasks aggregated table
    sql_file = general.get_sql_file(f"{SQL_PATH}/gold_aggregated")
    for name, query in sql_file.items():
        upstreams = general.find_upstream_task_from_sql(query, name)
        aggregated_task = PythonOperator(
            task_id=name,
            python_callable=general.transform_gold_fact,
            op_kwargs={"table": name, "query": query},
            depends_on_past=True,
        )

        if upstreams:
            for task_id in upstreams:
                dag.get_task(task_id) >> aggregated_task
        else:
            aggregated_task

        terminal_tasks.append(aggregated_task)

end = EmptyOperator(task_id="end", dag=dag)
for task in terminal_tasks:
    task >> end
