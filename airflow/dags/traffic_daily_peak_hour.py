from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="traffic_daily_peak_hour",
    default_args=default_args,
    description="Compute daily peak hour per junction from HDFS",
    start_date=datetime(2026, 2, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    compute_daily_peak = SparkSubmitOperator(
        task_id="compute_daily_peak_hour",
        conn_id="spark_default",
        application="/opt/spark-apps/daily_peak_hour.py",
        conf={"spark.master": "spark://spark-master:7077"},
        name="daily_peak_hour_job",
    )

    compute_daily_peak
