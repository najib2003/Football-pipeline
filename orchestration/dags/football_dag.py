# orchestration/dags/football_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys, os

sys.path.insert(0, '/opt/airflow/dags')
default_args = {
    'owner':            'football_pipeline',
    'depends_on_past':  False,
    'start_date':       datetime(2024, 1, 1),
    'retries':          3,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='football_daily_pipeline',
    default_args=default_args,
    description='Daily football data sync and transformation',
    schedule='0 6 * * *',   # Every day at 06:00 UTC
    catchup=False,
    tags=['football', 'bigdata'],
) as dag:

    # Task 1: Fetch latest data from API
    fetch_data = BashOperator(
        task_id='fetch_football_data',
        bash_command='docker exec football_ingestion python fetch_data.py',
    )

    # Task 2: Run Spark transformations
    run_spark = BashOperator(
        task_id='run_spark_transformations',
        bash_command=(
            'docker exec spark_master /opt/spark/bin/spark-submit '
            '--packages org.postgresql:postgresql:42.6.0 '
            '/opt/processing/spark_transform.py'
        ),
    )

    # Task 3: Validate data quality
    def validate_data_quality():
        import psycopg2
        conn = psycopg2.connect(
            dbname='football', user='admin',
            password='football123', host='postgres'
        )
        cur = conn.cursor()
        # Check that we have recent matches
        cur.execute("SELECT COUNT(*) FROM matches WHERE match_date > NOW() - INTERVAL '7 days'")
        recent = cur.fetchone()[0]

        # Check team stats were computed
        cur.execute('SELECT COUNT(*) FROM team_stats_agg')
        stats_count = cur.fetchone()[0]

        conn.close()
        print(f'Recent matches: {recent}, Teams with stats: {stats_count}')

        if stats_count == 0:
            raise ValueError('team_stats_agg is empty — Spark job may have failed')

        return {'recent_matches': recent, 'teams_computed': stats_count}

    validate = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
    )

    # Define the pipeline order
    fetch_data >> run_spark >> validate
