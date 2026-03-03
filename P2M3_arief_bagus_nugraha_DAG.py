'''
=================================================
Milestone 3 
Name  : Arief Bagus Nugraha
Batch : RMT-013

=================================================
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments untuk DAG
default_args = {
    'owner': 'arief_bagus',
    'start_date': datetime(2024, 11, 1), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definisi DAG
with DAG(
    dag_id='P2M3_arief_bagus_nugraha_DAG',
    default_args=default_args,
    # Cron: Menit 10,20,30 | Jam 9 | Setiap hari | Setiap bulan | Hari ke-6 (Sabtu)
    schedule='10,20,30 9 * * 6', 
    catchup=False,
    tags=['pyspark', 'mongodb'],
) as dag:
   
    # Task 1: Extract
    extract_task = BashOperator(
        task_id='extract_data',
        bash_command='python3 /home/jovyan/work/extract.py'
    )

    # Task 2: Transform
    transform_task = BashOperator(
        task_id='transform_data',
        bash_command='python3 /home/jovyan/work/transform.py'
    )

    # Task 3: Load
    load_task = BashOperator(
        task_id='load_data',
        bash_command='python3 /home/jovyan/work/load.py'
    )

    # Alur kerja (Dependency)
    extract_task >> transform_task >> load_task