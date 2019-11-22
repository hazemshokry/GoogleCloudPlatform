import datetime
import airflow
from airflow import DAG
from airflow.operators import dummy_operator, bash_operator
from airflow.contrib.operators import gcs_to_bq


TOMORROW = datetime.datetime.now() + datetime.timedelta(days=1)

default_args = {
    'owner': 'Hazem Sayed',
    'depends_on_past': False,
    'email': ['hazemshokry91@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': TOMORROW,
}

dag = DAG('Airflow_Bigquery',
          default_args=default_args,
          description='Load and transform data from Google cloud storage to Google bigquery with Airflow',
        )

start_operator = dummy_operator.DummyOperator(task_id='Begin_execution',  dag=dag)

create_dataset = bash_operator.BashOperator(
        task_id='create_airflow_iot_dataset',
        bash_command='bq mk iot',
        dag=dag)


load_csv = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq',
    bucket='bucket1_hazem',
    source_objects=['heartRate-final.csv'],
    destination_project_dataset_table='iot.heartRateTable',
    trigger_rule='all_done',
    skip_leading_rows= 1,
    schema_fields=[
        {'name': 'sensorID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'uniqueID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'heartRate', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

start_operator >> create_dataset
create_dataset >> load_csv
