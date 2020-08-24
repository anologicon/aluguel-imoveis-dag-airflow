# airflow related
from airflow import models
from airflow import DAG
from operators import DataSourceToCsv,CsvToStorage

# other packages
from datetime import datetime, timedelta

gcs_to_bq = None  # type: Any
try:
    from airflow.contrib.operators import gcs_to_bq
except ImportError:
    pass

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately       when it is
    # detected in the Cloud Storage bucket.
    # set your start_date : airflow will run previous dags if dags #since startdate has not run
#notify email is a python function that sends notification email upon failure    
    'start_date': datetime(2020, 8, 10, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'project_id' : 'aluguel-data-project',
    'retries': 1,
    'on_failure_callback': '',
    'retry_delay': timedelta(minutes=5),
}
with models.DAG(
    dag_id='data_get_imoveis',
    # Continue to run DAG once per day
    schedule_interval = timedelta(hours=12),
    catchup = True,
    default_args=default_dag_args) as dag:

    task1 = DataSourceToCsv(
        task_id='fetch_from_scrap'
    )

    task2 = CsvToStorage(
        task_id = 'save_imoveis'
    )

    load_csv = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='cloud_to_bigquery',
        bucket='aluguel-data-scraper',
        source_format='CSV',
        source_objects=['alugueis.csv'],
        destination_project_dataset_table='alugueis.alugueis',
        schema_fields=[
            {'name': 'Valor', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'TipoAluguel', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Iptu', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Area', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Quartos', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Banheiros', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Vagas', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'Cidade', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Endereco', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=True,
        dag=dag
    )

    #fluxo de execução
    task1
    task2.set_upstream([task1])
    load_csv.set_upstream([task2])
