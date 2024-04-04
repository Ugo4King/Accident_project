# accident.py

from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig


@dag(
    start_date=datetime(2024, 4, 3),
    schedule=None,
    catchup=False,
    tags=['accident'],
)
def accident():

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='include/dataset/vehicle_data.csv',
        dst='raw/traffic_data.csv',
        bucket='data_zoomcamp_ugo',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    upload_csv_vehicle_data = LocalFilesystemToGCSOperator(
        task_id='upload_csv_vehicle_data',
        src='include/dataset/vehicle_data.csv',
        dst='raw/vehicle_data.csv',
        bucket='data_zoomcamp_ugo',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )
    
    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='gcp',
    )

    gcs_to_raw = aql.load_file(
        task_id='gcs_to_raw',
        input_file=File(
            'gs://data_zoomcamp_ugo/raw/traffic_data.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='traffic_data',
            conn_id='gcp',
            metadata=Metadata(schema='retail')
        ),
        use_native_support=False,
    )
    gcs_to_bq = aql.load_file(
        task_id='gcs_to_bq',
        input_file=File(
            'gs://data_zoomcamp_ugo/raw/vehicle_data.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='vehicle_data',
            conn_id='gcp',
            metadata=Metadata(schema='retail')
        ),
        use_native_support=False,
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    check_load()

    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )
accident()