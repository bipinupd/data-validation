from airflow import utils, AirflowException
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta, timezone
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import os
import great_expectations as ge
from great_expectations.data_context.types.base import DataContextConfig
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator

default_args = {
    "owner": "Bipin",
    "start_date": utils.dates.days_ago(1)
}
table_name = "rideinfo"
schema = [
        {"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "tpep_pickup_datetime", "type": "STRING", "mode": "NULLABLE"},
        {"name": "tpep_dropoff_datetime", "type": "STRING", "mode": "NULLABLE"},
        {"name": "passenger_count", "type": "INTEGER", "mode": "NULLABLE"},       
        {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "RatecodeID", "type": "STRING", "mode": "NULLABLE"},
        {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "payment_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"}
        ]

project_config = DataContextConfig(
    config_version=2,
    plugins_directory=None,
    config_variables_file_path=None,
    datasources={
        "ride_db": {
            "data_asset_type": {
                "class_name": "SqlAlchemyDataset",
                "module_name": "great_expectations.dataset",
            },
            "class_name": "SqlAlchemyDatasource",
            "module_name": "great_expectations.datasource",
            "credentials": {
                "url": "bigquery://{}/ride_ds?credentials_path=/home/airflow/gcs/data/etl-sa.json".format(Variable.get("gcp_project"))
            },
        }
    },
    stores={
         "expectations_GCS_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleGCSStoreBackend",
                "project": Variable.get("gcp_project"),  
                "bucket": Variable.get("store_bucket"),
                "prefix": "expectations"
            }
        },
        "validations_GCS_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleGCSStoreBackend",
                "project": Variable.get("gcp_project"),
                "bucket": Variable.get("store_bucket"),
                "prefix": "validation"
            }
        },
        "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
    },
    expectations_store_name="expectations_GCS_store",
    validations_store_name="validations_GCS_store",
    evaluation_parameter_store_name="evaluation_parameter_store",
    data_docs_sites={
        "gs_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleGCSStoreBackend",
                "project": Variable.get("gcp_project"),
                "bucket": Variable.get("site_bucket"),
                "prefix": "",
            },
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
            "show_how_to_buttons": True,
        }
    },
    validation_operators={
        "action_list_operator": {
            "class_name": "ActionListValidationOperator",
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ],
        }
    },
    anonymous_usage_statistics={
      "enabled": True
    }
)

def validate_data(ds, **kwargs):
    dag_context = kwargs
    # Retrieve your data context
    context = ge.data_context.BaseDataContext(project_config=project_config)
    datasource_name = 'ride_db'
    batch_kwargs = {'table': '{}_temp'.format(table_name), 'datasource': datasource_name}
    expectation_suite_name = 'ride_ds-{}.warning'.format(table_name)
    run_id = {
        "run_name": dag_context['dag_run'].dag_id + ":" + dag_context['dag_run'].run_id ,  # insert your own run_name here
        "run_time": datetime.now(timezone.utc)
        }
    batch = context.get_batch(batch_kwargs, expectation_suite_name)
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch],
        run_id=run_id)
    if not results.success:
        raise AirflowException ("Validation not successful")


# The DAG definition
dag = DAG(
    dag_id='Validate_And_Load_Data',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval=timedelta(days=1))


delete_bq_table_if_exits = BigQueryTableDeleteOperator(
    task_id="delete_federated_table_if_exits",
    deletion_dataset_table="{}.ride_ds.{}_temp".format(Variable.get("gcp_project"), table_name),
    gcp_conn_id="etl-sa",
    ignore_if_missing=True,
    dag=dag)

gcs_to_bq_federated = GoogleCloudStorageToBigQueryOperator(
    task_id="gcs_to_bq_federated",
    bucket=Variable.get("raw_bucket"),
    source_objects=["inbox/*.csv"],
    destination_project_dataset_table="{}.ride_ds.{}_temp".format(Variable.get("gcp_project"), table_name),
    source_format="CSV",
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    schema_fields=schema,
    external_table=True,
    google_cloud_storage_conn_id="etl-sa",
    bigquery_conn_id="etl-sa",
    ignore_unknown_values=True,
    allow_jagged_rows=True,
    skip_leading_rows=1,
    dag=dag)

start =  DummyOperator(
    task_id='start',
    dag=dag)

validate_data = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag)

validate_data.doc_md= """\
    Call the python function `validate_data`. The function call great expectations related API 
    and uploads the validation result in GCS bucket. App Engine serves the web page.
    """

load_table_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id="load_table_to_bq",
    bucket=Variable.get("raw_bucket"),
    source_objects=["inbox/*.csv"],
    destination_project_dataset_table="{}.ride_ds.{}".format(Variable.get("gcp_project"), table_name),
    source_format="CSV",
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND',
    schema_fields=schema,
    ignore_unknown_values=True,
    allow_jagged_rows=True,
    google_cloud_storage_conn_id="etl-sa",
    bigquery_conn_id="etl-sa",
    skip_leading_rows=1,
    dag=dag)

move_object_to_archived = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id='move_object_to_archived',
    source_bucket=Variable.get("raw_bucket"),
    source_object='inbox/*.csv',
    destination_bucket=Variable.get("archive_bucket"),
    destination_object='archive/{}/'.format(datetime.utcnow()),
    google_cloud_storage_conn_id="etl-sa",
    move_object=True,
    dag=dag)

move_failed_object_to_failed_prefix = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id='move_failed_object_to_failed_prefix',
    source_bucket=Variable.get("raw_bucket"),
    source_object='inbox/*.csv',
    destination_bucket=Variable.get("raw_bucket"),
    destination_object='validation_failed/{}/'.format(datetime.utcnow()),
    google_cloud_storage_conn_id="etl-sa",
    trigger_rule=TriggerRule.ONE_FAILED,
    move_object=True,
    dag=dag)


start >> delete_bq_table_if_exits >> gcs_to_bq_federated >> validate_data 
validate_data >> load_table_to_bq >> move_object_to_archived
validate_data >> move_failed_object_to_failed_prefix