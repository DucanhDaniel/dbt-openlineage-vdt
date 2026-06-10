import os
from datetime import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Path to the dbt project inside the Airflow container
# In Astro, files in the local 'dags' folder are mounted to '/usr/local/airflow/dags'
DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt_openlineage"

# Define the Cosmos profile configuration
# We use the PostgresUserPasswordProfileMapping to map Airflow connection to dbt profile
profile_config = ProfileConfig(
    profile_name="dbt_openlineage",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default", # This is the Airflow connection ID we will create
        profile_args={"schema": "public"}
    )
)

with DAG(
    dag_id="dbt_transformation_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "openlineage"],
) as dag:

    # DbtTaskGroup automatically creates Airflow tasks for each dbt model
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            # Tells Cosmos to run the dbt command via dbt-ol (OpenLineage wrapper)
            dbt_executable_path="dbt-ol",
        ),
        operator_args={
            "install_deps": True, # Automatically runs `dbt deps` before running
        }
    )

    dbt_transform
