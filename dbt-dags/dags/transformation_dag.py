import os
from datetime import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, InvocationMode
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.operators.local import AbstractDbtLocalBase

# Monkey-patch Cosmos to emit OpenLineage on failure.
# By default, Cosmos drops OpenLineage data (including dataset assertions) when a dbt test fails.
from airflow.providers.openlineage.extractors.base import OperatorLineage

def custom_calc_ol_events(self, env, project_dir, dbt_command_line=None):
    import os, inspect
    from openlineage.common.provider.dbt.local import DbtLocalArtifactProcessor
    from cosmos.constants import OPENLINEAGE_PRODUCER
    from cosmos.settings import LINEAGE_NAMESPACE
    
    for key, value in env.items():
        os.environ[key] = str(value)

    processor_kwargs = dict(
        producer=OPENLINEAGE_PRODUCER,
        job_namespace=LINEAGE_NAMESPACE,
        project_dir=project_dir,
        profile_name=self.profile_config.profile_name,
        target=self.profile_config.target_name,
    )
    sig = inspect.signature(DbtLocalArtifactProcessor.__init__)
    if "dbt_command_line" in sig.parameters and dbt_command_line is not None:
        processor_kwargs["dbt_command_line"] = dbt_command_line
    openlineage_processor = DbtLocalArtifactProcessor(**processor_kwargs)
    openlineage_processor.should_raise_on_unsupported_command = False
    
    try:
        events = openlineage_processor.parse()
        self.openlineage_events_completes = events.completes
        # Monkey-patch: Also save fails!
        self.openlineage_events_fails = events.fails
    except Exception as e:
        self.log.debug("Unable to parse OpenLineage events", stack_info=True)

def custom_get_ol_facets_on_failure(self, task_instance) -> OperatorLineage:
    inputs = []
    outputs = []
    run_facets = {}
    job_facets = {}
    
    events_fails = None
    if hasattr(self, "openlineage_events_fails"):
        events_fails = self.openlineage_events_fails
    elif hasattr(task_instance, "openlineage_events_fails"):
        events_fails = task_instance.openlineage_events_fails

    if events_fails:
        for failed in events_fails:
            [inputs.append(input_) for input_ in failed.inputs if input_ not in inputs]
            [outputs.append(output) for output in failed.outputs if output not in outputs]
            run_facets = {**run_facets, **failed.run.facets}
            job_facets = {**job_facets, **failed.job.facets}

    return OperatorLineage(
        inputs=inputs,
        outputs=outputs,
        run_facets=run_facets,
        job_facets=job_facets,
    )

AbstractDbtLocalBase.calculate_openlineage_events_completes = custom_calc_ol_events
AbstractDbtLocalBase.get_openlineage_facets_on_failure = custom_get_ol_facets_on_failure

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
        profile_args={"schema": "analytics"}
    )
)

with DAG(
    dag_id="dbt_transformation_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "openlineage"],
) as dag:

    # DbtTaskGroup automatically creates Airflow tasks for each dbt model
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        operator_args={
            "install_deps": True, # Automatically runs `dbt deps` before running
        },
        execution_config=ExecutionConfig(
            invocation_mode=InvocationMode.SUBPROCESS
        )
    )

    dbt_transform
