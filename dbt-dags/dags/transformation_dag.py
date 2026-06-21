import os
from datetime import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, InvocationMode, RenderConfig
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

def custom_handle_exception_subprocess(self, result):
    from airflow.exceptions import AirflowException
    try:
        from airflow.sdk.exceptions import AirflowSkipException
    except ImportError:
        from airflow.exceptions import AirflowSkipException
        
    if self.skip_exit_code is not None and result.exit_code == self.skip_exit_code:
        raise AirflowSkipException(f"dbt command returned exit code {self.skip_exit_code}. Skipping.")
    elif result.exit_code != 0:
        err_lines = []
        if hasattr(result, "full_output"):
            err_lines = result.full_output
        elif hasattr(result, "output"):
            err_lines = result.output
            
        # Get the last 30 lines of the dbt log to show the actual error
        log_snippet = "\n".join(err_lines[-30:]) if len(err_lines) > 30 else "\n".join(err_lines)
        
        raise AirflowException(
            f"dbt command failed with exit code {result.exit_code}.\n"
            f"--- DBT LOG SNIPPET ---\n{log_snippet}\n--- END DBT LOG ---"
        )

AbstractDbtLocalBase.calculate_openlineage_events_completes = custom_calc_ol_events
AbstractDbtLocalBase.get_openlineage_facets_on_failure = custom_get_ol_facets_on_failure
AbstractDbtLocalBase.handle_exception_subprocess = custom_handle_exception_subprocess

def emit_missing_openlineage_events(context):
    from airflow.models.dagrun import DagRun
    from airflow.utils.state import State
    from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter
    from openlineage.client.client import OpenLineageClient
    from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
    from airflow.hooks.base import BaseHook
    from airflow.utils import timezone
    import json
    import os
    import logging

    log = logging.getLogger("airflow.task")
    dag_run: DagRun = context.get('dag_run')
    if not dag_run:
        return

    # Find tasks that didn't run due to upstream failures
    with open("/tmp/callback.log", "a") as f:
        f.write(f"Callback executed for dag_run: {dag_run.run_id}\n")
        try:
            from sqlalchemy import create_engine, text
            from airflow.utils.state import State
            
            # Use explicit local astro postgres connection string
            conn_string = "postgresql://postgres:postgres@postgres:5432/postgres"
            engine = create_engine(conn_string)
            
            failed_tis = []
            with engine.connect() as conn:
                query = text("""
                    SELECT task_id, try_number, map_index, state
                    FROM task_instance
                    WHERE dag_id = :dag_id 
                      AND run_id = :run_id
                      AND state IN ('upstream_failed', 'skipped', 'failed')
                """)
                result = conn.execute(query, {"dag_id": dag_run.dag_id, "run_id": dag_run.run_id})
                failed_tis = [dict(row._mapping) for row in result]
                
            f.write(f"Found {len(failed_tis)} skipped/upstream_failed/failed tasks\n")
        except Exception as e:
            f.write(f"Error getting task instances: {e}\n")
            import traceback
            f.write(traceback.format_exc() + "\n")
            return
        
        if not failed_tis:
            return

    # Extract namespace from Airflow connection
    try:
        conn = BaseHook.get_connection("postgres_default")
        namespace = f"postgres://{conn.host}:{conn.port}"
    except Exception:
        namespace = "postgres://postgres:5432"

    # Read manifest.json for lineage info
    manifest_path = "/usr/local/airflow/dags/dbt_openlineage/target/manifest.json"
    manifest = {}
    if os.path.exists(manifest_path):
        with open(manifest_path, "r") as f:
            manifest = json.load(f)

    try:
        from openlineage.client.client import OpenLineageClient
        from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter
        
        ol_client = OpenLineageClient()
        adapter = OpenLineageAdapter()

        with open(manifest_path, "r") as f:
            manifest = json.load(f)
            
        nodes = manifest.get("nodes", {})
        
        with open("/tmp/callback.log", "a") as f:
            f.write(f"Will process {len(failed_tis)} tasks\n")
            
        for ti in failed_tis:
            task_id = ti["task_id"]
            if not task_id.startswith("dbt_transform."):
                continue
            
            parts = task_id.split(".")
            if len(parts) < 3:
                continue
            model_name = parts[-2]
            task_type = parts[-1] 
            
            if task_type not in ("run", "test"):
                continue
                
            model_node_id = f"model.dbt_openlineage.{model_name}"
            node = nodes.get(model_node_id)
            
            if not node:
                continue
                
            database = node.get("database", "postgres")
            schema = node.get("schema", "public")
            alias = node.get("alias", model_name)
            
            inputs = []
            outputs = []
            
            if task_type == "run":
                if "depends_on" in node and "nodes" in node["depends_on"]:
                    for upstream_id in node["depends_on"]["nodes"]:
                        up_node = manifest.get("nodes", {}).get(upstream_id) or manifest.get("sources", {}).get(upstream_id)
                        if up_node:
                            up_db = up_node.get("database", "postgres")
                            up_schema = up_node.get("schema", "public")
                            up_alias = up_node.get("identifier") or up_node.get("alias") or up_node.get("name")
                            
                            inputs.append(Dataset(
                                namespace=namespace,
                                name=f"{up_db}.{up_schema}.{up_alias}"
                            ))
                            
                outputs = [Dataset(
                    namespace=namespace,
                    name=f"{database}.{schema}.{alias}"
                )]
            elif task_type == "test":
                # For test tasks, the model itself is the input, and there are no outputs
                inputs = [Dataset(
                    namespace=namespace,
                    name=f"{database}.{schema}.{alias}"
                )]
            
            job_name = f"{dag_run.dag_id}.{task_id}"
            job_namespace = os.getenv("OPENLINEAGE_NAMESPACE", "default")
            
            logical_date = getattr(dag_run, "logical_date", None)
            if logical_date is None:
                logical_date = getattr(dag_run, "run_after", None)
            if logical_date is None:
                logical_date = getattr(dag_run, "execution_date", None)
            if logical_date is None:
                from airflow.utils import timezone
                logical_date = timezone.utcnow()
                
            run_id = adapter.build_task_instance_run_id(
                dag_id=dag_run.dag_id,
                task_id=task_id,
                try_number=ti["try_number"],
                logical_date=logical_date,
                map_index=ti["map_index"]
            )
            
            from datetime import timedelta
            
            event_time_dt = timezone.utcnow()
            start_time = (event_time_dt - timedelta(seconds=1)).isoformat()
            abort_time = event_time_dt.isoformat()
            
            terminal_state = RunState.FAIL if ti["state"] == "failed" else RunState.ABORT
            log.info(f"Emitting missing OpenLineage START/{terminal_state.name} for {job_name}")
            
            start_event = RunEvent(
                eventType=RunState.START,
                eventTime=start_time,
                run=Run(runId=run_id),
                job=Job(namespace=job_namespace, name=job_name),
                inputs=inputs,
                outputs=outputs,
                producer="https://github.com/apache/airflow/tree/providers-openlineage/custom-callback"
            )
            ol_client.emit(start_event)
            
            terminal_event = RunEvent(
                eventType=terminal_state,
                eventTime=abort_time,
                run=Run(runId=run_id),
                job=Job(namespace=job_namespace, name=job_name),
                inputs=inputs,
                outputs=outputs,
                producer="https://github.com/apache/airflow/tree/providers-openlineage/custom-callback"
            )
            ol_client.emit(terminal_event)
            
            with open("/tmp/callback.log", "a") as f:
                f.write(f"Successfully emitted for {job_name}\n")
                
    except Exception as e:
        with open("/tmp/callback.log", "a") as f:
            f.write(f"Error in processing: {e}\n")
            import traceback
            f.write(traceback.format_exc() + "\n")

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
    on_failure_callback=emit_missing_openlineage_events,
) as dag:

    # DbtTaskGroup automatically creates Airflow tasks for each dbt model
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        render_config=RenderConfig(
            should_detach_multiple_parents_tests=True
        ),
        operator_args={
            "install_deps": True, # Automatically runs `dbt deps` before running
        },
        execution_config=ExecutionConfig(
            invocation_mode=InvocationMode.SUBPROCESS
        )
    )

    dbt_transform
