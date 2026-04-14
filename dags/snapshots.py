from airflow.sdk import dag, Asset, task
from pendulum import datetime
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

snowflake_bronze_asset = Asset("nba_bronze_snowflake_load_complete")
snowflake_snapshots_asset = Asset("nba_snapshots_load_complete")

@dag(
    dag_id="nba_dbt_transform_snapshots",
    schedule=[snowflake_bronze_asset],
    catchup=False,
    max_active_runs=1,
    tags=['nba', 'dbt', 'snapshots']
)
def nba_dbt_transform_snapshots():

    snapshots = DbtTaskGroup(
        group_id="snapshots",
        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dbt/nba_pipeline",
        ),
        profile_config=ProfileConfig(
            profile_name="nba_pipeline",
            target_name="dev",
            profile_mapping=SnowflakeUserPasswordProfileMapping(
                conn_id="snowflake_nba_conn",
                profile_args={"schema": "SNAPSHOTS"},
            ),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/home/airflow/.local/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["path:snapshots"],
        ),
    )

    @task(outlets=[snowflake_snapshots_asset])
    def snapshots_complete():
        print("All snapshots built and tested successfully")
        return "success"

    snapshots >> snapshots_complete()   # ← runs after all bronze models finish

nba_dbt_transform_snapshots()