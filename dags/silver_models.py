from airflow.sdk import dag, Asset, task
from pendulum import datetime
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

snowflake_snapshots_asset = Asset("nba_snapshots_load_complete")
snowflake_silver_asset = Asset("nba_silver_load_complete")

@dag(
    dag_id="nba_dbt_transform_silver",
    schedule=[snowflake_snapshots_asset],
    catchup=False,
    max_active_runs=1,
    tags=['nba', 'dbt', 'silver']
)
def nba_dbt_transform_silver():

    silver_models = DbtTaskGroup(
        group_id="silver_models",
        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dbt/nba_pipeline",
        ),
        profile_config=ProfileConfig(
            profile_name="nba_pipeline",
            target_name="dev",
            profile_mapping=SnowflakeUserPasswordProfileMapping(
                conn_id="snowflake_nba_conn",
                profile_args={"schema": "SILVER"},
            ),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/home/airflow/.local/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["path:models/silver"],
        ),
    )

    @task(outlets=[snowflake_silver_asset])
    def silver_complete():
        print("All silver models built and tested successfully")
        return "success"

    silver_models >> silver_complete()   # ← runs after all bronze models finish

nba_dbt_transform_silver()