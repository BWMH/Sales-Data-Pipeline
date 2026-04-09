from airflow.sdk import dag, Asset, task
from pendulum import datetime
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

snowflake_raw_asset    = Asset("nba_raw_snowflake_load_complete")
snowflake_bronze_asset = Asset("nba_bronze_snowflake_load_complete")

@dag(
    dag_id="nba_dbt_transform_bronze",
    schedule=[snowflake_raw_asset],
    catchup=False,
    max_active_runs=1,
    tags=['nba', 'dbt', 'bronze']
)
def nba_dbt_transform_bronze():

    bronze_models = DbtTaskGroup(
        group_id="bronze_models",
        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dbt/nba_pipeline",
        ),
        profile_config=ProfileConfig(
            profile_name="nba_pipeline",
            target_name="dev",
            profile_mapping=SnowflakeUserPasswordProfileMapping(
                conn_id="snowflake_nba_conn",
                profile_args={"schema": "BRONZE"},
            ),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/home/airflow/.local/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["path:models/bronze"],
        ),
    )

    @task(outlets=[snowflake_bronze_asset])
    def bronze_complete():
        print("All bronze models built and tested successfully")
        return "success"

    bronze_models >> bronze_complete()   # ← runs after all bronze models finish

nba_dbt_transform_bronze()