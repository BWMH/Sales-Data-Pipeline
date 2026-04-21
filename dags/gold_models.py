from airflow.sdk import dag, Asset, task
from pendulum import datetime
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

snowflake_silver_asset = Asset("nba_silver_load_complete")

@dag(
    dag_id="nba_dbt_transform_gold",
    schedule=[snowflake_silver_asset],
    catchup=False,
    max_active_runs=1,
    tags=['nba', 'dbt', 'gold']
)
def nba_dbt_transform_gold():

    gold_models = DbtTaskGroup(
        group_id="gold_models",
        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dbt/nba_pipeline",
        ),
        profile_config=ProfileConfig(
            profile_name="nba_pipeline",
            target_name="dev",
            profile_mapping=SnowflakeUserPasswordProfileMapping(
                conn_id="snowflake_nba_conn",
                profile_args={"schema": "GOLD"},
            ),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/home/airflow/.local/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["path:models/gold", "path:seeds"],
        ),
    )

    @task
    def gold_complete():
        print("All gold models built and tested successfully")
        return "success"

    gold_models >> gold_complete()   # ← runs after all bronze models finish

nba_dbt_transform_gold()