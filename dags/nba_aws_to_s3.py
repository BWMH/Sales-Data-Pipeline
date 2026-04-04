from airflow.sdk import dag, task, Asset
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime
from datetime import timedelta

aws_raw_asset = Asset("AWS://NBA.RAW.NBA_FACT")
snowflake_raw_asset = Asset("snowflake://NBA.RAW.NBA_RAW")
@dag(
    dag_id="nba_s3_to_snowflake",
    start_date=datetime(2025, 10, 21, tz="UTC"),
    # schedule="@daily",
    schedule=[aws_raw_asset],
    catchup=True,
    max_active_runs=1,
    tags=['nba', 'aws', 'snowflake']
)
def nba_s3_to_snowflake():
    
    @task(outlets=[snowflake_raw_asset])
    def load_to_snowflake(**context):
        asset_events = context['triggering_asset_events']
        event        = asset_events[aws_raw_asset][0]
        ds_nodash    = event.extra['date']
        folders_list = ['box_scores','games','player_details','standings','team_details','teams']
        ds_nodash = (context['logical_date'] - timedelta(days=1)).strftime('%Y%m%d')
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_nba_conn')
        for table_name in folders_list:
            sql = f"""
            COPY INTO NBA.RAW.{table_name.upper()} (raw_json, file_name)
            FROM (
                SELECT 
                    $1 AS raw_json,
                    METADATA$FILENAME AS file_name
                FROM @NBA.RAW.NBA_S3_STAGE/source/{table_name}/date={ds_nodash}/
            )
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE'
            FORCE = TRUE;
            """
            
            hook.run(sql, autocommit=True)
            print(f"Loaded {table_name} data for {ds_nodash}")

        
        return f"Success: {ds_nodash}"
    
    load_to_snowflake()

nba_s3_to_snowflake()