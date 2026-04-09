from airflow.sdk import dag, task, Asset  
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime
from datetime import timedelta

aws_raw_asset = Asset("nba_raw_s3_upload_complete")
snowflake_raw_asset = Asset("nba_raw_snowflake_load_complete")
@dag(
    dag_id="nba_s3_to_snowflake",
    start_date=datetime(2025, 10, 21, tz="UTC"),
    schedule="0 11 * * *",
    # schedule=[aws_raw_asset],
    catchup=False,
    max_active_runs=1,
    tags=['nba', 'aws', 'snowflake']
)
def nba_s3_to_snowflake():
    
    @task(outlets=[snowflake_raw_asset])
    def load_to_snowflake(**context):
        folders_list = ['box_scores','games','player_details','standings','team_details','teams']
        # ds_nodash = (context['data_interval_start'] - timedelta(days=1)).strftime('%Y%m%d')
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