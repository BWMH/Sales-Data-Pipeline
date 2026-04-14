from nba_api.stats.endpoints import leaguegamefinder
import pandas as pd
import boto3
from pendulum import datetime
from datetime import datetime as dt, timedelta
import os
import time
import json
import numpy as np
from dotenv import load_dotenv
from pathlib import Path

SEASON     = "2025-26"
SLEEP_TIME = 1
base_dir = Path(__file__).resolve().parent
env_file = base_dir / ".env"

load_dotenv(dotenv_path=env_file)

def upload_to_s3(records: list, folder_name: str, date_str: str):
    if not records:
        print(f"Skipping upload — no records for {folder_name}")
        return

    AWS_REGION = os.getenv('AWS_REGION')
    print(AWS_REGION)
    AWS_BUCKET = os.getenv('S3_BUCKET_NEW')
    print(AWS_BUCKET)
    s3_client  = boto3.client('s3', region_name=AWS_REGION)
    file_key   = f"source/{folder_name}/date={date_str}/{folder_name}.json"

    def json_converter(obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return str(obj)

    s3_client.put_object(
        Bucket=AWS_BUCKET,
        Key=file_key,
        Body=json.dumps(records, default=json_converter),
        ContentType='application/json'
    )
    print(f"Uploaded {folder_name} → s3://{AWS_BUCKET}/{file_key}")


# 1. Fetch the ENTIRE season at once
gamefinder = leaguegamefinder.LeagueGameFinder(
    season_nullable="2025-26",
    league_id_nullable="00"
)
all_games = gamefinder.get_data_frames()[0]

# 2. Group by date and upload to S3
for game_date, group in all_games.groupby('GAME_DATE'):
    date_nodash = game_date.replace("-", "")
    records = []
    
    for _, row in group.iterrows():
        records.append({
            "game_id":    row["GAME_ID"],
            "game_date":  row["GAME_DATE"],
            "season":     SEASON,
            "matchup":    row["MATCHUP"],
            "team_id":    row["TEAM_ID"],
            "pts":        row["PTS"],      
            "plus_minus": row["PLUS_MINUS"], 
            "wl":         row["WL"], 
            "fetched_at": "2026-04-12T00:00:00" # Backfill marker
        })
    
    # Call your existing upload_to_s3 function
    upload_to_s3(records, "games", date_nodash)