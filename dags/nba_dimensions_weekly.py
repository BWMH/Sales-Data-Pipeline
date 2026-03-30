import boto3
from pendulum import datetime
from datetime import datetime as dt, timedelta
import os
import time
import json
import numpy as np

from airflow.sdk import dag, task
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import (
    leaguegamefinder,
    boxscoretraditionalv3,
    commonplayerinfo,
    teamdetails,
    leaguestandings,
    boxscoresummaryv3,
)

SEASON     = "2025-26"
SLEEP_TIME = 0.6


def upload_to_s3(records: list, folder_name: str, date_str: str):
    if not records:
        print(f"Skipping upload — no records for {folder_name}")
        return

    AWS_REGION = os.getenv('AWS_REGION')
    AWS_BUCKET = os.getenv('S3_BUCKET_NEW')
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


@dag(
    dag_id="nba_dimensions_weekly",
    start_date=datetime(year=2026, month=3, day=22, tz="UTC"),
     schedule="0 0 * * 0",
    is_paused_upon_creation=False,
    catchup=False,
    max_active_runs=1,
    tags=['nba', 'dimension'],
    default_args={
        'retries': 1,
        'retry_delay': 300,
    }
)
def nba_dimensions():

    @task
    def get_teams(**context) -> dict:
        logical_date = context['logical_date']
        date         = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
        date_nodash  = date.replace("-", "")

        print("Fetching teams...")
        all_teams = teams.get_teams()
        now       = dt.utcnow().isoformat()
        records   = []

        for team in all_teams:
            records.append({
                "team_id":      team["id"],
                "team_name":    team["full_name"],
                "abbreviation": team["abbreviation"],
                "nickname":     team["nickname"],
                "city":         team["city"],
                "state":        team["state"],
                "year_founded": team["year_founded"],
                "fetched_at":   now,
                "updated_at":   now,
            })

        print(f"{len(records)} teams fetched")
        upload_to_s3(records, "teams", date_nodash)
        return {"count": len(records), "date_nodash": date_nodash}


    @task
    def get_team_details(**context) -> dict:
        logical_date = context['logical_date']
        date         = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
        date_nodash  = date.replace("-", "")

        print("Fetching team details...")
        all_teams = teams.get_teams()
        records   = []

        for team in all_teams:
            try:
                details    = teamdetails.TeamDetails(team_id=team["id"])
                background = details.team_background.get_data_frame()

                if not background.empty:
                    row = background.iloc[0]
                    now = dt.utcnow().isoformat()
                    records.append({
                        "team_id":             team["id"],
                        "abbreviation":        row.get("ABBREVIATION"),
                        "nickname":            row.get("NICKNAME"),
                        "year_founded":        row.get("YEARFOUNDED"),
                        "city":                row.get("CITY"),
                        "arena":               row.get("ARENA"),
                        "arena_capacity":      row.get("ARENACAPACITY"),
                        "owner":               row.get("OWNER"),
                        "general_manager":     row.get("GENERALMANAGER"),
                        "head_coach":          row.get("HEADCOACH"),
                        "dleague_affiliation": row.get("DLEAGUEAFFILIATION"),
                        "fetched_at":          now,
                        "updated_at":          now,
                    })

                time.sleep(SLEEP_TIME)

            except Exception as e:
                print(f"Skipping team {team['full_name']}: {e}")
                time.sleep(SLEEP_TIME)

        print(f"{len(records)} team details fetched")
        upload_to_s3(records, "team_details", date_nodash)
        return {"count": len(records)}


    @task
    def get_player_details(**context) -> dict:
        logical_date = context['logical_date']
        date         = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
        date_nodash  = date.replace("-", "")

        print("Fetching active players...")
        active_players = players.get_active_players()
        player_ids     = [p["id"] for p in active_players]

        print(f"Fetching details for {len(player_ids)} players...")
        records = []

        for i, player_id in enumerate(player_ids):
            try:
                info    = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
                details = info.common_player_info.get_data_frame()

                if not details.empty:
                    row = details.iloc[0]
                    now = dt.utcnow().isoformat()
                    records.append({
                        "player_id":         player_id,
                        "full_name":         row.get("DISPLAY_FIRST_LAST"),
                        "team_id":           row.get("TEAM_ID"),
                        "team_name":         row.get("TEAM_NAME"),
                        "team_abbreviation": row.get("TEAM_ABBREVIATION"),
                        "jersey_number":     row.get("JERSEY"),
                        "position":          row.get("POSITION"),
                        "height":            row.get("HEIGHT"),
                        "weight":            row.get("WEIGHT"),
                        "birthdate":         row.get("BIRTHDATE"),
                        "country":           row.get("COUNTRY"),
                        "draft_year":        row.get("DRAFT_YEAR"),
                        "draft_round":       row.get("DRAFT_ROUND"),
                        "draft_number":      row.get("DRAFT_NUMBER"),
                        "from_year":         row.get("FROM_YEAR"),
                        "to_year":           row.get("TO_YEAR"),
                        "fetched_at":        now,
                        "updated_at":        now,
                    })

                if (i + 1) % 50 == 0:
                    print(f"    ... {i + 1}/{len(player_ids)} players done")

                time.sleep(SLEEP_TIME)

            except Exception as e:
                print(f"Skipping player {player_id}: {e}")
                time.sleep(SLEEP_TIME)

        print(f"{len(records)} player details fetched")
        upload_to_s3(records, "player_details", date_nodash)
        return {"count": len(records)}


    get_teams()
    get_team_details()
    get_player_details()

nba_dimensions()