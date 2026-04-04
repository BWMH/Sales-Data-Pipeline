import boto3
from pendulum import datetime
from datetime import datetime as dt, timedelta
import os
import time
import json
import numpy as np

from airflow.sdk import dag, task, Asset
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import (
    leaguegamefinder,
    boxscoretraditionalv3,
    commonplayerinfo,
    teamdetails,
    leaguestandings,
    boxscoresummaryv3,
)
aws_raw_asset = Asset("AWS://NBA.RAW.NBA_FACT")
SEASON     = "2025-26"
SLEEP_TIME = 1


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
    dag_id="nba_facts_daily",
    start_date=datetime(year=2025, month=10, day=21, tz="UTC"),
     schedule="0 10 * * *",
    is_paused_upon_creation=False,
    catchup=True,
    max_active_runs=1,
    tags=['nba', 'aws'],
    default_args={
        'retries': 3,
        'retry_delay': 300,
    }
)
def nba_facts():

    @task
    def get_games(**context) -> dict:
        logical_date = context['logical_date']
        date         = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
        date_nodash  = date.replace("-", "")

        print(f"Fetching games for {date}...")
        gamefinder = leaguegamefinder.LeagueGameFinder(
            season_nullable=SEASON,
            date_from_nullable=date,
            date_to_nullable=date,
            league_id_nullable="00"
        )
        games_df = gamefinder.get_data_frames()[0]

        if games_df.empty:
            print(f"No games on {date}")
            return {"game_ids": [], "date_nodash": date_nodash}

        games_df = games_df.drop_duplicates(subset=["GAME_ID"])
        now      = dt.utcnow().isoformat()
        records  = []

        for _, row in games_df.iterrows():
            records.append({
                "game_id":    row["GAME_ID"],
                "game_date":  row["GAME_DATE"],
                "season":     SEASON,
                "matchup":    row["MATCHUP"],
                "team_id":    row["TEAM_ID"],
                "fetched_at": now,
            })

        print(f"{len(records)} games fetched")
        upload_to_s3(records, "games", date_nodash)
        return {
            "game_ids":   [r["game_id"] for r in records],
            "date_nodash": date_nodash
        }


    @task
    def get_box_scores(games_result: dict, **context) -> dict:
        game_ids    = games_result.get("game_ids", [])
        date_nodash = games_result.get("date_nodash")

        if not game_ids:
            print("No games to fetch box scores for")
            return {"count": 0, "date_nodash": date_nodash}

        print(f"Fetching box scores for {len(game_ids)} games...")
        records = []

        for i, game_id in enumerate(game_ids):
            try:
                now          = dt.utcnow().isoformat()
                box_score    = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
                player_stats = box_score.player_stats.get_data_frame()
                summary      = boxscoresummaryv3.BoxScoreSummaryV3(game_id=game_id)
                inactive_players = summary.inactive_players.get_data_frame()

                for _, row in player_stats.iterrows():
                    records.append({
                        "game_id":            row.get("gameId"),
                        "player_id":          row.get("personId"),
                        "first_name":         row.get("firstName"),
                        "family_name":        row.get("familyName"),
                        "team_id":            row.get("teamId"),
                        "team_city":          row.get("teamCity"),
                        "team_name":          row.get("teamName"),
                        "team_abbreviation":  row.get("teamTricode"),
                        "status":             "ACTIVE",
                        "jersey_number":      row.get("jerseyNum"),
                        "position":           row.get("position"),
                        "minutes":            row.get("minutes"),
                        "points":             row.get("points"),
                        "rebounds_offensive": row.get("reboundsOffensive"),
                        "rebounds_defensive": row.get("reboundsDefensive"),
                        "rebounds_total":     row.get("reboundsTotal"),
                        "assists":            row.get("assists"),
                        "steals":             row.get("steals"),
                        "blocks":             row.get("blocks"),
                        "turnovers":          row.get("turnovers"),
                        "fouls":              row.get("foulsPersonal"),
                        "fg_made":            row.get("fieldGoalsMade"),
                        "fg_attempted":       row.get("fieldGoalsAttempted"),
                        "fg_pct":             row.get("fieldGoalsPercentage"),
                        "fg3_made":           row.get("threePointersMade"),
                        "fg3_attempted":      row.get("threePointersAttempted"),
                        "fg3_pct":            row.get("threePointersPercentage"),
                        "ft_made":            row.get("freeThrowsMade"),
                        "ft_attempted":       row.get("freeThrowsAttempted"),
                        "ft_pct":             row.get("freeThrowsPercentage"),
                        "plus_minus":         row.get("plusMinusPoints"),
                        "fetched_at":         now,
                    })

                for _, row in inactive_players.iterrows():
                    record = {
                        "game_id":           game_id,
                        "player_id":         row.get("personId"),
                        "first_name":        row.get("firstName"),
                        "family_name":       row.get("familyName"),
                        "team_id":           row.get("teamId"),
                        "team_city":         row.get("teamCity"),
                        "team_name":         row.get("teamName"),
                        "team_abbreviation": row.get("teamTricode"),
                        "status":            "INACTIVE",
                        "minutes":           "00:00",
                        "fetched_at":        now,
                    }
                    for stat in ["points", "rebounds_offensive", "rebounds_defensive",
                                 "rebounds_total", "assists", "steals", "blocks",
                                 "turnovers", "fouls", "fg_made", "fg_attempted",
                                 "fg_pct", "fg3_made", "fg3_attempted", "fg3_pct",
                                 "ft_made", "ft_attempted", "ft_pct", "plus_minus"]:
                        record[stat] = 0
                    records.append(record)

                if (i + 1) % 10 == 0:
                    print(f"    ... {i + 1}/{len(game_ids)} games done")

                time.sleep(SLEEP_TIME)

            except Exception as e:
                print(f"  ⚠️  Skipping game {game_id}: {e}")
                time.sleep(SLEEP_TIME)

        print(f"{len(records)} box score rows fetched")
        upload_to_s3(records, "box_scores", date_nodash)
        return {"count": len(records), "date_nodash": date_nodash}


    @task
    def get_standings(**context) -> dict:
        logical_date = context['logical_date']
        date         = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
        date_nodash  = date.replace("-", "")

        print(f"Fetching standings for {SEASON}...")
        standings = leaguestandings.LeagueStandings(season=SEASON)
        df        = standings.standings.get_data_frame()
        now       = dt.utcnow().isoformat()
        records   = []

        for _, row in df.iterrows():
            records.append({
                "team_id":         row.get("TeamID"),
                "team_name":       row.get("TeamName"),
                "conference":      row.get("Conference"),
                "division":        row.get("Division"),
                "wins":            row.get("WINS"),
                "losses":          row.get("LOSSES"),
                "win_pct":         row.get("WinPCT"),
                "conference_rank": row.get("ConferenceRecord"),
                "home_record":     row.get("HOME"),
                "road_record":     row.get("ROAD"),
                "last_10":         row.get("L10"),
                "streak":          row.get("CurrentStreak"),
                "fetched_at":      now,
                "updated_at":      now,
            })

        print(f"{len(records)} standings fetched")
        upload_to_s3(records, "standings", date_nodash)
        return {"count": len(records)}
    
    @task(outlets=[aws_raw_asset])
    def pipeline_complete(box_scores_result: dict, standings_result: dict, **context) -> str:
        """
        Final task — waits for both box_scores AND standings to finish
        before emitting the asset to trigger downstream Snowflake DAG.
        """
        date_nodash = box_scores_result.get("date_nodash")

        context['outlet_events'][aws_raw_asset].extra = {"date": date_nodash}

        print(f"All facts uploaded for {date_nodash}")
        print(f"  Box scores: {box_scores_result['count']}")
        print(f"  Standings:  {standings_result['count']}")
        return f"Success: {date_nodash}"




   
    standings_result  = get_standings()
    games_result      = get_games()
    box_scores_result = get_box_scores(games_result)
    pipeline_complete(box_scores_result, standings_result)

   
nba_facts()