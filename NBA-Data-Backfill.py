import json
import time
import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import boto3

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
load_dotenv()
print(f"Region is:{os.getenv('AWS_REGION')}")
print(f"Bucket is:{os.getenv('S3_BUCKET_NEW')}")
def upload_to_s3(df, folder_name, date_str):
    if df.empty:
        return
    
    AWS_REGION = os.getenv('AWS_REGION')
    AWS_BUCKET = os.getenv('S3_BUCKET_NEW')
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    
    file_key = f"source/{folder_name}/date={date_str}/{folder_name}.json"
    
    s3_client.put_object(
        Bucket=AWS_BUCKET,
        Key=file_key,
        Body=df.to_json(orient="records"),
        ContentType='application/json'
    )
    print(f"  📤 Uploaded {folder_name} to S3: {file_key}")

def get_teams() -> pd.DataFrame:
    print("Fetching teams...")
    all_teams = teams.get_teams()
    now       = datetime.utcnow().isoformat()
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

    print(f"  ✅ {len(records)} teams fetched")
    return pd.DataFrame(records)


def get_team_details() -> pd.DataFrame:
    print("Fetching team details...")
    all_teams = teams.get_teams()
    records   = []

    for team in all_teams:
        try:
            details = teamdetails.TeamDetails(team_id=team["id"])
            background = details.team_background.get_data_frame()

            if not background.empty:
                row = background.iloc[0]
                now = datetime.utcnow().isoformat()
                records.append({
                    "team_id":             team["id"],
                    "team_name":           row.get("TEAM_NAME"),
                    "conference":          row.get("TEAM_CONFERENCE"),
                    "division":            row.get("TEAM_DIVISION"),
                    "arena":               row.get("ARENA"),
                    "arena_capacity":      row.get("ARENACAPACITY"),
                    "owner":               row.get("OWNER"),
                    "head_coach":          row.get("HEADCOACH"),
                    "dleague_affiliation": row.get("DLEAGUEAFFILIATION"),
                    "fetched_at":          now,
                    "updated_at":          now, 
                })

            time.sleep(SLEEP_TIME)

        except Exception as e:
            print(f"  ⚠️  Skipping team {team['full_name']}: {e}")
            time.sleep(SLEEP_TIME)

    print(f"  ✅ {len(records)} team details fetched")
    return pd.DataFrame(records)


def get_player_details() -> pd.DataFrame:
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
                now = datetime.utcnow().isoformat()
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

    print(f"  ✅ {len(records)} player details fetched")
    return pd.DataFrame(records)


def get_games(date: str, season: str = SEASON) -> pd.DataFrame:
    """
    Get games for a specific date.
    date format: "YYYY-MM-DD"
    Returns empty DataFrame if no games that day.
    """
    print(f"Fetching games for {date}...")

    gamefinder = leaguegamefinder.LeagueGameFinder(
        season_nullable=season,
        date_from_nullable=date,
        date_to_nullable=date,
        league_id_nullable="00"    # 00 = NBA
    )
    games_df = gamefinder.get_data_frames()[0]

    if games_df.empty:
        print(f"  ℹ️  No games on {date}")
        return pd.DataFrame()


    games_df = games_df.drop_duplicates(subset=["GAME_ID"])
    now      = datetime.utcnow().isoformat()

    records = []
    for _, row in games_df.iterrows():
        records.append({
            "game_id":    row["GAME_ID"],
            "game_date":  row["GAME_DATE"],
            "season":     season,
            "matchup":    row["MATCHUP"],
            "team_id":    row["TEAM_ID"],
            "fetched_at": now,
        })

    print(f"  ✅ {len(records)} games fetched")
    return pd.DataFrame(records)


def get_box_scores(game_ids: list) -> pd.DataFrame:
    if not game_ids:
        print("  ℹ️  No games to fetch box scores for")
        return pd.DataFrame()

    print(f"Fetching box scores for {len(game_ids)} games...")
    records = []

    for i, game_id in enumerate(game_ids):
        try:
            box_score    = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
            player_stats = box_score.player_stats.get_data_frame()
            now          = datetime.utcnow().isoformat()
            summary = boxscoresummaryv3.BoxScoreSummaryV3(game_id=game_id)
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
                    "status":            "ACTIVE",
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
                inactive_record = {
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
                    "fetched_at":         now,
                }
                
                stats_to_zero = [
                    "points", "rebounds_offensive", "rebounds_defensive", "rebounds_total",
                    "assists", "steals", "blocks", "turnovers", "fouls", "fg_made", 
                    "fg_attempted", "fg_pct", "fg3_made", "fg3_attempted", "fg3_pct", 
                    "ft_made", "ft_attempted", "ft_pct", "plus_minus"
                ]
                
                # Fill them with 0 so the DataFrame is perfectly uniform
                for stat in stats_to_zero:
                    inactive_record[stat] = 0
                    
                inactive_record["fetched_at"] = now
                records.append(inactive_record)
            if (i + 1) % 10 == 0:
                print(f"    ... {i + 1}/{len(game_ids)} games done")

            time.sleep(SLEEP_TIME)

        except Exception as e:
            print(f"  ⚠️  Skipping game {game_id}: {e}")
            time.sleep(SLEEP_TIME)

    print(f"  ✅ {len(records)} box score rows fetched")
    return pd.DataFrame(records)


def get_standings(season: str = SEASON) -> pd.DataFrame:
    """
    Full standings snapshot — fetched daily.
    dbt snapshot tracks wins/losses/rank changes (SCD2).
    """
    print(f"  📡 Fetching standings for {season}...")

    standings = leaguestandings.LeagueStandings(season=season)
    df        = standings.standings.get_data_frame()
    now       = datetime.utcnow().isoformat()

    records = []
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

    print(f"  ✅ {len(records)} team standings fetched")
    return pd.DataFrame(records)



def run_pipeline(date: str = None):
    if date is None:
        date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    date_nodash = date.replace("-", "")
    print(f"📅 Processing date: {date}\n")

    # ── 1. Dimensions (full refresh daily — dbt snapshot detects changes) ──
    print("── Dimensions ──────────────────────────────")
    teams_df          = get_teams()
    team_details_df   = get_team_details()
    player_details_df = get_player_details()
    standings_df      = get_standings()

    # ── 2. Daily game data (date-filtered) ──
    print("\n── Game data ───────────────────────────────")
    games_df = get_games(date)

    if not games_df.empty:
        game_ids      = games_df["game_id"].tolist()
        box_scores_df = get_box_scores(game_ids)
    else:
        box_scores_df = pd.DataFrame()

    # ── 3. Save ──
    AWS_REGION = os.getenv('AWS_REGION')
    
    if not games_df.empty:
        upload_to_s3(games_df, "games", date_nodash)
    
    if not box_scores_df.empty:
        upload_to_s3(box_scores_df, "box_scores", date_nodash)

    upload_to_s3(standings_df, "standings", date_nodash)
    upload_to_s3(teams_df, "teams", date_nodash)
    upload_to_s3(team_details_df, "team_details", date_nodash)
    upload_to_s3(player_details_df, "player_details", date_nodash)
   
    # ── 4. Summary ──
    print(f"\n✨ Done!")
    print(f"  Teams:      {len(teams_df)}")
    print(f"  Players:    {len(player_details_df)}")
    print(f"  Games:      {len(games_df)}")
    print(f"  Box scores: {len(box_scores_df)}")
    print(f"  Standings:  {len(standings_df)}")

if __name__ == "__main__":
    load_dotenv()
    run_pipeline()
    
    
