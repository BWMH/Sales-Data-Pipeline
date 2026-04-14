SELECT
    game_date, game_team_id, game_id, matchup, season, api_fetched_at, team_id, plus_minus, pts, wl
FROM {{ ref('bronze_games') }}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY game_team_id
    ORDER BY api_fetched_at DESC
) = 1