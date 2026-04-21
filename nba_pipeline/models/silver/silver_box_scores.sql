SELECT
    BOX_SCORE_ID, 
    GAME_ID, 
    PLAYER_ID, 
    TEAM_ID, 
    JERSEY_NUMBER, 
    POSITION, 
    STATUS, 
    CASE WHEN MINUTES_PLAYED IS NULL OR MINUTES_PLAYED = '' THEN '00:00' ELSE MINUTES_PLAYED END AS MINUTES_PLAYED,
    POINTS, 
    ASSISTS, 
    STEALS, 
    BLOCKS, 
    TURNOVERS, 
    PERSONAL_FOULS, 
    PLUS_MINUS, 
    O_REB, 
    D_REB, 
    TOTAL_REB, 
    fg_made, 
    fg_attempted, 
    fg_pct, 
    fg3_made, 
    fg3_attempted, 
    fg3_pct, 
    ft_made, 
    ft_attempted, 
    ft_pct, 
    api_fetched_at
FROM {{ ref('bronze_box_scores') }}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY BOX_SCORE_ID
    ORDER BY api_fetched_at DESC
) = 1