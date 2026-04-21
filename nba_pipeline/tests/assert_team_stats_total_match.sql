SELECT
    team_id,
    current_season_total_games,
    (current_season_wins + current_season_losses) as calculated_total
FROM {{ ref('mart_team_analytics') }}
WHERE current_season_total_games != (current_season_wins + current_season_losses)