SELECT
    {{ dbt_utils.generate_surrogate_key(['g.game_id', 'g.team_id']) }} as team_game_pk,
    g.game_id,
    g.team_id,
    g.game_date,
    g.matchup,
    g.plus_minus,
    g.wl,
    g.season,
    g.pts as team_total_point,
    opp.team_id AS opponent_team_id,
    opp.abbreviation AS opponent_abbreviation
FROM {{ ref('silver_games') }} as g 
LEFT JOIN {{ ref('silver_teams') }} as t
ON g.team_id = t.team_id 
LEFT JOIN {{ ref('silver_teams') }} as opp
    ON (
        (LEFT(g.matchup, 3) = opp.abbreviation AND opp.abbreviation != t.abbreviation)
        OR 
        (RIGHT(g.matchup, 3) = opp.abbreviation AND opp.abbreviation != t.abbreviation)
    )
WHERE g.game_date >= '2025-10-21' AND LEFT(g.game_id, 3) = '002'