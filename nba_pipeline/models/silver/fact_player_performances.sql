SELECT
    {{ dbt_utils.generate_surrogate_key(['bs.game_id', 'bs.player_id']) }} as performance_pk,
    bs.player_id, 
    bs.game_id, 
    bs.team_id, 
    bs.status, 
    bs.minutes_played,
    bs.points, 
    bs.assists, 
    bs.steals,
    bs.blocks, 
    bs.turnovers, 
    bs.personal_fouls, 
    bs.plus_minus, 
    bs.o_reb, 
    bs.d_reb, 
    bs.total_reb, 
    bs.fg_made,
    bs.fg_attempted, 
    bs.fg_pct, 
    bs.fg3_made, 
    bs.fg3_attempted, 
    bs.fg3_pct, 
    bs.ft_made, 
    bs.ft_attempted, 
    bs.ft_pct,
    CASE WHEN bs.fg_attempted > 0 THEN (bs.fg_made / bs.fg_attempted) ELSE 0 END as fg_pct_calc,
    g.game_date,
    g.season
FROM {{ ref('silver_box_scores') }} as bs
LEFT JOIN {{ ref('silver_games') }} as g 
ON bs.game_id = g.game_id
AND bs.team_id = g.team_id

