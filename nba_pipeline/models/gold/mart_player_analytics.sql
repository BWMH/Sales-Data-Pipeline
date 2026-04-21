WITH PLAYER_BASIC_INFO AS (
    SELECT
        player_id,
        full_name,
        birthdate,
        country,
        height,
        weight_lbs,
        jersey_number,
        position,
        team_id,
        draft_year,
        draft_round,
        draft_number,
        career_from_year,
        career_to_year,
        data_freshness_at
    FROM {{ ref('dim_players') }} 
), 
ALL_TIME_PLAYER_STATS AS (
    SELECT
        player_id,
        COUNT(DISTINCT game_id) AS total_games,
        AVG(points) AS avg_points,
        AVG(assists) AS avg_assists,
        AVG(steals) AS avg_steals,
        AVG(blocks) AS avg_blocks,
        AVG(turnovers) AS avg_turnovers,
        AVG(total_reb) AS avg_total_reb,
        MAX(GAME_DATE) AS last_active_date,
        SUM(fg_made)*1.0/NULLIF(SUM(fg_attempted), 0) AS fg_made_pct,
        SUM(fg3_made)*1.0/NULLIF(SUM(fg3_attempted), 0) AS fg3_made_pct,
        SUM(ft_made)*1.0/NULLIF(SUM(ft_attempted), 0) AS ft_made_pct
    FROM {{ ref('fact_player_performances') }}
    WHERE STATUS = 'ACTIVE'
    GROUP BY player_id
), 

CURRENT_SEASON_PLAYER_STATS AS (
    SELECT
        player_id,
        COUNT(DISTINCT game_id) AS total_games,
        AVG(points) AS avg_points,
        AVG(assists) AS avg_assists,
        AVG(steals) AS avg_steals,
        AVG(blocks) AS avg_blocks,
        AVG(turnovers) AS avg_turnovers,
        AVG(total_reb) AS avg_total_reb,
        SUM(fg_made)*1.0/NULLIF(SUM(fg_attempted), 0) AS fg_made_pct,
        SUM(fg3_made)*1.0/NULLIF(SUM(fg3_attempted), 0) AS fg3_made_pct,
        SUM(ft_made)*1.0/NULLIF(SUM(ft_attempted), 0) AS ft_made_pct
    FROM {{ ref('fact_player_performances') }}
    WHERE season = '{{var("current_season")}}' AND STATUS = 'ACTIVE'
    GROUP BY player_id
), 

RECENT_TEN_PLAYER_STATS AS (
    SELECT
        player_id,
        AVG(points) AS avg_points,
        AVG(assists) AS avg_assists,
        AVG(total_reb) AS avg_total_reb,
        SUM(fg_made)*1.0/NULLIF(SUM(fg_attempted), 0) AS fg_made_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY GAME_DATE DESC) AS RN
        FROM {{ ref('fact_player_performances') }}
        WHERE STATUS = 'ACTIVE'
    )
    WHERE RN <= 10
    GROUP BY player_id
)

SELECT  
    PBI.*,
    
    RANK() OVER(ORDER BY CSPS.avg_points DESC) AS season_pts_rank,
    RANK() OVER(ORDER BY CSPS.avg_assists DESC) AS season_ast_rank,
    RANK() OVER(ORDER BY CSPS.avg_total_reb DESC) AS season_reb_rank,

    ATPS.total_games AS all_time_total_games,
    ATPS.avg_points AS all_time_avg_points,
    ATPS.fg_made_pct AS all_time_fg_pct,
    ATPS.last_active_date,

    CSPS.total_games AS current_season_total_games,
    CSPS.avg_points AS current_season_avg_points,
    CSPS.avg_assists AS current_season_avg_assists,
    CSPS.avg_total_reb AS current_season_avg_reb,
    CSPS.fg_made_pct AS current_season_fg_pct,
    CSPS.fg3_made_pct AS current_season_fg3_pct,

    RTPS.avg_points AS recent_10_avg_points,
    RTPS.avg_assists AS recent_10_avg_assists,
    RTPS.avg_total_reb AS recent_10_avg_reb,
    RTPS.fg_made_pct AS recent_10_fg_pct

FROM PLAYER_BASIC_INFO PBI
LEFT JOIN ALL_TIME_PLAYER_STATS ATPS ON PBI.player_id = ATPS.player_id
LEFT JOIN CURRENT_SEASON_PLAYER_STATS CSPS ON PBI.player_id = CSPS.player_id
LEFT JOIN RECENT_TEN_PLAYER_STATS RTPS ON PBI.player_id = RTPS.player_id