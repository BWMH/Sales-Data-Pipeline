WITH CONFERENCE_LOOKUP AS (
    SELECT * FROM {{ ref('nba_conference_mapping') }}
),
TEAM_BASIC_INFO AS(
SELECT
    DISTINCT
    T.team_id,
    T.team_name,
    T.abbreviation,
    T.nickname,
    T.city,
    T.state,
    T.year_founded,
    T.arena_name,
    T.arena_capacity,
    T.head_coach,
    T.general_manager,
    T.owner,
    T.dleague_affiliation,
    T.data_freshness_at,
    C.conference
    FROM {{ ref('dim_teams') }} T
    LEFT JOIN CONFERENCE_LOOKUP C ON T.team_id = C.team_id
), 
ALL_TIME_TEAM_GAME_STATS AS(
    SELECT
    team_id,
    COUNT(DISTINCT game_id) AS total_games,
    sum(case when wl = 'W' then 1 else 0 end) as total_wins,
    total_games - total_wins as total_losses,
    total_wins*1.0/total_games as win_pct,
    AVG(team_total_point) AS avg_team_point
    FROM {{ ref('fact_team_games') }}
    GROUP BY team_id
),  CURRENT_SEASON_TEAM_GAME_STATS AS(
    SELECT
    team_id,
    COUNT(DISTINCT game_id) AS total_games,
    sum(case when wl = 'W' then 1 else 0 end) as total_wins,
    total_games - total_wins as total_losses,
    total_wins*1.0/total_games as win_pct,
    AVG(team_total_point) AS avg_team_point
    FROM {{ ref('fact_team_games') }}
    WHERE season = '{{var("current_season")}}'
    GROUP BY team_id
), RECENT_TEN_TEAM_GAME_STATS AS(
    SELECT
    team_id,
    COUNT(DISTINCT game_id) AS total_games,
    sum(case when wl = 'W' then 1 else 0 end) as total_wins,
    total_games - total_wins as total_losses,
    total_wins*1.0/total_games as win_pct,
    AVG(team_total_point) AS avg_team_point
    FROM(
    SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY GAME_DATE DESC) AS RN
    FROM {{ ref('fact_team_games') }})
    WHERE RN <= 10
    GROUP BY team_id
)
SELECT  
TBI.* ,
ATTGS.total_games AS all_time_total_games,
ATTGS.total_wins AS all_time_wins,
ATTGS.total_losses AS all_time_losses,
ATTGS.win_pct AS all_time_win_pct,
CSTGS.total_games AS current_season_total_games,
CSTGS.total_wins AS current_season_wins,
CSTGS.total_losses AS current_season_losses,
CSTGS.win_pct AS current_season_win_pct,
RANK() OVER(PARTITION BY TBI.conference ORDER BY CSTGS.win_pct DESC) AS conference_rank,
CSTGS.avg_team_point AS current_season_avg_team_point,
RTTGS.total_wins AS recent_10_games_wins,
RTTGS.total_losses AS recent_10_games_losses,
RTTGS.win_pct AS recent_10_games_win_pct,
RTTGS.avg_team_point AS recent_10_games_avg_team_point
FROM TEAM_BASIC_INFO TBI
LEFT JOIN ALL_TIME_TEAM_GAME_STATS ATTGS
ON TBI.team_id = ATTGS.team_id
LEFT JOIN CURRENT_SEASON_TEAM_GAME_STATS CSTGS
ON TBI.team_id = CSTGS.team_id
LEFT JOIN RECENT_TEN_TEAM_GAME_STATS RTTGS
ON TBI.team_id = RTTGS.team_id