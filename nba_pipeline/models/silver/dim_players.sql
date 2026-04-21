WITH base_players AS (
    -- This is your existing clean list from Silver
    SELECT
        {{ dbt_utils.generate_surrogate_key(['player_id']) }} as player_pk,
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
        CASE 
            WHEN career_to_year < 2024 THEN 'Retired' 
            ELSE 'Active' 
        END AS player_status,
        last_updated_at AS data_freshness_at
    FROM {{ ref('silver_player_details') }}
),

-- Identify IDs that exist in the facts but are missing from the dimensions
orphaned_ids AS (
    SELECT DISTINCT 
        player_id
    FROM {{ ref('fact_player_performances') }}
    WHERE player_id NOT IN (SELECT player_id FROM base_players)
)


SELECT * FROM base_players
UNION ALL
SELECT
    {{ dbt_utils.generate_surrogate_key(['player_id']) }} as player_pk,
    player_id,
    'Unknown Player (' || player_id || ')' AS full_name,
    NULL AS birthdate,
    'N/A' AS country,
    NULL AS height,
    NULL AS weight_lbs,
    NULL AS jersey_number,
    'N/A' AS position,
    NULL AS team_id,
    NULL AS draft_year,
    NULL AS draft_round,
    NULL AS draft_number,
    NULL AS career_from_year,
    NULL AS career_to_year,
    'Unknown' AS player_status,
    CURRENT_TIMESTAMP() AS data_freshness_at
FROM orphaned_ids