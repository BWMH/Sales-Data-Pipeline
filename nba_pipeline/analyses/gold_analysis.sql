SELECT
    -- 1. Metadata & Keys
    VALUE:game_date::timestamp_ntz           as game_date,
    VALUE:game_id::string            as game_id,
    VALUE:matchup::string              as matchup,
    VALUE:season::string              as season,
    VALUE:team_id::string            as team_id,
    file_name                       as source_file,
    inserted_at                     as loaded_at_utc,
    VALUE:fetched_at::timestamp_ntz as api_fetched_at
FROM 
    {{ source('raw', 'GAMES') }},
    LATERAL FLATTEN(input => RAW_JSON) -- VALUE now represents each item in RAW_JSON