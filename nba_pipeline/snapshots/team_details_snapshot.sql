{% snapshot team_details_snapshot %}

{{
    config(
      target_database='NBA',
      target_schema='snapshots',
      unique_key='team_id',
      strategy='check',
      check_cols=['head_coach', 'general_manager', 'owner', 'arena_name', 'city'],
    )
}}

SELECT * FROM {{ ref('bronze_team_details') }}  
QUALIFY ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY loaded_at_utc DESC) = 1

{% endsnapshot %}

