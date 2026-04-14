{% snapshot player_details_snapshot %}

{{
    config(
      target_database='NBA',
      target_schema='snapshots',
      unique_key='player_id',
      strategy='check',
      updated_at='api_fetched_at',
      check_cols=['full_name', 'country', 'jersey_number', 'position', 'height','weight_lbs','team_id'],
    )
}}

select * from {{ ref('bronze_player_details') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY loaded_at_utc DESC) = 1


{% endsnapshot %}