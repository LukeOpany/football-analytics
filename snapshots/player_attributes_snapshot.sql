{% snapshot player_attributes_snapshot %}

{{
    config(
        target_schema='analytics_dev',
        unique_key='id',
        strategy='check',
        check_cols=['overall_rating', 'potential', 'preferred_foot']
    )
}}

select * from {{ source('raw', 'player_attributes') }}

{% endsnapshot %}