{{
    config(
        materialized='table'
    )
}}

with deaths_source as (
    select 
        *
    from
        {{ source('project', 'death_causes') }}
)

select * from deaths_source