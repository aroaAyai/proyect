{% snapshot customer_timestamp_snp_bronze %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='_fivetran_synced'
) }}

select *
from {{ source('google_sheets', 'customers') }}

{% endsnapshot %}
