{% snapshot transaction_timestamp_snp_bronze %}
{{ config(
    target_schema='snapshots',
    unique_key='account_id',
    strategy='timestamp',
    updated_at='_fivetran_synced'
) }}

select *
from {{ source('bank', 'transaction') }}

{% endsnapshot %}
