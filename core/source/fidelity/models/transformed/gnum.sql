{{ config(
    pre_hook = [
        "truncate table if exists {{ this }}"
    ]
) }}
select
    {{ generate_fixed_width_select(
        mapping_relation=ref('mapping_config'),
        target_table_name='GNUM',
        src_table_name='231_GNUMS',
        record_type='D'
    ) }},
    CONVERT_TIMEZONE('America/Los_Angeles', CURRENT_TIMESTAMP()) AS etl_load_timestamp,
    {{derive_business_date(src_table_name='GNUMS',dbt_src_name='raw')}} as date_of_data
from {{source('raw','GNUMS')}} SRC
WHERE
SUBSTR(SRC.DATA,1,1) like 'D'
