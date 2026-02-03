{{ config(
    pre_hook = [
        "truncate table if exists {{ this }}"
    ]
) }}
select
    {{ generate_fixed_width_select(
        mapping_relation=ref('mapping_config'),
        target_table_name='EMPLOYER',
        src_table_name='231_Name_And_Address',
        record_number='107',
        record_type='D'
    ) }},
    CONVERT_TIMEZONE('America/Los_Angeles', CURRENT_TIMESTAMP()) AS etl_load_timestamp,
    {{derive_business_date(src_table_name='NAME_AND_ADDRESS',dbt_src_name='raw')}} as date_of_data
from {{source('raw','NAME_AND_ADDRESS')}} SRC
WHERE
SUBSTR(SRC.DATA,2,3) like '107'
