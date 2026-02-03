{% macro derive_business_date(
    src_table_name,
    dbt_src_name
) %}

{% if execute %}

    {% set sql %}
        select
            SRC_COLUMN_NAME,
            HEADER,
            DATE_START_POSITION,
            DATE_END_POSITION,
            DATE_FORMET
        from {{ source('metadata', 'source_metadata') }}
        where upper(src_table_name) = upper('{{ src_table_name }}')
          and upper(dbt_src_name) = upper('{{ dbt_src_name }}')
    {% endset %}

    {% set results = run_query(sql) %}
    {% set rows = results.rows %}

    {% set select_list = [] %}

    {% for row in rows %}
        {% set src_col = row[0] %}
        {% set hdr     = row[1] %}
        {% set start   = row[2] %}
        {% set len     = row[3] %}
        {% set formet  = row[4] %}

        {% set sql_src %}
            select
                substr({{ src_col }}, {{ start }}, {{ len }}) as data
            from {{ source(dbt_src_name, src_table_name) }}
            where substr({{ src_col }}, 1, 1) = '{{ hdr }}'
        {% endset %}

        {% set query_result = run_query(sql_src) %}

        {% set date_string = query_result.columns[0].values()[0] %}

        {% set expr %}
            to_date('{{ date_string }}', '{{ formet }}')
        {% endset %}

        {% do select_list.append(expr | trim) %}
    {% endfor %}

    {{ return(select_list | join(',\n    ')) }}

{% endif %}
{% endmacro %}
