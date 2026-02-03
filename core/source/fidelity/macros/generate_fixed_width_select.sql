{% macro generate_fixed_width_select(
    mapping_relation,
    target_table_name,
    src_table_name,
    record_number=None,
    record_type=None
) %}

{% if execute %}

    {% set sql %}
        select
            src_column_name,
            target_column_name,
            start_index,
            length
        from {{ mapping_relation }}
        where UPPER(target_table_name) = UPPER('{{ target_table_name }}')
        AND UPPER(src_table_name) = UPPER('{{ src_table_name }}')
        {% if record_number is not none %}
          and record_number = '{{ record_number }}'
        {% endif %}
        {% if record_type is not none %}
          and record_type = '{{ record_type }}'
        {% endif %}
        order by start_index
    {% endset %}

    {% set results = run_query(sql) %}
    {% set rows = results.rows %}
    {% set select_list = [] %}

    {% for row in rows %}
        {% set src_col = row[0] %}
        {% set tgt_col = row[1] %}
        {% set start = row[2] %}
        {% set len = row[3] %}

        {% set expr %}
            substr({{ src_col }}, {{ start }}, {{ len }}) as {{ tgt_col }}
        {% endset %}

        {% do select_list.append(expr | trim) %}
    {% endfor %}

    {{ return(select_list | join(',\n    ')) }}

{% else %}
    {{ return('*') }}
{% endif %}

{% endmacro %}
