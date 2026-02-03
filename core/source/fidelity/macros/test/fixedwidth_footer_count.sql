{% test fixedwidth_footer_count(
    model,
    column_name,
    footer_start_pos,
    footer_length
) %}

with classified_rows as (
    select
        {{ column_name }} as line,
        case
            when left({{ column_name }}, 1) = 'H' then 'HEADER'
            when left({{ column_name }}, 1) = 'T' then 'FOOTER'
            else 'DATA'
        end as row_type
    from {{ model }}
),
data_rows as (
    select *
    from classified_rows
    where row_type = 'DATA'
      and trim(line) <> ''
),
footer as (
    select
        try_to_number(
            trim(substr(line, {{ footer_start_pos }}, {{ footer_length }}))
        ) as footer_count
    from classified_rows
    where row_type = 'FOOTER'
),
final as (
    select
        (select count(*) from data_rows) as actual_count,
        (select footer_count from footer) as expected_count
)
select *
from final
where actual_count != expected_count
   or expected_count is null

{% endtest %}
