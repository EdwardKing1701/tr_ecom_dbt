{% macro get_incremental_replace_sql(arg) -%}

    {%- set dest_cols_csv = get_quoted_csv(arg['dest_columns'] | map(attribute="name")) -%}
    {%- set unique_key = arg['unique_key'] -%}

    {% if unique_key %}
        {% if unique_key is not sequence or unique_key is string %}
            {% set unique_key = [unique_key] %}
        {% endif %}
        {% set key = '||'.join(unique_key) %}
        delete from {{ arg['target_relation'] }}
        where {{ key }} in (
            select distinct {{ key }} from {{ arg['temp_relation'] }}
        )
        {%- if arg['incremental_predicates'] %}
            {% for predicate in arg['incremental_predicates'] %}
                and {{ predicate }}
            {% endfor %}
        {%- endif -%};
    {% endif %}

    insert into {{ arg['target_relation'] }} ({{ dest_cols_csv }})
        select {{ dest_cols_csv }}
        from {{ arg['temp_relation'] }}

{%- endmacro %}