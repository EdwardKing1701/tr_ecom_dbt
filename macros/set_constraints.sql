{% macro set_constraints() %}
{% if config.get('materialized', 'view') is in ['table', 'incremental'] and is_incremental() is false %}
    {% set pk = config.get('pk', default=[]) %}
    {% if pk|length > 0 %}
        alter table {{this}} add primary key({{','.join(pk)}});
    {% endif %}

    {% set mask = config.get('mask', default={}) %}
    {% for policy in mask %}
        {% for c in mask[policy] %}
            alter table {{this}} modify column {{c}} set masking policy {{policy}};
        {% endfor %}
    {% endfor %}
{% endif %}
{% endmacro %}