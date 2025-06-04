{% macro drop_stale_obj(db_name=target.database,schema=target.schema, days=7, dry_run=True) %}
{% set get_drop_command_query %}
    select
        case
            when table_type = 'VIEW' then table_type
            else 'TABLE'
        end as drop_type , 
        'DROP ' || drop_type || ' {{db_name|upper}}.'||table_schema||'.'|| table_name ||';'
    from {{db_name}}.information_schema.tables
    where table_schema = {{schema}}
    and last_altered < current_date - {{days}}
{% endset%}

{{ log('Generating log for clean up queries', info =true)}}

{% set drop_query = run_query(get_drop_command_query).column[1].values() %}

{% for q in drop_query  %}
    {% if dry_run%}
        {{ log(q, info=true )}}
    {% else %}
        {{ log('dropping object with command : '~q, info = true)}}
        {% do run_query(q) %}
    {% endif%}    
{% endfor %}

{% endmacro %}