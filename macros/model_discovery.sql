{% macro parse_model_list(models_var) %}
{#-
    Parses the models variable which can be either a list or a comma-separated string.

    Args:
        models_var: Either a list ['model1', 'model2'] or string 'model1,model2'

    Returns:
        A list of model names
-#}
{% if models_var is string and models_var | length > 0 %}
    {% set model_list = models_var.split(',') | map('trim') | reject('equalto', '') | list %}
{% elif models_var is iterable and models_var is not string %}
    {% set model_list = models_var | list %}
{% else %}
    {% set model_list = [] %}
{% endif %}
{{ return(model_list) }}
{% endmacro %}


{% macro get_effective_project(project_var) %}
{#-
    Returns the effective project ID, defaulting to target.project if not specified.

    Args:
        project_var: Project variable value (may be empty string)

    Returns:
        Project ID to use
-#}
{% if project_var and project_var | length > 0 %}
    {{ return(project_var) }}
{% else %}
    {{ return(target.project) }}
{% endif %}
{% endmacro %}


{% macro is_configured() %}
{#-
    Checks if the package has minimum configuration to run.
    Returns true if both prod_dataset and compare_dataset are set.
    Does NOT raise errors - allows graceful skipping.
-#}
{% set prod_dataset = var('bq_schema_compare_prod_dataset', '') %}
{% set compare_dataset = var('bq_schema_compare_compare_dataset', '') %}

{% if prod_dataset and prod_dataset | length > 0 and compare_dataset and compare_dataset | length > 0 %}
    {{ return(true) }}
{% else %}
    {{ return(false) }}
{% endif %}
{% endmacro %}


{% macro empty_result_set() %}
{#-
    Returns an empty result set with the standard schema comparison columns.
    Used when the package is not configured or no models are specified.
-#}
select
    cast(null as string) as table_name,
    cast(null as string) as column_name,
    cast(null as string) as status,
    cast(null as string) as prod_data_type,
    cast(null as string) as compare_data_type,
    cast(null as int64) as prod_position,
    cast(null as int64) as compare_position,
    cast(null as timestamp) as prod_last_modified,
    cast(null as timestamp) as compare_last_modified
limit 0
{% endmacro %}


{% macro get_all_tables(project, dataset) %}
{#-
    Returns a query to get all table names in a dataset with their last modified time.

    Args:
        project: BigQuery project ID
        dataset: BigQuery dataset name

    Returns:
        SQL query selecting table_name, last_modified from __TABLES__
-#}
select
    table_id as table_name,
    timestamp_millis(last_modified_time) as last_modified
from `{{ project }}`.`{{ dataset }}`.`__TABLES__`
where type = 1  -- 1 = table, 2 = view
{% endmacro %}
