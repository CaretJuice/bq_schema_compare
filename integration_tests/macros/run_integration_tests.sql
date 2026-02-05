{#-
    Integration test macros for bq_schema_compare package.

    These tests verify that the comparison macros correctly identify
    schema differences between datasets.

    Run with: dbt run-operation run_integration_tests
-#}


{% macro run_integration_tests() %}
{#-
    Main test runner that executes all integration tests.
    Returns a summary of test results.
-#}

{{ log("", info=True) }}
{{ log("=" * 60, info=True) }}
{{ log("Running bq_schema_compare Integration Tests", info=True) }}
{{ log("=" * 60, info=True) }}
{{ log("", info=True) }}

{% set test_results = [] %}

{# Test 1: compare_schemas with specific tables #}
{% set result = test_compare_schemas_specific_tables() %}
{% do test_results.append(result) %}

{# Test 2: compare_datasets full comparison #}
{% set result = test_compare_datasets_full() %}
{% do test_results.append(result) %}

{# Test 3: Identical tables should have no differences #}
{% set result = test_identical_tables_no_diff() %}
{% do test_results.append(result) %}

{# Test 4: Type mismatch detection #}
{% set result = test_type_mismatch_detection() %}
{% do test_results.append(result) %}

{# Test 5: Column diff detection #}
{% set result = test_column_diff_detection() %}
{% do test_results.append(result) %}

{# Test 6: Empty model list returns empty result #}
{% set result = test_empty_model_list() %}
{% do test_results.append(result) %}

{# Summary #}
{{ log("", info=True) }}
{{ log("=" * 60, info=True) }}
{{ log("Test Summary", info=True) }}
{{ log("=" * 60, info=True) }}

{% set passed = test_results | selectattr('passed') | list | length %}
{% set failed = test_results | rejectattr('passed') | list | length %}

{% for result in test_results %}
    {% if result.passed %}
        {{ log("✓ " ~ result.name, info=True) }}
    {% else %}
        {{ log("✗ " ~ result.name ~ ": " ~ result.message, info=True) }}
    {% endif %}
{% endfor %}

{{ log("", info=True) }}
{{ log("Passed: " ~ passed ~ "/" ~ (passed + failed), info=True) }}

{% if failed > 0 %}
    {{ exceptions.raise_compiler_error("Integration tests failed: " ~ failed ~ " test(s) failed") }}
{% else %}
    {{ log("All tests passed!", info=True) }}
{% endif %}

{% endmacro %}


{% macro test_compare_schemas_specific_tables() %}
{#- Test that compare_schemas works with a list of specific tables -#}
{% set test_name = "compare_schemas with specific tables" %}
{{ log("Running: " ~ test_name, info=True) }}

{% set prod_dataset = var('test_prod_dataset') %}
{% set compare_dataset = var('test_compare_dataset') %}
{% set prod_project = target.project %}
{% set compare_project = target.project %}

{% set comparison_sql %}
{{ bq_schema_compare.compare_all_tables(
    prod_project, prod_dataset,
    compare_project, compare_dataset,
    ['type_mismatch', 'column_diff']
) }}
{% endset %}

{% if execute %}
    {% set results = run_query(comparison_sql) %}

    {# Should find differences in both tables #}
    {% if results | length >= 2 %}
        {{ return({'name': test_name, 'passed': true, 'message': ''}) }}
    {% else %}
        {{ return({'name': test_name, 'passed': false, 'message': 'Expected at least 2 differences, got ' ~ (results | length)}) }}
    {% endif %}
{% endif %}

{{ return({'name': test_name, 'passed': true, 'message': ''}) }}
{% endmacro %}


{% macro test_compare_datasets_full() %}
{#- Test that compare_datasets_full finds all expected differences -#}
{% set test_name = "compare_datasets_full finds all differences" %}
{{ log("Running: " ~ test_name, info=True) }}

{% set prod_dataset = var('test_prod_dataset') %}
{% set compare_dataset = var('test_compare_dataset') %}
{% set prod_project = target.project %}
{% set compare_project = target.project %}

{% set comparison_sql %}
{{ bq_schema_compare.compare_datasets_full(
    prod_project, prod_dataset,
    compare_project, compare_dataset
) }}
{% endset %}

{% if execute %}
    {% set results = run_query(comparison_sql) %}

    {# Should find: table_prod_only, table_compare_only, type_mismatch, column differences #}
    {% set statuses = results.columns['status'].values() | list %}

    {% set has_table_prod_only = 'table_prod_only' in statuses %}
    {% set has_table_compare_only = 'table_compare_only' in statuses %}
    {% set has_type_mismatch = 'type_mismatch' in statuses %}
    {% set has_column_diff = 'prod_only' in statuses or 'compare_only' in statuses %}

    {% if has_table_prod_only and has_table_compare_only and has_type_mismatch and has_column_diff %}
        {{ return({'name': test_name, 'passed': true, 'message': ''}) }}
    {% else %}
        {% set missing = [] %}
        {% if not has_table_prod_only %}{% do missing.append('table_prod_only') %}{% endif %}
        {% if not has_table_compare_only %}{% do missing.append('table_compare_only') %}{% endif %}
        {% if not has_type_mismatch %}{% do missing.append('type_mismatch') %}{% endif %}
        {% if not has_column_diff %}{% do missing.append('column_diff') %}{% endif %}
        {{ return({'name': test_name, 'passed': false, 'message': 'Missing statuses: ' ~ missing | join(', ')}) }}
    {% endif %}
{% endif %}

{{ return({'name': test_name, 'passed': true, 'message': ''}) }}
{% endmacro %}


{% macro test_identical_tables_no_diff() %}
{#- Test that identical tables produce no differences -#}
{% set test_name = "identical tables have no differences" %}
{{ log("Running: " ~ test_name, info=True) }}

{% set prod_dataset = var('test_prod_dataset') %}
{% set compare_dataset = var('test_compare_dataset') %}
{% set prod_project = target.project %}
{% set compare_project = target.project %}

{% set comparison_sql %}
{{ bq_schema_compare.compare_all_tables(
    prod_project, prod_dataset,
    compare_project, compare_dataset,
    ['identical_table']
) }}
{% endset %}

{% if execute %}
    {% set results = run_query(comparison_sql) %}

    {% if results | length == 0 %}
        {{ return({'name': test_name, 'passed': true, 'message': ''}) }}
    {% else %}
        {{ return({'name': test_name, 'passed': false, 'message': 'Expected 0 differences for identical table, got ' ~ (results | length)}) }}
    {% endif %}
{% endif %}

{{ return({'name': test_name, 'passed': true, 'message': ''}) }}
{% endmacro %}


{% macro test_type_mismatch_detection() %}
{#- Test that type mismatches are correctly detected -#}
{% set test_name = "type mismatch detection" %}
{{ log("Running: " ~ test_name, info=True) }}

{% set prod_dataset = var('test_prod_dataset') %}
{% set compare_dataset = var('test_compare_dataset') %}
{% set prod_project = target.project %}
{% set compare_project = target.project %}

{% set comparison_sql %}
{{ bq_schema_compare.compare_all_tables(
    prod_project, prod_dataset,
    compare_project, compare_dataset,
    ['type_mismatch']
) }}
{% endset %}

{% if execute %}
    {% set results = run_query(comparison_sql) %}

    {# Should find exactly one type_mismatch for 'amount' column #}
    {% set type_mismatches = [] %}
    {% for row in results %}
        {% if row['status'] == 'type_mismatch' and row['column_name'] == 'amount' %}
            {% do type_mismatches.append(row) %}
        {% endif %}
    {% endfor %}

    {% if type_mismatches | length == 1 %}
        {% set row = type_mismatches[0] %}
        {% if row['prod_data_type'] == 'INT64' and row['compare_data_type'] == 'FLOAT64' %}
            {{ return({'name': test_name, 'passed': true, 'message': ''}) }}
        {% else %}
            {{ return({'name': test_name, 'passed': false, 'message': 'Wrong types: ' ~ row['prod_data_type'] ~ ' vs ' ~ row['compare_data_type']}) }}
        {% endif %}
    {% else %}
        {{ return({'name': test_name, 'passed': false, 'message': 'Expected 1 type_mismatch for amount, got ' ~ (type_mismatches | length)}) }}
    {% endif %}
{% endif %}

{{ return({'name': test_name, 'passed': true, 'message': ''}) }}
{% endmacro %}


{% macro test_column_diff_detection() %}
{#- Test that column differences are correctly detected -#}
{% set test_name = "column diff detection" %}
{{ log("Running: " ~ test_name, info=True) }}

{% set prod_dataset = var('test_prod_dataset') %}
{% set compare_dataset = var('test_compare_dataset') %}
{% set prod_project = target.project %}
{% set compare_project = target.project %}

{% set comparison_sql %}
{{ bq_schema_compare.compare_all_tables(
    prod_project, prod_dataset,
    compare_project, compare_dataset,
    ['column_diff']
) }}
{% endset %}

{% if execute %}
    {% set results = run_query(comparison_sql) %}

    {# Should find old_column as prod_only and new_column as compare_only #}
    {% set prod_only_cols = [] %}
    {% set compare_only_cols = [] %}

    {% for row in results %}
        {% if row['status'] == 'prod_only' %}
            {% do prod_only_cols.append(row['column_name']) %}
        {% elif row['status'] == 'compare_only' %}
            {% do compare_only_cols.append(row['column_name']) %}
        {% endif %}
    {% endfor %}

    {% if 'old_column' in prod_only_cols and 'new_column' in compare_only_cols %}
        {{ return({'name': test_name, 'passed': true, 'message': ''}) }}
    {% else %}
        {{ return({'name': test_name, 'passed': false, 'message': 'prod_only: ' ~ prod_only_cols | join(',') ~ ', compare_only: ' ~ compare_only_cols | join(',')}) }}
    {% endif %}
{% endif %}

{{ return({'name': test_name, 'passed': true, 'message': ''}) }}
{% endmacro %}


{% macro test_empty_model_list() %}
{#- Test that empty model list returns empty result -#}
{% set test_name = "empty model list returns empty result" %}
{{ log("Running: " ~ test_name, info=True) }}

{% set prod_dataset = var('test_prod_dataset') %}
{% set compare_dataset = var('test_compare_dataset') %}
{% set prod_project = target.project %}
{% set compare_project = target.project %}

{% set comparison_sql %}
{{ bq_schema_compare.compare_all_tables(
    prod_project, prod_dataset,
    compare_project, compare_dataset,
    []
) }}
{% endset %}

{% if execute %}
    {% set results = run_query(comparison_sql) %}

    {% if results | length == 0 %}
        {{ return({'name': test_name, 'passed': true, 'message': ''}) }}
    {% else %}
        {{ return({'name': test_name, 'passed': false, 'message': 'Expected 0 results for empty model list, got ' ~ (results | length)}) }}
    {% endif %}
{% endif %}

{{ return({'name': test_name, 'passed': true, 'message': ''}) }}
{% endmacro %}
