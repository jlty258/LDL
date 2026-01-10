-- 数据库工具宏
-- 处理不同数据库的语法差异

-- 日期截断函数（适配不同数据库）
{% macro date_trunc(datepart, date_expr) %}
  {{ return(adapter.dispatch('date_trunc', 'database_utils')(datepart, date_expr)) }}
{% endmacro %}

{% macro default__date_trunc(datepart, date_expr) %}
  DATE_TRUNC('{{ datepart }}', {{ date_expr }})
{% endmacro %}

{% macro mysql__date_trunc(datepart, date_expr) %}
  DATE_FORMAT({{ date_expr }}, 
    CASE '{{ datepart }}'
      WHEN 'year' THEN '%Y-01-01'
      WHEN 'month' THEN '%Y-%m-01'
      WHEN 'day' THEN '%Y-%m-%d'
    END
  )
{% endmacro %}

-- 季度提取函数
{% macro extract_quarter(date_expr) %}
  {{ return(adapter.dispatch('extract_quarter', 'database_utils')(date_expr)) }}
{% endmacro %}

{% macro default__extract_quarter(date_expr) %}
  EXTRACT(QUARTER FROM {{ date_expr }})
{% endmacro %}

{% macro mysql__extract_quarter(date_expr) %}
  QUARTER({{ date_expr }})
{% endmacro %}

-- 字符串连接函数
{% macro concat_strings(*args) %}
  {{ return(adapter.dispatch('concat_strings', 'database_utils')(*args)) }}
{% endmacro %}

{% macro default__concat_strings(*args) %}
  CONCAT({{ args|join(', ') }})
{% endmacro %}

{% macro mysql__concat_strings(*args) %}
  CONCAT({{ args|join(', ') }})
{% endmacro %}

{% macro postgres__concat_strings(*args) %}
  {{ args|join(' || ') }}
{% endmacro %}

-- 时间差计算函数
{% macro datediff(datepart, start_date, end_date) %}
  {{ return(adapter.dispatch('datediff', 'database_utils')(datepart, start_date, end_date)) }}
{% endmacro %}

{% macro mysql__datediff(datepart, start_date, end_date) %}
  TIMESTAMPDIFF({{ datepart }}, {{ start_date }}, {{ end_date }})
{% endmacro %}

{% macro postgres__datediff(datepart, start_date, end_date) %}
  EXTRACT({{ datepart }} FROM ({{ end_date }} - {{ start_date }}))
{% endmacro %}
