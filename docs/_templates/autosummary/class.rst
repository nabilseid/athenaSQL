.. Workaround to avoid documenting __init__.

{% extends "!autosummary/class.rst" %}

{% if '__init__' in methods %}
{% set caught_result = methods.remove('__init__') %}
{% endif %}
    
{% block methods %}
{% if methods %}

   .. rubric:: Methods

   .. autosummary::
      {% for item in methods %}
      {%- if item not in inherited_members %}
         ~{{ name }}.{{ item }}
      {%- endif %}
      {%- endfor %}

{% endif %}
{% endblock %}