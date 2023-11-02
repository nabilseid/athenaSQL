{{ objname }}
{{ underline }}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}

    {% if '__init__' in methods %}
      {% set caught_result = methods.remove('__init__') %}
    {% endif %}

    {% block methods_summary %}
    {% if methods %}

    .. rubric:: Methods

    .. autosummary::
    {% for item in methods %}
       ~{{ name }}.{{ item }}
    {%- endfor %}

    {% endif %}
    {% endblock %}

    {% block attributes_summary %}
    {% if attributes %}

    .. rubric:: Attributes

    .. autosummary::
    {% for item in attributes %}
       ~{{ name }}.{{ item }}
    {%- endfor %}

    {% endif %}
    {% endblock %}

    {% block methods_documentation %}
    {% if methods %}

    .. rubric:: Methods Documentation

    {% for item in methods %}
    .. automethod:: {{ item }}
    {%- endfor %}

    {% endif %}
    {% endblock %}

    {% block attributes_documentation %}
    {% if attributes %}

    .. rubric:: Attributes Documentation

    {% for item in attributes %}
    .. autoattribute:: {{ item }}
    {%- endfor %}

    {% endif %}
    {% endblock %}