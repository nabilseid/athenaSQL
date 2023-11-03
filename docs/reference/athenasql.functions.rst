Functions
=========

A collections of trino supported built-in SQL functions. Collection is in synch
with `trino functions <https://trino.io/docs/current/functions.html>`_.

Normal
------

.. currentmodule:: athenaSQL.functions

.. autosummary::
    :toctree: api/
    
    sqrt
    abs
    mean
    geometric_mean
    stddev
    variance


Aggregation
-----------

.. autosummary::
    :toctree: api/

    any_value
    arbitrary
    array_agg
    avg
    bool_and
    bool_or
    checksum
    count
    count_if
    every
    geometric_mean
    max
    min
    sum


String
------

.. autosummary::
    :toctree: api/

    chr
    codepoint
    length
    upper
    lower
    ltrim
    reverse
    rtrim
    trim
    word_stem
    hamming_distance
    levenshtein_distance
    concat
    replace
    substring


Window
------

.. autosummary::
    :toctree: api/

    cume_dist
    dense_rank
    percent_rank
    rank
    row_number
    ntile
    first_value
    last_value
    nth_value
    lead
    lag


Regular expression
------------------

.. autosummary::
    :toctree: api/

    regexp_count
    regexp_extract_all
    regexp_extract
    regexp_position
    regexp_replace
    regexp_split
    regexp_like