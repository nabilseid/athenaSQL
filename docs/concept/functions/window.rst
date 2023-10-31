Window Functions
================

``cume_dist()``: Returns the cumulative distribution of a value in a
group of values.

``dense_rank()``: Returns the rank of a value in a group of values.

``percent_rank()``: Returns the percentage ranking of a value in group
of values.

``rank()``: Returns the rank of a value in a group of values.

``row_number()``: Returns a unique sequential number for each row.

``ntile(n)``: Divides the rows for each window partition into n buckets
ranging from 1 to at most n.

``first_value(x)``: Returns the first value of the window.

``last_value(x)``: Returns the last value of the window.

``nth_value(x, offset)``: Returns the value at the specified offset from
the beginning of the window.

``lead(x, [offset, [default_value]])``: Returns the value at offset rows
after the current row in the window partition.

``lag(x, [offset, [default_value]])``: Returns the value at offset rows
before the current row in the window partition.