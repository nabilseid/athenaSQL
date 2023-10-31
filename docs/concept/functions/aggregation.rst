Aggregation Functions
=====================

``any_value(col)``: Returns an arbitrary non-null value x, if one
exists.

``arbitrary(col)``: Returns an arbitrary non-null value of x, if one
exists. Identical to ``any_value()``.

``array_agg(col)``: Returns an array created from the input x elements.

``avg(col)``: Returns the average (arithmetic mean) of all input values.

``bool_and(col)``: Returns TRUE if every input value is TRUE, otherwise
FALSE.

``bool_or(col)``: Returns TRUE if any input value is TRUE, otherwise
FALSE.

``checksum(col)``: Returns an order-insensitive checksum of the given
values.

``count(col)``: Returns the number of non-null input values.

``count_if(col)``: Returns the number of TRUE input values. This
function is equivalent to count(CASE WHEN x THEN 1 END).

``every(col)``: This is an alias for ``bool_and()``.

``geometric_mean(col)``: Returns the geometric mean of all input values.

``max(col)``: Returns the maximum value of all input values.

``min(col)``: Returns the minimum value of all input values.

``sum(col)``: Returns the sum of all input values.