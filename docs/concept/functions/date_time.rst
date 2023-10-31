Date and Time Functions
=======================

``date(col)``: This is an alias for CAST(x AS date).

``last_day_of_month(col)``: Returns the last day of the month.

``from_iso8601_timestamp(col)``: Parses the ISO 8601 formatted string
into a timestamp with time zone.

Date and Time Convenience Extraction
------------------------------------

``day(col)``: Returns the day of the month from x.

``day_of_month(col)``: This is an alias for day().

``day_of_week(col)``: Returns the ISO day of the week from x. The value
ranges from 1 (Monday) to 7 (Sunday).

``day_of_year(col)``: Returns the day of the year from x. The value
ranges from 1 to 366.

``dow(col)``: This is an alias for day_of_week().

``doy(col)``: This is an alias for day_of_year().

``hour(col)``: Returns the hour of the day from x. The value ranges from
0 to 23.

``millisecond(col)``: Returns the millisecond of the second from x.

``minute(col)``: Returns the minute of the hour from x.

``month(col)``: Returns the month of the year from x.

``quarter(col)``: Returns the quarter of the year from x. The value
ranges from 1 to 4.

``second(col)``: Returns the second of the minute from x.

``timezone_hour(col)``: Returns the hour of the time zone offset from
timestamp.

``timezone_minute(col)``: Returns the minute of the time zone offset
from timestamp.

``week(col)``: Returns the ISO week of the year from x. The value ranges
from 1 to 53.

``week_of_year(col)``: This is an alias for week().

``year(col)``: Returns the year from x.

``year_of_week(col)``: Returns the year of the ISO week from x.

``yow(col)``: This is an alias for year_of_week().