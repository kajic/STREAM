Please see docs/user.pdf or docs/user.ps for details of installing and
using STREAM.

# In this fork

Added slide parameter to range windows. 

A slide must be specified immediately after the window operator
`RANGE`. It is currently not possible to define a slide together
with the `ROWS` operator. The slide is expressed using the same
time specification as range. 
Finally, specifying a range smaller than the slide has undefined
results.

The following example query defines a window with a range of 10
seconds, and a slide of 5 seconds:

```sql
SELECT * 
FROM S [RANGE 10 SECONDS SLIDE 5 SECONDS];
```

When a tuple arrives with a time stamp which cannot be included in
the 10 second range (because the distance from the oldest tuple in
the window, to the most recently arrived, is greater than 10
seconds), the window will make some number of 5 second
slides. Most often one slide will be enough, but if the stream
hasn't produced any tuples for more than 5 seconds, more than one
slide will be required. Once the window has moved, all tuples with
time stamps between the windows original position and the new one
will be dropped.


The window `[RANGE 1 DAY SLIDE 1 DAY]` is equivalent to the ``Today
Window'' described at http://infolab.stanford.edu/stream/cql-benchmark.html.  
It can be used to define the ExpOutStr stream in the following
way:

```sql
SELECT RSTREAM(query_id, E.car_id, -1 * SUM(credit)) 
FROM ExpQueryStr [NOW] as Q, AccTransStr [RANGE 1 DAY SLIDE 1 DAY] as T 
WHERE Q.car_id = T.car_id;
```
