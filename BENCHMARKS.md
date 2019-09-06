These are some very basic and unscientific benchmarks of various commands
provided by `xsv`. Please see below for more information.

These benchmarks were run with
[worldcitiespop_mil.csv](https://burntsushi.net/stuff/worldcitiespop_mil.csv),
which is a random 1,000,000 row subset of the world city population dataset
from the [Data Science Toolkit](https://github.com/petewarden/dstkdata).

These benchmarks were run on an Intel i7-6900K (8 CPUs, 16 threads) with 64GB
of memory.

```
count                   0.11 seconds   413.76  MB/sec
flatten                 4.54 seconds   10.02   MB/sec
flatten_condensed       4.45 seconds   10.22   MB/sec
frequency               1.82 seconds   25.00   MB/sec
index                   0.12 seconds   379.28  MB/sec
sample_10               0.18 seconds   252.85  MB/sec
sample_1000             0.18 seconds   252.85  MB/sec
sample_100000           0.29 seconds   156.94  MB/sec
search                  0.27 seconds   168.56  MB/sec
select                  0.14 seconds   325.09  MB/sec
search                  0.13 seconds   350.10  MB/sec
select                  0.13 seconds   350.10  MB/sec
sort                    2.18 seconds   20.87   MB/sec
slice_one_middle        0.08 seconds   568.92  MB/sec
slice_one_middle_index  0.01 seconds   4551.36 MB/sec
stats                   1.09 seconds   41.75   MB/sec
stats_index             0.15 seconds   303.42  MB/sec
stats_everything        1.94 seconds   23.46   MB/sec
stats_everything_index  0.93 seconds   48.93   MB/sec
```

### Details

The purpose of these benchmarks is to provide a rough ballpark estimate of how
fast each command is. My hope is that they can also catch significant
performance regressions.

The `count` command can be viewed as a sort of baseline of the fastest possible
command that parses every record in CSV data.

The benchmarks that end with `_index` are run with indexing enabled.
