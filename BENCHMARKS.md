These are some very basic and unscientific benchmarks of various commands
provided by `xsv`. Please see below for more information.

These benchmarks were run with
[worldcitiespop_mil.csv](http://burntsushi.net/stuff/worldcitiespop_mil.csv),
which is a random 1,000,000 row subset of the world city population dataset
from the [Data Science Toolkit](https://github.com/petewarden/dstkdata).

These benchmarks were run on an Intel i3930K (6 CPUs, 12 threads) with 32GB of
memory.

```
count                   0.26 seconds    175.05 MB/sec
flatten                 5.76 seconds    7.90 MB/sec
flatten_condensed       5.91 seconds    7.70 MB/sec
frequency               2.83 seconds    16.08 MB/sec
index                   0.30 seconds    151.71 MB/sec
sample_10               0.45 seconds    101.14 MB/sec
sample_1000             0.48 seconds    94.82 MB/sec
sample_100000           0.64 seconds    71.11 MB/sec
search                  0.87 seconds    52.31 MB/sec
select                  0.45 seconds    101.14 MB/sec
sort                    3.51 seconds    12.96 MB/sec
slice_one_middle        0.22 seconds    206.88 MB/sec
slice_one_middle_index  0.01 seconds    4551.36 MB/sec
stats                   1.50 seconds    30.34 MB/sec
stats_index             0.25 seconds    182.05 MB/sec
stats_everything        4.74 seconds    9.60 MB/sec
stats_everything_index  3.31 seconds    13.75 MB/sec
```

### Details

The purpose of these benchmarks is to provide a rough ballpark estimate of how
fast each command is. My hope is that they can also catch significant
performance regressions.

The `count` command can be viewed as a sort of baseline of the fastest possible
command that parses every record in CSV data.

The benchmarks that end with `_index` are run with indexing enabled.

