These are some very basic and unscientific benchmarks of various commands
provided by `xsv`. Please see below for more information.

These benchmarks were run with
[worldcitiespop_mil.csv](http://burntsushi.net/stuff/worldcitiespop_mil.csv),
which is a random 1,000,000 row subset of the world city population dataset
from the [Data Science Toolkit](https://github.com/petewarden/dstkdata).

These benchmarks were run on an Intel i3930K (6 CPUs, 12 threads) with 32GB of
memory.

```
count                   0.54 seconds  84.28 MB/sec
flatten                 4.26 seconds  10.68 MB/sec
flatten_condensed       4.40 seconds  10.34 MB/sec
frequency               3.50 seconds  13.00 MB/sec
index                   0.52 seconds  87.52 MB/sec
sample_10               0.74 seconds  61.50 MB/sec
sample_1000             0.71 seconds  64.10 MB/sec
sample_100000           0.87 seconds  52.31 MB/sec
search                  1.11 seconds  41.00 MB/sec
select                  0.70 seconds  65.01 MB/sec
sort                    3.79 seconds  12.00 MB/sec
slice_one_middle        0.34 seconds  133.86 MB/sec
slice_one_middle_index  0.01 seconds  4551.36 MB/sec
stats                   1.56 seconds  29.17 MB/sec
stats_index             0.29 seconds  156.94 MB/sec
stats_everything        4.84 seconds  9.40 MB/sec
stats_everything_index  3.58 seconds  12.71 MB/sec
```


### Details

The purpose of these benchmarks is to provide a rough ballpark estimate of how
fast each command is. My hope is that they can also catch significant
performance regressions.

The `count` command can be viewed as a sort of baseline of the fastest possible
command that parses every record in CSV data.

The benchmarks that end with `_index` are run with indexing enabled.

