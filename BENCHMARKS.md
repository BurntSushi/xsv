These are some very basic and unscientific benchmarks of various commands
provided by `xsv`. Please see below for more information.

These benchmarks were run with
[worldcitiespop_mil.csv](http://burntsushi.net/stuff/worldcitiespop_mil.csv),
which is a random 1,000,000 row subset of the world city population dataset
from the [Data Science Toolkit](https://github.com/petewarden/dstkdata).

These benchmarks were run on an Intel i3930K (6 CPUs, 12 threads) with 32GB of
memory.

```
count                   0.28 seconds    162.54 MB/sec
flatten                 5.31 seconds    8.57 MB/sec
flatten_condensed       5.39 seconds    8.44 MB/sec
frequency               2.54 seconds    17.91 MB/sec
index                   0.27 seconds    168.56 MB/sec
sample_10               0.47 seconds    96.83 MB/sec
sample_1000             0.49 seconds    92.88 MB/sec
sample_100000           0.62 seconds    73.40 MB/sec
search                  0.71 seconds    64.10 MB/sec
select                  0.47 seconds    96.83 MB/sec
sort                    3.36 seconds    13.54 MB/sec
slice_one_middle        0.22 seconds    206.88 MB/sec
slice_one_middle_index  0.01 seconds    4551.36 MB/sec
stats                   1.37 seconds    33.22 MB/sec
stats_index             0.23 seconds    197.88 MB/sec
stats_everything        3.90 seconds    11.67 MB/sec
stats_everything_index  2.58 seconds    17.64 MB/sec
```

### Details

The purpose of these benchmarks is to provide a rough ballpark estimate of how
fast each command is. My hope is that they can also catch significant
performance regressions.

The `count` command can be viewed as a sort of baseline of the fastest possible
command that parses every record in CSV data.

The benchmarks that end with `_index` are run with indexing enabled.

