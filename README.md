xsv is a command line program for indexing, slicing, analyzing, splitting
and joining CSV files. Commands should be simple, fast and composable:

1. Simple tasks should be easy.
2. Performance trade offs should be exposed in the CLI interface.
3. Composition should not come at the expense of performance.

This README contains information on how to
[install `xsv`](https://github.com/BurntSushi/xsv#installation), in addition to
a quick tour of several commands.

Dual-licensed under MIT or the [UNLICENSE](https://unlicense.org).

### How to install

This forked version of xsv can be installed using cargo:

```
cargo install --git https://github.com/medialab/xsv.git --branch prod
```

To install optional features (such as `lang` or `py`, for instance):

```
# Everything
cargo install --git https://github.com/medialab/xsv.git --branch prod --all-features

# Selected features
cargo install --git https://github.com/medialab/xsv.git --branch prod --features lang,py
```

**New Features**

* `xsv apply`
* `xsv behead`
* `xsv datefmt`
* `xsv enum`
* `xsv explode`
* `xsv flatten --pretty`
* `xsv foreach`
* `xsv frequency --pretty`
* `xsv join --prefix-left/--prefix-right`
* `xsv jsonl`
* `xsv lang`
* `xsv lua`
* `xsv pseudo`
* `xsv py`
* `xsv replace`
* `xsv reverse --in-memory`
* `xsv search --exact`
* `xsv search --flag col`
* `xsv shuffle`
* `xsv sort -u`
* `xsv stats --pretty`

### Available commands

* **apply** - Apply series of string transformations to a CSV column.
* **behead** - Drop headers from CSV file.
* **cat** - Concatenate CSV files by row or by column.
* **count** - Count the rows in a CSV file. (Instantaneous with an index.)
* **datefmt** - Add a column with the date from a CSV column in a specified format and timezone.
* **enum** - Add a new column enumerating rows by adding a column of incremental or uuid identifiers. Can also be used to copy a column or fill a new column with a constant value.
* **explode** - Explode rows into multiple ones by splitting a column value based on the
given separator.
* **foreach** - Loop over a CSV file to execute bash commands.
* **fixlengths** - Force a CSV file to have same-length records by either
  padding or truncating them.
* **flatten** - A flattened view of CSV records. Useful for viewing one record
  at a time. e.g., `xsv slice -i 5 data.csv | xsv flatten`.
* **fmt** - Reformat CSV data with different delimiters, record terminators
  or quoting rules. (Supports ASCII delimited data.)
* **frequency** - Build frequency tables of each column in CSV data. (Uses
  parallelism to go faster if an index is present.)
* **headers** - Show the headers of CSV data. Or show the intersection of all
  headers between many CSV files.
* **index** - Create an index for a CSV file. This is very quick and provides
  constant time indexing into the CSV file.
* **input** - Read CSV data with exotic quoting/escaping rules.
* **jsonl** - Convert newline-delimited JSON to CSV.
* **join** - Inner, outer and cross joins. Uses a simple hash index to make it
  fast.
* **lang**, *optional* - Add a column with the language detected in a given CSV column.
* **lua**, *optional* - Execute a Lua script over CSV lines to transform, aggregate or filter them.
* **partition** - Partition CSV data based on a column value.
* **pseudo** - Pseudonymise the value of the given column by replacing them by an incremental identifier.
* **py**, *optional* - Evaluate a Python expression over CSV lines to transform, aggregate or filter them.
* **sample** - Randomly draw rows from CSV data using reservoir sampling (i.e.,
  use memory proportional to the size of the sample).
* **replace** - Replace CSV data using a regex.
* **reverse** - Reverse order of rows in CSV data.
* **search** - Run a regex over CSV data. Applies the regex to each field
  individually and shows only matching rows.
* **select** - Select or re-order columns from CSV data.
* **shuffle** - Shuffle rows of a CSV file.
* **slice** - Slice rows from any part of a CSV file. When an index is present,
  this only has to parse the rows in the slice (instead of all rows leading up
  to the start of the slice).
* **sort** - Sort CSV data.
* **split** - Split one CSV file into many CSV files of N chunks.
* **stats** - Show basic types and statistics of each column in the CSV file.
  (i.e., mean, standard deviation, median, range, etc.)
* **table** - Show aligned output of any CSV data using
  [elastic tabstops](https://github.com/BurntSushi/tabwriter).


### A whirlwind tour

Let's say you're playing with some of the data from the
[Data Science Toolkit](https://github.com/petewarden/dstkdata), which contains
several CSV files. Maybe you're interested in the population counts of each
city in the world. So grab the data and start examining it:

```bash
$ curl -LO https://burntsushi.net/stuff/worldcitiespop.csv
$ xsv headers worldcitiespop.csv
1   Country
2   City
3   AccentCity
4   Region
5   Population
6   Latitude
7   Longitude
```

The next thing you might want to do is get an overview of the kind of data that
appears in each column. The `stats` command will do this for you:

```bash
$ xsv stats worldcitiespop.csv --everything | xsv table
field       type     min            max            min_length  max_length  mean          stddev         median     mode         cardinality
Country     Unicode  ad             zw             2           2                                                   cn           234
City        Unicode   bab el ahmar  Þykkvibaer     1           91                                                  san jose     2351892
AccentCity  Unicode   Bâb el Ahmar  ïn Bou Chella  1           91                                                  San Antonio  2375760
Region      Unicode  00             Z9             0           2                                        13         04           397
Population  Integer  7              31480498       0           8           47719.570634  302885.559204  10779                   28754
Latitude    Float    -54.933333     82.483333      1           12          27.188166     21.952614      32.497222  51.15        1038349
Longitude   Float    -179.983333    180            1           14          37.08886      63.22301       35.28      23.8         1167162
```

The `xsv table` command takes any CSV data and formats it into aligned columns
using [elastic tabstops](https://github.com/BurntSushi/tabwriter). You'll
notice that it even gets alignment right with respect to Unicode characters.

So, this command takes about 12 seconds to run on my machine, but we can speed
it up by creating an index and re-running the command:

```bash
$ xsv index worldcitiespop.csv
$ xsv stats worldcitiespop.csv --everything | xsv table
...
```

Which cuts it down to about 8 seconds on my machine. (And creating the index
takes less than 2 seconds.)

Notably, the same type of "statistics" command in another
[CSV command line toolkit](https://csvkit.readthedocs.io/)
takes about 2 minutes to produce similar statistics on the same data set.

Creating an index gives us more than just faster statistics gathering. It also
makes slice operations extremely fast because *only the sliced portion* has to
be parsed. For example, let's say you wanted to grab the last 10 records:

```bash
$ xsv count worldcitiespop.csv
3173958
$ xsv slice worldcitiespop.csv -s 3173948 | xsv table
Country  City               AccentCity         Region  Population  Latitude     Longitude
zw       zibalonkwe         Zibalonkwe         06                  -19.8333333  27.4666667
zw       zibunkululu        Zibunkululu        06                  -19.6666667  27.6166667
zw       ziga               Ziga               06                  -19.2166667  27.4833333
zw       zikamanas village  Zikamanas Village  00                  -18.2166667  27.95
zw       zimbabwe           Zimbabwe           07                  -20.2666667  30.9166667
zw       zimre park         Zimre Park         04                  -17.8661111  31.2136111
zw       ziyakamanas        Ziyakamanas        00                  -18.2166667  27.95
zw       zizalisari         Zizalisari         04                  -17.7588889  31.0105556
zw       zuzumba            Zuzumba            06                  -20.0333333  27.9333333
zw       zvishavane         Zvishavane         07      79876       -20.3333333  30.0333333
```

These commands are *instantaneous* because they run in time and memory
proportional to the size of the slice (which means they will scale to
arbitrarily large CSV data).

Switching gears a little bit, you might not always want to see every column in
the CSV data. In this case, maybe we only care about the country, city and
population. So let's take a look at 10 random rows:

```bash
$ xsv select Country,AccentCity,Population worldcitiespop.csv \
  | xsv sample 10 \
  | xsv table
Country  AccentCity       Population
cn       Guankoushang
za       Klipdrift
ma       Ouled Hammou
fr       Les Gravues
la       Ban Phadèng
de       Lüdenscheid      80045
qa       Umm ash Shubrum
bd       Panditgoan
us       Appleton
ua       Lukashenkivske
```

Whoops! It seems some cities don't have population counts. How pervasive is
that?

```bash
$ xsv frequency worldcitiespop.csv --limit 5
field,value,count
Country,cn,238985
Country,ru,215938
Country,id,176546
Country,us,141989
Country,ir,123872
City,san jose,328
City,san antonio,320
City,santa rosa,296
City,santa cruz,282
City,san juan,255
AccentCity,San Antonio,317
AccentCity,Santa Rosa,296
AccentCity,Santa Cruz,281
AccentCity,San Juan,254
AccentCity,San Miguel,254
Region,04,159916
Region,02,142158
Region,07,126867
Region,03,122161
Region,05,118441
Population,(NULL),3125978
Population,2310,12
Population,3097,11
Population,983,11
Population,2684,11
Latitude,51.15,777
Latitude,51.083333,772
Latitude,50.933333,769
Latitude,51.116667,769
Latitude,51.133333,767
Longitude,23.8,484
Longitude,23.2,477
Longitude,23.05,476
Longitude,25.3,474
Longitude,23.1,459
```

(The `xsv frequency` command builds a frequency table for each column in the
CSV data. This one only took 5 seconds.)

So it seems that most cities do not have a population count associated with
them at all. No matter—we can adjust our previous command so that it only
shows rows with a population count:

```bash
$ xsv search -s Population '[0-9]' worldcitiespop.csv \
  | xsv select Country,AccentCity,Population \
  | xsv sample 10 \
  | xsv table
Country  AccentCity       Population
es       Barañáin         22264
es       Puerto Real      36946
at       Moosburg         4602
hu       Hejobaba         1949
ru       Polyarnyye Zori  15092
gr       Kandíla          1245
is       Ólafsvík         992
hu       Decs             4210
bg       Sliven           94252
gb       Leatherhead      43544
```

Erk. Which country is `at`? No clue, but the Data Science Toolkit has a CSV
file called `countrynames.csv`. Let's grab it and do a join so we can see which
countries these are:

```bash
curl -LO https://gist.githubusercontent.com/anonymous/063cb470e56e64e98cf1/raw/98e2589b801f6ca3ff900b01a87fbb7452eb35c7/countrynames.csv
$ xsv headers countrynames.csv
1   Abbrev
2   Country
$ xsv join --no-case  Country sample.csv Abbrev countrynames.csv | xsv table
Country  AccentCity       Population  Abbrev  Country
es       Barañáin         22264       ES      Spain
es       Puerto Real      36946       ES      Spain
at       Moosburg         4602        AT      Austria
hu       Hejobaba         1949        HU      Hungary
ru       Polyarnyye Zori  15092       RU      Russian Federation | Russia
gr       Kandíla          1245        GR      Greece
is       Ólafsvík         992         IS      Iceland
hu       Decs             4210        HU      Hungary
bg       Sliven           94252       BG      Bulgaria
gb       Leatherhead      43544       GB      Great Britain | UK | England | Scotland | Wales | Northern Ireland | United Kingdom
```

Whoops, now we have two columns called `Country` and an `Abbrev` column that we
no longer need. This is easy to fix by re-ordering columns with the `xsv
select` command:

```bash
$ xsv join --no-case  Country sample.csv Abbrev countrynames.csv \
  | xsv select 'Country[1],AccentCity,Population' \
  | xsv table
Country                                                                              AccentCity       Population
Spain                                                                                Barañáin         22264
Spain                                                                                Puerto Real      36946
Austria                                                                              Moosburg         4602
Hungary                                                                              Hejobaba         1949
Russian Federation | Russia                                                          Polyarnyye Zori  15092
Greece                                                                               Kandíla          1245
Iceland                                                                              Ólafsvík         992
Hungary                                                                              Decs             4210
Bulgaria                                                                             Sliven           94252
Great Britain | UK | England | Scotland | Wales | Northern Ireland | United Kingdom  Leatherhead      43544
```

Perhaps we can do this with the original CSV data? Indeed we can—because
joins in `xsv` are fast.

```bash
$ xsv join --no-case Abbrev countrynames.csv Country worldcitiespop.csv \
  | xsv select '!Abbrev,Country[1]' \
  > worldcitiespop_countrynames.csv
$ xsv sample 10 worldcitiespop_countrynames.csv | xsv table
Country                      City                   AccentCity             Region  Population  Latitude    Longitude
Sri Lanka                    miriswatte             Miriswatte             36                  7.2333333   79.9
Romania                      livezile               Livezile               26      1985        44.512222   22.863333
Indonesia                    tawainalu              Tawainalu              22                  -4.0225     121.9273
Russian Federation | Russia  otar                   Otar                   45                  56.975278   48.305278
France                       le breuil-bois robert  le Breuil-Bois Robert  A8                  48.945567   1.717026
France                       lissac                 Lissac                 B1                  45.103094   1.464927
Albania                      lumalasi               Lumalasi               46                  40.6586111  20.7363889
China                        motzushih              Motzushih              11                  27.65       111.966667
Russian Federation | Russia  svakino                Svakino                69                  55.60211    34.559785
Romania                      tirgu pancesti         Tirgu Pancesti         38                  46.216667   27.1
```

The `!Abbrev,Country[1]` syntax means, "remove the `Abbrev` column and remove
the second occurrence of the `Country` column." Since we joined with
`countrynames.csv` first, the first `Country` name (fully expanded) is now
included in the CSV data.

This `xsv join` command takes about 7 seconds on my machine. The performance
comes from constructing a very simple hash index of one of the CSV data files
given. The `join` command does an inner join by default, but it also has left,
right and full outer join support too.


### Installation

Binaries for Windows, Linux and macOS are available [from Github](https://github.com/BurntSushi/xsv/releases/latest).

If you're a **macOS Homebrew** user, then you can install xsv
from homebrew-core:

```
$ brew install xsv
```

If you're a **macOS MacPorts** user, then you can install xsv
from the [official ports](https://www.macports.org/ports.php?by=name&substr=xsv):

```
$ sudo port install xsv
```

If you're a **Nix/NixOS** user, you can install xsv from nixpkgs:

```
$ nix-env -i xsv
```

Alternatively, you can compile from source by
[installing Cargo](https://crates.io/install)
([Rust's](https://www.rust-lang.org/) package manager)
and installing `xsv` using Cargo:

```bash
cargo install xsv
```

Compiling from this repository also works similarly:

```bash
git clone git://github.com/BurntSushi/xsv
cd xsv
cargo build --release
```

Compilation will probably take a few minutes depending on your machine. The
binary will end up in `./target/release/xsv`.


### Benchmarks

I've compiled some [very rough
benchmarks](https://github.com/BurntSushi/xsv/blob/master/BENCHMARKS.md) of
various `xsv` commands.


### Motivation

Here are several valid criticisms of this project:

1. You shouldn't be working with CSV data because CSV is a terrible format.
2. If your data is gigabytes in size, then CSV is the wrong storage type.
3. Various SQL databases provide all of the operations available in `xsv` with
   more sophisticated indexing support. And the performance is a zillion times
   better.

I'm sure there are more criticisms, but the impetus for this project was a 40GB
CSV file that was handed to me. I was tasked with figuring out the shape of the
data inside of it and coming up with a way to integrate it into our existing
system. It was then that I realized that every single CSV tool I knew about was
woefully inadequate. They were just too slow or didn't provide enough
flexibility. (Another project I had comprised of a few dozen CSV files. They
were smaller than 40GB, but they were each supposed to represent the same kind
of data. But they all had different column and unintuitive column names. Useful
CSV inspection tools were critical here—and they had to be reasonably fast.)

The key ingredients for helping me with my task were indexing, random sampling,
searching, slicing and selecting columns. All of these things made dealing with
40GB of CSV data a bit more manageable (or dozens of CSV files).

Getting handed a large CSV file *once* was enough to launch me on this quest.
From conversations I've had with others, CSV data files this large don't seem
to be a rare event. Therefore, I believe there is room for a tool that has a
hope of dealing with data that large.


### Naming collision

This project is unrelated to another similar project with the same name:
https://mj.ucw.cz/sw/xsv/
