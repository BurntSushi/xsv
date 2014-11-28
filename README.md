xsv is a command line program for indexing, slicing, analyzing, splitting
and joining CSV files. There are two primary goals: performance and
compositionality. To be more concrete:

1. With xsv, it should be easy to perform simple tasks.
2. Behavior that affects performance should be made explicit (and documented)
   in the command line interface.
3. xsv commands should be composable, but not at the expense of performance.

This README contains information on how to install `xsv` and a full set of
examples that demonstrate much of its functionality.

[![Build status](https://api.travis-ci.org/BurntSushi/xsv.png)](https://travis-ci.org/BurntSushi/xsv)


### A whirlwind tour

Let's say you're playing with some of the data from the
[Data Science Toolkit](https://github.com/petewarden/dstkdata), which contains
several CSV files. Maybe you're interested in the population counts of each
city in the world. So grab the data and start examining it:

```bash
$ curl -LO http://burntsushi.net/stuff/worldcitiespop.csv
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
using [elastic tabs](https://github.com/BurntSushi/tabwriter). You'll notice
that it even gets alignment right with respect to Unicode characters.

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
[CSV command line toolkit](https://csvkit.readthedocs.org/en/0.9.0/)
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
them at all. No matter---we can adjust our previous command so that it only
shows rows with a population cound:

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

Perhaps we can do this with the original CSV data? Indeed we can---because
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
given.


### Installation

Installing `xsv` is a bit hokey right now. Ideally, I could release binaries
for Linux, Mac and Windows. Currently, I'm only able to release binaries for
Linux because I don't know how to cross compile Rust programs.

With that said, you can grab the
[latest release](https://github.com/BurntSushi/xsv/releases/latest)
(Linux x86_64 binary) from GitHub:

```bash
$ curl -sOL https://github.com/BurntSushi/xsv/releases/download/0.4.9/xsv-0.4.9-x86_64-unknown-linux-gnu.tar.gz
$ tar xf xsv-0.4.9-x86_64-unknown-linux-gnu.tar.gz
$ cd xsv-0.4.9-x86_64-unknown-linux-gnu/
$ ./xsv --version
0.4.9
```

Alternatively, you can compile from source by
[installing Cargo](https://crates.io/install)
([Rust's](http://www.rust-lang.org/) package manager)
and building `xsv`:

```bash
git clone git://github.com/BurntSushi/xsv
cd xsv
cargo build --release
```

Compilation will probably take 1-2 minutes depending on your machine. The
binary will end up in `./target/release/xsv`.

**WORK IN PROGRESS**.

