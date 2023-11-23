# A love letter to the CSV format

*Or why people pretending CSV is dead are wrong*

Every month or so, a new blog article declaring the near demise of CSV in favor of some "obviously superior" format ([parquet](https://parquet.apache.org/), newline-delimited JSON, [MessagePack](https://msgpack.org/) records etc.) find its ways to the reader's eyes. Sadly those articles often offer a very narrow and biased comparison and often fail to understand what makes CSV a seemingly unkillable staple of data serialization.

It is therefore my intention, through this article, to write a love letter to this data format, often criticized for the wrong reasons, even more so when it is somehow deemed "cool" to hate on it. My point is not, far from it, to say that CSV is a silver bullet but rather to shine a light on some of the format's sometimes overlooked strengths.

## 1. CSV is dead simple

The specification of CSV holds in its title: "comma separated values". Okay, it's a lie, but still, the specification holds in a tweet and can be explained to anybody in seconds: commas separate values, new lines separate rows. Now quote values containing commas and line breaks, double your quotes, and that's it. This is so simple you might even invent it yourself without knowing it already exists while learning how to program.

Of course it does not mean you should not use a dedicated CSV parser/writer because you *will* mess something up.

## 2. CSV is a collective idea

No one owns CSV. It has no real specification (yes, I know about the controversial ex-post [RFC 4180](https://datatracker.ietf.org/doc/html/rfc4180)), just a set of rules everyone kinda agrees to respect implicitly. It is, and will forever remain, an open and free collective idea.

## 3. CSV is text

Like JSON, YAML or XML, CSV is just plain text, that you are free to encode however you like. CSV is not a binary format, can be opened with any text editor and does not require any specialized program to be read (I am looking at you XLS...).

## 4. CSV is streamable

CSV can be read row by row very easily without requiring more memory than what is needed to fit a single row. This also means that a trivial program that anyone can write is able to read gigabytes of CSV data with only some kilobytes of RAM.

By comparison, column-oriented data formats such as parquet are not able to stream files row by row without requiring you to jump here and there in the file or to buffer the memory cleverly so you don't tank read performance.

But of course, CSV is terrible if you are only interested in specific columns because you will indeed need to read all of a row only to access the part you are interested in.

Column-oriented data format are of course a very good fit for the dataframes mindset of R, pandas and such. But critics of CSV coming from this set of pratices tend to only care about use-cases where everything is expected to fit into memory.

## 5. CSV can be appended to

It is trivial to add new rows at the end of a CSV file and it is very efficient to do so. Just open the file in append mode (`a+`) and get going.

Once again, column-oriented data formats cannot do this, or at least not in a straightforward manner. They can actually be regarded as on-disk dataframes, and like with dataframes, adding a column is very efficient while adding a new row really isn't.

## 6. CSV is dynamically typed

Please don't flee. Let me explain why this is sometimes a good thing. Sometimes when dealing with data, you might like to have some flexibility, especially across programming languages, when parsing serialized data.

Consider JavaScript, for instance, that is unable to represent 64 bits integers. Or what languages, frameworks and libraries consider as null values (don't get me started on pandas and null values). CSV lets you parse values as you see fit and is in fact dynamically typed. But this is as much of a strength as it can become a potential footgun if you are not careful.

Note also, but this might be hard to do with higher-level languages such as python and JavaScript, that you are not required to decode the text at all to process CSV cell values and that you can work directly on the binary representation of the text for performance reasons.

## 7. CSV is succinct

Having the headers written only once at the beginning of the file means the amount of formal repetition of the format is naturally very low. Consider a list of objects in JSON or the equivalent in XML and you will quickly see the cost of repeating keys everywhere. That does not mean JSON and XML will not compress very well, but few formats exhibit this level of natural conciseness.

What's more, strings are often already optimally represented and the overhead of the format itself (some commas and quotes here and there) is kept to a minimum. Of course, statically-typed numbers could be represented more concisely, but you will not save up an order of magnitude there neither.

## 8. Reverse CSV is still valid CSV

This one is not often realized by everyone but a reversed (byte by byte) CSV file, is still valid CSV. This is only made possible because of the genius idea to escape quotes by doubling them, which means escaping is a palindrom. It would not work if CSV used a backslash-based escaping scheme, as is most common when representing string literals.

But why should you care? Well, this means you can read very efficiently and very easily the last rows of a CSV file. Just feed the bytes of your file in reverse order to a CSV parser, then reverse the yielded rows and their cells' bytes and you are done (maybe read the header row before though).

This means you can very well use a CSV output as a way to efficiently resume an aborted process. You can indeed read and parse the last rows of a CSV file in constant time since you don't need to read the whole file but only to position yourself at the end of the file to buffer the bytes in reverse and feed them to the parser.

## 9. Excel hates CSV

It clearly means CSV must be doing something right.

<!-- flaws about multiplexing, asv -->
