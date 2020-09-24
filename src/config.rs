use std::env;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use anyhow::Context;
use bstr::{ByteSlice, ByteVec};

use crate::app::{self, App, Args};
use crate::index::Indexed;
use crate::util;

#[derive(Debug)]
pub struct Patterns(Vec<String>);

impl Patterns {
    /// Defines both a positional 'pattern' argument (which can be provided
    /// zero or more times) and a 'pattern-file' flag (which can also be
    /// provided zero or more times).
    pub fn define(mut app: App) -> App {
        {
            const SHORT: &str = "A regex pattern (must be valid UTF-8).";
            app = app.arg(app::arg("pattern").multiple(true).help(SHORT));
        }
        {
            const SHORT: &str = "Read patterns from a file.";
            app = app.arg(
                app::flag("pattern-file")
                    .short("f")
                    .multiple(true)
                    .number_of_values(1)
                    .help(SHORT),
            );
        }
        app
    }

    /// Reads at least one pattern from either positional arguments (preferred)
    /// or from pattern files. If no patterns could be found, then an error
    /// is returned.
    pub fn get(args: &Args) -> anyhow::Result<Patterns> {
        if let Some(os_patterns) = args.values_of_os("pattern") {
            if args.value_of_os("pattern-file").is_some() {
                anyhow::bail!(
                    "cannot provide both positional patterns and \
                     --pattern-file"
                );
            }
            let mut patterns = vec![];
            for (i, p) in os_patterns.enumerate() {
                let p = match p.to_str() {
                    Some(p) => p,
                    None => anyhow::bail!("pattern {} is not valid UTF-8", i),
                };
                patterns.push(p.to_string());
            }
            Ok(Patterns(patterns))
        } else if let Some(pfile) = args.value_of_os("pattern-file") {
            let path = std::path::Path::new(pfile);
            let contents =
                std::fs::read_to_string(path).with_context(|| {
                    anyhow::anyhow!("failed to read {}", path.display())
                })?;
            Ok(Patterns(contents.lines().map(|x| x.to_string()).collect()))
        } else {
            Err(anyhow::anyhow!("no regex patterns given"))
        }
    }

    /// Returns a slice of the patterns read.
    pub fn as_strings(&self) -> &[String] {
        &self.0
    }
}

impl IntoIterator for Patterns {
    type IntoIter = std::vec::IntoIter<String>;
    type Item = String;

    fn into_iter(self) -> std::vec::IntoIter<String> {
        self.0.into_iter()
    }
}

/// A type that represents some kind of optional input data, usually a file or
/// stdin.
#[derive(Debug)]
pub enum Input {
    Path(PathBuf),
    Stdin,
}

impl Input {
    /// Defines a single required positional parameter that accepts a file
    /// path.
    pub fn define(app: App, desc: &'static str, name: &'static str) -> App {
        app.arg(app::arg(name).help(desc))
    }

    /// Reads the input with the given CLI arg name. If no value is present,
    /// then None is returned.
    pub fn get(args: &Args, name: &'static str) -> Option<Input> {
        let arg = args.value_of_os(name)?;
        if arg == "-" {
            Some(Input::Stdin)
        } else {
            Some(Input::Path(PathBuf::from(arg)))
        }
    }

    /// Reads the input with the given CLI arg name. If no value is present,
    /// then a stdin Input is returned.
    pub fn get_required(args: &Args, name: &'static str) -> Input {
        Input::get(args, name).unwrap_or(Input::Stdin)
    }

    /// Returns an unbuffered reader for this input.
    ///
    /// If there was a problem opening the reader, then this returns an error.
    pub fn reader(&self) -> anyhow::Result<Box<dyn io::Read + Send>> {
        match self.file_reader()? {
            None => Ok(Box::new(io::stdin())),
            Some((file, _)) => Ok(Box::new(file)),
        }
    }

    /// Returns a file reader (and its path) for this input if and only if this
    /// input corresponds to a named path.
    pub fn file_reader(&self) -> anyhow::Result<Option<(File, &Path)>> {
        let path = match self.path() {
            None => return Ok(None),
            Some(path) => path,
        };
        let file = File::open(&path).with_context(|| {
            format!("failed to open file {}", path.display())
        })?;
        Ok(Some((file, path)))
    }

    /// Create a file-backed read-only memory map from this input, if possible.
    /// If this input refers to stdin, then this returns None. If this is a
    /// path and there was a problem opening the memory map, then this returns
    /// an error.
    ///
    /// This is unsafe because creating memory maps is unsafe. In general,
    /// callers must assume that the underlying file is not mutated.
    pub unsafe fn mmap(&self) -> anyhow::Result<Option<memmap::Mmap>> {
        let path = match self.path() {
            None => return Ok(None),
            Some(path) => path,
        };
        let file = fs::File::open(path).with_context(|| {
            format!("failed to open file {}", path.display())
        })?;
        let mmap = memmap::Mmap::map(&file).with_context(|| {
            format!("failed to mmap file {}", path.display())
        })?;
        Ok(Some(mmap))
    }

    /// Configure a CSV reader builder based on the settings derived from the
    /// input name only.
    ///
    /// Essentially, if the input name ends with a common extension (like csv
    /// or tsv), then its delimiter will be automatically set based on that.
    ///
    /// Callers should prefer calling this before more explicit CSV
    /// configuration. For example, a -d/--delimiter flag should override this
    /// setting.
    pub fn csv_configure(&self, builder: &mut csv::ReaderBuilder) {
        if let Some(delimiter) = self.delimiter() {
            builder.delimiter(delimiter);
        }
    }

    /// If possible, infer the delimiter for the CSV data from the file
    /// extension, if one exists. If no file extension exists or if it is not
    /// recognized, then None is returned.
    pub fn delimiter(&self) -> Option<u8> {
        let ext = self.path().and_then(|p| p.extension())?;
        if ext == "tsv" || ext == "tab" {
            Some(b'\t')
        } else if ext == "csv" {
            Some(b',')
        } else {
            None
        }
    }

    /// Returns the file path associated with this input, if one exists. If
    /// this input corresponds to stdin, then None is returned.
    pub fn path(&self) -> Option<&Path> {
        match *self {
            Input::Stdin => None,
            Input::Path(ref p) => Some(p),
        }
    }

    /// Returns the path to the index file for this input, if possible.
    ///
    /// The index file path is generated from the input only if the input is
    /// given by a named file. The index file path corresponds to
    /// `{path-to-csv}.idx`.
    ///
    /// Note that the caller is responsible for determining whether the index
    /// is stale or not and whether the index exists.
    pub fn index_path(&self) -> Option<PathBuf> {
        // It's kind of crazy that there is no simple API for just tacking a
        // '.idx' on to the end of a file path. Instead, we just sacrifice
        // Windows. This is guaranteed to work losslessly on Unix.
        let mut path = Vec::from_path_lossy(self.path()?).into_owned();
        path.push_str(".idx");
        Some(path.into_path_buf_lossy())
    }
}

/// A type that represents some kind of required input data, usually a file or
/// stdin. This type ensures that the user provides some argument.
#[derive(Debug)]
pub struct InputRequired(Input);

impl InputRequired {
    /// Defines a single required positional parameter that accepts a file
    /// path.
    pub fn define(app: App, desc: &'static str, name: &'static str) -> App {
        app.arg(app::arg(name).help(desc).required(true))
    }

    /// Reads the input with the given CLI arg name. If no value is present,
    /// then this routine panics. (Because the arg parser should guarantee that
    /// a value is present.)
    pub fn get(args: &Args, name: &'static str) -> InputRequired {
        match Input::get(args, name).map(InputRequired) {
            Some(inp) => inp,
            None => panic!("required argument '{}' was not found", name),
        }
    }

    /// Returns an unbuffered reader for this input.
    ///
    /// If there was a problem opening the reader, then this returns an error.
    pub fn reader(&self) -> anyhow::Result<Box<dyn io::Read + Send>> {
        self.0.reader()
    }

    /// Returns a file reader (and its path) for this input if and only if this
    /// input corresponds to a named path.
    pub fn file_reader(&self) -> anyhow::Result<Option<(File, &Path)>> {
        self.0.file_reader()
    }

    /// Create a file-backed read-only memory map from this input, if possible.
    /// If this input refers to stdin, then this returns None. If this is a
    /// path and there was a problem opening the memory map, then this returns
    /// an error.
    ///
    /// This is unsafe because creating memory maps is unsafe. In general,
    /// callers must assume that the underlying file is not mutated.
    pub unsafe fn mmap(&self) -> anyhow::Result<Option<memmap::Mmap>> {
        self.0.mmap()
    }

    /// Returns the file path associated with this input, if one exists. If
    /// this input corresponds to stdin, then None is returned.
    pub fn path(&self) -> Option<&Path> {
        self.0.path()
    }

    /// Returns the path to the index file for this input, if possible.
    ///
    /// The index file path is generated from the input only if the input is
    /// given by a named file. The index file path corresponds to
    /// `{path-to-csv}.idx`.
    ///
    /// Note that the caller is responsible for determining whether the index
    /// is stale or not and whether the index exists.
    pub fn index_path(&self) -> Option<PathBuf> {
        self.0.index_path()
    }
}

/// A basic configuration for reading CSV data. This only defines common
/// options like delimiter and header settings which are used in virtually
/// every command that reads CSV. A more complete set of CSV reader settings
/// can be found in the 'xsv input' command.
#[derive(Debug)]
pub struct CsvRead {
    /// When explicitly given (via CLI or env var), this overrides any other
    /// kind of delimiter setting (e.g., inference from file extension).
    delimiter: Option<u8>,
    /// Whether the first row of the CSV data should be interpreted as headers
    /// or not.
    headers: bool,
}

impl CsvRead {
    /// Defines the common flags used for a CSV reader configuration.
    pub fn define(mut app: App) -> App {
        {
            const SHORT: &str = "The field delimiter for reading CSV data.";
            const LONG: &str = "\
The field delimiter for reading CSV data.
";
            app = app.arg(
                app::flag("delimiter")
                    .short("d")
                    .env("XSV_DELIMITER")
                    .help(SHORT)
                    .long_help(LONG),
            );
        }
        {
            const SHORT: &str =
                "Do not interpret the first record as a header.";
            const LONG: &str = "\
Do not interpret the first record as a header.
";
            app = app.arg(
                app::switch("no-headers")
                    .short("n")
                    .help(SHORT)
                    .long_help(LONG),
            );
        }
        app
    }

    /// Retrieve the "common" CSV reader configuration from the CLI.
    pub fn get(args: &Args) -> anyhow::Result<CsvRead> {
        let headers = !args.is_present("no-headers")
            && !util::is_env_true("XSV_NO_HEADERS");
        let delimiter = match args.value_of_os("delimiter") {
            None => None,
            Some(val) => Some(util::get_one_byte(&val)?),
        };
        Ok(CsvRead { delimiter, headers })
    }

    /// Configure a CSV reader builder with the CLI settings.
    ///
    /// Note that if no delimiter is specified, then it is not configured on
    /// the given reader. (If the reader is never configured with a delimiter,
    /// then it will use its default, ','.)
    pub fn csv_configure(&self, builder: &mut csv::ReaderBuilder) {
        let mut headers = self.headers;
        // This is the same behavior that xsv had before the rewrite. When set,
        // we toggle the meaning of the --no-headers flag. It seems a little
        // weird to me now at this point, but I figure we should keep it.
        if util::is_env_true("XSV_TOGGLE_HEADERS") {
            headers = !headers;
        }
        builder.has_headers(headers);
        if let Some(delimiter) = self.delimiter {
            builder.delimiter(delimiter);
        }
    }

    /// Return a CSV reader for the data pointed to by the given input.
    pub fn csv_reader(
        &self,
        inp: &Input,
    ) -> anyhow::Result<csv::Reader<Box<dyn io::Read + Send>>> {
        let mut builder = csv::ReaderBuilder::new();
        inp.csv_configure(&mut builder);
        self.csv_configure(&mut builder);
        Ok(builder.from_reader(inp.reader()?))
    }

    /// If an index file exists for the given input, then return an indexed
    /// CSV reader. Otherwise, return None.
    ///
    /// This returns an error if the index exists but there was a problem
    /// opening it. (Which may include a stale index.)
    pub fn indexed_csv_reader(
        &self,
        inp: &Input,
    ) -> anyhow::Result<Option<Indexed<File, File>>> {
        let (csv_file, csv_path) = match inp.file_reader()? {
            None => return Ok(None),
            Some(x) => x,
        };
        let idx_path = match inp.index_path() {
            None => return Ok(None),
            Some(idx_path) => idx_path,
        };
        let idx_file = match File::open(&idx_path) {
            Ok(file) => file,
            Err(err) => {
                // If the index file just doesn't exist, then that's fine,
                // we just don't use the index. But if there was some other
                // problem, propagate that to the user.
                if err.kind() == std::io::ErrorKind::NotFound {
                    return Ok(None);
                }
                return Err(anyhow::Error::new(err)
                    .context(format!("{}", idx_path.display())));
            }
        };
        // The index file exists, but is it stale? Doing this based on file
        // modification times isn't 100% correct, but I think it's probably
        // good enough.
        let csv_md = csv_file
            .metadata()
            .with_context(|| format!("{}", csv_path.display()))?;
        let idx_md = idx_file
            .metadata()
            .with_context(|| format!("{}", idx_path.display()))?;
        if util::last_modified(&csv_md) > util::last_modified(&idx_md) {
            anyhow::bail!(
                "csv file {} was last modified more recently than its \
                 index {}, please re-generate index",
                csv_path.display(),
                idx_path.display()
            );
        }

        let mut builder = csv::ReaderBuilder::new();
        inp.csv_configure(&mut builder);
        self.csv_configure(&mut builder);
        Indexed::open(builder.from_reader(csv_file), idx_file).map(Some)
    }
}
