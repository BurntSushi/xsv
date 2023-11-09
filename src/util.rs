use std::borrow::Cow;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str;
use std::thread;
use std::time;

use byteorder::{ByteOrder, LittleEndian};
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use colored::{Color, ColoredString, Colorize, Styles};
use csv;
use dateparser::parse_with_timezone;
use docopt::Docopt;
use num_cpus;
use numfmt::{Formatter, Numeric, Precision};
use rand::{SeedableRng, StdRng};
use serde::de::{Deserialize, DeserializeOwned, Deserializer, Error};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use config::{Config, Delimiter};
use select::SelectColumns;
use CliResult;

pub fn num_cpus() -> usize {
    num_cpus::get()
}

pub fn version() -> String {
    let (maj, min, pat, pre) = (
        option_env!("CARGO_PKG_VERSION_MAJOR"),
        option_env!("CARGO_PKG_VERSION_MINOR"),
        option_env!("CARGO_PKG_VERSION_PATCH"),
        option_env!("CARGO_PKG_VERSION_PRE"),
    );
    match (maj, min, pat, pre) {
        (Some(maj), Some(min), Some(pat), Some(pre)) => {
            if pre.is_empty() {
                format!("{}.{}.{}", maj, min, pat)
            } else {
                format!("{}.{}.{}-{}", maj, min, pat, pre)
            }
        }
        _ => "".to_owned(),
    }
}

pub fn get_args<T>(usage: &str, argv: &[&str]) -> CliResult<T>
where
    T: DeserializeOwned,
{
    Docopt::new(usage)
        .and_then(|d| {
            d.argv(argv.iter().copied())
                .version(Some(version()))
                .deserialize()
        })
        .map_err(From::from)
}

pub fn many_configs(
    inps: &[String],
    delim: Option<Delimiter>,
    no_headers: bool,
    select: Option<&SelectColumns>,
) -> Result<Vec<Config>, String> {
    let mut inps = inps.to_vec();
    if inps.is_empty() {
        inps.push("-".to_owned()); // stdin
    }
    let confs = inps
        .into_iter()
        .map(|p| {
            let mut conf = Config::new(&Some(p))
                .delimiter(delim)
                .no_headers(no_headers);

            if let Some(sel) = select {
                conf = conf.select(sel.clone());
            }

            conf
        })
        .collect::<Vec<_>>();
    errif_greater_one_stdin(&confs)?;
    Ok(confs)
}

pub fn errif_greater_one_stdin(inps: &[Config]) -> Result<(), String> {
    let nstd = inps.iter().filter(|inp| inp.is_std()).count();
    if nstd > 1 {
        return Err("At most one <stdin> input is allowed.".to_owned());
    }
    Ok(())
}

pub fn chunk_size(nitems: usize, njobs: usize) -> usize {
    if nitems < njobs {
        nitems
    } else {
        nitems / njobs
    }
}

pub fn num_of_chunks(nitems: usize, chunk_size: usize) -> usize {
    if chunk_size == 0 {
        return nitems;
    }
    let mut n = nitems / chunk_size;
    if nitems % chunk_size != 0 {
        n += 1;
    }
    n
}

pub fn last_modified(md: &fs::Metadata) -> u64 {
    use filetime::FileTime;
    FileTime::from_last_modification_time(md).seconds_relative_to_1970()
}

pub fn idx_path(csv_path: &Path) -> PathBuf {
    let mut p = csv_path
        .to_path_buf()
        .into_os_string()
        .into_string()
        .unwrap();
    p.push_str(".idx");
    PathBuf::from(&p)
}

pub type Idx = Option<usize>;

pub fn range(start: Idx, end: Idx, len: Idx, index: Idx) -> Result<(usize, usize), String> {
    match (start, end, len, index) {
        (None, None, None, Some(i)) => Ok((i, i + 1)),
        (_, _, _, Some(_)) => Err("--index cannot be used with --start, --end or --len".to_owned()),
        (_, Some(_), Some(_), None) => {
            Err("--end and --len cannot be used at the same time.".to_owned())
        }
        (_, None, None, None) => Ok((start.unwrap_or(0), ::std::usize::MAX)),
        (_, Some(e), None, None) => {
            let s = start.unwrap_or(0);
            if s > e {
                Err(format!(
                    "The end of the range ({}) must be greater than or\n\
                             equal to the start of the range ({}).",
                    e, s
                ))
            } else {
                Ok((s, e))
            }
        }
        (_, None, Some(l), None) => {
            let s = start.unwrap_or(0);
            Ok((s, s + l))
        }
    }
}

pub fn parse_timezone(tz: Option<String>) -> Result<Tz, String> {
    match tz {
        None => Ok(chrono_tz::UTC),
        Some(time_string) => time_string
            .parse::<Tz>()
            .or(Err(format!("{} is not a valid timezone", time_string))),
    }
}

pub fn parse_date(date: &str, tz: Tz, input_fmt: &Option<String>) -> Result<DateTime<Utc>, String> {
    match input_fmt {
        Some(fmt) => match tz.datetime_from_str(date, fmt) {
            Ok(time) => Ok(time.with_timezone(&Utc)),
            _ => Err(format!("{} is not a valid format", fmt)),
        },
        None => match parse_with_timezone(date, &tz) {
            Ok(time) => Ok(time),
            _ => Err(format!("Time format could not be inferred for {}", date)),
        },
    }
}

/// Create a directory recursively, avoiding the race conditons fixed by
/// https://github.com/rust-lang/rust/pull/39799.
fn create_dir_all_threadsafe(path: &Path) -> io::Result<()> {
    // Try 20 times. This shouldn't theoretically need to be any larger
    // than the number of nested directories we need to create.
    for _ in 0..20 {
        match fs::create_dir_all(path) {
            // This happens if a directory in `path` doesn't exist when we
            // test for it, and another thread creates it before we can.
            Err(ref err) if err.kind() == io::ErrorKind::AlreadyExists => {}
            other => return other,
        }
        // We probably don't need to sleep at all, because the intermediate
        // directory is already created.  But let's attempt to back off a
        // bit and let the other thread finish.
        thread::sleep(time::Duration::from_millis(25));
    }
    // Try one last time, returning whatever happens.
    fs::create_dir_all(path)
}

/// Represents a filename template of the form `"{}.csv"`, where `"{}"` is
/// the splace to insert the part of the filename generated by `xsv`.
#[derive(Clone, Debug)]
pub struct FilenameTemplate {
    prefix: String,
    suffix: String,
}

impl FilenameTemplate {
    /// Generate a new filename using `unique_value` to replace the `"{}"`
    /// in the template.
    pub fn filename(&self, unique_value: &str) -> String {
        format!("{}{}{}", &self.prefix, unique_value, &self.suffix)
    }

    /// Create a new, writable file in directory `path` with a filename
    /// using `unique_value` to replace the `"{}"` in the template.  Note
    /// that we do not output headers; the caller must do that if
    /// desired.
    pub fn writer<P>(
        &self,
        path: P,
        unique_value: &str,
    ) -> io::Result<csv::Writer<Box<dyn io::Write + 'static>>>
    where
        P: AsRef<Path>,
    {
        let filename = self.filename(unique_value);
        let full_path = path.as_ref().join(filename);
        if let Some(parent) = full_path.parent() {
            // We may be called concurrently, especially by parallel `xsv
            // split`, so be careful to avoid the `create_dir_all` race
            // condition.
            create_dir_all_threadsafe(parent)?;
        }
        let spath = Some(full_path.display().to_string());
        Config::new(&spath).writer()
    }
}

impl<'de> Deserialize<'de> for FilenameTemplate {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<FilenameTemplate, D::Error> {
        let raw = String::deserialize(d)?;
        let chunks = raw.split("{}").collect::<Vec<_>>();
        if chunks.len() == 2 {
            Ok(FilenameTemplate {
                prefix: chunks[0].to_owned(),
                suffix: chunks[1].to_owned(),
            })
        } else {
            Err(D::Error::custom(
                "The --filename argument must contain one '{}'.",
            ))
        }
    }
}

pub fn acquire_rng(seed: Option<usize>) -> StdRng {
    match seed {
        None => StdRng::from_rng(rand::thread_rng()).unwrap(),
        Some(seed) => {
            let mut buf = [0u8; 32];
            LittleEndian::write_u64(&mut buf, seed as u64);
            SeedableRng::from_seed(buf)
        }
    }
}

pub fn acquire_number_formatter() -> Formatter {
    Formatter::new()
        .precision(Precision::Significance(5))
        .separator(',')
        .unwrap()
}

pub fn acquire_stty_size() -> Option<termsize::Size> {
    if let Ok(output) = Command::new("/bin/sh")
        .arg("-c")
        .arg("stty size < /dev/tty")
        .output()
    {
        let text = String::from_utf8_lossy(&output.stdout);
        let parts = text.trim().split_whitespace().take(2).collect::<Vec<_>>();

        if parts.len() < 2 {
            return None;
        }

        let cols: u16 = if let Ok(c) = parts[1].parse() {
            c
        } else {
            return None;
        };

        let rows: u16 = if let Ok(r) = parts[0].parse() {
            r
        } else {
            return None;
        };

        return Some(termsize::Size { cols, rows });
    }

    None
}

pub fn acquire_term_cols(cols_override: &Option<usize>) -> usize {
    match cols_override {
        None => match termsize::get() {
            None => match acquire_stty_size() {
                None => 80,
                Some(size) => size.cols as usize,
            },
            Some(size) => size.cols as usize,
        },
        Some(c) => *c,
    }
}

pub fn acquire_term_rows() -> Option<usize> {
    match termsize::get() {
        None => match acquire_stty_size() {
            None => None,
            Some(size) => Some(size.rows as usize),
        },
        Some(size) => Some(size.rows as usize),
    }
}

pub fn pretty_print_float<T: Numeric>(f: &mut Formatter, x: T) -> String {
    let mut string = f.fmt2(x).to_string();

    if string.ends_with(".0") {
        string.truncate(string.len() - 2);
    }

    string
}

pub enum ColorOrStyles {
    Color(Color),
    Styles(Styles),
}

pub fn colorizer_by_type(string: &str) -> ColorOrStyles {
    match string.parse::<f64>() {
        Ok(_) => ColorOrStyles::Color(Color::Red),
        Err(_) => {
            if string.starts_with("http://") || string.starts_with("https://") {
                ColorOrStyles::Color(Color::Blue)
            } else {
                match string {
                    "true" | "TRUE" | "True" | "false" | "FALSE" | "False" | "yes" | "no" => {
                        ColorOrStyles::Color(Color::Cyan)
                    }
                    "null" | "na" | "NA" | "None" | "n/a" | "N/A" | "<empty>" => {
                        ColorOrStyles::Styles(Styles::Dimmed)
                    }
                    _ => ColorOrStyles::Color(Color::Green),
                }
            }
        }
    }
}

pub fn colorizer_by_rainbow(index: usize, string: &str) -> ColorOrStyles {
    if string == "<empty>" {
        return ColorOrStyles::Styles(Styles::Dimmed);
    }

    let index = index % 6;

    match index {
        0 => ColorOrStyles::Color(Color::Red),
        1 => ColorOrStyles::Color(Color::Green),
        2 => ColorOrStyles::Color(Color::Yellow),
        3 => ColorOrStyles::Color(Color::Blue),
        4 => ColorOrStyles::Color(Color::Magenta),
        5 => ColorOrStyles::Color(Color::Cyan),
        _ => unreachable!(),
    }
}

pub fn colorize(color_or_style: &ColorOrStyles, string: &str) -> ColoredString {
    match color_or_style {
        ColorOrStyles::Color(color) => string.color(*color),
        ColorOrStyles::Styles(styles) => match styles {
            Styles::Dimmed => string.dimmed(),
            _ => unimplemented!(),
        },
    }
}

pub fn unicode_aware_ellipsis(string: &str, max_width: usize) -> String {
    // Replacing some nasty stuff that can break representation
    let mut string = string.replace('\n', " ");
    string = string.replace('\r', " ");
    string = string.replace('\t', " ");
    // string = string.replace('\u{200F}', "");
    // string = string.replace('\u{200E}', "");

    let mut width: usize = 0;
    let graphemes = string.graphemes(true).collect::<Vec<_>>();
    let graphemes_count = graphemes.len();

    let mut take: usize = 0;

    for grapheme in graphemes.iter() {
        width += grapheme.width();

        if width <= max_width {
            take += 1;
            continue;
        }

        break;
    }

    let mut parts = graphemes.into_iter().take(take).collect::<Vec<&str>>();

    if graphemes_count > parts.len() {
        parts.pop();
        parts.push("…");
    }

    parts.into_iter().collect::<String>()
}

pub fn unicode_aware_pad<'a>(
    left: bool,
    string: &'a str,
    width: usize,
    padding: &str,
) -> Cow<'a, str> {
    let string_width = string.width();

    if string_width >= width {
        return Cow::Borrowed(string);
    }

    let mut padded = String::new();
    let padding = padding.repeat(width - string_width);

    if left {
        padded.push_str(&padding);
        padded.push_str(string);
    } else {
        padded.push_str(string);
        padded.push_str(&padding);
    }

    Cow::Owned(padded)
}

pub fn unicode_aware_rpad<'a>(string: &'a str, width: usize, padding: &str) -> Cow<'a, str> {
    unicode_aware_pad(false, string, width, padding)
}

fn has_rtl(string: &str) -> bool {
    unicode_bidi::BidiInfo::new(string, None).has_rtl()
}

pub fn unicode_aware_pad_with_ellipsis(
    left: bool,
    string: &str,
    width: usize,
    padding: &str,
) -> String {
    let mut string =
        unicode_aware_pad(left, &unicode_aware_ellipsis(string, width), width, padding)
            .into_owned();

    // NOTE: we force back to LTR at the end of the string, so it does not destroy
    // table formatting & wrapping.
    if has_rtl(&string) {
        string.push('\u{200E}');
    }

    string
}

pub fn unicode_aware_rpad_with_ellipsis(string: &str, width: usize, padding: &str) -> String {
    unicode_aware_pad_with_ellipsis(false, string, width, padding)
}

pub fn unicode_aware_lpad_with_ellipsis(string: &str, width: usize, padding: &str) -> String {
    unicode_aware_pad_with_ellipsis(true, string, width, padding)
}

pub fn unicode_aware_wrap(string: &str, max_width: usize, indent: usize) -> String {
    textwrap::wrap(string, max_width)
        .iter()
        .enumerate()
        .map(|(i, line)| {
            if i == 0 {
                line.to_string()
            } else {
                textwrap::indent(line, &" ".repeat(indent))
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub struct EmojiSanitizer {
    pattern: regex::Regex,
}

impl EmojiSanitizer {
    pub fn new() -> Self {
        let mut pattern = String::new();
        pattern.push_str("(?:");

        let mut all_emojis = emojis::iter().collect::<Vec<_>>();
        all_emojis.sort_by_key(|e| std::cmp::Reverse((e.as_bytes().len(), e.as_bytes())));

        for emoji in all_emojis {
            pattern.push_str(&regex::escape(emoji.as_str()));
            pattern.push('|');
        }

        pattern.pop();
        pattern.push(')');

        let pattern = regex::Regex::new(&pattern).unwrap();

        EmojiSanitizer { pattern }
    }

    pub fn sanitize(&self, string: &str) -> String {
        self.pattern
            .replace_all(string, |caps: &regex::Captures| {
                format!(
                    ":{}:",
                    match emojis::get(&caps[0]) {
                        None => "unknown_emoji",
                        Some(emoji) => match emoji.shortcode() {
                            None => "unknown_emoji",
                            Some(shortcode) => shortcode,
                        },
                    }
                )
            })
            .to_string()
    }
}

pub trait ImmutableRecordHelpers<'a> {
    type Cell;

    #[must_use]
    fn replace_at(&self, column_index: usize, new_value: Self::Cell) -> Self;
}

impl<'a> ImmutableRecordHelpers<'a> for csv::ByteRecord {
    type Cell = &'a [u8];

    fn replace_at(&self, column_index: usize, new_value: Self::Cell) -> Self {
        self.iter()
            .enumerate()
            .map(|(i, v)| if i == column_index { new_value } else { v })
            .collect()
    }
}

impl<'a> ImmutableRecordHelpers<'a> for csv::StringRecord {
    type Cell = &'a str;

    fn replace_at(&self, column_index: usize, new_value: Self::Cell) -> Self {
        self.iter()
            .enumerate()
            .map(|(i, v)| if i == column_index { new_value } else { v })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unicode_aware_ellipsis() {
        assert_eq!(unicode_aware_ellipsis("abcde", 10), "abcde".to_string());
        assert_eq!(unicode_aware_ellipsis("abcde", 5), "abcde".to_string());
        assert_eq!(unicode_aware_ellipsis("abcde", 4), "abc…".to_string());
        assert_eq!(unicode_aware_ellipsis("abcde", 3), "ab…".to_string());
    }

    #[test]
    fn test_emoji_sanitizer() {
        let sanitizer = EmojiSanitizer::new();

        assert_eq!(
            sanitizer.sanitize("👩 hello 👩‍👩‍👧‍👦"),
            ":woman: hello :family_woman_woman_girl_boy:"
        );
    }
}
