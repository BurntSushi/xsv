use crate::cmd;

const TEMPLATE_ROOT: &'static str = "\
{bin} {version}
{author}
{about}
USAGE:
    {usage}

TIP:
    use -h for short docs and --help for long docs

SUBCOMMANDS:
{subcommands}

OPTIONS:
{unified}";

const TEMPLATE_SUBCOMMAND: &'static str = "\
USAGE:
    {usage}

TIP:
    use -h for short docs and --help for long docs

SUBCOMMANDS:
{subcommands}

OPTIONS:
{unified}";

const TEMPLATE_LEAF: &'static str = "\
USAGE:
    {usage}

TIP:
    use -h for short docs and --help for long docs

ARGS:
{positionals}

OPTIONS:
{unified}";

const ABOUT: &'static str = "
regex-cli is a tool for interacting with regular expressions on the command
line. It is useful as a debugging aide, an ad hoc benchmarking tool and as a
way to conveniently pre-compile and embed regular expressions into Rust
code.
";

/// Convenience type alias for the Clap app type that we use.
pub type App = clap::App<'static, 'static>;

/// Convenience type alias for the Clap argument result type that we use.
pub type Args = clap::ArgMatches<'static>;

/// Convenience function for creating a new Clap sub-command.
///
/// This should be used for sub-commands that contain other sub-commands.
pub fn command(name: &'static str) -> App {
    clap::SubCommand::with_name(name)
        .author(clap::crate_authors!())
        .version(clap::crate_version!())
        .template(TEMPLATE_SUBCOMMAND)
        .setting(clap::AppSettings::UnifiedHelpMessage)
}

/// Convenience function for creating a new Clap sub-command.
///
/// This should be used for sub-commands that do NOT contain other
/// sub-commands.
pub fn leaf(name: &'static str) -> App {
    clap::SubCommand::with_name(name)
        .author(clap::crate_authors!())
        .version(clap::crate_version!())
        .template(TEMPLATE_LEAF)
        .setting(clap::AppSettings::UnifiedHelpMessage)
}

/// Convenience function for defining a Clap positional argument with the
/// given name.
pub fn arg(name: &'static str) -> clap::Arg {
    clap::Arg::with_name(name)
}

/// Convenience function for defining a Clap argument with a long flag name
/// that accepts a single value.
pub fn flag(name: &'static str) -> clap::Arg {
    clap::Arg::with_name(name).long(name).takes_value(true)
}

/// Convenience function for defining a Clap argument with a long flag name
/// that accepts no values. i.e., It is a boolean switch.
pub fn switch(name: &'static str) -> clap::Arg {
    clap::Arg::with_name(name).long(name)
}

/// Build the main Clap application.
pub fn root() -> App {
    clap::App::new("xsv")
        .author(clap::crate_authors!())
        .version(clap::crate_version!())
        .about(ABOUT)
        .template(TEMPLATE_ROOT)
        .max_term_width(100)
        .setting(clap::AppSettings::UnifiedHelpMessage)
        .arg(switch("quiet").short("q").global(true).help("Show less output."))
        .subcommand(cmd::count::define())
}
