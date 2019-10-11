use std::env;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process;
use std::str::FromStr;
use std::sync::atomic;
use std::time::Duration;

use csv;

use Csv;

static XSV_INTEGRATION_TEST_DIR: &'static str = "xit";

static NEXT_ID: atomic::AtomicUsize = atomic::AtomicUsize::new(0);

pub struct Workdir {
    root: PathBuf,
    dir: PathBuf,
    flexible: bool,
}

impl Workdir {
    pub fn new(name: &str) -> Workdir {
        let id = NEXT_ID.fetch_add(1, atomic::Ordering::SeqCst);
        let mut root = env::current_exe().unwrap()
                           .parent()
                           .expect("executable's directory")
                           .to_path_buf();
        if root.ends_with("deps") {
            root.pop();
        }
        let dir = root.join(XSV_INTEGRATION_TEST_DIR)
                      .join(name)
                      .join(&format!("test-{}", id));
        // println!("{:?}", dir);
        if let Err(err) = create_dir_all(&dir) {
            panic!("Could not create '{:?}': {}", dir, err);
        }
        Workdir { root: root, dir: dir, flexible: false }
    }

    pub fn flexible(mut self, yes: bool) -> Workdir {
        self.flexible = yes;
        self
    }

    pub fn create<T: Csv>(&self, name: &str, rows: T) {
        let mut wtr = csv::WriterBuilder::new()
            .flexible(self.flexible)
            .from_path(&self.path(name))
            .unwrap();
        for row in rows.to_vecs().into_iter() {
            wtr.write_record(row).unwrap();
        }
        wtr.flush().unwrap();
    }

    pub fn create_indexed<T: Csv>(&self, name: &str, rows: T) {
        self.create(name, rows);

        let mut cmd = self.command("index");
        cmd.arg(name);
        self.run(&mut cmd);
    }

    pub fn read_stdout<T: Csv>(&self, cmd: &mut process::Command) -> T {
        let stdout: String = self.stdout(cmd);
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(io::Cursor::new(stdout));

        let records: Vec<Vec<String>> = rdr
            .records()
            .collect::<Result<Vec<csv::StringRecord>, _>>()
            .unwrap()
            .into_iter()
            .map(|r| r.iter().map(|f| f.to_string()).collect())
            .collect();
        Csv::from_vecs(records)
    }

    pub fn command(&self, sub_command: &str) -> process::Command {
        let mut cmd = process::Command::new(&self.xsv_bin());
        cmd.current_dir(&self.dir).arg(sub_command);
        cmd
    }

    pub fn output(&self, cmd: &mut process::Command) -> process::Output {
        debug!("[{}]: {:?}", self.dir.display(), cmd);
        println!("[{}]: {:?}", self.dir.display(), cmd);
        let o = cmd.output().unwrap();
        if !o.status.success() {
            panic!("\n\n===== {:?} =====\n\
                    command failed but expected success!\
                    \n\ncwd: {}\
                    \n\nstatus: {}\
                    \n\nstdout: {}\n\nstderr: {}\
                    \n\n=====\n",
                   cmd, self.dir.display(), o.status,
                   String::from_utf8_lossy(&o.stdout),
                   String::from_utf8_lossy(&o.stderr))
        }
        o
    }

    pub fn run(&self, cmd: &mut process::Command) {
        self.output(cmd);
    }

    pub fn stdout<T: FromStr>(&self, cmd: &mut process::Command) -> T {
        let o = self.output(cmd);
        let stdout = String::from_utf8_lossy(&o.stdout);
        stdout.trim_matches(&['\r', '\n'][..]).parse().ok().expect(
            &format!("Could not convert from string: '{}'", stdout))
    }

    pub fn assert_err(&self, cmd: &mut process::Command) {
        let o = cmd.output().unwrap();
        if o.status.success() {
            panic!("\n\n===== {:?} =====\n\
                    command succeeded but expected failure!\
                    \n\ncwd: {}\
                    \n\nstatus: {}\
                    \n\nstdout: {}\n\nstderr: {}\
                    \n\n=====\n",
                   cmd, self.dir.display(), o.status,
                   String::from_utf8_lossy(&o.stdout),
                   String::from_utf8_lossy(&o.stderr));
        }
    }

    pub fn from_str<T: FromStr>(&self, name: &Path) -> T {
        let mut o = String::new();
        fs::File::open(name).unwrap().read_to_string(&mut o).unwrap();
        o.parse().ok().expect("fromstr")
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    pub fn xsv_bin(&self) -> PathBuf {
        self.root.join("xsv")
    }
}

impl fmt::Debug for Workdir {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "path={}", self.dir.display())
    }
}

// For whatever reason, `fs::create_dir_all` fails intermittently on Travis
// with a weird "file exists" error. Despite my best efforts to get to the
// bottom of it, I've decided a try-wait-and-retry hack is good enough.
fn create_dir_all<P: AsRef<Path>>(p: P) -> io::Result<()> {
    let mut last_err = None;
    for _ in 0..10 {
        if let Err(err) = fs::create_dir_all(&p) {
            last_err = Some(err);
            ::std::thread::sleep(Duration::from_millis(500));
        } else {
            return Ok(())
        }
    }
    Err(last_err.unwrap())
}
