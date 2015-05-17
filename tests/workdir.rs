use std::env;
use std::fmt;
use std::fs::{self, PathExt};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process;
use std::str::FromStr;
use std::sync::atomic;

use csv;

use Csv;

static XSV_INTEGRATION_TEST_DIR: &'static str = "xit";

static NEXT_ID: atomic::AtomicUsize = atomic::ATOMIC_USIZE_INIT;

pub struct Workdir {
    root: PathBuf,
    dir: PathBuf,
    flexible: bool,
}

impl Workdir {
    pub fn new(name: &str) -> Workdir {
        let id = NEXT_ID.fetch_add(1, atomic::Ordering::SeqCst);
        let root = env::current_exe().unwrap()
                       .parent()
                       .expect("executable's directory")
                       .to_path_buf();
        let dir = root.join(XSV_INTEGRATION_TEST_DIR)
                      .join(name)
                      .join(&format!("test-{}", id));

        // I don't get why this is necessary, but Travis seems to need it?
        // let md = fs::metadata(&dir);
        // if fs::metadata(&dir).map(|md| md.is_dir()).unwrap_or(false) {
            // if let Err(err) = fs::remove_dir_all(&dir) {
                // panic!("Could not remove directory '{:?}': {}", dir, err);
            // }
        // }
        // if fs::metadata(&dir).map(|md| md.is_file()).unwrap_or(false) {
            // if let Err(err) = fs::remove_file(&dir) {
                // panic!("Could not remove file '{:?}': {}", dir, err);
            // }
        // }
        if let Err(err) = fs::create_dir_all(&dir) {
            panic!("Could not create '{:?}': {}", dir, err);
        }
        Workdir { root: root, dir: dir, flexible: false }
    }

    pub fn flexible(mut self, yes: bool) -> Workdir {
        self.flexible = yes;
        self
    }

    pub fn create<T: Csv>(&self, name: &str, rows: T) {
        let mut wtr = match csv::Writer::from_file(&self.path(name)) {
            Ok(wtr) => wtr.flexible(self.flexible),
            Err(err) => panic!("Could not open '{:?}': {}",
                                self.path(name), err),
        };
        for row in rows.to_vecs().into_iter() {
            wtr.write(row.iter()).unwrap();
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
        let mut rdr = csv::Reader::from_string(self.stdout::<String>(cmd))
                                  .has_headers(false);
        Csv::from_vecs(rdr.records().collect::<Result<_, _>>().unwrap())
    }

    pub fn command(&self, sub_command: &str) -> process::Command {
        let mut cmd = process::Command::new(&self.xsv_bin());
        cmd.current_dir(&self.dir).arg(sub_command);
        cmd
    }

    pub fn output(&self, cmd: &mut process::Command) -> process::Output {
        debug!("[{}]: {:?}", self.dir.display(), cmd);
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
        stdout.trim().parse().ok().expect(
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
