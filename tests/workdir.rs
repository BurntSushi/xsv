use std::fmt;
use std::io;
use std::io::fs::{mod, PathExtensions};
use std::io::process;
use std::os;
use std::str::{FromStr, from_str};
use std::sync::atomic;

use csv;

use Csv;

static XSV_INTEGRATION_TEST_DIR: &'static str = "xit";

static NEXT_ID: atomic::AtomicUint = atomic::INIT_ATOMIC_UINT;

pub struct Workdir {
    root: Path,
    dir: Path,
    flexible: bool,
}

impl Workdir {
    pub fn new(name: &str) -> Workdir {
        let id = NEXT_ID.fetch_add(1, atomic::SeqCst);
        let root = os::self_exe_path().unwrap();
        let dir = root.clone()
                      .join(XSV_INTEGRATION_TEST_DIR)
                      .join(name)
                      .join(format!("test-{}", id));
        if dir.exists() {
            fs::rmdir_recursive(&dir).unwrap();
        }
        fs::mkdir_recursive(&dir, io::USER_DIR).unwrap();
        Workdir { root: root, dir: dir, flexible: false }
    }

    pub fn flexible(mut self, yes: bool) -> Workdir {
        self.flexible = yes;
        self
    }

    pub fn create<T: Csv>(&self, name: &str, rows: T) {
        let mut wtr = csv::Writer::from_file(&self.path(name))
                                  .flexible(self.flexible);
        for row in rows.to_vecs().into_iter() {
            wtr.write(row.iter()).unwrap();
        }
        wtr.flush().unwrap();
    }

    pub fn create_indexed<T: Csv>(&self, name: &str, rows: T) {
        self.create(name, rows);

        let mut cmd = self.command("index");
        cmd.arg(name);
        self.run(&cmd);
    }

    pub fn read<T: Csv>(&self, name: &str) -> T {
        let mut rdr = csv::Reader::from_file(&self.path(name))
                                  .has_headers(false);
        Csv::from_vecs(rdr.records().collect::<Result<_, _>>().unwrap())
    }

    pub fn read_stdout<T: Csv>(&self, cmd: &process::Command) -> T {
        let mut rdr = csv::Reader::from_string(self.stdout::<String>(cmd))
                                  .has_headers(false);
        Csv::from_vecs(rdr.records().collect::<Result<_, _>>().unwrap())
    }

    pub fn from_str<T: FromStr>(&self, name: &Path) -> T {
        let o = io::File::open(name).unwrap()
                         .read_to_string().unwrap();
        from_str(o.as_slice()).expect("fromstr")
    }

    pub fn command(&self, sub_command: &str) -> process::Command {
        let mut cmd = process::Command::new(self.xsv_bin());
        cmd.cwd(&self.dir).arg(sub_command);
        cmd
    }

    pub fn output(&self, cmd: &process::Command) -> process::ProcessOutput {
        debug!("[{}]: {}", self.dir.display(), cmd);
        let o = cmd.output().unwrap();
        if !o.status.success() {
            panic!("\n\n===== {} =====\n\
                    command failed but expected success!\
                    \n\ncwd: {}\
                    \n\nstatus: {}\
                    \n\nstdout: {}\n\nstderr: {}\
                    \n\n=====\n",
                   cmd, self.dir.display(), o.status,
                   String::from_utf8_lossy(o.output.as_slice()),
                   String::from_utf8_lossy(o.error.as_slice()))
        }
        o
    }

    pub fn run(&self, cmd: &process::Command) {
        self.output(cmd);
    }

    pub fn stdout<T: FromStr>(&self, cmd: &process::Command) -> T {
        let o = self.output(cmd);
        let stdout = String::from_utf8_lossy(o.output.as_slice());
        from_str(stdout.as_slice().trim()).expect(
            format!("Could not convert from string: '{}'",
                    stdout.as_slice()).as_slice())
    }

    pub fn assert_err(&self, cmd: &process::Command) {
        let o = cmd.output().unwrap();
        if o.status.success() {
            panic!("\n\n===== {} =====\n\
                    command succeeded but expected failure!\
                    \n\ncwd: {}\
                    \n\nstatus: {}\
                    \n\nstdout: {}\n\nstderr: {}\
                    \n\n=====\n",
                   cmd, self.dir.display(), o.status,
                   String::from_utf8_lossy(o.output.as_slice()),
                   String::from_utf8_lossy(o.error.as_slice()))
        }
    }

    pub fn path(&self, name: &str) -> Path {
        self.dir.join(name)
    }

    pub fn xsv_bin(&self) -> Path {
        self.root.join("xsv").clone()
    }
}

impl fmt::Show for Workdir {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "path={}", self.dir.display())
    }
}
