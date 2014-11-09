use std::fmt;
use std::io;
use std::io::fs::{mod, PathExtensions};
use std::io::process;
use std::os;
use std::sync::atomic;

use csv;

use Csv;

static XSV_INTEGRATION_TEST_DIR: &'static str = "xit";

static NEXT_ID: atomic::AtomicUint = atomic::INIT_ATOMIC_UINT;

pub struct Workdir {
    root: Path,
    dir: Path,
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
        Workdir { root: root, dir: dir }
    }

    pub fn create<T: Csv>(&self, name: &str, rows: T) {
        let mut wtr = csv::Writer::from_file(&self.path(name));
        for row in rows.to_vecs().into_iter() {
            wtr.write(row.into_iter()).unwrap();
        }
        wtr.flush().unwrap();
    }

    pub fn read<T: Csv>(&self, name: &str) -> T {
        let mut rdr = csv::Reader::from_file(&self.path(name))
                                  .has_headers(false);
        Csv::from_vecs(rdr.records().collect::<Result<_, _>>().unwrap())
    }

    pub fn command(&self, sub_command: &str) -> process::Command {
        let mut cmd = process::Command::new(self.xsv_bin());
        cmd.cwd(&self.dir).arg(sub_command);
        cmd
    }

    pub fn run(&self, cmd: &process::Command) {
        let o = cmd.output().unwrap();
        if !o.status.success() {
            panic!("'{}' ({}) failed in '{}'. \n\nstdout: {}\n\nstderr: {}",
                   cmd, o.status, self.dir.display(),
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
