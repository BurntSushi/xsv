use std::fmt;
use std::io;
use std::io::fs::{mod, PathExtensions};
use std::io::process;
use std::os;
use std::sync::atomic;

use csv;

static XSV_INTEGRATION_TEST_DIR: &'static str = ".xit";

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

    pub fn create<S: Str>(&self, name: &str, rows: Vec<Vec<S>>) {
        let mut wtr = csv::Writer::from_file(&self.path(name));
        for row in rows.into_iter() {
            wtr.write(row.into_iter()).unwrap();
        }
        wtr.flush().unwrap();
    }

    pub fn read(&self, name: &str) -> Vec<Vec<String>> {
        let mut rdr = csv::Reader::from_file(&self.path(name))
                                  .has_headers(false);
        rdr.records().collect::<Result<_, _>>().unwrap()
    }

    pub fn command(&self, sub_command: &str) -> process::Command {
        let mut cmd = process::Command::new(self.xsv_bin());
        cmd.arg(sub_command);
        cmd
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
