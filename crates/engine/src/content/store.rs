//! Sparse object files, one per cached item, addressed by object id.

use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use super::ObjectId;

/// How [`ObjectStore::clone_to`] produced the target file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloneOutcome {
    /// The target shares its blocks with the object.
    Cloned,
    /// The clone failed and the bytes were copied, occupying space twice.
    Copied { bytes: u64 },
}

/// Directory of sparse object files.
#[derive(Debug, Clone)]
pub struct ObjectStore {
    root: PathBuf,
}

impl ObjectStore {
    /// Opens the store at `root`, creating the directory if needed.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of creating `root`.
    pub fn open(root: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    #[must_use]
    pub fn path(&self, id: ObjectId) -> PathBuf {
        self.root.join(id.to_string())
    }

    /// Creates the object file of `id` with logical length `size` and no
    /// allocated blocks, replacing any previous file.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of creating or truncating the file.
    pub fn create(&self, id: ObjectId, size: u64) -> io::Result<PathBuf> {
        let path = self.path(id);
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(size)?;
        Ok(path)
    }

    /// Writes all of `bytes` at `offset` in the object of `id`.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of opening or writing the file.
    pub fn write_at(&self, id: ObjectId, offset: u64, bytes: &[u8]) -> io::Result<()> {
        OpenOptions::new()
            .write(true)
            .open(self.path(id))?
            .write_all_at(bytes, offset)
    }

    /// Reads into `buf` from `offset` until it is full or the file ends, and
    /// returns the number of bytes read.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of opening or reading the file.
    pub fn read_at(&self, id: ObjectId, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let file = File::open(self.path(id))?;
        let mut filled = 0;
        while let Some(rest) = buf.get_mut(filled..).filter(|rest| !rest.is_empty()) {
            let position = offset.saturating_add(filled as u64);
            match file.read_at(rest, position) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(filled)
    }

    /// Clones the object of `id` to `target`, copying when the filesystem
    /// cannot clone.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of the clone when the source is missing, access
    /// is denied or `target` exists, and otherwise the error of the copy.
    pub fn clone_to(&self, id: ObjectId, target: &Path) -> io::Result<CloneOutcome> {
        let source = self.path(id);
        match reflink_copy::reflink(&source, target) {
            Ok(()) => Ok(CloneOutcome::Cloned),
            Err(e)
                if matches!(
                    e.kind(),
                    ErrorKind::NotFound | ErrorKind::PermissionDenied | ErrorKind::AlreadyExists
                ) =>
            {
                Err(e)
            }
            Err(cause) => {
                tracing::warn!(
                    object = %id,
                    target = %target.display(),
                    error = %cause,
                    "clone failed; copying object instead"
                );
                let bytes = fs::copy(&source, target)?;
                Ok(CloneOutcome::Copied { bytes })
            }
        }
    }

    /// Removes the object file of `id`; a missing file is not an error.
    ///
    /// # Errors
    ///
    /// Returns any other I/O error of removing the file.
    pub fn delete(&self, id: ObjectId) -> io::Result<()> {
        match fs::remove_file(self.path(id)) {
            Err(e) if e.kind() != ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }
}
