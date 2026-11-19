//! An `NFSv3` client over TCP: MOUNT MNT on the server's port, then NFS
//! calls on the same port, each bounded so a silent server fails the test.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use nfs3_client::nfs3_types::nfs3::{
    ACCESS3args, COMMIT3args, CREATE3args, FSINFO3args, FSINFO3resok, FSSTAT3args, GETATTR3args,
    LOOKUP3args, MKDIR3args, Nfs3Option, Nfs3Result, PATHCONF3args, READ3args, READDIR3args,
    READDIRPLUS3args, REMOVE3args, RENAME3args, RMDIR3args, SETATTR3args, WRITE3args, WRITE3resok,
    cookieverf3, createhow3, diropargs3, fattr3, filename3, ftype3, nfs_fh3, nfsstat3, sattr3,
    stable_how, writeverf3,
};
use nfs3_client::nfs3_types::xdr_codec::{Opaque, Pack, Unpack};
use nfs3_client::tokio::{TokioConnector, TokioIo};
use nfs3_client::{Nfs3Connection, Nfs3ConnectionBuilder};
use tokio::net::TcpStream;

/// Bound on one call the server is expected to answer.
pub const ANSWER: Duration = Duration::from_secs(30);
/// READDIR and READDIRPLUS byte counts large enough for one page of a small
/// folder.
const WHOLE_PAGE: u32 = 1024 * 1024;

/// The outcome of one procedure: its result, or the status it failed with.
pub type Status<T> = std::result::Result<T, nfsstat3>;

/// The status code of `outcome`, `NFS3_OK` for a success.
pub fn code<T>(outcome: &Status<T>) -> nfsstat3 {
    outcome.as_ref().err().copied().unwrap_or(nfsstat3::NFS3_OK)
}

fn status<T, E>(result: Nfs3Result<T, E>) -> Status<T> {
    match result {
        Nfs3Result::Ok(value) => Ok(value),
        Nfs3Result::Err((code, _)) => Err(code),
    }
}

/// Awaits `call` for at most [`ANSWER`].
pub async fn bounded<T, E>(what: &str, call: impl Future<Output = Result<T, E>>) -> Result<T>
where
    E: std::error::Error + Send + Sync + 'static,
{
    tokio::time::timeout(ANSWER, call)
        .await
        .with_context(|| format!("{what}: no reply within {ANSWER:?}"))?
        .with_context(|| format!("{what}: RPC failed"))
}

/// One entry of a READDIR or READDIRPLUS page.
#[derive(Debug, Clone)]
pub struct Entry {
    pub name: String,
    pub fileid: u64,
    pub cookie: u64,
    pub handle: Option<nfs_fh3>,
    pub kind: Option<ftype3>,
}

/// One page of a directory listing.
#[derive(Debug)]
pub struct Page {
    pub entries: Vec<Entry>,
    pub verf: cookieverf3,
    pub eof: bool,
}

/// A WRITE the server accepted.
#[derive(Debug, Clone, Copy)]
pub struct Written {
    pub count: u32,
    pub committed: stable_how,
    pub verf: writeverf3,
}

impl From<WRITE3resok> for Written {
    fn from(ok: WRITE3resok) -> Self {
        Self {
            count: ok.count,
            committed: ok.committed,
            verf: ok.verf,
        }
    }
}

pub fn name(text: &str) -> filename3<'static> {
    filename3::from(text.as_bytes().to_vec())
}

fn dirop(dir: &nfs_fh3, entry: &str) -> diropargs3<'static> {
    diropargs3 {
        dir: dir.clone(),
        name: name(entry),
    }
}

fn option<T: Pack + Unpack>(value: Nfs3Option<T>) -> Option<T> {
    match value {
        Nfs3Option::Some(value) => Some(value),
        Nfs3Option::None => None,
    }
}

fn utf8(name: &filename3<'_>) -> String {
    String::from_utf8_lossy(name.as_ref()).into_owned()
}

/// The procedure a listing pages with.
#[derive(Debug, Clone, Copy)]
enum Paging {
    Readdir,
    Readdirplus,
}

/// A mounted export: the MOUNT reply's root handle and an NFS connection.
pub struct Client {
    conn: Nfs3Connection<TokioIo<TcpStream>>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("nfs3_port", &self.conn.nfs3_port)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Mounts `/` from the server at `addr`, MOUNT and NFS on its one port,
    /// from an unprivileged source port.
    pub async fn mount(addr: SocketAddr) -> Result<Self> {
        let conn = bounded(
            "MOUNT MNT /",
            Nfs3ConnectionBuilder::new(TokioConnector, addr.ip().to_string(), "/")
                .connect_from_privileged_port(false)
                .mount_port(addr.port())
                .nfs3_port(addr.port())
                .mount(),
        )
        .await?;
        Ok(Self { conn })
    }

    /// The export root handle the MOUNT reply carried.
    pub fn root(&self) -> nfs_fh3 {
        self.conn.root_nfs_fh3()
    }

    pub async fn lookup(&mut self, dir: &nfs_fh3, entry: &str) -> Result<Status<nfs_fh3>> {
        let args = LOOKUP3args {
            what: dirop(dir, entry),
        };
        let reply = bounded("LOOKUP", self.conn.lookup(&args)).await?;
        Ok(status(reply).map(|ok| ok.object))
    }

    /// Walks `path` from the export root by LOOKUP, as a client that never
    /// listed a folder on the way does.
    pub async fn walk(&mut self, path: &str) -> Result<nfs_fh3> {
        let mut dir = self.root();
        for entry in path.split('/') {
            dir = match self.lookup(&dir, entry).await? {
                Ok(fh) => fh,
                Err(code) => bail!("LOOKUP {entry} of {path}: {code:?}"),
            };
        }
        Ok(dir)
    }

    pub async fn getattr(&mut self, fh: &nfs_fh3) -> Result<Status<fattr3>> {
        let args = GETATTR3args { object: fh.clone() };
        let reply = bounded("GETATTR", self.conn.getattr(&args)).await?;
        Ok(status(reply).map(|ok| ok.obj_attributes))
    }

    pub async fn read_status(
        &mut self,
        fh: &nfs_fh3,
        offset: u64,
        count: u32,
    ) -> Result<Status<(Vec<u8>, bool)>> {
        let args = READ3args {
            file: fh.clone(),
            offset,
            count,
        };
        let reply = bounded("READ", self.conn.read(&args)).await?;
        Ok(status(reply).map(|ok| (ok.data.into_owned(), ok.eof)))
    }

    /// READ of `count` bytes at `offset`, failing on any status but OK.
    pub async fn read(&mut self, fh: &nfs_fh3, offset: u64, count: u32) -> Result<Vec<u8>> {
        match self.read_status(fh, offset, count).await? {
            Ok((data, _)) => Ok(data),
            Err(code) => bail!("READ {count} at {offset}: {code:?}"),
        }
    }

    pub async fn write_stable(
        &mut self,
        fh: &nfs_fh3,
        offset: u64,
        data: &[u8],
        stable: stable_how,
    ) -> Result<Status<Written>> {
        let args = WRITE3args {
            file: fh.clone(),
            offset,
            count: u32::try_from(data.len())?,
            stable,
            data: Opaque::borrowed(data),
        };
        let reply = bounded("WRITE", self.conn.write(&args)).await?;
        Ok(status(reply).map(Written::from))
    }

    /// UNSTABLE WRITE, as the client sends before a COMMIT.
    pub async fn write(&mut self, fh: &nfs_fh3, offset: u64, data: &[u8]) -> Result<nfsstat3> {
        let outcome = self
            .write_stable(fh, offset, data, stable_how::UNSTABLE)
            .await?;
        Ok(code(&outcome))
    }

    pub async fn set_size(&mut self, fh: &nfs_fh3, size: u64) -> Result<nfsstat3> {
        let args = SETATTR3args {
            object: fh.clone(),
            new_attributes: sattr3 {
                size: Nfs3Option::Some(size),
                ..sattr3::default()
            },
            guard: Nfs3Option::None,
        };
        let reply = bounded("SETATTR", self.conn.setattr(&args)).await?;
        Ok(code(&status(reply)))
    }

    pub async fn create_how(
        &mut self,
        dir: &nfs_fh3,
        entry: &str,
        how: createhow3,
    ) -> Result<Status<Option<nfs_fh3>>> {
        let args = CREATE3args {
            where_: dirop(dir, entry),
            how,
        };
        let reply = bounded("CREATE", self.conn.create(&args)).await?;
        Ok(status(reply).map(|ok| option(ok.obj)))
    }

    /// CREATE UNCHECKED with no attributes set.
    pub async fn create(&mut self, dir: &nfs_fh3, entry: &str) -> Result<nfsstat3> {
        let outcome = self
            .create_how(dir, entry, createhow3::UNCHECKED(sattr3::default()))
            .await?;
        Ok(code(&outcome))
    }

    pub async fn mkdir(&mut self, dir: &nfs_fh3, entry: &str) -> Result<nfsstat3> {
        let args = MKDIR3args {
            where_: dirop(dir, entry),
            attributes: sattr3::default(),
        };
        let reply = bounded("MKDIR", self.conn.mkdir(&args)).await?;
        Ok(code(&status(reply)))
    }

    pub async fn remove(&mut self, dir: &nfs_fh3, entry: &str) -> Result<nfsstat3> {
        let args = REMOVE3args {
            object: dirop(dir, entry),
        };
        let reply = bounded("REMOVE", self.conn.remove(&args)).await?;
        Ok(code(&status(reply)))
    }

    pub async fn rmdir(&mut self, dir: &nfs_fh3, entry: &str) -> Result<nfsstat3> {
        let args = RMDIR3args {
            object: dirop(dir, entry),
        };
        let reply = bounded("RMDIR", self.conn.rmdir(&args)).await?;
        Ok(code(&status(reply)))
    }

    pub async fn rename(
        &mut self,
        from: &nfs_fh3,
        from_name: &str,
        to: &nfs_fh3,
        to_name: &str,
    ) -> Result<nfsstat3> {
        let args = RENAME3args {
            from: dirop(from, from_name),
            to: dirop(to, to_name),
        };
        let reply = bounded("RENAME", self.conn.rename(&args)).await?;
        Ok(code(&status(reply)))
    }

    pub async fn commit(&mut self, fh: &nfs_fh3) -> Result<Status<writeverf3>> {
        let args = COMMIT3args {
            file: fh.clone(),
            offset: 0,
            count: 0,
        };
        let reply = bounded("COMMIT", self.conn.commit(&args)).await?;
        Ok(status(reply).map(|ok| ok.verf))
    }

    /// ACCESS of every right, with the object attributes of a success.
    pub async fn access(&mut self, fh: &nfs_fh3) -> Result<Status<Option<fattr3>>> {
        let args = ACCESS3args {
            object: fh.clone(),
            access: 0x3f,
        };
        let reply = bounded("ACCESS", self.conn.access(&args)).await?;
        Ok(status(reply).map(|ok| option(ok.obj_attributes)))
    }

    /// FSSTAT, with the object attributes of a success.
    pub async fn fsstat(&mut self, fh: &nfs_fh3) -> Result<Status<Option<fattr3>>> {
        let args = FSSTAT3args { fsroot: fh.clone() };
        let reply = bounded("FSSTAT", self.conn.fsstat(&args)).await?;
        Ok(status(reply).map(|ok| option(ok.obj_attributes)))
    }

    pub async fn fsinfo(&mut self, fh: &nfs_fh3) -> Result<Status<FSINFO3resok>> {
        let args = FSINFO3args { fsroot: fh.clone() };
        let reply = bounded("FSINFO", self.conn.fsinfo(&args)).await?;
        Ok(status(reply))
    }

    /// PATHCONF, with the object attributes of a success.
    pub async fn pathconf(&mut self, fh: &nfs_fh3) -> Result<Status<Option<fattr3>>> {
        let args = PATHCONF3args { object: fh.clone() };
        let reply = bounded("PATHCONF", self.conn.pathconf(&args)).await?;
        Ok(status(reply).map(|ok| option(ok.obj_attributes)))
    }

    pub async fn readdir_page(
        &mut self,
        dir: &nfs_fh3,
        cookie: u64,
        verf: cookieverf3,
        count: u32,
    ) -> Result<Status<Page>> {
        let args = READDIR3args {
            dir: dir.clone(),
            cookie,
            cookieverf: verf,
            count,
        };
        let reply = bounded("READDIR", self.conn.readdir(&args)).await?;
        Ok(status(reply).map(|ok| Page {
            entries: ok
                .reply
                .entries
                .into_inner()
                .into_iter()
                .map(|entry| Entry {
                    name: utf8(&entry.name),
                    fileid: entry.fileid,
                    cookie: entry.cookie,
                    handle: None,
                    kind: None,
                })
                .collect(),
            verf: ok.cookieverf,
            eof: ok.reply.eof,
        }))
    }

    pub async fn readdirplus_page(
        &mut self,
        dir: &nfs_fh3,
        cookie: u64,
        verf: cookieverf3,
        dircount: u32,
        maxcount: u32,
    ) -> Result<Status<Page>> {
        let args = READDIRPLUS3args {
            dir: dir.clone(),
            cookie,
            cookieverf: verf,
            dircount,
            maxcount,
        };
        let reply = bounded("READDIRPLUS", self.conn.readdirplus(&args)).await?;
        Ok(status(reply).map(|ok| Page {
            entries: ok
                .reply
                .entries
                .into_inner()
                .into_iter()
                .map(|entry| Entry {
                    name: utf8(&entry.name),
                    fileid: entry.fileid,
                    cookie: entry.cookie,
                    handle: option(entry.name_handle),
                    kind: option(entry.name_attributes).map(|attrs| attrs.type_),
                })
                .collect(),
            verf: ok.cookieverf,
            eof: ok.reply.eof,
        }))
    }

    /// Every entry of `dir` but `.` and `..`, paged by `procedure`.
    async fn entries(&mut self, dir: &nfs_fh3, procedure: Paging) -> Result<Status<Vec<Entry>>> {
        let mut entries = Vec::new();
        let mut cookie = 0;
        let mut verf = cookieverf3::default();
        loop {
            let page = match procedure {
                Paging::Readdir => self.readdir_page(dir, cookie, verf, WHOLE_PAGE).await?,
                Paging::Readdirplus => {
                    self.readdirplus_page(dir, cookie, verf, WHOLE_PAGE, WHOLE_PAGE)
                        .await?
                }
            };
            let page = match page {
                Ok(page) => page,
                Err(code) => return Ok(Err(code)),
            };
            if let Some(last) = page.entries.last() {
                cookie = last.cookie;
            }
            verf = page.verf;
            entries.extend(
                page.entries
                    .into_iter()
                    .filter(|entry| entry.name != "." && entry.name != ".."),
            );
            if page.eof {
                return Ok(Ok(entries));
            }
        }
    }

    /// Every entry of `dir` but `.` and `..`, paged by READDIRPLUS.
    pub async fn listing(&mut self, dir: &nfs_fh3) -> Result<Status<Vec<Entry>>> {
        self.entries(dir, Paging::Readdirplus).await
    }

    /// The sorted names READDIR lists in `dir`, `.` and `..` left out.
    pub async fn names(&mut self, dir: &nfs_fh3) -> Result<Status<Vec<String>>> {
        Ok(self.entries(dir, Paging::Readdir).await?.map(|entries| {
            let mut names: Vec<String> = entries.into_iter().map(|entry| entry.name).collect();
            names.sort();
            names
        }))
    }
}
