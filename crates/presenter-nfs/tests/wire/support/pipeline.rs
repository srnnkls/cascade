//! Several NFS calls outstanding on one TCP connection, as the kernel client
//! sends them: each call is one record, replies are matched by xid.

use std::time::Duration;

use anyhow::{Context as _, Result, bail, ensure};
use nfs3_client::nfs3_types::nfs3::{NFS_PROGRAM, PROGRAM, READ3args, VERSION, nfs_fh3, nfsstat3};
use nfs3_client::nfs3_types::rpc::{
    RPC_VERSION_2, accept_stat_data, call_body, fragment_header, msg_body, opaque_auth, reply_body,
    rpc_msg,
};
use nfs3_client::nfs3_types::xdr_codec::{Pack, Unpack};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A reply as received: its xid and the procedure's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reply {
    pub xid: u32,
    pub status: nfsstat3,
}

/// Sends READ of `count` bytes at `offset` of `file` under `xid` without
/// waiting for its reply.
pub async fn send_read(
    stream: &mut TcpStream,
    xid: u32,
    file: &nfs_fh3,
    offset: u64,
    count: u32,
) -> Result<()> {
    let msg = rpc_msg {
        xid,
        body: msg_body::CALL(call_body {
            rpcvers: RPC_VERSION_2,
            prog: PROGRAM,
            vers: VERSION,
            proc: NFS_PROGRAM::NFSPROC3_READ as u32,
            cred: opaque_auth::default(),
            verf: opaque_auth::default(),
        }),
    };
    let args = READ3args {
        file: file.clone(),
        offset,
        count,
    };
    let mut body = Vec::new();
    msg.pack(&mut body)?;
    args.pack(&mut body)?;
    let mut record = Vec::new();
    fragment_header::new(u32::try_from(body.len())?, true).pack(&mut record)?;
    record.extend_from_slice(&body);
    stream.write_all(&record).await?;
    Ok(())
}

/// Receives the next reply record within `limit`.
pub async fn next_reply(stream: &mut TcpStream, limit: Duration) -> Result<Reply> {
    tokio::time::timeout(limit, async {
        let mut body = Vec::new();
        loop {
            let mut mark = [0u8; 4];
            stream.read_exact(&mut mark).await?;
            let header = fragment_header::from(mark);
            let start = body.len();
            body.resize(start + usize::try_from(header.fragment_length())?, 0);
            stream
                .read_exact(body.get_mut(start..).context("fragment")?)
                .await?;
            if header.eof() {
                break;
            }
        }
        let mut rest = body.as_slice();
        let (msg, _) = rpc_msg::unpack(&mut rest)?;
        let msg_body::REPLY(reply) = msg.body else {
            bail!("xid {} is not a reply", msg.xid);
        };
        let reply_body::MSG_ACCEPTED(accepted) = reply else {
            bail!("reply to xid {} denied: {reply:?}", msg.xid);
        };
        ensure!(
            matches!(accepted.reply_data, accept_stat_data::SUCCESS),
            "reply to xid {}: {:?}",
            msg.xid,
            accepted.reply_data
        );
        let (status, _) = nfsstat3::unpack(&mut rest)?;
        Ok(Reply {
            xid: msg.xid,
            status,
        })
    })
    .await
    .with_context(|| format!("no reply within {limit:?}"))?
}
