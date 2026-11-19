//! ONC RPC record marking on the NFS server's TCP connections (RFC 5531
//! section 11): a record is one or more fragments, each behind a 4-byte mark
//! whose highest bit flags the last fragment and whose low 31 bits give the
//! fragment length. Calls and replies are framed this way in both directions.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context as _, bail, ensure};
use cascade_engine::backend::NullBackend;
use cascade_engine::vfs::VfsTree;
use cascade_presenter_nfs::nfs::context::NfsContext;
use cascade_presenter_nfs::nfs::server::{NfsServer, NfsServerConfig};
use nfs3_client::nfs3_types::mount::{self, MOUNT_PROGRAM, dirpath, mountstat3};
use nfs3_client::nfs3_types::nfs3::{self, NFS_PROGRAM};
use nfs3_client::nfs3_types::rpc::{
    RPC_VERSION_2, accept_stat_data, call_body, msg_body, opaque_auth, reply_body, rpc_msg,
};
use nfs3_client::nfs3_types::xdr_codec::{Opaque, Pack, Unpack};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

const LAST_FRAGMENT: u32 = 0x8000_0000;
/// Bound on any single exchange the server is expected to complete.
const ANSWER: Duration = Duration::from_secs(10);
/// Window in which a server that waits for more fragments must stay silent.
const SILENCE: Duration = Duration::from_millis(500);

fn call_message(xid: u32, program: u32, version: u32, procedure: u32) -> anyhow::Result<Vec<u8>> {
    let msg = rpc_msg {
        xid,
        body: msg_body::CALL(call_body {
            rpcvers: RPC_VERSION_2,
            prog: program,
            vers: version,
            proc: procedure,
            cred: opaque_auth::default(),
            verf: opaque_auth::default(),
        }),
    };
    let mut body = Vec::new();
    msg.pack(&mut body)?;
    Ok(body)
}

fn null_v3(xid: u32) -> anyhow::Result<Vec<u8>> {
    call_message(
        xid,
        nfs3::PROGRAM,
        nfs3::VERSION,
        NFS_PROGRAM::NFSPROC3_NULL as u32,
    )
}

fn mount_mnt(xid: u32) -> anyhow::Result<Vec<u8>> {
    let mut body = call_message(
        xid,
        mount::PROGRAM,
        mount::VERSION,
        MOUNT_PROGRAM::MOUNTPROC3_MNT as u32,
    )?;
    dirpath(Opaque::borrowed(b"/")).pack(&mut body)?;
    Ok(body)
}

fn mark(len: usize, last: bool) -> anyhow::Result<[u8; 4]> {
    let len = u32::try_from(len)?;
    ensure!(
        len < LAST_FRAGMENT,
        "fragment of {len} bytes exceeds 31 bits"
    );
    Ok((if last { len | LAST_FRAGMENT } else { len }).to_be_bytes())
}

/// `body` as a record split at `cuts`, the last-fragment bit on the final
/// fragment only.
fn record_in_fragments(body: &[u8], cuts: &[usize]) -> anyhow::Result<Vec<u8>> {
    let mut bounds = vec![0];
    bounds.extend(cuts.iter().copied().filter(|&c| c > 0 && c < body.len()));
    bounds.push(body.len());
    let mut out = Vec::new();
    for (i, pair) in bounds.windows(2).enumerate() {
        let [start, end] = *pair else {
            bail!("window of two expected");
        };
        let fragment = body.get(start..end).context("cut outside the body")?;
        out.extend_from_slice(&mark(fragment.len(), i + 2 == bounds.len())?);
        out.extend_from_slice(fragment);
    }
    Ok(out)
}

fn record(body: &[u8]) -> anyhow::Result<Vec<u8>> {
    record_in_fragments(body, &[])
}

/// A reply record as received: the fragment marks and the reassembled body.
struct Reply {
    marks: Vec<u32>,
    body: Vec<u8>,
}

async fn read_record(stream: &mut TcpStream) -> anyhow::Result<Reply> {
    let mut marks = Vec::new();
    let mut body = Vec::new();
    loop {
        let mut raw = [0u8; 4];
        stream.read_exact(&mut raw).await.context("reply mark")?;
        let mark = u32::from_be_bytes(raw);
        marks.push(mark);
        let len = usize::try_from(mark & !LAST_FRAGMENT)?;
        ensure!(
            body.len() + len <= 16 * 1024 * 1024,
            "reply mark {mark:#010x} claims an implausible record"
        );
        let start = body.len();
        body.resize(start + len, 0);
        let fragment = body.get_mut(start..).context("fragment slice")?;
        stream
            .read_exact(fragment)
            .await
            .context("reply fragment")?;
        if mark & LAST_FRAGMENT != 0 {
            return Ok(Reply { marks, body });
        }
    }
}

async fn answered(stream: &mut TcpStream) -> anyhow::Result<Reply> {
    timeout(ANSWER, read_record(stream))
        .await
        .context("no complete reply record within the bound")?
}

/// The xid of `reply` and the procedure result it carries, the call
/// accepted and successful.
fn accepted(reply: &Reply) -> anyhow::Result<(u32, &[u8])> {
    let mut rest = reply.body.as_slice();
    let (msg, _) = rpc_msg::unpack(&mut rest)?;
    let msg_body::REPLY(body) = msg.body else {
        bail!("xid {} is not a reply", msg.xid);
    };
    let reply_body::MSG_ACCEPTED(accepted) = body else {
        bail!("reply to xid {} denied: {body:?}", msg.xid);
    };
    ensure!(
        matches!(accepted.reply_data, accept_stat_data::SUCCESS),
        "reply to xid {}: {:?}",
        msg.xid,
        accepted.reply_data
    );
    Ok((msg.xid, rest))
}

/// The procedure result of an accepted, successful reply to `xid`.
fn accepted_result(reply: &Reply, xid: u32) -> anyhow::Result<&[u8]> {
    let (got_xid, result) = accepted(reply)?;
    ensure!(got_xid == xid, "reply xid {got_xid}, expected {xid}");
    Ok(result)
}

fn ensure_mounted(mut result: &[u8]) -> anyhow::Result<()> {
    let (status, _) = mountstat3::unpack(&mut result)?;
    ensure!(matches!(status, mountstat3::MNT3_OK), "MNT status {status}");
    Ok(())
}

async fn expect_null(stream: &mut TcpStream, xid: u32) -> anyhow::Result<()> {
    let reply = answered(stream).await?;
    ensure!(
        accepted_result(&reply, xid)?.is_empty(),
        "NULL carries no result"
    );
    Ok(())
}

async fn expect_mnt(stream: &mut TcpStream, xid: u32) -> anyhow::Result<()> {
    let reply = answered(stream).await?;
    ensure_mounted(accepted_result(&reply, xid)?)
}

/// Nothing arrives within [`SILENCE`]: the server is still waiting.
async fn expect_silence(stream: &mut TcpStream) -> anyhow::Result<()> {
    let mut byte = [0u8; 1];
    match timeout(SILENCE, stream.read(&mut byte)).await {
        Err(_) => Ok(()),
        Ok(Ok(0)) => bail!("server closed the connection instead of waiting"),
        Ok(Ok(_)) => bail!("server answered before the record was complete"),
        Ok(Err(e)) => bail!("connection failed while waiting: {e}"),
    }
}

fn null_ctx() -> Arc<NfsContext> {
    let vfs = Arc::new(RwLock::new(VfsTree::new(Arc::new(NullBackend::new(
        "test",
    )))));
    Arc::new(NfsContext::new(vfs))
}

async fn connect(server: &NfsServer) -> anyhow::Result<TcpStream> {
    let stream = TcpStream::connect(server.local_addr).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

async fn start(ctx: Arc<NfsContext>) -> anyhow::Result<NfsServer> {
    NfsServer::start(NfsServerConfig::default(), ctx).await
}

#[tokio::test(flavor = "multi_thread")]
async fn call_in_one_last_fragment_is_answered() -> anyhow::Result<()> {
    let server = start(null_ctx()).await?;
    let mut stream = connect(&server).await?;

    stream.write_all(&record(&null_v3(1)?)?).await?;
    expect_null(&mut stream, 1).await?;
    stream.write_all(&record(&mount_mnt(2)?)?).await?;
    expect_mnt(&mut stream, 2).await?;

    server.stop()
}

#[tokio::test(flavor = "multi_thread")]
async fn reply_is_a_record_ending_in_a_last_fragment() -> anyhow::Result<()> {
    let server = start(null_ctx()).await?;
    let mut stream = connect(&server).await?;

    stream.write_all(&record(&null_v3(5)?)?).await?;
    let reply = answered(&mut stream).await?;

    let (last, earlier) = reply.marks.split_last().context("no fragment")?;
    ensure!(last & LAST_FRAGMENT != 0, "final mark {last:#010x}");
    ensure!(
        earlier.iter().all(|m| m & LAST_FRAGMENT == 0),
        "last-fragment bit before the final fragment: {earlier:#010x?}"
    );
    let total: usize = reply
        .marks
        .iter()
        .map(|m| usize::try_from(m & !LAST_FRAGMENT))
        .sum::<Result<_, _>>()?;
    ensure!(total == reply.body.len(), "fragment lengths sum to {total}");
    ensure!(
        accepted_result(&reply, 5)?.is_empty(),
        "NULL reply of {} bytes carries a result",
        reply.body.len()
    );

    server.stop()
}

#[tokio::test(flavor = "multi_thread")]
async fn call_over_several_fragments_is_reassembled_and_answered_once() -> anyhow::Result<()> {
    let server = start(null_ctx()).await?;
    let mut stream = connect(&server).await?;

    stream
        .write_all(&record_in_fragments(&null_v3(7)?, &[4, 13, 30])?)
        .await?;
    expect_null(&mut stream, 7).await?;
    expect_silence(&mut stream).await?;

    stream
        .write_all(&record_in_fragments(&mount_mnt(8)?, &[1, 2, 3])?)
        .await?;
    expect_mnt(&mut stream, 8).await?;
    expect_silence(&mut stream).await?;

    server.stop()
}

fn three_records() -> anyhow::Result<Vec<u8>> {
    let mut bytes = record(&null_v3(21)?)?;
    bytes.extend(record_in_fragments(&mount_mnt(22)?, &[10])?);
    bytes.extend(record_in_fragments(&null_v3(23)?, &[6, 30])?);
    Ok(bytes)
}

/// Exactly one reply to each of the three calls, matched by xid in whatever
/// order they arrive (RFC 5531), then nothing more.
async fn expect_three_replies(stream: &mut TcpStream) -> anyhow::Result<()> {
    let mut replies = Vec::new();
    for _ in 0..3 {
        replies.push(answered(stream).await?);
    }
    let mut xids = Vec::new();
    for reply in &replies {
        let (xid, result) = accepted(reply)?;
        match xid {
            21 | 23 => ensure!(result.is_empty(), "NULL {xid} carries no result"),
            22 => ensure_mounted(result)?,
            other => bail!("reply to xid {other}, which no call carried"),
        }
        xids.push(xid);
    }
    xids.sort_unstable();
    ensure!(xids == [21, 22, 23], "replies to xids {xids:?}");
    expect_silence(stream).await
}

#[tokio::test(flavor = "multi_thread")]
async fn back_to_back_calls_in_one_write_are_answered_once_per_xid() -> anyhow::Result<()> {
    let server = start(null_ctx()).await?;
    let mut stream = connect(&server).await?;

    stream.write_all(&three_records()?).await?;
    expect_three_replies(&mut stream).await?;

    server.stop()
}

#[tokio::test(flavor = "multi_thread")]
async fn back_to_back_calls_split_at_arbitrary_bytes_are_answered_once_per_xid()
-> anyhow::Result<()> {
    let server = start(null_ctx()).await?;
    let mut stream = connect(&server).await?;
    let bytes = three_records()?;
    let null_record = record(&null_v3(21)?)?.len();

    // Inside the first mark, inside the first body, inside the second record's
    // first mark, and single bytes around the third record.
    let cuts = [
        2,
        9,
        null_record + 3,
        null_record + 30,
        bytes.len() - 5,
        bytes.len() - 1,
    ];
    let mut start_at = 0;
    for end in cuts.into_iter().chain([bytes.len()]) {
        stream
            .write_all(bytes.get(start_at..end).context("cut")?)
            .await?;
        stream.flush().await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        start_at = end;
    }
    expect_three_replies(&mut stream).await?;

    server.stop()
}

#[tokio::test(flavor = "multi_thread")]
async fn bare_length_is_a_non_final_fragment_and_waits_for_the_last() -> anyhow::Result<()> {
    let server = start(null_ctx()).await?;
    let mut stream = connect(&server).await?;
    let call = null_v3(51)?;

    stream.write_all(&mark(call.len(), false)?).await?;
    stream.write_all(&call).await?;
    expect_silence(&mut stream).await?;

    stream.write_all(&mark(0, true)?).await?;
    expect_null(&mut stream, 51).await?;

    server.stop()
}
