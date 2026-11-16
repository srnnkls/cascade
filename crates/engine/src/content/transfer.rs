//! Transfer layer of the content cache: the waiter list of an object, the
//! object a download fills, and the deadline-bound calls it makes.
//!
//! The waiter list is the lock-level piece the loom model checks; it takes
//! its lock from [`crate::loom_sync`].

use std::future::Future;
use std::ops::Range;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering as StdOrdering};
use std::sync::{Arc, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use anyhow::{Result, anyhow};
use tokio::sync::broadcast;
use tokio::time::Instant;

use super::ObjectId;
use super::range_map::RangeMap;
use super::replica::Replica;
use super::store::{CloneOutcome, ObjectStore};
use crate::backend::TransientError;
use crate::db::StateDb;
use crate::loom_sync::{Mutex, MutexGuard};
use crate::types::{FileEntry, Revision};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

/// Settings of reads and downloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferConfig {
    /// Bytes fetched beyond the end of a ranged read.
    pub read_ahead: u64,
    /// Upper bound of one backend read.
    pub request_size: u64,
    /// Deadline of each backend call.
    pub call_timeout: Duration,
    /// A download nobody waits on stops after this long without a request.
    pub idle_stop: Duration,
    /// Consecutive failed reads after which a download fails its waiters.
    pub max_errors: u32,
    /// Pause before a failed read is tried again.
    pub retry_delay: Duration,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            read_ahead: 8 * MIB,
            request_size: MIB,
            call_timeout: Duration::from_secs(15),
            idle_stop: Duration::from_mins(1),
            max_errors: 10,
            retry_delay: Duration::from_secs(1),
        }
    }
}

/// Failure of a fetch a caller matches on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TransferError {
    /// The requested content version is gone from the backend.
    #[error("the requested version is no longer available")]
    VersionNoLongerAvailable,
}

/// Outcome a waiter receives when its range will not arrive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Failure {
    /// The remote changed and the object was discarded.
    Changed,
    /// The backend cannot be reached.
    Unreachable(String),
    /// The download failed for another reason.
    Failed(String),
}

impl Failure {
    pub(crate) fn of(error: &anyhow::Error) -> Self {
        match error.downcast_ref::<TransientError>() {
            Some(TransientError::Unreachable(message)) => Self::Unreachable(message.clone()),
            _ => Self::Failed(format!("{error:#}")),
        }
    }

    pub(crate) fn into_error(self) -> anyhow::Error {
        match self {
            Self::Changed => TransferError::VersionNoLongerAvailable.into(),
            Self::Unreachable(message) => TransientError::Unreachable(message).into(),
            Self::Failed(message) => anyhow!(message),
        }
    }
}

pub(crate) fn is_unreachable(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<TransientError>(),
        Some(TransientError::Unreachable(_))
    )
}

/// `ranges` without the bytes of `cut`.
fn subtract(ranges: Vec<Range<u64>>, cut: &Range<u64>) -> Vec<Range<u64>> {
    ranges
        .into_iter()
        .flat_map(|range| {
            let left = range.start..cut.start.clamp(range.start, range.end);
            let right = cut.end.clamp(range.start, range.end)..range.end;
            [left, right].into_iter().filter(|part| !part.is_empty())
        })
        .collect()
}

/// The part of `gap` up to the next multiple of `chunk`.
fn first_piece(gap: &Range<u64>, chunk: u64) -> Range<u64> {
    let boundary = (gap.start / chunk).saturating_add(1).saturating_mul(chunk);
    gap.start..gap.end.min(boundary)
}

#[derive(Debug)]
struct Waiter {
    id: u64,
    range: Range<u64>,
    waker: Option<Waker>,
}

#[derive(Debug, Default)]
struct WaitState {
    present: RangeMap,
    in_flight: Vec<Range<u64>>,
    waiters: Vec<Waiter>,
    outcomes: Vec<(u64, Result<(), Failure>)>,
    next_id: u64,
    closed: Option<Failure>,
}

impl WaitState {
    /// Sub-ranges of `range` neither present nor being fetched.
    fn claimable(&self, range: Range<u64>) -> Vec<Range<u64>> {
        self.in_flight
            .iter()
            .fold(self.present.missing(range), |gaps, busy| {
                subtract(gaps, busy)
            })
    }

    fn release(&mut self, range: &Range<u64>) {
        let in_flight = std::mem::take(&mut self.in_flight);
        self.in_flight = subtract(in_flight, range);
    }

    /// Settles every waiter `outcome` picks, returning their wakers.
    fn settle(
        &mut self,
        outcome: impl Fn(&Self, &Waiter) -> Option<Result<(), Failure>>,
    ) -> Vec<Waker> {
        let waiters = std::mem::take(&mut self.waiters);
        let mut wakers = Vec::new();
        for waiter in waiters {
            match outcome(self, &waiter) {
                Some(result) => {
                    self.outcomes.push((waiter.id, result));
                    wakers.extend(waiter.waker);
                }
                None => self.waiters.push(waiter),
            }
        }
        wakers
    }
}

fn wake(wakers: Vec<Waker>) -> usize {
    let woken = wakers.len();
    wakers.into_iter().for_each(Waker::wake);
    woken
}

/// Ranges of one object that are present or being fetched, and the callers
/// waiting for ranges to arrive, in arrival order.
#[derive(Debug)]
pub(crate) struct WaiterList {
    state: Mutex<WaitState>,
}

impl WaiterList {
    pub(crate) fn new(present: RangeMap) -> Self {
        Self {
            state: Mutex::new(WaitState {
                present,
                ..WaitState::default()
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, WaitState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers a waiter for `range`; it resolves once every byte of the
    /// range is present or the range will not arrive.
    pub(crate) fn wait(&self, range: Range<u64>) -> Wait<'_> {
        let mut state = self.lock();
        let ready = match &state.closed {
            Some(failure) => Some(Err(failure.clone())),
            None if state.present.missing(range.clone()).is_empty() => Some(Ok(())),
            None => None,
        };
        let id = ready.is_none().then(|| {
            let id = state.next_id;
            state.next_id = id.wrapping_add(1);
            state.waiters.push(Waiter {
                id,
                range,
                waker: None,
            });
            id
        });
        Wait {
            list: self,
            id,
            ready,
        }
    }

    pub(crate) fn missing(&self, range: Range<u64>) -> Vec<Range<u64>> {
        self.lock().present.missing(range)
    }

    pub(crate) fn has_waiters(&self) -> bool {
        !self.lock().waiters.is_empty()
    }

    /// Claims the parts of `range` nobody fetches, split at multiples of
    /// `chunk`, for the caller to fetch.
    pub(crate) fn claim(&self, range: Range<u64>, chunk: u64) -> Vec<Range<u64>> {
        let chunk = chunk.max(1);
        let mut state = self.lock();
        let mut pieces = Vec::new();
        for mut gap in state.claimable(range) {
            while !gap.is_empty() {
                let piece = first_piece(&gap, chunk);
                gap.start = piece.end;
                pieces.push(piece);
            }
        }
        state.in_flight.extend(pieces.iter().cloned());
        pieces
    }

    /// Claims the next piece of a download of `whole`: the chunk around the
    /// first unclaimed byte of the earliest waiter, or with no such waiter the
    /// lowest unclaimed chunk.
    pub(crate) fn claim_next(&self, whole: Range<u64>, chunk: u64) -> Option<Range<u64>> {
        let chunk = chunk.max(1);
        let mut state = self.lock();
        let wanted = state
            .waiters
            .iter()
            .find_map(|waiter| state.claimable(waiter.range.clone()).into_iter().next());
        let piece = match wanted {
            Some(gap) => {
                let block_start = (gap.start / chunk).saturating_mul(chunk).max(whole.start);
                let block = block_start..block_start.saturating_add(chunk).min(whole.end);
                state
                    .claimable(block)
                    .into_iter()
                    .find(|part| part.contains(&gap.start))
                    .unwrap_or_else(|| first_piece(&gap, chunk))
            }
            None => first_piece(&state.claimable(whole).into_iter().next()?, chunk),
        };
        state.in_flight.push(piece.clone());
        Some(piece)
    }

    /// Gives up a claim without filling it.
    pub(crate) fn release(&self, range: &Range<u64>) {
        self.lock().release(range);
    }

    /// Records `range` as present and wakes the waiters it completes.
    pub(crate) fn fill(&self, range: Range<u64>) -> usize {
        let mut state = self.lock();
        state.release(&range);
        state.present.insert(range);
        let wakers = state.settle(|state, waiter| {
            state
                .present
                .missing(waiter.range.clone())
                .is_empty()
                .then_some(Ok(()))
        });
        drop(state);
        wake(wakers)
    }

    /// Gives up the claim of `range` and fails the waiters overlapping it.
    pub(crate) fn fail_overlapping(&self, range: &Range<u64>, failure: &Failure) -> usize {
        let mut state = self.lock();
        state.release(range);
        let wakers = state.settle(|_, waiter| {
            (waiter.range.start < range.end && range.start < waiter.range.end)
                .then(|| Err(failure.clone()))
        });
        drop(state);
        wake(wakers)
    }

    /// Fails every waiter.
    pub(crate) fn fail_all(&self, failure: &Failure) -> usize {
        let wakers = self.lock().settle(|_, _| Some(Err(failure.clone())));
        wake(wakers)
    }

    /// Fails every waiter and every later wait.
    pub(crate) fn close(&self, failure: &Failure) -> usize {
        let mut state = self.lock();
        state.closed = Some(failure.clone());
        let wakers = state.settle(|_, _| Some(Err(failure.clone())));
        drop(state);
        wake(wakers)
    }
}

/// A registered waiter; dropping it withdraws the registration.
#[derive(Debug)]
pub(crate) struct Wait<'a> {
    list: &'a WaiterList,
    id: Option<u64>,
    ready: Option<Result<(), Failure>>,
}

impl Future for Wait<'_> {
    type Output = Result<(), Failure>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(ready) = self.ready.take() {
            return Poll::Ready(ready);
        }
        let Some(id) = self.id else {
            return Poll::Ready(Ok(()));
        };
        let mut state = self.list.lock();
        if let Some(index) = state.outcomes.iter().position(|(owner, _)| *owner == id) {
            let (_, outcome) = state.outcomes.swap_remove(index);
            drop(state);
            self.id = None;
            return Poll::Ready(outcome);
        }
        if let Some(waiter) = state.waiters.iter_mut().find(|waiter| waiter.id == id) {
            waiter.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl Drop for Wait<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            let mut state = self.list.lock();
            state.waiters.retain(|waiter| waiter.id != id);
            state.outcomes.retain(|(owner, _)| *owner != id);
        }
    }
}

#[derive(Debug, Default)]
struct Gate {
    discarded: bool,
    complete: bool,
    handed_out: RangeMap,
    /// Writes into the object so far.
    written: u64,
    /// Value of `written` when the revision was last found unchanged.
    verified: u64,
}

#[derive(Debug)]
struct Activity {
    downloading: bool,
    last_wanted: Instant,
}

/// A partial object being filled for one content version of an item.
///
/// Writes, clones, reads and the discard of its file pass one gate, so no
/// byte lands in or leaves the object file after it was discarded.
#[derive(Debug)]
pub(crate) struct Transfer {
    /// Replica of the backend holding the item.
    pub(crate) replica: Arc<Replica>,
    pub(crate) uid: u64,
    pub(crate) object_id: ObjectId,
    pub(crate) content_version: String,
    pub(crate) source_revision: Revision,
    pub(crate) entry: FileEntry,
    pub(crate) size: u64,
    /// Ranges handed out from a discarded predecessor, refetched before any
    /// handoff of this object.
    pub(crate) carried: Vec<Range<u64>>,
    pub(crate) waiters: WaiterList,
    gate: std::sync::Mutex<Gate>,
    activity: std::sync::Mutex<Activity>,
}

/// Identity and source of a [`Transfer`].
#[derive(Debug)]
pub(crate) struct TransferSource {
    pub(crate) replica: Arc<Replica>,
    pub(crate) uid: u64,
    pub(crate) object_id: ObjectId,
    pub(crate) content_version: String,
    pub(crate) source_revision: Revision,
    pub(crate) entry: FileEntry,
    pub(crate) size: u64,
}

fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Transfer {
    pub(crate) fn new(source: TransferSource, present: RangeMap, carried: &RangeMap) -> Self {
        let size = source.size;
        Self {
            replica: source.replica,
            uid: source.uid,
            object_id: source.object_id,
            content_version: source.content_version,
            source_revision: source.source_revision,
            entry: source.entry,
            size,
            carried: carried
                .ranges()
                .iter()
                .map(|range| range.start.min(size)..range.end.min(size))
                .filter(|range| !range.is_empty())
                .collect(),
            waiters: WaiterList::new(present),
            gate: std::sync::Mutex::default(),
            activity: std::sync::Mutex::new(Activity {
                downloading: false,
                last_wanted: Instant::now(),
            }),
        }
    }

    pub(crate) const fn whole(&self) -> Range<u64> {
        0..self.size
    }

    pub(crate) fn is_discarded(&self) -> bool {
        lock(&self.gate).discarded
    }

    pub(crate) fn is_complete(&self) -> bool {
        let gate = lock(&self.gate);
        gate.complete && !gate.discarded
    }

    /// Writes fetched bytes and records their range. Returns `false` when the
    /// object was discarded.
    pub(crate) fn write(
        &self,
        store: &ObjectStore,
        db: &StateDb,
        range: Range<u64>,
        bytes: &[u8],
    ) -> Result<bool> {
        let mut gate = lock(&self.gate);
        if gate.discarded {
            return Ok(false);
        }
        store.write_at(self.object_id, range.start, bytes)?;
        db.add_content_range(self.object_id, range.clone())?;
        gate.written = gate.written.saturating_add(1);
        self.waiters.fill(range);
        drop(gate);
        Ok(true)
    }

    /// Clones the object to `target`, or `None` when it was discarded.
    pub(crate) fn clone_to(
        &self,
        store: &ObjectStore,
        target: &Path,
    ) -> Result<Option<CloneOutcome>> {
        let gate = lock(&self.gate);
        if gate.discarded {
            return Ok(None);
        }
        let outcome = store.clone_to(self.object_id, target)?;
        drop(gate);
        Ok(Some(outcome))
    }

    /// Reads from the object, or `None` when it was discarded.
    pub(crate) fn read_at(
        &self,
        store: &ObjectStore,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<Option<usize>> {
        let gate = lock(&self.gate);
        if gate.discarded {
            return Ok(None);
        }
        let read = store.read_at(self.object_id, offset, buf)?;
        drop(gate);
        Ok(Some(read))
    }

    /// Whether bytes read from this object need a revision check before they
    /// are served: it is partial and its download runs or ranges were written
    /// since the last check. Returns the write count to pass to
    /// [`Self::mark_verified`] once the check found no change.
    pub(crate) fn check_due(&self) -> Option<u64> {
        let downloading = lock(&self.activity).downloading;
        let gate = lock(&self.gate);
        let due =
            !gate.complete && !gate.discarded && (downloading || gate.written != gate.verified);
        due.then_some(gate.written)
    }

    /// Records that the revision was unchanged after `written` writes.
    pub(crate) fn mark_verified(&self, written: u64) {
        let mut gate = lock(&self.gate);
        gate.verified = gate.verified.max(written);
    }

    pub(crate) fn mark_handed_out(&self, range: Range<u64>) {
        lock(&self.gate).handed_out.insert(range);
    }

    /// Marks the object complete once every byte is present.
    pub(crate) fn finish(&self, db: &StateDb) -> Result<()> {
        let mut gate = lock(&self.gate);
        if gate.discarded || gate.complete || !self.waiters.missing(self.whole()).is_empty() {
            return Ok(());
        }
        db.mark_content_complete(self.object_id)?;
        gate.complete = true;
        Ok(())
    }

    /// Deletes the object and its file and fails every waiter. Returns the
    /// ranges handed out from it, or `None` when it was already discarded.
    pub(crate) fn discard(&self, store: &ObjectStore, db: &StateDb) -> Result<Option<RangeMap>> {
        let mut gate = lock(&self.gate);
        if gate.discarded {
            return Ok(None);
        }
        gate.discarded = true;
        let mut handed_out = std::mem::take(&mut gate.handed_out);
        for range in &self.carried {
            handed_out.insert(range.clone());
        }
        let deleted = db
            .delete_content_object(self.object_id)
            .and_then(|()| Ok(store.delete(self.object_id)?));
        drop(gate);
        self.waiters.close(&Failure::Changed);
        deleted.map(|()| Some(handed_out))
    }

    /// Records a request; returns whether the caller must start the download.
    pub(crate) fn want(&self) -> bool {
        let mut activity = lock(&self.activity);
        activity.last_wanted = Instant::now();
        !std::mem::replace(&mut activity.downloading, true)
    }

    /// Time since the last request.
    pub(crate) fn idle_for(&self) -> Duration {
        lock(&self.activity).last_wanted.elapsed()
    }

    /// Ends the download unless a waiter still needs it or, with `idle`, a
    /// request arrived within that time.
    pub(crate) fn try_stop(&self, idle: Option<Duration>) -> bool {
        let mut activity = lock(&self.activity);
        if !self.is_discarded() && self.waiters.has_waiters() {
            return false;
        }
        if idle.is_some_and(|idle| activity.last_wanted.elapsed() < idle) {
            return false;
        }
        activity.downloading = false;
        true
    }
}

/// Signal the cache raises for the host app.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheSignal {
    /// A backend call succeeded after one failed as unreachable.
    ErrorResolved,
}

/// Bounds backend calls by a deadline and raises
/// [`CacheSignal::ErrorResolved`] on the first success after an unreachable
/// failure.
#[derive(Debug)]
pub(crate) struct Remote {
    timeout: Duration,
    unreachable: AtomicBool,
    signals: broadcast::Sender<CacheSignal>,
}

impl Remote {
    pub(crate) fn new(timeout: Duration) -> Self {
        let (signals, _) = broadcast::channel(16);
        Self {
            timeout,
            unreachable: AtomicBool::new(false),
            signals,
        }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<CacheSignal> {
        self.signals.subscribe()
    }

    pub(crate) async fn call<T>(&self, op: impl Future<Output = Result<T>>) -> Result<T> {
        let result = tokio::time::timeout(self.timeout, op)
            .await
            .unwrap_or_else(|_| {
                Err(
                    TransientError::Unreachable(format!("no reply within {:?}", self.timeout))
                        .into(),
                )
            });
        match &result {
            Ok(_) => {
                if self.unreachable.swap(false, StdOrdering::AcqRel)
                    && self.signals.send(CacheSignal::ErrorResolved).is_err()
                {
                    tracing::debug!("error resolved with no subscriber");
                }
            }
            Err(error) if is_unreachable(error) => {
                self.unreachable.store(true, StdOrdering::Release);
            }
            Err(_) => {}
        }
        result
    }
}
