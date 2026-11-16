//! Loom models of the content cache's lock-level pieces, driving the
//! production [`WaiterList`] and [`Residency`] through the `cascade_loom`
//! sync alias.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Wake, Waker};

use loom::thread;

use super::ObjectId;
use super::evict::Residency;
use super::range_map::RangeMap;
use super::transfer::{Failure, WaiterList};
use crate::loom_sync::{Arc, AtomicBool, Ordering};

struct Unparker(thread::Thread);

impl Wake for Unparker {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(std::sync::Arc::new(Unparker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}

fn waiting(
    list: &Arc<WaiterList>,
    range: std::ops::Range<u64>,
) -> thread::JoinHandle<Result<(), Failure>> {
    let list = Arc::clone(list);
    thread::spawn(move || block_on(list.wait(range)))
}

#[test]
fn loom_model_two_waiters_and_a_completing_range_are_each_woken_once() {
    loom::model(|| {
        let list = Arc::new(WaiterList::new(RangeMap::default()));
        let first = waiting(&list, 0..10);
        let second = waiting(&list, 5..20);

        list.fill(0..20);

        assert_eq!(first.join().unwrap(), Ok(()));
        assert_eq!(second.join().unwrap(), Ok(()));
        assert!(!list.has_waiters());
        assert_eq!(list.fill(0..20), 0);
    });
}

#[test]
fn loom_model_a_partial_fill_wakes_only_the_covered_waiter() {
    loom::model(|| {
        let list = Arc::new(WaiterList::new(RangeMap::default()));
        let covered = waiting(&list, 0..10);
        let pending = waiting(&list, 10..20);

        list.fill(0..10);
        assert_eq!(covered.join().unwrap(), Ok(()));
        list.fill(10..20);

        assert_eq!(pending.join().unwrap(), Ok(()));
        assert!(!list.has_waiters());
    });
}

#[test]
fn loom_model_a_close_racing_a_fill_settles_each_waiter_once() {
    loom::model(|| {
        let list = Arc::new(WaiterList::new(RangeMap::default()));
        let waiter = waiting(&list, 0..10);
        let closer = {
            let list = Arc::clone(&list);
            thread::spawn(move || list.close(&Failure::Changed))
        };

        list.fill(0..10);
        closer.join().unwrap();

        let outcome = waiter.join().unwrap();
        assert!(
            matches!(outcome, Ok(()) | Err(Failure::Changed)),
            "{outcome:?}"
        );
        assert!(!list.has_waiters());
    });
}

const OBJECT: ObjectId = ObjectId::new(7);

#[test]
fn loom_model_a_reader_guard_and_an_eviction_mark_never_hold_together() {
    loom::model(|| {
        let residency = Arc::new(Residency::default());
        let reading = Arc::new(AtomicBool::new(false));
        let reader = {
            let residency = Arc::clone(&residency);
            let reading = Arc::clone(&reading);
            thread::spawn(move || {
                let Some(guard) = Residency::guard(&residency, OBJECT) else {
                    return false;
                };
                reading.store(true, Ordering::SeqCst);
                reading.store(false, Ordering::SeqCst);
                drop(guard);
                true
            })
        };

        let marked = Residency::mark(&residency, OBJECT);
        if marked.is_some() {
            assert!(
                !reading.load(Ordering::SeqCst),
                "evicted under a live reader"
            );
        }
        let read = reader.join().unwrap();
        assert!(
            read || marked.is_some(),
            "a guard was refused without a mark"
        );
        drop(marked);

        assert_eq!(residency.readers(OBJECT), 0);
        assert!(!residency.is_marked(OBJECT));
    });
}

#[test]
fn loom_model_two_sweeps_marking_one_object_admit_one() {
    loom::model(|| {
        let residency = Arc::new(Residency::default());
        let other = {
            let residency = Arc::clone(&residency);
            thread::spawn(move || Residency::mark(&residency, OBJECT))
        };

        let mine = Residency::mark(&residency, OBJECT);
        let theirs = other.join().unwrap();

        assert!(
            mine.is_some() != theirs.is_some(),
            "marks held at once: {}",
            usize::from(mine.is_some()) + usize::from(theirs.is_some())
        );
        drop((mine, theirs));
        assert!(!residency.is_marked(OBJECT));
    });
}

#[test]
fn loom_model_a_dropped_mark_admits_readers_and_the_last_guard_admits_a_mark() {
    loom::model(|| {
        let residency = Arc::new(Residency::default());
        let first = Residency::guard(&residency, OBJECT).unwrap();
        let nested = {
            let residency = Arc::clone(&residency);
            thread::spawn(move || {
                let guard = Residency::guard(&residency, OBJECT);
                assert!(guard.is_some(), "a guard was refused while another is live");
            })
        };
        let releaser = thread::spawn(move || drop(first));

        nested.join().unwrap();
        releaser.join().unwrap();

        let marked = Residency::mark(&residency, OBJECT);
        assert!(
            marked.is_some(),
            "the last guard's drop left the object busy"
        );
        assert!(Residency::guard(&residency, OBJECT).is_none());
        drop(marked);
        assert!(Residency::guard(&residency, OBJECT).is_some());
    });
}
