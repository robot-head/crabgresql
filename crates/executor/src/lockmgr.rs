//! In-memory row-lock manager for concurrent writers. Per `(table, rowid)`
//! exclusive/shared locks, transaction-scoped (released at COMMIT/ROLLBACK). A
//! blocked writer awaits a per-holder `Notify`; the holder's `release_all`
//! wakes it. A wait-for graph (each waiting xid -> the xid it blocks on) is
//! checked eagerly for cycles before blocking, aborting the would-be waiter
//! with a deadlock error. Purely in-memory: after a restart no transactions are
//! in flight, so no lock state must survive.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // wired in by the SP6 cutover (Task 5)
pub enum LockMode {
    Shared,
    Exclusive,
}

/// Result of a non-blocking lock attempt.
#[allow(dead_code)] // wired in by the SP6 cutover (Task 5)
pub enum Acquire {
    Acquired,
    /// Held by `holder` (one of the holders) — an xid to wait on.
    Conflict(u64),
}

/// Result of the eager cycle check.
#[allow(dead_code)] // wired in by the SP6 cutover (Task 5)
pub enum CycleCheck {
    Ok,
    Deadlock,
}

struct RowLock {
    mode: LockMode,
    holders: HashSet<u64>,
}

struct Inner {
    locks: HashMap<(catalog::TableId, u64), RowLock>,
    notifiers: HashMap<u64, Arc<Notify>>, // holder xid -> notifier (woken on release)
    wait_for: HashMap<u64, u64>,          // waiter xid -> holder xid
}

#[allow(dead_code)] // wired in by the SP6 cutover (Task 5)
pub(crate) struct RowLockManager {
    inner: Mutex<Inner>,
}

#[allow(dead_code)] // wired in by the SP6 cutover (Task 5)
impl RowLockManager {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                locks: HashMap::new(),
                notifiers: HashMap::new(),
                wait_for: HashMap::new(),
            }),
        }
    }

    /// Non-blocking acquire. Idempotent if `my_xid` already holds compatibly; a
    /// sole shared holder may upgrade to exclusive.
    pub fn try_acquire(
        &self,
        table: catalog::TableId,
        rowid: u64,
        mode: LockMode,
        my_xid: u64,
    ) -> Acquire {
        let mut g = self.inner.lock().expect("lockmgr");
        match g.locks.get_mut(&(table, rowid)) {
            None => {
                let mut holders = HashSet::new();
                holders.insert(my_xid);
                g.locks.insert((table, rowid), RowLock { mode, holders });
                Acquire::Acquired
            }
            Some(lock) => {
                if lock.holders.contains(&my_xid) {
                    if mode == LockMode::Exclusive && lock.mode == LockMode::Shared {
                        if lock.holders.len() == 1 {
                            lock.mode = LockMode::Exclusive;
                            Acquire::Acquired
                        } else {
                            let other = *lock
                                .holders
                                .iter()
                                .find(|&&h| h != my_xid)
                                .expect("other holder");
                            Acquire::Conflict(other)
                        }
                    } else {
                        Acquire::Acquired
                    }
                } else if mode == LockMode::Shared && lock.mode == LockMode::Shared {
                    lock.holders.insert(my_xid);
                    Acquire::Acquired
                } else {
                    Acquire::Conflict(*lock.holders.iter().next().expect("a holder"))
                }
            }
        }
    }

    /// Block until `holder` releases, after an eager deadlock check. `Err(())`
    /// means registering the wait would close a cycle (caller maps to 40P01).
    /// Lost-wakeup-free via `Notified::enable()` before releasing the guard.
    pub async fn wait_for(&self, my_xid: u64, holder: u64) -> Result<(), ()> {
        let notify = {
            let mut g = self.inner.lock().expect("lockmgr");
            if matches!(
                check_cycle(&g.wait_for, holder, my_xid),
                CycleCheck::Deadlock
            ) {
                return Err(());
            }
            g.wait_for.insert(my_xid, holder);
            Arc::clone(
                g.notifiers
                    .entry(holder)
                    .or_insert_with(|| Arc::new(Notify::new())),
            )
        };
        let notified = notify.notified();
        tokio::pin!(notified);
        // Register this waiter NOW, while we still control ordering: a
        // `notify_waiters()` from the holder's `release_all` after we drop the
        // guard below still wakes us (no lost wakeup). `notify` is held by Arc,
        // so the inner-mutex guard is already dropped at this point.
        notified.as_mut().enable();
        notified.await;
        self.inner
            .lock()
            .expect("lockmgr")
            .wait_for
            .remove(&my_xid);
        Ok(())
    }

    /// Release every lock held by `my_xid`, wake its waiters, clear its edge.
    pub fn release_all(&self, my_xid: u64) {
        let mut g = self.inner.lock().expect("lockmgr");
        g.locks.retain(|_, lock| {
            lock.holders.remove(&my_xid);
            !lock.holders.is_empty()
        });
        g.wait_for.remove(&my_xid);
        if let Some(n) = g.notifiers.remove(&my_xid) {
            n.notify_waiters();
        }
    }

    #[cfg(test)]
    pub(crate) fn wait_for_register_only(&self, waiter: u64, holder: u64) {
        self.inner
            .lock()
            .expect("lockmgr")
            .wait_for
            .insert(waiter, holder);
    }
    #[cfg(test)]
    pub(crate) fn check_cycle(&self, holder: u64, my_xid: u64) -> CycleCheck {
        check_cycle(&self.inner.lock().expect("lockmgr").wait_for, holder, my_xid)
    }
}

/// Would adding `my_xid -> holder` close a cycle? Walk the chain from `holder`;
/// if it reaches `my_xid`, the edge closes a cycle.
fn check_cycle(wait_for: &HashMap<u64, u64>, holder: u64, my_xid: u64) -> CycleCheck {
    let mut cur = holder;
    let mut seen = HashSet::new();
    loop {
        if cur == my_xid {
            return CycleCheck::Deadlock;
        }
        if !seen.insert(cur) {
            return CycleCheck::Ok; // pre-existing cycle not through my_xid
        }
        match wait_for.get(&cur) {
            Some(&next) => cur = next,
            None => return CycleCheck::Ok,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusive_conflicts_shared_coexists() {
        let m = RowLockManager::new();
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 10),
            Acquire::Acquired
        ));
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 11),
            Acquire::Conflict(10)
        ));
        assert!(matches!(
            m.try_acquire(1, 2, LockMode::Shared, 11),
            Acquire::Acquired
        ));
        assert!(matches!(
            m.try_acquire(1, 2, LockMode::Shared, 12),
            Acquire::Acquired
        ));
        assert!(matches!(
            m.try_acquire(1, 2, LockMode::Exclusive, 13),
            Acquire::Conflict(_)
        ));
    }

    #[test]
    fn release_all_frees_rows_and_is_reacquirable() {
        let m = RowLockManager::new();
        m.try_acquire(1, 1, LockMode::Exclusive, 10);
        m.try_acquire(1, 2, LockMode::Exclusive, 10);
        m.release_all(10);
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 11),
            Acquire::Acquired
        ));
        assert!(matches!(
            m.try_acquire(1, 2, LockMode::Exclusive, 11),
            Acquire::Acquired
        ));
    }

    #[test]
    fn reacquire_by_same_holder_is_idempotent() {
        let m = RowLockManager::new();
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 10),
            Acquire::Acquired
        ));
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 10),
            Acquire::Acquired
        ));
    }

    #[test]
    fn shared_holder_upgrades_to_exclusive_when_sole() {
        let m = RowLockManager::new();
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Shared, 10),
            Acquire::Acquired
        ));
        // sole shared holder upgrades to exclusive
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 10),
            Acquire::Acquired
        ));
        // now another exclusive conflicts
        assert!(matches!(
            m.try_acquire(1, 1, LockMode::Exclusive, 11),
            Acquire::Conflict(10)
        ));
    }

    #[tokio::test]
    async fn wait_for_resumes_when_holder_releases() {
        use std::sync::Arc;
        let m = Arc::new(RowLockManager::new());
        m.try_acquire(1, 1, LockMode::Exclusive, 10);
        let m2 = Arc::clone(&m);
        let waiter = tokio::spawn(async move {
            assert!(matches!(
                m2.try_acquire(1, 1, LockMode::Exclusive, 11),
                Acquire::Conflict(10)
            ));
            m2.wait_for(11, 10).await.expect("not a deadlock");
        });
        tokio::task::yield_now().await;
        m.release_all(10);
        waiter.await.expect("waiter completes"); // must not hang
    }

    #[tokio::test]
    async fn wait_for_does_not_lose_a_wakeup_under_race() {
        // Stress: holder releases immediately; the waiter must still wake. Run a
        // few iterations to shake out a lost-wakeup.
        use std::sync::Arc;
        for _ in 0..50 {
            let m = Arc::new(RowLockManager::new());
            m.try_acquire(1, 1, LockMode::Exclusive, 10);
            let m2 = Arc::clone(&m);
            let waiter = tokio::spawn(async move {
                let _ = m2.try_acquire(1, 1, LockMode::Exclusive, 11);
                m2.wait_for(11, 10).await
            });
            let m3 = Arc::clone(&m);
            let releaser = tokio::spawn(async move {
                m3.release_all(10);
            });
            releaser.await.expect("releaser");
            // must not hang: bound the wait so a lost wakeup fails the test
            // instead of hanging forever
            tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
                .await
                .expect("waiter did not hang")
                .expect("waiter task")
                .expect("not a deadlock");
        }
    }

    #[test]
    fn cycle_check_detects_a_two_cycle() {
        let m = RowLockManager::new();
        m.wait_for_register_only(10, 11); // 10 waits for 11
        // now 11 -> 10 would close the cycle; ask "does my_xid=11 waiting for
        // holder=10 close a cycle?": walk from holder=10 -> 11 -> ==11 -> yes.
        assert!(matches!(m.check_cycle(10, 11), CycleCheck::Deadlock));
        // a non-closing edge is fine: my_xid=99 waiting for holder=11; walk from
        // 11 -> (no edge) -> Ok.
        assert!(matches!(m.check_cycle(11, 99), CycleCheck::Ok));
    }
}
