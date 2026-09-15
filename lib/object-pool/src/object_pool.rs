use std::{fmt, num::NonZeroUsize};

/// Reuses owned objects under one owner's mutable access, with unrestricted growth.
///
/// Mutating operations require `&mut self`; the pool has no internal locks or
/// runtime borrow checks. Checkout returns the actual `T`, with no guard,
/// automatic return, or reset callback. Callers reset objects as needed before
/// returning them with [`put`](Self::put) or reusing them.
///
/// A checked-out object can move to another thread when `T: Send`. Return it to
/// the owner through an application channel before putting it back in the pool.
/// Objects that stay on the owner's thread need no `Send` or `Sync` bounds.
/// The owner regularly calls [`maintenance_tick`](Self::maintenance_tick). Each
/// group of the configured number of ticks discards its minimum available count, including
/// temporary dips between ticks. Checkout updates that minimum as objects leave.
/// The pool keeps no clock or history collection, starts no timer, and cannot
/// reach checked-out values. The owner determines the maintenance cadence.
pub struct ObjectPool<T> {
    available: Vec<T>,
    min_available: usize,
    tick_count: usize,
    trim_after_ticks: usize,
}

impl<T> ObjectPool<T> {
    /// Creates an empty pool that considers trimming every `trim_after_ticks` calls.
    ///
    /// Allocates no object or collection storage. The nonzero argument specifies
    /// a number of ticks, not a duration. Calling
    /// [`maintenance_tick`](Self::maintenance_tick) once per second with a threshold
    /// of 600 gives approximately ten minutes between decisions.
    /// Availability is tracked from construction, including the initially empty
    /// pool. The first group therefore reclaims nothing and establishes the next
    /// group's starting count.
    ///
    /// A threshold of one considers trimming on every tick. Zero is excluded by
    /// the argument type, and there is no implicit default threshold.
    pub const fn new(trim_after_ticks: NonZeroUsize) -> Self {
        Self {
            available: Vec::new(),
            min_available: 0, // The first group starts with an empty pool.
            tick_count: 0,
            trim_after_ticks: trim_after_ticks.get(),
        }
    }

    /// Takes the most recently returned object, or `None` if none is available.
    ///
    /// Transfers ownership without cloning the object or allocating a wrapper.
    /// This does not wait for an object to be returned. Updates the group's minimum
    /// after checkout, so even availability dips between maintenance ticks count.
    #[inline]
    pub fn try_take(&mut self) -> Option<T> {
        let object = self.available.pop();
        self.min_available = self.min_available.min(self.available.len());
        object
    }

    /// Takes an available object, or calls `create` exactly once if the pool is empty.
    ///
    /// The fallback runs on the caller's thread. It can capture and consume
    /// values; it is neither boxed nor stored. A fallback panic propagates and
    /// leaves the empty pool usable.
    #[inline]
    pub fn take_or_else(&mut self, create: impl FnOnce() -> T) -> T {
        self.try_take().unwrap_or_else(create)
    }

    /// Returns an object for reuse, preserving its contents and internal capacity.
    ///
    /// Accepts newly created objects as well as checked-out objects. The collection
    /// grows as needed and has no maximum length. Resetting an object, such as
    /// clearing a byte buffer's length, is the caller's responsibility.
    /// Returns increase availability and leave any earlier minimum intact.
    #[inline]
    pub fn put(&mut self, object: T) {
        self.available.push(object);
    }

    /// Returns the number of available objects, excluding checkouts.
    pub fn len(&self) -> usize {
        self.available.len()
    }

    /// Reports whether the pool currently has no available objects.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Advances the maintenance tick count and returns the number of objects discarded.
    ///
    /// Checkout tracks the minimum available count throughout each group of ticks.
    /// Calls before the configured threshold return zero without removing objects.
    /// The final call discards that minimum and starts a new group at the retained
    /// available count. This reset happens even when no objects are removed.
    /// For example, a minimum of 80 with 120 currently available discards 80 and
    /// retains 40. A group that reaches zero discards nothing. The initially empty
    /// pool therefore gets one complete group to accumulate reusable objects.
    ///
    /// The owner controls when to tick. This method reads no clock, and delays do
    /// not reset the group. Every checkout contributes to the minimum, including
    /// one whose object is returned before the next tick. Returns never erase a
    /// previous low point. Timing depends entirely on the owner's maintenance cadence.
    ///
    /// Retained objects stay at the beginning of the vector. Truncation preserves
    /// its allocation and capacity, even when all available objects are removed.
    /// Checked-out objects are unaffected. Tracking the minimum allocates no storage.
    ///
    /// Group state resets before discarded objects are destroyed on the caller's
    /// thread. If a destructor panics and the caller catches the unwind, the pool
    /// retains the surviving prefix and tracking continues in the fresh group.
    pub fn maintenance_tick(&mut self) -> usize {
        self.tick_count += 1;

        if self.tick_count < self.trim_after_ticks {
            return 0;
        }

        // Checkout tracks every decrease, so the minimum cannot exceed this length.
        let remove = self.min_available;
        let retain = self.available.len() - remove;
        // Rebase even after a zero minimum, and reset before destructors can panic.
        self.min_available = retain;
        self.tick_count = 0;
        self.available.truncate(retain);

        remove
    }
}

impl<T> fmt::Debug for ObjectPool<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Avoid exposing buffer contents or requiring a Debug bound on T.
        f.debug_struct("ObjectPool")
            .field("available", &self.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        num::NonZeroUsize,
        panic::{AssertUnwindSafe, catch_unwind},
        rc::Rc,
        sync::mpsc,
        thread,
    };

    use bytes::BytesMut;

    use super::ObjectPool;

    const TRIM_AFTER_TICKS: NonZeroUsize = NonZeroUsize::new(3).unwrap();

    // Called at the beginning of a group; also checks that trimming is not early.
    fn finish_group<T>(pool: &mut ObjectPool<T>) -> usize {
        for _ in 1..pool.trim_after_ticks {
            assert_eq!(pool.maintenance_tick(), 0);
        }
        pool.maintenance_tick()
    }

    #[test]
    fn empty_pool_allocates_nothing_and_accepts_unrestricted_returns() {
        let mut pool = ObjectPool::new(TRIM_AFTER_TICKS);
        assert!(pool.is_empty());
        assert_eq!(pool.try_take(), None);
        assert_eq!(pool.available.capacity(), 0);
        for value in 0..1024 {
            pool.put(value);
        }
        assert_eq!(pool.len(), 1024);
        for value in (0..1024).rev() {
            assert_eq!(pool.try_take(), Some(value));
        }
        assert!(pool.is_empty());
        // Buffer handoffs update the minimum but do not advance maintenance.
        assert_eq!(pool.tick_count, 0);
        assert_eq!(pool.min_available, 0);
    }

    #[test]
    fn fallback_is_called_only_on_a_miss_and_can_consume_captures() {
        let mut pool = ObjectPool::new(TRIM_AFTER_TICKS);
        let owned = String::from("created");
        let object = pool.take_or_else(|| owned);
        assert_eq!(object, "created");
        pool.put(object);
        assert_eq!(pool.take_or_else(|| panic!("must reuse")), "created");
    }

    #[test]
    fn a_factory_panic_leaves_the_pool_usable() {
        let mut pool = ObjectPool::new(TRIM_AFTER_TICKS);
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                pool.take_or_else(|| panic!("factory failed"))
            }))
            .is_err()
        );
        assert!(pool.is_empty());
        pool.put(9);
        assert_eq!(pool.try_take(), Some(9));
    }

    #[test]
    fn bytes_mut_reuse_preserves_allocation_and_caller_controls_reset() {
        let mut pool = ObjectPool::new(TRIM_AFTER_TICKS);
        let mut buffer = BytesMut::with_capacity(4096);
        buffer.extend_from_slice(b"payload");
        let address = buffer.as_ptr();
        let capacity = buffer.capacity();
        pool.put(buffer);

        let mut buffer = pool.take_or_else(|| panic!("must reuse"));
        assert_eq!(&buffer[..], b"payload");
        assert_eq!(buffer.as_ptr(), address);
        assert_eq!(buffer.capacity(), capacity);
        buffer.clear();
        pool.put(buffer);
        let buffer = pool.try_take().unwrap();
        assert!(buffer.is_empty());
        assert_eq!(buffer.as_ptr(), address);
        assert_eq!(buffer.capacity(), capacity);
    }

    #[test]
    fn trims_only_on_the_final_tick_using_the_minimum_from_any_position() {
        // Exercise minima before the first, middle, and final tick, including zero.
        for (samples, expected_remove) in [
            ([2, 7, 5], 2),
            ([7, 2, 5], 2),
            ([7, 5, 2], 2),
            ([0, 7, 5], 0),
            ([7, 0, 5], 0),
            ([7, 5, 0], 0),
        ] {
            let mut pool = ObjectPool::new(TRIM_AFTER_TICKS);
            for _ in 0..samples[0] {
                pool.put(());
            }
            assert_eq!(finish_group(&mut pool), 0); // Initially empty group.
            for (index, count) in samples.into_iter().enumerate() {
                while pool.len() < count {
                    pool.put(());
                }
                while pool.len() > count {
                    pool.try_take().unwrap();
                }
                let expected = if index == 2 { expected_remove } else { 0 };
                assert_eq!(pool.maintenance_tick(), expected, "samples: {samples:?}");
                assert_eq!(pool.len(), count - expected, "samples: {samples:?}");
            }
        }
    }

    #[test]
    fn a_zero_minimum_does_not_carry_into_the_next_group() {
        let mut pool = ObjectPool::new(TRIM_AFTER_TICKS);
        assert_eq!(pool.maintenance_tick(), 0);
        for value in 0..4 {
            pool.put(value);
        }
        assert_eq!(pool.maintenance_tick(), 0);
        assert_eq!(pool.maintenance_tick(), 0);
        assert_eq!(pool.len(), 4);

        assert_eq!(pool.maintenance_tick(), 0);
        assert_eq!(pool.maintenance_tick(), 0);
        assert_eq!(pool.maintenance_tick(), 4);
        assert!(pool.is_empty());
    }

    #[test]
    fn each_group_starts_with_a_fresh_minimum_after_a_positive_trim() {
        let mut pool = ObjectPool::new(NonZeroUsize::new(2).unwrap());
        for value in 0..2 {
            pool.put(value);
        }
        assert_eq!(finish_group(&mut pool), 0);
        assert_eq!(pool.maintenance_tick(), 0);
        for value in 2..7 {
            pool.put(value);
        }
        assert_eq!(pool.maintenance_tick(), 2);
        assert_eq!(pool.len(), 5);

        assert_eq!(pool.maintenance_tick(), 0);
        for value in 7..10 {
            pool.put(value);
        }
        assert_eq!(pool.maintenance_tick(), 5);
        assert_eq!(pool.len(), 3);
    }

    #[test]
    fn trim_keeps_prefix_and_reuses_collection_allocation_on_regrowth() {
        let mut pool = ObjectPool::new(TRIM_AFTER_TICKS);
        let objects: Vec<_> = (0..6).map(Rc::new).collect();
        for object in &objects[..4] {
            pool.put(Rc::clone(object));
        }
        assert_eq!(finish_group(&mut pool), 0);
        assert_eq!(pool.maintenance_tick(), 0); // Minimum 4 is the number to discard.
        for object in &objects[4..] {
            pool.put(Rc::clone(object));
        }
        let address = pool.available.as_ptr();
        let capacity = pool.available.capacity();
        assert_eq!(pool.maintenance_tick(), 0);
        assert_eq!(pool.maintenance_tick(), 4);
        assert_eq!(pool.len(), 2);
        assert_eq!(pool.available.as_ptr(), address);
        assert_eq!(pool.available.capacity(), capacity);
        for (id, object) in objects.iter().enumerate() {
            assert_eq!(Rc::strong_count(object), if id < 2 { 2 } else { 1 });
        }

        assert_eq!(pool.maintenance_tick(), 0);
        assert_eq!(pool.maintenance_tick(), 0);
        assert_eq!(pool.maintenance_tick(), 2);
        assert!(pool.is_empty());
        assert_eq!(pool.available.as_ptr(), address);
        assert_eq!(pool.available.capacity(), capacity);
        assert!(objects.iter().all(|object| Rc::strong_count(object) == 1));

        for object in objects {
            pool.put(object);
        }
        assert_eq!(pool.len(), 6); // Trimming never establishes a growth limit.
        assert_eq!(pool.available.as_ptr(), address);
        assert_eq!(pool.available.capacity(), capacity);
    }

    #[test]
    fn trimming_an_empty_pool_keeps_its_collection_storage() {
        let mut pool = ObjectPool::new(TRIM_AFTER_TICKS);
        for value in 0..128 {
            pool.put(value);
        }
        while pool.try_take().is_some() {}
        let address = pool.available.as_ptr();
        let capacity = pool.available.capacity();
        assert!(capacity >= 128);
        for _ in 0..6 {
            assert_eq!(pool.maintenance_tick(), 0);
        }
        assert_eq!(pool.available.as_ptr(), address);
        assert_eq!(pool.available.capacity(), capacity);
    }

    #[test]
    fn a_single_tick_group_rebases_after_empty_periods() {
        let mut pool = ObjectPool::new(NonZeroUsize::new(1).unwrap());
        assert_eq!(pool.maintenance_tick(), 0);
        pool.put(1);
        pool.put(2);
        assert_eq!(pool.maintenance_tick(), 0); // This group also started empty.
        assert_eq!(pool.maintenance_tick(), 2);
        assert!(pool.is_empty());
        pool.put(3);
        assert_eq!(pool.maintenance_tick(), 0);
        assert_eq!(pool.maintenance_tick(), 1);
        assert!(pool.is_empty());
    }

    #[test]
    fn the_largest_threshold_can_finish_and_start_another_group() {
        let mut pool = ObjectPool::new(NonZeroUsize::new(usize::MAX).unwrap());
        pool.put(1);
        pool.put(2);
        assert_eq!(pool.maintenance_tick(), 0);
        // Skip an impractical number of identical observations to test the boundary.
        pool.tick_count = usize::MAX - 1;
        assert_eq!(pool.maintenance_tick(), 0); // Finish the initially empty group.
        pool.tick_count = usize::MAX - 1;
        assert_eq!(pool.maintenance_tick(), 2);
        assert!(pool.is_empty());
        pool.put(3);
        assert_eq!(pool.maintenance_tick(), 0);
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn emptying_and_refilling_between_ticks_prevents_trimming_for_one_group() {
        let mut pool = ObjectPool::new(NonZeroUsize::new(2).unwrap());
        pool.put(1);
        pool.put(2);
        assert_eq!(finish_group(&mut pool), 0);
        assert_eq!(pool.maintenance_tick(), 0);
        let first = pool.try_take().unwrap();
        let second = pool.try_take().unwrap();
        assert!(pool.is_empty());
        pool.put(first);
        pool.put(second);
        assert_eq!(pool.maintenance_tick(), 0); // The temporary zero cannot be erased.
        assert_eq!(pool.len(), 2);
        assert_eq!(finish_group(&mut pool), 2); // Reset prevents a permanent zero.
        assert!(pool.is_empty());
    }

    #[test]
    fn returns_do_not_erase_a_temporary_nonzero_low_point() {
        let mut pool = ObjectPool::new(TRIM_AFTER_TICKS);
        for value in 0..4 {
            pool.put(value);
        }
        assert_eq!(finish_group(&mut pool), 0);
        let borrowed: Vec<_> = (0..3).map(|_| pool.try_take().unwrap()).collect();
        assert_eq!(pool.len(), 1);
        for object in borrowed {
            pool.put(object);
        }
        assert_eq!(pool.len(), 4);
        assert_eq!(finish_group(&mut pool), 1);
        assert_eq!(pool.len(), 3);
    }

    #[test]
    fn checked_out_objects_outlive_trimming_and_the_pool() {
        let object = Rc::new(String::from("alive"));
        let mut pool = ObjectPool::new(NonZeroUsize::new(1).unwrap());
        pool.put(Rc::clone(&object));
        pool.put(Rc::clone(&object));
        assert_eq!(finish_group(&mut pool), 0);
        let checked_out = pool.try_take().unwrap();
        assert_eq!(pool.maintenance_tick(), 1);
        drop(pool);
        assert_eq!(Rc::strong_count(&object), 2);
        assert_eq!(&**checked_out, "alive");
        drop(checked_out);
        assert_eq!(Rc::strong_count(&object), 1);
    }

    #[test]
    fn dropping_pool_releases_all_available_objects() {
        let object = Rc::new(());
        let mut pool = ObjectPool::new(TRIM_AFTER_TICKS);
        pool.put(Rc::clone(&object));
        pool.put(Rc::clone(&object));
        drop(pool);
        assert_eq!(Rc::strong_count(&object), 1);
    }

    #[test]
    fn a_destructor_panic_preserves_retained_objects_and_resets_the_group() {
        struct DropProbe {
            drops: Rc<Cell<usize>>,
            panic_on_drop: Rc<Cell<bool>>,
        }

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.drops.set(self.drops.get() + 1);
                if self.panic_on_drop.replace(false) {
                    panic!("destructor failed");
                }
            }
        }

        let mut pool = ObjectPool::new(NonZeroUsize::new(2).unwrap());
        let drops = Rc::new(Cell::new(0));
        let no_panic = Rc::new(Cell::new(false));
        let panic_once = Rc::new(Cell::new(true));
        for _ in 0..2 {
            pool.put(DropProbe {
                drops: Rc::clone(&drops),
                panic_on_drop: Rc::clone(&no_panic),
            });
        }
        assert_eq!(finish_group(&mut pool), 0);
        assert_eq!(pool.maintenance_tick(), 0);
        pool.put(DropProbe {
            drops: Rc::clone(&drops),
            panic_on_drop: Rc::clone(&panic_once),
        });
        pool.put(DropProbe {
            drops: Rc::clone(&drops),
            panic_on_drop: Rc::clone(&no_panic),
        });
        let address = pool.available.as_ptr();
        let capacity = pool.available.capacity();
        let result = catch_unwind(AssertUnwindSafe(|| pool.maintenance_tick()));
        // If a regression panics before truncation, avoid a second panic on cleanup.
        panic_once.set(false);
        assert!(result.is_err());
        assert_eq!(drops.get(), 2); // The entire discarded tail is dropped.
        assert_eq!(pool.len(), 2);
        assert_eq!(pool.available.as_ptr(), address);
        assert_eq!(pool.available.capacity(), capacity);
        assert_eq!(pool.tick_count, 0);
        assert_eq!(pool.min_available, 2);
        assert_eq!(pool.maintenance_tick(), 0);
        assert_eq!(pool.maintenance_tick(), 2);
        assert_eq!(drops.get(), 4);
        assert!(pool.is_empty());
    }

    #[test]
    fn owned_objects_move_between_threads_without_requiring_sync() {
        // Only the checked-out object crosses threads; the pool stays local.
        // Cell is Send but not Sync.
        let mut pool = ObjectPool::new(TRIM_AFTER_TICKS);
        pool.put(Cell::new(41));
        let object = pool.try_take().unwrap();
        let (sender, receiver) = mpsc::channel();
        let worker = thread::spawn(move || {
            object.set(object.get() + 1);
            sender.send(object).unwrap();
        });
        pool.put(receiver.recv().unwrap());
        worker.join().unwrap();
        assert_eq!(pool.try_take().unwrap().get(), 42);
    }

    #[test]
    fn local_objects_need_neither_send_nor_sync() {
        let mut pool = ObjectPool::new(NonZeroUsize::new(1).unwrap());
        let object = Rc::new(Cell::new(1));
        pool.put(Rc::clone(&object));
        assert_eq!(finish_group(&mut pool), 0);
        let checked_out = pool.try_take().unwrap();
        checked_out.set(2);
        pool.put(checked_out);
        assert_eq!(pool.maintenance_tick(), 0); // Checkout emptied the pool in this group.
        assert_eq!(pool.maintenance_tick(), 1);
        assert!(pool.is_empty());
        assert_eq!(Rc::strong_count(&object), 1);
        assert_eq!(object.get(), 2);
    }

    #[test]
    fn debug_reports_count_without_requiring_debug_or_revealing_values() {
        struct Opaque;
        let mut pool = ObjectPool::new(TRIM_AFTER_TICKS);
        pool.put(Opaque);
        assert_eq!(format!("{pool:?}"), "ObjectPool { available: 1, .. }");
    }
}
