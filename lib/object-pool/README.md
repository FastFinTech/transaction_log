# Object pool

This crate provides the workspace's generic, synchronous [`ObjectPool<T>`]. A
single owner reuses whole objects without internal locking, tracks demand on
checkout, and periodically reclaims surplus through
[`maintenance_tick()`](ObjectPool::maintenance_tick).
This README covers the thin `src/lib.rs` entry point and the implementation and
same-file tests in `src/object_pool.rs`. It is included in generated Rustdoc.

## Purpose and ownership

The immediate use case is an executor's local pool of `BytesMut` allocations for
a record writer. The pool is independent of bytes, record framing, Tokio, sockets,
and acknowledgement policy. It has no runtime dependencies and forbids unsafe
code. `bytes` is a development dependency for allocation-reuse tests.

[`ObjectPool::new(trim_after_ticks: NonZeroUsize)`](ObjectPool::new) creates an
empty pool without allocating object or collection storage. `trim_after_ticks`
configures how many calls to `maintenance_tick()` complete one maintenance group.
Zero is excluded by the type; there is no default threshold. The owner chooses
the maintenance cadence.

Checkout, return, and maintenance require `&mut self`. Objects live in a plain
`Vec<T>`, alongside the minimum available count and maintenance counters. There
are no internal locks, runtime borrow checks, clocks, or history allocations.
The pool imposes no `Send` or `Sync` bound on `T`; it can reuse `Rc<Cell<_>>` locally.
Rust's normal auto-trait rules determine whether ownership of the pool or its
checked-out objects may move to another thread.

`try_take()` pops the most recently returned object or returns `None`.
`take_or_else(create)` returns one or invokes its ordinary `FnOnce` factory
synchronously on a miss. The factory may consume captures and is neither boxed
nor stored. Checkout transfers the actual `T`, without a wrapper or reference
into the pool. The object can outlive the pool and is not automatically returned
when dropped.

`put(object)` accepts checked-out or newly created objects, preserving their
contents and internal capacity. The caller resets each object as appropriate,
for example with `BytesMut::clear()`, before returning or using it again.
There is no maximum object count, byte count, or checkout count. A miss may create
another object; a return grows collection storage as needed. Allocation failure
follows Rust's normal allocation behavior, not a pool-exhaustion error.

`len()` and `is_empty()` describe available objects only. They exclude objects
held by writers, output queues, drivers, and return jobs. The pool cannot reclaim
checked-out objects.

## Availability tracking and trimming

The pool tracks the lowest available count throughout each maintenance group,
including activity between ticks. Ticks advance the maintenance schedule; they
do not sample availability. Every `try_take()` updates the minimum after popping;
`take_or_else()` uses that same path. Returning an object only increases
availability, so `put()` leaves the earlier minimum intact. A checkout followed
by a return before the next tick must still affect the group's minimum.

The minimum is historical state, not a current-availability counter. Incrementing
it on return would erase an earlier dip. `Vec::len()` already exposes the vector's
stored count, so no duplicate current-availability field is needed.

`maintenance_tick()` increments the tick count. Before the configured threshold,
it returns zero and removes nothing. At the threshold it:

1. Saves the group's minimum as the number of objects to discard.
2. Computes `retain = available.len() - remove`. This subtraction is valid because
   checkout tracks every decrease, while returns can only increase availability.
3. Resets the tick count to zero and the minimum to `retain`, starting a new group.
4. Truncates the object vector to `retain` and returns `remove`.

The minimum is the number to discard: a minimum of 80 with 120 currently available
means discarding 80 and retaining 40. Reset occurs after every complete group,
even when its minimum was zero and no objects were removed. The next group's
minimum starts at the retained count, and object activity immediately begins
contributing; tracking does not wait for the next tick.

Construction starts with a minimum of zero because the pool is empty. The first
complete group therefore reclaims nothing. If 20 objects have accumulated by its
end, the next group begins at 20. This provides an initial accumulation period
without making the minimum permanently zero. Any later group that reaches zero
also reclaims nothing, then resets to its ending available count.

Groups are consecutive and do not overlap; this is not a rolling time window.
The starting count contributes to each group's minimum. After trimming empties
the pool, a subsequent group also starts at zero and allows returned objects to
accumulate before they can be reclaimed in a later group. A return value of zero
means no objects were discarded; it does not distinguish an unfinished group
from a completed group whose minimum was zero.

```rust
use object_pool::ObjectPool;
use std::num::NonZeroUsize;

let mut pool = ObjectPool::new(NonZeroUsize::new(2).unwrap());
for value in 0..4 {
    pool.put(value);
}
assert_eq!(pool.maintenance_tick(), 0);
assert_eq!(pool.maintenance_tick(), 0); // Initial group started empty; next starts at 4.

let object = pool.try_take().unwrap(); // Available count falls to 3.
pool.put(object);                     // Returning it preserves that low point.
assert_eq!(pool.maintenance_tick(), 0);
assert_eq!(pool.maintenance_tick(), 3); // Discard 3 of the 4 available objects.
assert_eq!(pool.len(), 1);

// The next group starts at the retained count, not at the previous minimum.
assert_eq!(pool.maintenance_tick(), 0);
assert_eq!(pool.maintenance_tick(), 1);
assert!(pool.is_empty());
```

The policy measures spare object count, not each object's idle duration or its
separately allocated payload size. Normal checkout pops from the end. Trimming
retains the beginning of the vector and destroys the surplus tail in place,
favoring older returns for retention. Object identity does not affect eligibility.

`Vec::truncate` keeps the available vector's allocation and capacity even when
all objects are removed. Spare slots hold no initialized objects or payload
allocations. This handle storage is kept for refilling and released when the pool
is dropped. No scratch vector, handle copying, or `shrink_to_fit` is needed.
The retained count is not a growth limit. Reclaimed object allocations return
to the allocator; this does not promise an immediate reduction in process RSS.

Factories and destructors run synchronously under the caller's exclusive access.
They cannot reenter the same pool through safe mutable access. Panics propagate.
If the caller catches a factory panic, the empty pool remains usable. If a trimmed
object's destructor panics, the vector already has its retained length, and the
counters and minimum already describe the new group. This ordering must be
preserved so catching the unwind does not reuse the previous group's minimum.

## Maintenance ownership and writer integration

The pool starts no timer or task. The owner should call `maintenance_tick()` on a
regular schedule and manage the timer's lifetime. For example,
`trim_after_ticks = NonZeroUsize::new(600).unwrap()` with one tick per second gives
approximately ten minutes between trimming decisions. A threshold of one
considers trimming at every call, using the minimum since construction or the
previous call.

Only calls advance the maintenance schedule. Delayed calls extend a group; rapid
calls shorten it. Time passing does not clear tracking state or cause trimming.
All availability changes still contribute during a delay. The caller must not
assume the pool enforces a wall-clock interval.

Writer integration and the owner timer remain planned. The intended arrangement
is one writer and one local pool per command-handler executor, with a socket
driver on another thread. The writer holds an eagerly acquired buffer. Submission
acquires a replacement before moving the whole completed buffer through an
unbounded SPSC output queue; no per-record split is needed.

After output finishes using a buffer, the driver can return ownership through the
executor's existing jobs MPSC. The executor clears the buffer and calls `put`.
Application job types, return callbacks, queues, error/drop handling, and timing
belong to the integration layer. A writer's own buffer and any queued buffers are
checked out and cannot be trimmed. Acknowledgement latency, input backpressure,
and connection policy remain concerns of the command handler and connection owner.

```rust
use object_pool::ObjectPool;
use std::{num::NonZeroUsize, sync::mpsc};

let mut pool = ObjectPool::new(NonZeroUsize::new(600).unwrap());
let mut buffer = pool.take_or_else(|| Vec::<u8>::with_capacity(4096));
buffer.extend_from_slice(b"serialized bytes");

let (sender, receiver) = mpsc::channel();
let worker = std::thread::spawn(move || {
    assert_eq!(buffer.as_slice(), b"serialized bytes");
    // After output finishes using all bytes, send ownership back to the owner.
    sender.send(buffer).unwrap();
});
let mut buffer = receiver.recv().unwrap();
buffer.clear();
pool.put(buffer);
worker.join().unwrap();
assert_eq!(pool.len(), 1);
assert_eq!(pool.maintenance_tick(), 0);
```

## Hot-path rationale and evidence

Checkout performs a vector pop followed by a minimum update using the vector's
stored length. Return performs a push. Neither operation reads a clock,
allocates tracking storage, synchronizes, or updates the maintenance tick counter.
Checkout itself does not allocate; a `take_or_else()` factory may allocate on a
miss, and `put()` may grow the vector when its existing capacity is insufficient.
Updating the minimum on checkout is deliberate: intermittent snapshots would
miss brief demand spikes and could reclaim capacity the executor actually uses.

LIFO reuse favors recently returned allocations. Accessing a checked-out object
involves no pool call, wrapper, availability check, or reference-count update by
the pool. `take_or_else` is generic and inline, requiring no dynamic dispatch.
Pool operations belong at buffer handoffs, not on every record or serializer field.

Tracking storage is constant regardless of the tick threshold or lifetime. A tick
before the threshold does constant work. A completed group additionally destroys
its surplus objects, so destructor cost belongs to executor maintenance. No scan
of historical samples or temporary allocation is needed.

These are implementation properties, not measured throughput claims. No pool or
integrated writer throughput benchmark has been run. Existing reader benchmarks
do not measure this crate. Measure realistic buffer handoffs and reclamation
before asserting an end-to-end throughput improvement.

## Verification and maintenance

Tests live beside `ObjectPool`. They cover unrestricted growth, LIFO ownership
transfer, factory invocation and panic recovery, real `BytesMut` allocation reuse,
caller-controlled reset, checked-out lifetimes, and cross-thread returns to the
local owner. Local non-Send objects are supported too.

Maintenance tests cover the initial empty group, exact tick thresholds, minima
at different points in a group, temporary zero and nonzero dips between ticks,
returns preserving minima, recovery after zero and positive trims, thresholds
of one and `usize::MAX`, prefix retention, exact release counts, vector capacity
preservation through partial/complete/no-op trims and refilling, and resetting
the group before destructor panics. No clocks, sleeps, or long workloads are
needed. Both examples above are compilable documentation tests.

Run `cargo test -p object-pool --locked` and its `--release` variant, formatting,
workspace Clippy, and Rustdoc with warnings denied. Keep API docs and this README
synchronized with the continuous availability tracking, tick-count policy,
ownership contracts, and implemented/planned integration boundary.
