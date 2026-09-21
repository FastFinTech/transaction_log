# Object pool

Reuse owned objects, such as byte buffers, under one owner's mutable access.
The pool tracks demand and lets the owner periodically discard surplus objects.
It is synchronous, has no internal locking, and starts no background work.

## Types and modules

| Type | Responsibility |
| --- | --- |
| [`ObjectPool<T>`] | Own available objects, transfer them to callers for reuse, and reclaim observed surplus. |

## Usage

Take an available object or create one on a miss. Reset it as needed, then return
it explicitly when its work is complete:

```rust
use object_pool::ObjectPool;

let mut pool = ObjectPool::new();
let mut buffer = pool.take_or_else(|| Vec::<u8>::with_capacity(4096));
buffer.extend_from_slice(b"serialized bytes");

// After the consumer has finished with the bytes, return the empty allocation.
buffer.clear();
pool.put(buffer);
assert_eq!(pool.len(), 1);
```

The owner separately calls [`reclaim_unused()`](ObjectPool::reclaim_unused) at
its chosen maintenance interval. See [reclamation](#reclamation) for the rule
used to decide how many objects to discard.

<details>
<summary>Design and maintenance notes</summary>

An owned object can move to another thread when `T: Send`. The pool stays with
its owner; an application channel can return the object after work completes.
For example:

```rust
use object_pool::ObjectPool;
use std::sync::mpsc;

let mut pool = ObjectPool::new();
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
assert_eq!(pool.reclaim_unused(), 0);
```

</details>

## Behavior and guarantees

### Ownership and reuse

Checkout, return, and reclamation require `&mut self`. Checkout transfers the
actual `T`; it can outlive the pool, and dropping it does not return it.
The caller owns resetting objects before returning or reusing them.

| Operation | Contract |
| --- | --- |
| [`new()`](ObjectPool::new) / `Default` | Create an empty pool without allocating. `T: Default` is unnecessary. |
| [`try_take()`](ObjectPool::try_take) | Take the most recently returned object, or return `None` immediately. |
| [`take_or_else(create)`](ObjectPool::take_or_else) | Take an object, or call the supplied factory exactly once on a miss. |
| [`put(object)`](ObjectPool::put) | Accept an existing or newly created object, preserving its contents and capacity. |
| [`len()`](ObjectPool::len) / [`is_empty()`](ObjectPool::is_empty) | Count available objects only; checked-out objects are excluded. |
| [`reclaim_unused()`](ObjectPool::reclaim_unused) | Discard the observed surplus, return the number discarded, and begin a new observation window. |

The pool has no object or byte limit and cannot reclaim checked-out objects.
It imposes no `Send` or `Sync` bounds on `T`; local values such as `Rc<Cell<_>>`
are supported. Ordinary Rust auto traits govern moving ownership between threads.

### Reclamation

Each maintenance window runs from construction or the previous reclamation call.
`reclaim_unused()` discards the **lowest available count observed during that
window**, including brief dips between calls. With 120 objects currently
available and a window minimum of 80, it discards 80 and retains 40.

Every call starts a new window at the retained count. The first call discards
nothing because construction starts empty. Any later window that reaches zero
also discards nothing. If no available objects are needed during a window,
reclamation can empty the pool. Checked-out objects remain unaffected.

The owner chooses the cadence, for example once every ten minutes. Time passing
alone causes no reclamation or reset. After a delayed timer, call once and begin
the next observation window; repeated catch-up calls could immediately discard
the objects just retained.

### Failures and memory release

Factories and destructors run synchronously, and their panics propagate. If a
factory panic is caught, the empty pool remains usable. If a reclamation
destructor panics and its unwind is caught, the retained prefix remains available
and tracking continues in the new window. Allocation failure follows Rust's
normal allocation behavior.

Reclamation drops surplus objects but retains the pool's own vector capacity for
reuse. Releasing object allocations to the allocator does not promise an
immediate reduction in process memory usage.

<details>
<summary>Design and maintenance notes</summary>

**Availability tracking.** Objects live in a plain `Vec<T>` alongside the minimum
available count. Every `try_take()` updates the minimum after popping, and
`take_or_else()` uses that same path. Returning an object only increases
availability, so `put()` leaves the earlier minimum intact. Incrementing the
minimum on return would erase a temporary dip. `Vec::len()` already supplies the
current count; a duplicate counter is unnecessary.

**Reclamation and panic safety.** Each reclamation call:

1. Saves the window's minimum as the number of objects to discard.
2. Computes `retain = available.len() - remove`. Checkout tracks every decrease,
   and returns only increase availability, so the subtraction is valid.
3. Resets the minimum to `retain`, starting a fresh window even when nothing is
   discarded.
4. Truncates the vector to `retain` and returns `remove`.

Resetting before truncation is essential: if a destructor panics, the vector
already has its retained length and the minimum describes the new window.
Catching the unwind must not reuse the previous window's minimum. Factories and
destructors execute under the owner's exclusive access and cannot reenter the
same pool through safe mutable access.

**Window boundaries.** Windows are consecutive and non-overlapping. The starting
count contributes to the minimum; this is not a rolling time window or a periodic
sample. Delayed calls extend the window, rapid calls shorten it, and every
checkout still contributes. The owner manages the timer's lifetime; there are
no intermediate observation calls or interval settings inside the pool.

Construction's zero minimum allows an initial accumulation period. If 20 objects
have accumulated by the first maintenance call, that call discards none and
starts the next window at 20. Likewise, a later zero minimum resets to the
ending available count. A zero return means the completed window reached zero
availability; it does not prevent reclamation in the next window.

```rust
use object_pool::ObjectPool;

let mut pool = ObjectPool::new();
for value in 0..4 {
    pool.put(value);
}
assert_eq!(pool.reclaim_unused(), 0); // Initial window started empty; next starts at 4.

let object = pool.try_take().unwrap(); // Available count falls to 3.
pool.put(object);                     // Returning it preserves that low point.
assert_eq!(pool.reclaim_unused(), 3); // Discard 3 of the 4 available objects.
assert_eq!(pool.len(), 1);

// The next window starts at the retained count, not at the previous minimum.
assert_eq!(pool.reclaim_unused(), 1);
assert!(pool.is_empty());
```

**Object selection and storage.** The policy measures spare object count rather
than individual idle durations or payload sizes. Checkout pops from the end;
reclamation retains the vector's beginning and destroys the surplus tail in
place, favoring older returns for retention. Object identity does not affect
eligibility, and the retained count is not a growth limit.

`Vec::truncate` preserves collection capacity even after removing every object.
Spare slots contain no initialized objects or payload allocations. This storage
is reused on refill and released when the pool is dropped. No scratch vector,
handle copying, or `shrink_to_fit` is needed.

</details>

## Performance

Checkout performs a vector pop and a minimum update without allocation. Return
performs a vector push and may grow collection storage. A factory invoked on a
miss may allocate. Reclamation does constant bookkeeping plus the cost of
dropping the surplus objects.

**No pool throughput benchmarks have been run.** These costs describe the
implementation; the existing record I/O measurements do not measure this crate.

<details>
<summary>Design and maintenance notes</summary>

Updating the minimum on every checkout preserves brief demand spikes that
intermittent snapshots would miss. Tracking storage remains constant regardless
of window duration or pool lifetime. There are no clocks, history allocations,
internal locks, runtime borrow checks, synchronization, or historical scans.

LIFO reuse favors recently returned allocations. Accessing a checked-out object
involves no pool call, wrapper, availability check, or reference-count update.
The `take_or_else` factory is an inline, generic `FnOnce`: it can consume captures
and is neither boxed nor stored, requiring no dynamic dispatch.

Pool operations belong at buffer handoffs rather than on every record or
serializer field. Destructor cost belongs to the owner's maintenance work.
Neither standalone pool nor integrated writer throughput has been measured;
measure realistic handoffs and reclamation before claiming an end-to-end gain.

</details>

## Validation

From the repository root:

```sh
cargo test -p object-pool --locked
cargo test -p object-pool --release --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo rustdoc -p object-pool --locked -- -D warnings
```

The test commands run unit tests and all three README examples, including those
inside expandable notes. When changing this document's layout, also check the
generated `target/doc/object_pool/index.html` for working links, readable tables,
and notes that expand correctly.

<details>
<summary>Design and maintenance notes</summary>

Tests live beside `ObjectPool`. Preserve coverage of unrestricted growth, LIFO
ownership transfer, factory invocation and panic recovery, real `BytesMut`
allocation reuse, caller-controlled reset, checked-out lifetimes, cross-thread
returns to the local owner, and local non-Send objects. Default construction
must support types without a `Default` implementation.

Reclamation tests cover the initial empty window, minima at different points,
temporary zero and nonzero dips between calls, returns preserving minima,
recovery after zero and positive reclamation, prefix retention, exact release
counts, and resetting before destructor panics. They also cover vector capacity
preservation through partial, complete, and no-op reclamation and refilling.
No clocks, sleeps, or long workloads are needed.

Keep API docs, this README, and tests synchronized with continuous availability
tracking, owner-defined windows, ownership contracts, and the boundary between
implemented behavior and future integration. Preserve the Rustdoc inclusion and
executable examples when changing the layout.

</details>
