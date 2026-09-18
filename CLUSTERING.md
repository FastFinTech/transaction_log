# Clustering and read replicas

This document records the agreed initial deployment and recovery design.
[Validated configuration](services/transaction-log/src/clustering/configuration/README.md)
and [raw inputs](services/transaction-log/src/configuration/README.md)
are implemented, including startup loading and static validation. Startup prints the version and exits without accessing storage. Setup, networking, cluster enrollment,
replication, command-handler connections and the lifecycle below remain planned.
The [main README](README.md) tracks overall progress.

## Permanent mode and fixed membership

A database is initialized as either single or cluster. Its persisted mode is
permanent: standalone storage cannot later become clustered, and clustered storage
cannot be opened standalone. Configuration changes and process restarts do not
change initialized state. Local mode/node/domain metadata and no-overwrite
publication are implemented in the [storage module](services/transaction-log/src/storage/README.md#clustering-configuration-persistence).
Cluster enrollment and complete cross-platform durable publication remain to be specified.
The same permanence applies to `--cluster-mode`; future startup must reject disagreement with stored configuration.

Each cluster has exactly one permanent master and two permanent replicas:
`master`, `replica-1`, `replica-2`. There is no replica count setting, arbitrary
member list, live enrollment, election, promotion or automatic master failover.
The local node name (`--cluster-my-node-name`) is permanent for initialized storage.
Restarting or editing configuration must not change it; startup must reject a
configured slot that differs from persisted local configuration. This startup check remains planned.
If the master fails, commands remain unavailable until the same logical master
recovers. Deployment may replace its process while preserving storage and identity.

The planned master generates a cluster UUID at first establishment; ordinary
restarts retain it. `EstablishedClusteringConfiguration` represents that UUID
with deployment configuration in either mode, and the storage provider saves and
restores it directly. UUID establishment and startup use remain unimplemented.
The configured domain is a routing namespace, not a cluster ID.
Persisted member/storage identities must prevent replacement or empty storage from
silently substituting for an initialized member. Exactly two processes claiming
replica slots do not prove a safe recovery. Enrollment, storage
identities and durable-history reconciliation remain to be specified.

## Minimal configuration and derived DNS

Single mode needs only `TL_CLUSTER_MODE=single`. A cluster member supplies:

```text
TL_CLUSTER_MODE=cluster
TL_CLUSTER_MY_NODE_NAME=replica-1
TL_CLUSTER_DOMAIN=payments.example.com
```

The application derives `master.payments.example.com`,
`replica-1.payments.example.com` and `replica-2.payments.example.com`.
Deployment owns DNS/routing those names to the intended instances. No individual
hostname mappings are configured. All machines use the same domain and differ
only in their explicit local node slot. Domain case/whitespace normalize; an
optional trailing root dot is retained. Derived names must fit hostname limits.
The configuration specifications own parsing and error contracts.

Replicas initiate clustering connections to the master. Cluster, command-handler
and read traffic use fixed separate ports; numbers and listener wiring are not
selected yet. Runtime handshakes must verify expected peer slots and reject
duplicate active slots. Different DNS names may lead to the same process; DNS
is neither authentication nor storage identity. Shared agreement compares validated
cluster domains, excluding local slots. Membership stores only the local slot and
domain; the three node hostnames are derived when needed.

Local debugging can use three containers with network aliases, or separate native
processes with local address mappings and listeners bound to distinct addresses.
Each node needs separate storage. Development networking and debugger timeout
behavior are not yet implemented. In Kubernetes, peer names must be available
before application readiness to avoid a startup DNS/readiness cycle; address
publication and pod startup ordering belong to deployment design.

## Coordinated operating sessions

During startup, accept clustering connections and perform recovery/resynchronization.
For fresh storage, first validate inputs and inspect the root without modifying it.
Only after all members establish the cluster and agree may startup publish permanent
local configuration. Failed first connection/settings attempts must leave fresh storage
unassigned so settings can be corrected. The executable currently performs only the
read-only cluster check; establishment and delayed publication remain planned.
Do not admit command handlers until all three fixed members are connected and
their durable histories reconciled. Open sockets alone are insufficient readiness.
Recovery must preserve acknowledged data even if the master lacks records retained
by replicas; a restart cannot simply declare the master's log authoritative.

Once serving, membership is frozen. Losing either replica ends the cluster session:
the master stops command admission, invalidates handler connections and exits.
There is no degraded operation, runtime rejoin or replacement connection. Master
failure also ends command service. Restart/resynchronization is a coordinated
cluster operation, cascading to command-handler restart/reconnection. Replicas must
not continue exposing a stale session as ready. A master process restart alone is
not proof that all members have entered a fresh, reconciled session.

Deployment restarts failed processes, and every new session passes the full startup
gate. This intentionally favors a simpler consistent recovery boundary over
continuous availability and avoids potentially terabyte-scale catch-up during
normal operation. Failure detection, timeouts, coordination, shutdown sequencing
and readiness publication remain unspecified. No runtime enforcement exists yet.

An interrupted command has an unknown outcome, not proof of failure: committed
data may outlive a lost acknowledgement. Stable command identities, outcome
resolution and duplicate prevention must make handler recovery safe. Restarting
processes alone does not supply those guarantees.

Reliable operation for months and 99.99% availability are targets, not established
results. Full-session recovery duration and interruption frequency must be measured;
there are no cluster performance measurements or availability guarantees yet.

## Message transport

The [message I/O crate](lib/message-io/README.md) implements async
`read_message::<T>()` and `write_message(&value)` for Tokio byte sources/destinations
and Serde types. A four-byte little-endian body length precedes one Postcard value,
bounded to 1 MiB. The crate owns framing, EOF, completion and cancellation contracts
and is intended for intermittent non-hot-path use.

Application-version agreement precedes the clustering handshake. Application
schemas, cluster/session identity checks and connection integration remain planned;
generic message I/O adds no type identifier or version negotiation. Bulk record
replication has not been assigned this encoding.

## Kubernetes deployment responsibility

Exclusive execution of the logical master is required. The intended deployment
uses a StatefulSet with one master pod and a persistent volume using
`ReadWriteOncePod` with a supported CSI driver. StatefulSets provide at-most-one
semantics for a pod identity; `ReadWriteOncePod` constrains access to one pod.
Ordinary `ReadWriteOnce` constrains access to one node and can allow several pods
on that node to access the same volume.

Forced replacement requires confirming that the old master stopped. Force-deleting
a StatefulSet pod can violate at-most-one semantics if the old process is running.
The design relies on deployment enforcement, without master election or distributed
locking. Kubernetes manifests and operational procedures are not implemented.

References: [StatefulSet force deletion](https://kubernetes.io/docs/tasks/run-application/force-delete-stateful-set-pod/)
and [volume access modes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes).

## Open durability and read contracts

- Write acknowledgement: receipt, append, flush and durable synchronization differ.
- Startup recovery, commitment evidence and reconciliation of divergent histories.
- Failure detection and whether lag/persistence errors end a session.
- In-flight outcomes, replay, duplicate prevention and handler recovery.
- Read freshness and admission during startup or after session loss.
- Safe replacement of failed storage without discarding acknowledged data.

These require further design before runtime implementation.
