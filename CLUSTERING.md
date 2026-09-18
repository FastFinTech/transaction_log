# Clustering and read replicas

This document records the initial deployment and cluster lifecycle design.
In-memory deployment configuration and static membership validation are implemented
in [clustering::configuration](services/transaction-log/src/clustering/configuration/README.md).
Replication and command-handler connections are planned; the service
does not implement the lifecycle rules yet. The [main README](README.md) tracks overall
implementation progress.

## Permanent deployment mode

A database is initialized as either a singleton or a member of a cluster. Its
mode is permanent for that initialized database: singleton storage cannot later
be opened in clustered mode, and clustered storage cannot be opened in singleton
mode. Restarting a process or editing its configuration does not change the mode.

"Initialized" describes this boundary more precisely than "started": the choice
belongs to persisted database state, rather than an individual process lifetime.
The initialization format and crash-safe publication procedure remain to be
specified.

## Fixed membership and master

Cluster membership is explicitly defined by configuration. The master is a
permanent role; there is no master election, replica promotion or automatic
failover. If the master fails, command processing is unavailable until that same
logical master restarts. A replacement Kubernetes pod can run the same logical
master using its persisted storage and identity.

The master generates a cluster ID during first initialization and persists it.
Ordinary restarts retain that ID. The ID distinguishes this initialized cluster
from another cluster, including a newly initialized master that happens to use
the same network address. Replica connections must belong to that cluster and
match configured member identities. Multiple connections from one replica must
not count as multiple replicas.

Configured member identities use the implemented `MemberId` value. Its parsing,
name constraints, trimming and case-sensitive comparison rules are specified in
the [member identity documentation](services/transaction-log/src/clustering/configuration/README.md#member-identity-and-parsing).
Member names identify roles within the configuration; the persisted cluster ID
identifies the initialized cluster itself.

A cluster ID does not establish that a replica's data is current. An old replica
from the same cluster still needs its state checked and any required catch-up.
The cluster-ID format, initial replica enrollment and handshake remain to be
specified.

## Configured hostnames and fixed ports

Network locations remain separate from logical member identity. The agreed
direction is configured DNS hostnames and fixed service ports, with replicas
initiating connections to the permanent master. This needs resolution of
configured names, rather than discovery that changes or enrolls cluster members.
DNS can locate replacement pods without changing their logical identities.
Cluster traffic, command-handler traffic and read traffic use separate ports;
the actual port numbers have not been selected.

The standalone `Hostname` type implements parsing and validation, documented in
the [hostname specification](services/transaction-log/src/clustering/configuration/README.md#hostname-parsing).
It performs no DNS lookup. `MemberId` still represents only a member name.
`Member` combines a validated `MemberId` and `Hostname`. `ClusterDefinition`
owns the shared master and replica members; `ClusterMembership` pairs that
definition with a local member ID selecting this process's role. Conflict checks
compare IDs independently of hostnames.
Parsed hostnames must also be unique across the master and replicas because
service ports are fixed. Hostname parsing trims and lowercases input before these
comparisons. This catches repeated configured names without resolving DNS aliases
or proving distinct network addresses.
Fixed-port endpoint wiring and connection setup remain planned.

The implemented definition agreement check compares master and replica IDs and
hostnames, with replica input order ignored. Replicas are stored in canonical
member-ID order. Typed mismatches identify a different master, changed member
hostname, missing replica or unexpected replica. Local IDs are validated against
the definition but intentionally excluded from shared agreement. Handshake
serialization, cluster-ID verification and active duplicate-ID detection remain
unimplemented.

## Startup and loss of replicas

During clustered startup, accept only application cluster connections. Do not
accept command-handler connections until the entire configured cluster is
connected. The precise recovery and readiness boundary for opening those
connections remains to be defined; an open socket alone does not prove that a
replica is ready to participate in replication.

Once serving commands, the master must exit if fewer than two replicas remain
attached, regardless of the reason for their loss. Kubernetes is responsible for
restarting the process. This deliberately avoids an application state that keeps
the master alive while waiting to resume command processing. Admission must stop
when the failure condition is detected; shutdown must not keep accepting commands.

Every restart goes through the full-cluster startup gate again. With more than
two configured replicas, ongoing operation can tolerate a missing member while
at least two remain, but startup cannot. A prolonged outage can therefore cause
repeated restarts or leave a restarted master waiting for the missing members.
This favors the required replication conditions over command availability.

Detection of replica loss, whether lag or persistence failure makes an attached
replica ineligible, and handling of commands already in flight remain open.
Process exit does not imply that an interrupted command accepted no data or can
be safely replayed.

## Kubernetes deployment responsibility

Exclusive execution of the logical master is a deployment requirement. The
intended Kubernetes deployment uses a StatefulSet with one master pod and a
persistent volume using `ReadWriteOncePod` with a supported CSI driver.
StatefulSets provide at-most-one semantics for a pod identity; `ReadWriteOncePod`
constrains volume access to one pod. Ordinary `ReadWriteOnce` constrains access to
one node and can permit multiple pods on that node to access the volume.

Forced replacement requires confirming that the old master has stopped.
Force-deleting a StatefulSet pod can violate its at-most-one semantics if the
old process is still running. The application design relies on this deployment
contract rather than adding master election or distributed locking.

References: [StatefulSet force deletion](https://kubernetes.io/docs/tasks/run-application/force-delete-stateful-set-pod/)
and [persistent volume access modes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes).

Kubernetes manifests and operational procedures have not yet been implemented.

## Open replication and read contracts

The lifecycle choices above do not yet define:

- What a replica must acknowledge before a command succeeds: received bytes,
  appended records, flushed data and durable synchronization are distinct events.
- Recovery, catch-up and the exact conditions under which a replica counts toward
  the minimum of two.
- In-flight command outcomes, client retry rules and duplicate prevention after
  a disconnect or master restart.
- Read-replica freshness guarantees, including whether disconnected or lagging
  replicas may continue serving reads.
- How a failed member can be replaced without admitting an unintended replica.

These require further design discussion before implementation. There are no
cluster performance measurements or availability guarantees yet.
