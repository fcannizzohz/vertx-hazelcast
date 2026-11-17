<!-- Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it). -->

# Open issues

## P1: Dispatch registry callbacks through the cluster event executor

Location: `vertx-hazelcast/src/main/java/io/vertx/spi/cluster/hazelcast/NodeRegistry.java`, registry listener callbacks.

Registry callbacks currently invoke removal processing directly on Hazelcast event threads. If another node departs while this manager is leaving, a callback can pass the `active` check and continue after `leave()` finishes. Removing the listener does not wait for an executing callback, and shutdown only waits for `eventExecutor`.

`handleMembersRemoved()` can therefore republish this manager's node information after cleanup has removed it, leaving a ghost node after a clean shutdown. This is particularly relevant when using an externally owned Hazelcast member.

Required fix: dispatch registry callbacks through `executeClusterEvent()` and ensure shutdown waits for in-flight processing before removing this manager's registration.

Regression coverage: hold a removal callback in flight while the manager leaves, then release it and verify that shutdown completes without recreating the departed manager's registry entry.

## P2: Preserve replacement registrations when processing delayed removal notifications

Location: `vertx-hazelcast/src/main/java/io/vertx/spi/cluster/hazelcast/NodeRegistry.java`, removal listener callback, and `HazelcastClusterManager.handleMembersRemoved()`.

Closing and recreating Vert.x with the same externally owned Hazelcast member reuses its node ID. A delayed removal callback for the previous registration calls `handleMembersRemoved()`, which unconditionally calls `cleanNodeInfos()` after scanning subscriptions. If the replacement publishes its registration during that interval, the old callback deletes the new entry.

The replacement ignores removal events for its own ID, so it remains absent from node discovery.

Required fix: registry removal notifications must not unconditionally remove the key again after Hazelcast has already removed the original registration.

Regression coverage: delay removal processing across a restart using the same node ID, publish the replacement registration, then resume the old callback and verify the replacement remains discoverable.

## Review validation notes

The reviewer reported reproducing both interleavings using proxy-backed Hazelcast objects. Test compilation succeeded during review, but socket-based tests could not run because the review sandbox prohibited socket binding. Existing tests do not cover these interleavings.
