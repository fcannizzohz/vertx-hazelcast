/*
 * Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it)
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 */

package io.vertx.spi.cluster.hazelcast;

import com.hazelcast.map.IMap;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastNodeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Shared Vert.x node registry for members and clients.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
final class NodeRegistry {
  private UUID lifecycleListenerId;
  private UUID registryListenerId;

  public void joined(HazelcastClusterManager manager) {
    lifecycleListenerId = manager.getHazelcastInstance().getLifecycleService().addLifecycleListener(manager);
    IMap<String, HazelcastNodeInfo> nodeInfo = manager.nodeInfoMap();
    // One registration preserves Hazelcast's ordering between event types for the same key.
    registryListenerId = nodeInfo.addEntryListener(new RegistryListener(manager), false);
  }

  private static void removed(HazelcastClusterManager manager, String nodeId) {
    if (Objects.equals(manager.nodeIdValue(), nodeId)) return;
    manager.executeClusterEvent(() -> {
      // A delayed notification must not remove the subscriptions of an already registered replacement.
      if (manager.isActive() && !manager.nodeInfoMap().containsKey(nodeId)) {
        manager.handleMembersRemoved(Collections.singleton(nodeId));
      }
    });
  }

  private static class RegistryListener implements EntryAddedListener<String, HazelcastNodeInfo>,
    EntryRemovedListener<String, HazelcastNodeInfo>, EntryExpiredListener<String, HazelcastNodeInfo> {

    private final HazelcastClusterManager manager;

    private RegistryListener(HazelcastClusterManager manager) { this.manager = manager; }

    @Override
    public void entryAdded(EntryEvent<String, HazelcastNodeInfo> event) {
      String nodeId = event.getKey();
      if (!Objects.equals(manager.nodeIdValue(), nodeId)) {
        manager.executeClusterEvent(() -> manager.handleMemberAdded(nodeId));
      }
    }

    @Override
    public void entryRemoved(EntryEvent<String, HazelcastNodeInfo> event) { removed(manager, event.getKey()); }

    @Override
    public void entryExpired(EntryEvent<String, HazelcastNodeInfo> event) { removed(manager, event.getKey()); }
  }

  public List<String> getNodes(HazelcastClusterManager manager) {
    List<String> nodes = new ArrayList<>(manager.nodeInfoMap().keySet());
    Collections.sort(nodes);
    return nodes;
  }

  void memberRemoved(HazelcastClusterManager manager, String nodeId) {
    if (nodeId == null) return;
    manager.executeClusterEvent(() -> {
      // Check when processing the queued event: a supplied member may have restarted with the same node id.
      boolean replaced = manager.getHazelcastInstance().getCluster().getMembers().stream()
        .anyMatch(member -> nodeId.equals(member.getAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE)));
      if (!replaced) manager.nodeInfoMap().remove(nodeId);
    });
  }

  public void leaving(HazelcastClusterManager manager) {
    UUID lifecycleId = lifecycleListenerId;
    UUID registryId = registryListenerId;
    lifecycleListenerId = null;
    registryListenerId = null;
    removeListeners(
      () -> { if (lifecycleId != null) manager.getHazelcastInstance().getLifecycleService().removeLifecycleListener(lifecycleId); },
      () -> { if (registryId != null) manager.nodeInfoMap().removeEntryListener(registryId); });
  }

  static void removeListeners(Runnable... removals) {
    Throwable failure = null;
    for (Runnable removal : removals) {
      try {
        removal.run();
      } catch (RuntimeException | Error problem) {
        if (failure == null) failure = problem;
        else if (failure != problem) failure.addSuppressed(problem);
      }
    }
    if (failure instanceof RuntimeException) throw (RuntimeException) failure;
    if (failure != null) throw (Error) failure;
  }

  public void cleanup(HazelcastClusterManager manager) {
    String nodeId = manager.nodeIdValue();
    manager.nodeInfoMap().remove(nodeId);
    manager.cleanSubs(Collections.singleton(nodeId));
  }

}
