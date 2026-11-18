/*
 * Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it)
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 */

package io.vertx.spi.cluster.hazelcast;

import com.hazelcast.map.IMap;
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
  private UUID addedListenerId;
  private UUID removedListenerId;
  private UUID expiredListenerId;

  public void joined(HazelcastClusterManager manager) {
    lifecycleListenerId = manager.getHazelcastInstance().getLifecycleService().addLifecycleListener(manager);
    IMap<String, HazelcastNodeInfo> nodeInfo = manager.nodeInfoMap();
    addedListenerId = nodeInfo.addEntryListener((EntryAddedListener<String, HazelcastNodeInfo>) event -> {
      if (!Objects.equals(manager.nodeIdValue(), event.getKey())) {
        manager.executeClusterEvent(() -> manager.handleMemberAdded(event.getKey()));
      }
    }, true);
    removedListenerId = nodeInfo.addEntryListener((EntryRemovedListener<String, HazelcastNodeInfo>) event -> {
      if (!Objects.equals(manager.nodeIdValue(), event.getKey())) {
        removed(manager, event.getKey());
      }
    }, true);
    expiredListenerId = nodeInfo.addEntryListener((EntryExpiredListener<String, HazelcastNodeInfo>) event -> {
      if (!Objects.equals(manager.nodeIdValue(), event.getKey())) {
        removed(manager, event.getKey());
      }
    }, false);
  }

  private void removed(HazelcastClusterManager manager, String nodeId) {
    manager.executeClusterEvent(() -> {
      // A delayed notification must not remove the subscriptions of an already registered replacement.
      if (manager.isActive() && !manager.nodeInfoMap().containsKey(nodeId)) {
        manager.handleMembersRemoved(Collections.singleton(nodeId));
      }
    });
  }

  public List<String> getNodes(HazelcastClusterManager manager) {
    List<String> nodes = new ArrayList<>(manager.nodeInfoMap().keySet());
    Collections.sort(nodes);
    return nodes;
  }

  public void leaving(HazelcastClusterManager manager) {
    if (lifecycleListenerId != null) {
      manager.getHazelcastInstance().getLifecycleService().removeLifecycleListener(lifecycleListenerId);
    }
    IMap<String, HazelcastNodeInfo> nodeInfo = manager.nodeInfoMap();
    if (addedListenerId != null) nodeInfo.removeEntryListener(addedListenerId);
    if (removedListenerId != null) nodeInfo.removeEntryListener(removedListenerId);
    if (expiredListenerId != null) nodeInfo.removeEntryListener(expiredListenerId);
  }

  public void cleanup(HazelcastClusterManager manager) {
    String nodeId = manager.nodeIdValue();
    manager.nodeInfoMap().remove(nodeId);
    manager.cleanSubs(Collections.singleton(nodeId));
  }

}
