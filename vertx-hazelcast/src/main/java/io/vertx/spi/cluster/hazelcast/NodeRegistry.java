/*
 * Copyright (c) 2011-2026 The original author or authors
 * Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it)
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.spi.cluster.hazelcast;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.map.listener.EntryMergedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastNodeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared Vert.x node registry for members and clients.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
final class NodeRegistry {
  private static final String LEFT_MEMBER_PREFIX = "left-member:";
  private UUID lifecycleListenerId;
  private UUID registryListenerId;
  private UUID departureListenerId;
  private IMap<String, String> clusterInfo;
  private volatile List<String> nodes = Collections.emptyList();
  // Retain member identities so delayed registry/merge events cannot misclassify them as clients.
  private final Set<String> memberNodeIds = ConcurrentHashMap.newKeySet();
  private final Map<String, UUID> currentMembers = new ConcurrentHashMap<>();
  private final Set<UUID> inactiveMembers = ConcurrentHashMap.newKeySet();

  public void joined(HazelcastClusterManager manager) {
    memberNodeIds.clear();
    currentMembers.clear();
    inactiveMembers.clear();
    nodes = Collections.emptyList();
    clusterInfo = manager.getHazelcastInstance().getMap("__vertx.clusterInfo");
    lifecycleListenerId = manager.getHazelcastInstance().getLifecycleService().addLifecycleListener(manager);
    IMap<String, HazelcastNodeInfo> nodeInfo = manager.nodeInfoMap();
    // One registration preserves Hazelcast's ordering between event types for the same key.
    registryListenerId = nodeInfo.addEntryListener(new RegistryListener(manager), false);
    departureListenerId = clusterInfo.addEntryListener(new DepartureListener(manager), true);
    // Join runs on a worker. Serialize the initial snapshot with events registered before the scan.
    manager.runClusterEvent(() -> refreshNodes(manager));
  }

  private void removed(HazelcastClusterManager manager, String nodeId) {
    if (Objects.equals(manager.nodeIdValue(), nodeId)) return;
    manager.executeClusterEvent(() -> {
      // A delayed notification must not remove the subscriptions of an already registered replacement.
      if (manager.isActive() && !memberNodeIds.contains(nodeId) && currentMember(manager, nodeId) == null
        && !manager.nodeInfoMap().containsKey(nodeId)) {
        removeNode(nodeId);
        manager.handleMembersRemoved(Collections.singleton(nodeId));
      }
    });
  }

  private class RegistryListener implements EntryAddedListener<String, HazelcastNodeInfo>,
    EntryRemovedListener<String, HazelcastNodeInfo>, EntryExpiredListener<String, HazelcastNodeInfo>,
    EntryMergedListener<String, HazelcastNodeInfo> {

    private final HazelcastClusterManager manager;

    private RegistryListener(HazelcastClusterManager manager) { this.manager = manager; }

    @Override
    public void entryAdded(EntryEvent<String, HazelcastNodeInfo> event) {
      String nodeId = event.getKey();
      if (!Objects.equals(manager.nodeIdValue(), nodeId)) {
        manager.executeClusterEvent(() -> {
          Member member = currentMember(manager, nodeId);
          if (member != null) {
            // A supplied member can host a new Vert.x instance without physically rejoining Hazelcast.
            if (!clusterInfo.containsKey(departureKey(member))) {
              inactiveMembers.remove(member.getUuid());
              addNode(nodeId);
              manager.handleMemberAdded(nodeId);
            }
          } else if (!memberNodeIds.contains(nodeId)) {
            addNode(nodeId);
            manager.handleMemberAdded(nodeId);
          }
        });
      }
    }

    @Override
    public void entryMerged(EntryEvent<String, HazelcastNodeInfo> event) { entryAdded(event); }

    @Override
    public void entryRemoved(EntryEvent<String, HazelcastNodeInfo> event) { removed(manager, event.getKey()); }

    @Override
    public void entryExpired(EntryEvent<String, HazelcastNodeInfo> event) { removed(manager, event.getKey()); }
  }

  private class DepartureListener implements EntryAddedListener<String, String>, EntryMergedListener<String, String>,
    EntryRemovedListener<String, String> {
    private final HazelcastClusterManager manager;
    DepartureListener(HazelcastClusterManager manager) { this.manager = manager; }

    @Override
    public void entryAdded(EntryEvent<String, String> event) {
      String key = event.getKey();
      String nodeId = event.getValue();
      if (!key.startsWith(LEFT_MEMBER_PREFIX) || Objects.equals(manager.nodeIdValue(), nodeId)) return;
      manager.executeClusterEvent(() -> {
        Member member = currentMember(manager, nodeId);
        if (member != null && key.equals(departureKey(member)) && Objects.equals(clusterInfo.get(key), nodeId)
          && inactiveMembers.add(member.getUuid())) {
          removeNode(nodeId);
          manager.handleMembersRemoved(Collections.singleton(nodeId));
        }
      });
    }

    @Override
    public void entryMerged(EntryEvent<String, String> event) { entryAdded(event); }

    @Override
    public void entryRemoved(EntryEvent<String, String> event) {
      String key = event.getKey();
      if (!key.startsWith(LEFT_MEMBER_PREFIX)) return;
      manager.executeClusterEvent(() -> {
        for (Member member : manager.getHazelcastInstance().getCluster().getMembers()) {
          String nodeId = member.getAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE);
          if (nodeId != null && !Objects.equals(manager.nodeIdValue(), nodeId) && key.equals(departureKey(member))
            && !clusterInfo.containsKey(key)) {
            inactiveMembers.remove(member.getUuid());
            addNode(nodeId);
            manager.handleMemberAdded(nodeId);
          }
        }
      });
    }
  }

  private static String departureKey(Member member) { return LEFT_MEMBER_PREFIX + member.getUuid(); }

  void memberRejoining(HazelcastClusterManager manager) {
    clusterInfo.remove(departureKey(manager.getHazelcastInstance().getCluster().getLocalMember()));
  }

  void memberLeaving(HazelcastClusterManager manager) {
    Member member = manager.getHazelcastInstance().getCluster().getLocalMember();
    clusterInfo.put(departureKey(member), manager.nodeIdValue());
  }

  public List<String> getNodes(HazelcastClusterManager manager) {
    if (!manager.isActive()) return new ArrayList<>();
    List<String> snapshot = nodes;
    String ownId = manager.nodeIdValue();
    if (ownId == null || snapshot.contains(ownId)) return new ArrayList<>(snapshot);
    Set<String> result = new TreeSet<>(snapshot);
    result.add(ownId);
    return new ArrayList<>(result);
  }

  // All mutations run on the cluster event executor; readers only copy the published immutable snapshot.
  private void addNode(String nodeId) {
    Set<String> updated = new TreeSet<>(nodes);
    updated.add(nodeId);
    nodes = Collections.unmodifiableList(new ArrayList<>(updated));
  }

  private void removeNode(String nodeId) {
    List<String> updated = new ArrayList<>(nodes);
    updated.remove(nodeId);
    nodes = Collections.unmodifiableList(updated);
  }

  void refreshNodes(HazelcastClusterManager manager) {
    Set<String> members = new TreeSet<>();
    Set<String> departures = clusterInfo.keySet();
    for (Member member : manager.getHazelcastInstance().getCluster().getMembers()) {
      String id = member.getAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE);
      if (id != null) {
        memberNodeIds.add(id);
        if (!departures.contains(departureKey(member))) members.add(id);
      }
    }
    memberNodeIds.addAll(members);
    Set<String> nodes = new TreeSet<>(manager.nodeInfoMap().keySet());
    nodes.removeAll(memberNodeIds);
    nodes.addAll(members);
    this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
  }

  void memberAdded(HazelcastClusterManager manager, Member member) {
    String nodeId = member.getAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE);
    if (nodeId == null) return;
    memberNodeIds.add(nodeId);
    currentMembers.put(nodeId, member.getUuid());
    if (!Objects.equals(manager.nodeIdValue(), nodeId)) {
      manager.executeClusterEvent(() -> {
        Member current = currentMember(manager, nodeId);
        if (current != null && current.getUuid().equals(member.getUuid())) {
          if (clusterInfo.containsKey(departureKey(member))) {
            inactiveMembers.add(member.getUuid());
            removeNode(nodeId);
          } else {
            addNode(nodeId);
            manager.handleMemberAdded(nodeId);
          }
        }
      });
    }
  }

  private Member currentMember(HazelcastClusterManager manager, String nodeId) {
    return manager.getHazelcastInstance().getCluster().getMembers().stream()
      .filter(member -> nodeId.equals(member.getAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE)))
      .findFirst().orElse(null);
  }

  void memberRemoved(HazelcastClusterManager manager, Member member) {
    String nodeId = member.getAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE);
    if (nodeId == null) return;
    manager.executeClusterEvent(() -> {
      // Check when processing the queued event: a supplied member may have restarted with the same node id.
      if (currentMember(manager, nodeId) != null || !currentMembers.remove(nodeId, member.getUuid())) return;
      removeNode(nodeId);
      manager.nodeInfoMap().remove(nodeId);
      clusterInfo.remove(departureKey(member));
      // Membership is authoritative even when partition loss already erased the registry entry without an event.
      if (!inactiveMembers.remove(member.getUuid())) manager.handleMembersRemoved(Collections.singleton(nodeId));
    });
  }

  public void leaving(HazelcastClusterManager manager) {
    UUID lifecycleId = lifecycleListenerId;
    UUID registryId = registryListenerId;
    UUID departureId = departureListenerId;
    lifecycleListenerId = null;
    registryListenerId = null;
    departureListenerId = null;
    removeListeners(
      () -> {
        if (lifecycleId != null) {
          manager.getHazelcastInstance().getLifecycleService().removeLifecycleListener(lifecycleId);
        }
      },
      () -> { if (registryId != null) manager.nodeInfoMap().removeEntryListener(registryId); },
      () -> { if (departureId != null) clusterInfo.removeEntryListener(departureId); });
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
