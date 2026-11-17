/*
 * Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it)
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 */

package io.vertx.spi.cluster.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Hazelcast client mode. Client-side Vert.x nodes are tracked in {@code __vertx.nodeInfo}.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public final class ClientMode implements HazelcastMode {

  private final ClientConfig config;
  private HazelcastInstance instance;
  private String nodeId;

  @Override
  public String nodeId() { return nodeId; }
  private final NodeRegistry registry = new NodeRegistry();

  public ClientMode(ClientConfig config) {
    this.config = Objects.requireNonNull(config);
  }

  public ClientConfig clientConfig() {
    return config;
  }

  @Override
  public HazelcastInstance start(HazelcastClusterManager manager) {
    nodeId = UUID.randomUUID().toString();
    instance = HazelcastClient.newHazelcastClient(config);
    return instance;
  }

  @Override
  public void joined(HazelcastClusterManager manager) { registry.joined(manager); }

  @Override
  public List<String> getNodes(HazelcastClusterManager manager) { return registry.getNodes(manager); }

  @Override
  public void leaving(HazelcastClusterManager manager) { registry.leaving(manager); }

  @Override
  public void cleanup(HazelcastClusterManager manager) { registry.cleanup(manager); }

  @Override
  public boolean isHealthy() {
    // A successful registry read confirms connectivity and this Vert.x node's registration.
    return instance != null && instance.getLifecycleService().isRunning()
      && instance.getMap("__vertx.nodeInfo").containsKey(nodeId);
  }

  @Override
  public void stop() {
    if (instance != null) instance.getLifecycleService().shutdown();
  }
}
