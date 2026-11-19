/*
 * Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it)
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 */

package io.vertx.spi.cluster.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.vertx.core.VertxException;

import java.util.List;
import java.util.UUID;

/**
 * Hazelcast embedded-member mode.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public final class MemberMode implements HazelcastMode, com.hazelcast.cluster.MembershipListener {

  private Config config;
  private final java.util.function.Function<Config, HazelcastInstance> instanceFactory;
  private HazelcastClusterManager manager;
  private final HazelcastInstance provided;
  private HazelcastInstance instance;
  private String nodeId;

  @Override
  public String nodeId() { return nodeId; }

  public MemberMode() { this((Config) null); }

  public MemberMode(Config config) {
    this(config, Hazelcast::newHazelcastInstance);
  }

  /** Create an owned member through the supplied factory. */
  public MemberMode(Config config, java.util.function.Function<Config, HazelcastInstance> instanceFactory) {
    this.config = config;
    this.provided = null;
    this.instanceFactory = java.util.Objects.requireNonNull(instanceFactory, "The member factory cannot be null");
  }

  public MemberMode(HazelcastInstance instance) {
    this.instanceFactory = Hazelcast::newHazelcastInstance;
    this.provided = java.util.Objects.requireNonNull(instance);
    this.config = instance.getConfig();
  }

  @Override
  public Config memberConfig() { return config; }

  @Override
  public void configureMember(Config config) {
    if (provided != null && config != this.config) {
      throw new IllegalStateException("Cannot configure an externally supplied Hazelcast member");
    }
    this.config = config;
  }

  private UUID membershipListenerId;
  private final NodeRegistry registry = new NodeRegistry();

  @Override
  public void validateConfiguration() { ConfigUtil.validateMode("member", provided != null); }

  @Override
  public HazelcastInstance start(HazelcastClusterManager manager) {
    validateConfiguration();
    instance = null;
    if (provided != null) {
      nodeId = provided.getCluster().getLocalMember().getAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE);
      if (nodeId == null) {
        throw new VertxException("Vert.x node id not defined in Hazelcast member attributes", true);
      }
      instance = provided;
      validatePlatform();
      return instance;
    }

    if (config == null) {
      config = manager.loadConfig();
      if (config == null) {
        config = new Config();
      }
    }
    config.setProperty("hazelcast.shutdownhook.enabled", "false");
    nodeId = UUID.randomUUID().toString();
    config.getMemberAttributeConfig().setAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE, nodeId);
    instance = instanceFactory.apply(config);
    validatePlatform();
    return instance;
  }

  @Override
  public void joined(HazelcastClusterManager manager) {
    this.manager = manager;
    registry.joined(manager);
    membershipListenerId = instance.getCluster().addMembershipListener(this);
  }

  @Override
  public List<String> getNodes(HazelcastClusterManager manager) {
    return registry.getNodes(manager);
  }

  @Override
  public void leaving(HazelcastClusterManager manager) {
    UUID membershipId = membershipListenerId;
    membershipListenerId = null;
    NodeRegistry.removeListeners(
      () -> registry.leaving(manager),
      () -> { if (membershipId != null) instance.getCluster().removeMembershipListener(membershipId); });
  }

  @Override
  public void cleanup(HazelcastClusterManager manager) {
    registry.cleanup(manager);
  }

  @Override
  public void memberAdded(com.hazelcast.cluster.MembershipEvent event) {
    // Registry publication announces Vert.x arrivals.
  }

  @Override
  public void memberRemoved(com.hazelcast.cluster.MembershipEvent event) {
    String nodeId = event.getMember().getAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE);
    if (nodeId != null) {
      manager.executeClusterEvent(() -> manager.nodeInfoMap().remove(nodeId));
    }
  }

  @Override
  public boolean isHealthy() {
    return instance != null && instance.getLifecycleService().isRunning()
      && instance.getPartitionService().isClusterSafe();
  }

  private void validatePlatform() {
    if (io.netty.util.internal.PlatformDependent.isOsx() && com.hazelcast.config.NetworkConfig.class.getModule().isNamed()
        && instance.getConfig().getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
      throw new VertxException("Hazelcast detected on module path multicast join not supported on Mac");
    }
  }

  @Override
  public void stop() {
    if (provided == null && instance != null) instance.getLifecycleService().shutdown();
  }

  @Override
  public String clusterHost() {
    if (provided != null) return null;
    String host = System.getProperty("hazelcast.local.localAddress");
    if (host != null) return host;
    return config.getNetworkConfig().getPublicAddress() == null
      ? instance.getCluster().getLocalMember().getAddress().getHost() : null;
  }

  @Override
  public String clusterPublicHost() {
    if (provided != null) return null;
    String host = System.getProperty("hazelcast.local.publicAddress");
    return host != null ? host : config.getNetworkConfig().getPublicAddress();
  }
}
