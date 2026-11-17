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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Hazelcast client mode. Client-side Vert.x nodes are tracked in {@code __vertx.nodeInfo}.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public final class ClientMode implements HazelcastMode {

  private ClientConfig config;
  private final java.util.function.Function<ClientConfig, HazelcastInstance> instanceFactory;
  private HazelcastInstance instance;
  private String nodeId;

  @Override
  public String nodeId() { return nodeId; }
  private final NodeRegistry registry = new NodeRegistry();

  /** Load client XML when the mode starts. */
  public ClientMode() { instanceFactory = HazelcastClient::newHazelcastClient; }

  public ClientMode(ClientConfig config) {
    this(config, HazelcastClient::newHazelcastClient);
  }

  /** Create an owned client through the supplied factory. */
  public ClientMode(ClientConfig config, java.util.function.Function<ClientConfig, HazelcastInstance> instanceFactory) {
    this.config = Objects.requireNonNull(config);
    this.instanceFactory = Objects.requireNonNull(instanceFactory, "The client factory cannot be null");
  }

  public ClientConfig clientConfig() {
    return config;
  }

  @Override
  public void validateConfiguration() {
    ConfigUtil.validateMode("client", false);
    if (config == null) config = ConfigUtil.loadClientConfig();
    var network = config.getNetworkConfig();
    boolean addresses = !network.getAddresses().isEmpty();
    if (network.getAddresses().stream().anyMatch(address -> address == null || address.isBlank())) {
      throw new IllegalArgumentException("Client member addresses must not be blank");
    }
    var discovery = network.getDiscoveryConfig();
    boolean configuredDiscovery = !discovery.getDiscoveryStrategyConfigs().isEmpty()
      || discovery.getDiscoveryServiceProvider() != null
      || network.getAwsConfig().isEnabled()
      || network.getAzureConfig().isEnabled()
      || network.getGcpConfig().isEnabled()
      || network.getKubernetesConfig().isEnabled()
      || network.getEurekaConfig().isEnabled()
      || network.getCloudConfig().isEnabled();
    // Auto-detection is enabled by default in ClientConfig, so it is not explicit configuration.
    if (!addresses && !configuredDiscovery) {
      throw new IllegalArgumentException("Client mode requires explicit member addresses or a configured discovery provider. "
        + "Set vertx.hazelcast.client.config, provide client-cluster.xml, or inject a ClientConfig with a connection target. "
        + "The packaged default-client-cluster.xml is a template and does not select a cluster.");
    }
  }

  @Override
  public HazelcastInstance start(HazelcastClusterManager manager) {
    validateConfiguration();
    nodeId = UUID.randomUUID().toString();
    instance = instanceFactory.apply(config);
    try {
      // Install member-side settings before requesting any distributed objects.
      instance.getConfig().addMapConfig(new MapConfig("__vertx.haInfo").setBackupCount(1));
      instance.getConfig().addMapConfig(new MapConfig("__vertx.nodeInfo").setBackupCount(1));
      instance.getConfig().addMultiMapConfig(new MultiMapConfig("__vertx.subs")
        .setBackupCount(1).setValueCollectionType(MultiMapConfig.ValueCollectionType.SET));
      return instance;
    } catch (RuntimeException e) {
      try {
        stop();
      } catch (RuntimeException shutdownFailure) {
        e.addSuppressed(shutdownFailure);
      }
      throw new IllegalStateException("Cannot install Vert.x map and multimap configuration on the Hazelcast cluster", e);
    }
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
