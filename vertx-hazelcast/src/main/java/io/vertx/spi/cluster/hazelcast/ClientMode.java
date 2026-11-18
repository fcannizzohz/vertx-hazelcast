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
import io.vertx.core.VertxException;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastNodeInfo;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hazelcast client mode. Client-side Vert.x nodes are tracked in {@code __vertx.nodeInfo}.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public final class ClientMode implements HazelcastMode {

  public static final String LEASE_TTL_SECONDS = "vertx.hazelcast.client.lease.ttl.seconds";
  public static final String HEARTBEAT_INTERVAL_SECONDS = "vertx.hazelcast.client.heartbeat.interval.seconds";

  private ClientConfig config;
  private final java.util.function.Function<ClientConfig, HazelcastInstance> instanceFactory;
  private HazelcastInstance instance;
  private String nodeId;
  private long leaseTtlSeconds;
  private long heartbeatIntervalSeconds;
  private boolean registrationPublished;
  private volatile boolean leaving;
  private volatile ScheduledExecutorService heartbeatExecutor;
  private final AtomicBoolean renewalPending = new AtomicBoolean();
  private final AtomicBoolean leaseLost = new AtomicBoolean();
  private final AtomicLong lastRenewal = new AtomicLong();

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
    leaseTtlSeconds = positiveProperty(LEASE_TTL_SECONDS, 60);
    heartbeatIntervalSeconds = positiveProperty(HEARTBEAT_INTERVAL_SECONDS, 10);
    if (heartbeatIntervalSeconds > leaseTtlSeconds / 3) {
      throw new IllegalArgumentException(HEARTBEAT_INTERVAL_SECONDS + " must be at most one third of " + LEASE_TTL_SECONDS);
    }
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

  private long positiveProperty(String name, long defaultValue) {
    String value = config.getProperty(name);
    if (value == null) value = System.getProperty(name, Long.toString(defaultValue));
    try {
      long result = Long.parseLong(value);
      if (result > 0) return result;
    } catch (NumberFormatException ignore) {
    }
    throw new IllegalArgumentException(name + " must be a positive number of seconds");
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
  public synchronized void publishNodeInfo(HazelcastClusterManager manager, NodeInfo nodeInfo) {
    if (leaving || leaseLost.get()) throw new VertxException("Hazelcast client node lease is no longer valid");
    HazelcastNodeInfo value = new HazelcastNodeInfo(nodeInfo);
    if (registrationPublished) {
      // Never recreate an expired lease. In particular, another node's departure must not resurrect us.
      if (manager.nodeInfoMap().replace(nodeId, value) == null) {
        loseLease(manager);
        throw new VertxException("Hazelcast client node lease expired");
      }
      return;
    }
    long started = System.nanoTime();
    manager.nodeInfoMap().put(nodeId, value, leaseTtlSeconds, TimeUnit.SECONDS);
    registrationPublished = true;
    lastRenewal.set(started);
    heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(task -> {
      Thread thread = new Thread(task, "vertx-hazelcast-client-lease");
      thread.setDaemon(true);
      return thread;
    });
    heartbeatExecutor.scheduleAtFixedRate(() -> heartbeat(manager), heartbeatIntervalSeconds,
      heartbeatIntervalSeconds, TimeUnit.SECONDS);
  }

  private void heartbeat(HazelcastClusterManager manager) {
    if (leaving || leaseLost.get()) return;
    long started = System.nanoTime();
    // Check the last acknowledged renewal independently of blocked Hazelcast calls and Vert.x event loops.
    if (started - lastRenewal.get() >= TimeUnit.SECONDS.toNanos(leaseTtlSeconds - heartbeatIntervalSeconds)) {
      loseLease(manager);
      return;
    }
    if (!renewalPending.compareAndSet(false, true)) return;
    manager.renewClientLease(leaseTtlSeconds).onComplete(result -> {
      try {
        if (leaving || leaseLost.get()) return;
        if (result.succeeded()) {
          if (result.result()) {
            // Conservatively measure the grant from request dispatch, not from a potentially delayed reply.
            lastRenewal.accumulateAndGet(started, Math::max);
          } else {
            loseLease(manager);
          }
        }
        // A failed request does not extend the local deadline; the watchdog will close the node if needed.
      } finally {
        renewalPending.set(false);
      }
    });
  }

  private void loseLease(HazelcastClusterManager manager) {
    if (!leaving && leaseLost.compareAndSet(false, true)) {
      if (heartbeatExecutor != null) heartbeatExecutor.shutdownNow();
      manager.clientLeaseLost();
    }
  }

  @Override
  public List<String> getNodes(HazelcastClusterManager manager) { return registry.getNodes(manager); }

  @Override
  public synchronized void leaving(HazelcastClusterManager manager) {
    leaving = true;
    if (heartbeatExecutor != null) heartbeatExecutor.shutdownNow();
    registry.leaving(manager);
  }

  @Override
  public void cleanup(HazelcastClusterManager manager) { registry.cleanup(manager); }

  @Override
  public boolean isHealthy() {
    // A successful registry read confirms connectivity and this Vert.x node's registration.
    return !leaving && !leaseLost.get() && instance != null && instance.getLifecycleService().isRunning()
      && instance.getMap("__vertx.nodeInfo").containsKey(nodeId);
  }

  @Override
  public void stop() {
    leaving = true;
    if (heartbeatExecutor != null) heartbeatExecutor.shutdownNow();
    if (instance != null) instance.getLifecycleService().shutdown();
  }
}
