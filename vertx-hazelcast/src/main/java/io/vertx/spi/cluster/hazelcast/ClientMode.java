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
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.ExtendedMapEntry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import io.vertx.core.VertxException;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastNodeInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hazelcast client mode. Client-side Vert.x nodes are tracked in {@code __vertx.nodeInfo}.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public final class ClientMode implements HazelcastMode {

  private static final Logger log = LoggerFactory.getLogger(ClientMode.class);
  private static final String CLUSTER_INFO_MAP = "__vertx.clusterInfo";

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
  private final AtomicLong renewalPending = new AtomicLong(-1);
  private final AtomicBoolean leaseLost = new AtomicBoolean();
  private final AtomicLong lastRenewal = new AtomicLong();
  private UUID membershipListenerId;
  private volatile boolean connected = true;
  private volatile boolean recovering;
  private volatile String clusterEpoch;
  private final AtomicLong connectionGeneration = new AtomicLong();
  private final Object lifecycleLock = new Object();

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
    synchronized (this) {
      if (heartbeatExecutor != null) heartbeatExecutor.shutdownNow();
      heartbeatExecutor = null;
      registrationPublished = false;
      leaving = false;
      instance = null;
      nodeId = UUID.randomUUID().toString();
      membershipListenerId = null;
      lastRenewal.set(0);
      leaseLost.set(false);
      synchronized (lifecycleLock) {
        connected = true;
        recovering = false;
        clusterEpoch = null;
        renewalPending.set(-1);
        connectionGeneration.incrementAndGet();
      }
    }
    instance = instanceFactory.apply(config);
    try {
      installConfiguration();
      clusterEpoch = readClusterEpoch();
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

  private void installConfiguration() {
    // Repeat bootstrap after a cluster change: dynamic configuration is also lost on a full restart.
    instance.getConfig().addMapConfig(new MapConfig("__vertx.haInfo").setBackupCount(1));
    instance.getConfig().addMapConfig(new MapConfig("__vertx.nodeInfo").setBackupCount(1));
    instance.getConfig().addMapConfig(new MapConfig(CLUSTER_INFO_MAP).setBackupCount(1));
    instance.getConfig().addMultiMapConfig(new MultiMapConfig("__vertx.subs")
      .setBackupCount(1).setValueCollectionType(MultiMapConfig.ValueCollectionType.SET));
  }

  private String readClusterEpoch() {
    String proposed = UUID.randomUUID().toString();
    String existing = instance.<String, String>getMap(CLUSTER_INFO_MAP).putIfAbsent("epoch", proposed);
    return existing == null ? proposed : existing;
  }

  @Override
  public void stateChanged(HazelcastClusterManager manager, LifecycleEvent event) {
    // Lifecycle callbacks must never wait for the mode lock held by blocking map operations.
    synchronized (lifecycleLock) {
      switch (event.getState()) {
        case CLIENT_DISCONNECTED:
          recovering = true;
          connected = false;
          connectionGeneration.incrementAndGet();
          break;
        case CLIENT_CONNECTED:
        case CLIENT_CHANGED_CLUSTER:
          recovering = true;
          connected = true;
          long generation = connectionGeneration.incrementAndGet();
          manager.executeClusterEvent(() -> recover(manager, generation));
          break;
        default:
          break;
      }
    }
  }

  @Override
  public boolean isRecovering() { return recovering; }

  private void recover(HazelcastClusterManager manager, long generation) {
    if (!canRecover(manager, generation)) return;
    try {
      installConfiguration();
      String nextEpoch = readClusterEpoch();
      boolean newCluster = !nextEpoch.equals(clusterEpoch);
      manager.recoverClientState(info -> {
        synchronized (this) {
          if (!canRecover(manager, generation)) throw new VertxException("Client connection changed during recovery");
          long started = System.nanoTime();
          HazelcastNodeInfo value = new HazelcastNodeInfo(info);
          if (newCluster) {
            manager.nodeInfoMap().put(nodeId, value, leaseTtlSeconds, TimeUnit.SECONDS);
          } else if (!manager.nodeInfoMap().executeOnKey(nodeId, new UpdateNodeInfo(value))
            || !manager.nodeInfoMap().setTtl(nodeId, leaseTtlSeconds, TimeUnit.SECONDS)) {
            loseLease(manager);
            throw new VertxException("Cannot recover an expired lease in the same cluster");
          }
          if (!canRecover(manager, generation)) throw new VertxException("Client connection changed during recovery");
          lastRenewal.set(started);
        }
      }, newCluster);
      synchronized (lifecycleLock) {
        if (canRecover(manager, generation)) {
          clusterEpoch = nextEpoch;
          renewalPending.set(-1);
          recovering = false;
        }
      }
    } catch (RuntimeException failure) {
      if (canRecover(manager, generation)) {
        log.error("Failed to restore Vert.x state after reconnecting to Hazelcast", failure);
        loseLease(manager);
      }
    }
  }

  private boolean canRecover(HazelcastClusterManager manager, long generation) {
    return manager.isActive() && connected && !leaving && !leaseLost.get()
      && connectionGeneration.get() == generation;
  }

  @Override
  public void joined(HazelcastClusterManager manager) {
    registry.joined(manager);
    membershipListenerId = instance.getCluster().addMembershipListener(new MembershipListener() {
      @Override
      public void memberAdded(MembershipEvent event) {
        // The node registry announces Vert.x arrivals; data-only members are not Vert.x nodes.
      }

      @Override
      public void memberRemoved(MembershipEvent event) {
        String departedNodeId = event.getMember().getAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE);
        if (departedNodeId != null) {
          manager.executeClusterEvent(() -> {
            if (manager.isActive()) manager.nodeInfoMap().remove(departedNodeId);
          });
        }
      }
    });
  }

  @Override
  public synchronized void publishNodeInfo(HazelcastClusterManager manager, NodeInfo nodeInfo) {
    if (leaving || leaseLost.get()) throw new VertxException("Hazelcast client node lease is no longer valid");
    HazelcastNodeInfo value = new HazelcastNodeInfo(nodeInfo);
    if (registrationPublished) {
      if (recovering) return;
      // Never recreate an expired lease. In particular, another node's departure must not resurrect us.
      if (!manager.nodeInfoMap().executeOnKey(nodeId, new UpdateNodeInfo(value))) {
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
    long generation = connectionGeneration.get();
    if (!connected || recovering) return;
    if (!renewalPending.compareAndSet(-1, generation)) return;
    if (connectionGeneration.get() != generation) {
      renewalPending.compareAndSet(generation, -1);
      return;
    }
    manager.renewClientLease(leaseTtlSeconds).onComplete(result -> {
      try {
        if (leaving || leaseLost.get() || recovering || connectionGeneration.get() != generation) return;
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
        renewalPending.compareAndSet(generation, -1);
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
    UUID membershipId = membershipListenerId;
    membershipListenerId = null;
    NodeRegistry.removeListeners(
      () -> { if (membershipId != null) instance.getCluster().removeMembershipListener(membershipId); },
      () -> registry.leaving(manager));
  }

  @Override
  public void cleanup(HazelcastClusterManager manager) { registry.cleanup(manager); }

  @Override
  public boolean isHealthy() {
    if (!canCheckHealth()) return false;
    long generation = connectionGeneration.get();
    try {
      // Bound the read even if the connection disappears after checking the tracked state.
      Object registration = instance.getMap("__vertx.nodeInfo").getAsync(nodeId)
        .toCompletableFuture().get(1, TimeUnit.SECONDS);
      return registration != null && canCheckHealth() && generation == connectionGeneration.get();
    } catch (TimeoutException e) {
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException e) {
      throw new VertxException("Failed to read the Hazelcast client node registration", e.getCause());
    }
  }

  private boolean canCheckHealth() {
    return connected && !recovering && !leaving && !leaseLost.get() && instance != null
      && instance.getLifecycleService().isRunning();
  }

  /** Update an existing registration without resetting its lease to the map's default TTL. */
  public static final class UpdateNodeInfo implements EntryProcessor<String, HazelcastNodeInfo, Boolean>, DataSerializable {
    private HazelcastNodeInfo value;

    public UpdateNodeInfo() { }

    UpdateNodeInfo(HazelcastNodeInfo value) { this.value = value; }

    @Override
    public Boolean process(Map.Entry<String, HazelcastNodeInfo> entry) {
      if (entry.getValue() == null) return false;
      ((ExtendedMapEntry<String, HazelcastNodeInfo>) entry).setValueWithoutChangingExpiryTime(value);
      return true;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException { out.writeObject(value); }

    @Override
    public void readData(ObjectDataInput in) throws IOException { value = in.readObject(); }
  }

  @Override
  public void stop() {
    leaving = true;
    if (heartbeatExecutor != null) heartbeatExecutor.shutdownNow();
    if (instance != null) instance.getLifecycleService().shutdown();
  }
}
