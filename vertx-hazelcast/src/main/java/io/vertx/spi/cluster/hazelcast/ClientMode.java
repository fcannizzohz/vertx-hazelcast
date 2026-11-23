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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.map.IMap;
import io.vertx.core.VertxException;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.spi.cluster.hazelcast.impl.ConfigureVertxObjects;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastNodeInfo;
import io.vertx.spi.cluster.hazelcast.impl.UpdateNodeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hazelcast client mode. Client-side Vert.x nodes are tracked in {@code __vertx.nodeInfo}.
 *
 * <p>Three operations run on members and therefore need this library on the member classpath:
 * <ul>
 *   <li>{@link io.vertx.spi.cluster.hazelcast.impl.UpdateNodeInfo} renews a registration without resetting its lease,
 *   which {@code IMap.put} and {@code IMap.replace} would do;</li>
 *   <li>{@link io.vertx.spi.cluster.hazelcast.impl.ConfigureVertxObjects} inspects member configuration, which a
 *   client cannot read;</li>
 *   <li>{@code SubsMapHelper.RemoveSubscriptions} avoids downloading the subscription table to every observer.</li>
 * </ul>
 * Shared counters already required the library on members.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
final class ClientMode implements HazelcastMode {

  private static final Logger log = LoggerFactory.getLogger(ClientMode.class);
  private static final String CLUSTER_INFO_MAP = "__vertx.clusterInfo";
  private static final long CONFIGURATION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);

  public static final String LEASE_TTL_SECONDS = "vertx.hazelcast.client.lease.ttl.seconds";
  public static final String HEARTBEAT_INTERVAL_SECONDS = "vertx.hazelcast.client.heartbeat.interval.seconds";
  public static final String HEALTH_CHECK_TIMEOUT_MILLIS = "vertx.hazelcast.client.healthcheck.timeout.millis";

  private ClientConfig config;
  private final java.util.function.Function<ClientConfig, HazelcastInstance> instanceFactory;
  private HazelcastInstance instance;
  private String nodeId;
  private long leaseTtlSeconds;
  private long heartbeatIntervalSeconds;
  private long healthCheckTimeoutMillis;
  private boolean registrationPublished;
  private volatile boolean leaving;
  private volatile ScheduledExecutorService heartbeatExecutor;
  private volatile ExecutorService renewalExecutor;
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

  /** Load client XML when configuration is requested or the mode starts. */
  ClientMode() { instanceFactory = HazelcastClient::newHazelcastClient; }

  ClientMode(ClientConfig config) {
    this(config, HazelcastClient::newHazelcastClient);
  }

  /** Create an owned client through the supplied factory. */
  ClientMode(ClientConfig config, java.util.function.Function<ClientConfig, HazelcastInstance> instanceFactory) {
    this.config = Objects.requireNonNull(config);
    this.instanceFactory = Objects.requireNonNull(instanceFactory, "The client factory cannot be null");
  }

  public ClientConfig clientConfig() {
    if (config == null) config = ConfigUtil.loadClientConfig();
    return config;
  }

  @Override
  public void configureClient(ClientConfig config) {
    this.config = Objects.requireNonNull(config, "The Hazelcast client config cannot be null.");
  }

  @Override
  public void validateConfiguration() {
    ConfigUtil.validateMode("client", false);
    clientConfig();
    leaseTtlSeconds = positiveProperty(LEASE_TTL_SECONDS, 60);
    heartbeatIntervalSeconds = positiveProperty(HEARTBEAT_INTERVAL_SECONDS, 10);
    healthCheckTimeoutMillis = positiveProperty(HEALTH_CHECK_TIMEOUT_MILLIS, 1000);
    if (heartbeatIntervalSeconds > leaseTtlSeconds / 3) {
      throw new IllegalArgumentException(HEARTBEAT_INTERVAL_SECONDS + " must be at most one third of "
          + LEASE_TTL_SECONDS);
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
      throw new IllegalArgumentException("Client mode requires explicit member addresses or a configured discovery "
        + "provider. Set vertx.hazelcast.client.config, provide client-cluster.xml, or inject a ClientConfig with a "
        + "connection target. "
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
    throw new IllegalArgumentException(name + " must be a positive integer");
  }

  @Override
  public HazelcastInstance start(HazelcastClusterManager manager) {
    validateConfiguration();
    return startValidated(manager);
  }

  @Override
  public HazelcastInstance startValidated(HazelcastClusterManager manager) {
    synchronized (this) {
      if (heartbeatExecutor != null) heartbeatExecutor.shutdownNow();
      if (renewalExecutor != null) renewalExecutor.shutdownNow();
      heartbeatExecutor = null;
      renewalExecutor = null;
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
      throw new IllegalStateException("Cannot install Vert.x map and multimap configuration on the Hazelcast cluster",
          e);
    }
  }

  private void installConfiguration() {
    installConfiguration(instance, CONFIGURATION_TIMEOUT_MILLIS);
  }

  /**
   * Validate and install Vert.x object configuration on every member, waiting at most {@code timeoutMillis} overall.
   * A member that leaves while running the task is skipped: dynamic configuration installed on any surviving member
   * reaches the others, and members that join later receive it from the cluster.
   */
  static void installConfiguration(HazelcastInstance instance, long timeoutMillis) {
    // Repeat bootstrap after a cluster change: dynamic configuration is also lost on a full restart.
    var tasks = instance.getExecutorService("__vertx.config").submitToAllMembers(new ConfigureVertxObjects());
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    int completed = 0;
    MemberLeftException departed = null;
    for (var task : tasks.entrySet()) {
      try {
        task.getValue().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        completed++;
      } catch (InterruptedException failure) {
        Thread.currentThread().interrupt();
        throw new VertxException("Interrupted while configuring Vert.x objects on Hazelcast members", failure);
      } catch (TimeoutException failure) {
        throw new VertxException("Timed out after " + timeoutMillis
            + " ms configuring Vert.x objects on Hazelcast member "
          + task.getKey(), failure);
      } catch (ExecutionException failure) {
        MemberLeftException left = memberLeft(failure);
        if (left == null) throw new VertxException("Cannot configure Vert.x objects on Hazelcast members",
            failure.getCause());
        log.debug("Hazelcast member " + task.getKey() + " left while configuring Vert.x objects; skipping it");
        departed = left;
      }
    }
    if (completed == 0 && departed != null) {
      throw new VertxException("Cannot configure Vert.x objects: every Hazelcast member left during bootstrap",
          departed);
    }
  }

  private static MemberLeftException memberLeft(ExecutionException failure) {
    if (failure instanceof MemberLeftException) return (MemberLeftException) failure;
    return failure.getCause() instanceof MemberLeftException ? (MemberLeftException) failure.getCause() : null;
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
    membershipListenerId = instance.getCluster().addMembershipListener(new InitialMembershipListener() {
      @Override
      public void init(InitialMembershipEvent event) {
        event.getMembers().forEach(member -> registry.memberAdded(manager, member));
      }

      @Override
      public void memberAdded(MembershipEvent event) {
        registry.memberAdded(manager, event.getMember());
      }

      @Override
      public void memberRemoved(MembershipEvent event) {
        registry.memberRemoved(manager, event.getMember());
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
    renewalExecutor = Executors.newSingleThreadExecutor(task -> {
      Thread thread = new Thread(task, "vertx-hazelcast-client-lease-renewal");
      thread.setDaemon(true);
      return thread;
    });
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
    IMap<String, HazelcastNodeInfo> leaseMap = manager.nodeInfoMap();
    String leaseNodeId = nodeId;
    long ttlSeconds = leaseTtlSeconds;
    ExecutorService executor = renewalExecutor;
    if (executor == null) {
      renewalPending.compareAndSet(generation, -1);
      return;
    }
    try {
      executor.execute(() -> {
        try {
          if (!canRecover(manager, generation) || recovering) return;
          boolean renewed = leaseMap.setTtl(leaseNodeId, ttlSeconds, TimeUnit.SECONDS);
          if (!canRecover(manager, generation) || recovering) return;
          if (renewed) {
            // Conservatively measure the grant from request dispatch, not from a potentially delayed reply.
            lastRenewal.accumulateAndGet(started, Math::max);
          } else {
            loseLease(manager);
          }
        } catch (RuntimeException failure) {
          // A failed request does not extend the local deadline; the independent watchdog fences the node.
        } finally {
          renewalPending.compareAndSet(generation, -1);
        }
      });
    } catch (RejectedExecutionException ignore) {
      // Shutdown may race dispatch. No request was sent and the lease was not renewed.
      renewalPending.compareAndSet(generation, -1);
    }
  }

  private void loseLease(HazelcastClusterManager manager) {
    if (!leaving && leaseLost.compareAndSet(false, true)) {
      // Do not interrupt the detecting thread before invoking the application's lease-loss handler.
      if (heartbeatExecutor != null) heartbeatExecutor.shutdown();
      if (renewalExecutor != null) renewalExecutor.shutdown();
      manager.clientLeaseLost();
    }
  }

  @Override
  public List<String> getNodes(HazelcastClusterManager manager) {
    return leaving || leaseLost.get() ? new ArrayList<>() : registry.getNodes(manager);
  }

  @Override
  public void refreshNodes(HazelcastClusterManager manager) { registry.refreshNodes(manager); }

  @Override
  public synchronized void leaving(HazelcastClusterManager manager) {
    leaving = true;
    if (heartbeatExecutor != null) heartbeatExecutor.shutdownNow();
    if (renewalExecutor != null) renewalExecutor.shutdownNow();
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
        .toCompletableFuture().get(healthCheckTimeoutMillis, TimeUnit.MILLISECONDS);
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

  @Override
  public void stop() {
    leaving = true;
    if (heartbeatExecutor != null) heartbeatExecutor.shutdownNow();
    if (renewalExecutor != null) renewalExecutor.shutdownNow();
    if (instance != null) instance.getLifecycleService().shutdown();
  }
}
