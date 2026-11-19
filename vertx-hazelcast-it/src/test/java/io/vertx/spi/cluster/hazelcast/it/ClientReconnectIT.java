/*
 * Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it)
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 */

package io.vertx.spi.cluster.hazelcast.it;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.map.IMap;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.internal.VertxInternal;
import io.vertx.ext.healthchecks.Status;
import io.vertx.spi.cluster.hazelcast.ClientMode;
import io.vertx.spi.cluster.hazelcast.ClusterHealthCheck;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastRegistrationInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
@Timeout(90)
public class ClientReconnectIT {
  private final String clusterName = "client-reconnect-" + UUID.randomUUID();
  private final List<Vertx> nodes = new ArrayList<>();
  private final List<TrackingManager> managers = new ArrayList<>();
  private final List<String> deployments = new ArrayList<>();
  private final Map<String, String> originalHa = new HashMap<>();
  private HazelcastInstance member;
  private int port;

  @BeforeEach
  public void startClusterAndClients() throws Exception {
    member = startMember(0);
    port = member.getCluster().getLocalMember().getAddress().getPort();
    for (int i = 0; i < 2; i++) {
      ClientConfig config = clientConfig();
      TrackingManager manager = new TrackingManager(config);
      VertxOptions options = new VertxOptions().setHAEnabled(true).setQuorumSize(1);
      options.getEventBusOptions().setHost("127.0.0.1");
      Vertx node = await(Vertx.builder().with(options).withClusterManager(manager).buildClustered());
      nodes.add(node);
      managers.add(manager);
      deployments.add(await(node.deployVerticle(RecoveryVerticle.class.getName(),
        new DeploymentOptions().setHa(true).setConfig(new io.vertx.core.json.JsonObject().put("address", "ha-" + i)))));
    }
    for (TrackingManager manager : managers) {
      originalHa.put(manager.getNodeId(), member.<String, String>getMap("__vertx.haInfo").get(manager.getNodeId()));
      manager.events.clear();
    }
    assertRestoredState();
  }

  @AfterEach
  public void stopClientsAndMember() throws Exception {
    List<Throwable> failures = new ArrayList<>();
    for (Vertx node : nodes) {
      try { await(node.close()); } catch (Throwable failure) { failures.add(failure); }
    }
    if (member != null) {
      try { member.getLifecycleService().terminate(); } catch (Throwable failure) { failures.add(failure); }
    }
    if (!failures.isEmpty()) {
      AssertionError failure = new AssertionError("Reconnect fixture cleanup failed");
      failures.forEach(failure::addSuppressed);
      throw failure;
    }
  }

  @Test
  public void fullExternalClusterRestartRestoresLiveClientsWithoutRestartingVertx() throws Exception {
    String previousEpoch = member.<String, String>getMap("__vertx.clusterInfo").get("epoch");
    member.getLifecycleService().terminate();
    eventually(() -> managers.stream().allMatch(manager -> manager.events.contains(CLIENT_DISCONNECTED)));
    assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
      for (Vertx node : nodes) {
        Promise<Status> health = Promise.promise();
        ClusterHealthCheck.createProcedure(node).handle(health);
        assertFalse(await(health.future()).isOk(), "Disconnected clients must promptly report down");
      }
    });
    // Leave enough time for old lease-renewal requests to fail or remain in flight.
    Thread.sleep(1500);
    member = startMember(port);
    assertRestoredState();
    assertNotEquals(previousEpoch, member.<String, String>getMap("__vertx.clusterInfo").get("epoch"));
    assertTrue(managers.stream().allMatch(manager -> manager.events.contains(CLIENT_CONNECTED)));
    assertEquals(1, member.getConfig().findMapConfig("__vertx.nodeInfo").getBackupCount());
    assertEquals(com.hazelcast.config.MultiMapConfig.ValueCollectionType.SET,
      member.getConfig().findMultiMapConfig("__vertx.subs").getValueCollectionType());
    assertCommunicationAndNewSubscriptions();
  }

  @Test
  public void changedClusterLifecycleEventRestoresClearedRegistrationsSubscriptionsAndHaInfo() throws Exception {
    String previousEpoch = member.<String, String>getMap("__vertx.clusterInfo").get("epoch");
    managers.forEach(manager -> manager.stateChanged(new LifecycleEvent(CLIENT_DISCONNECTED)));
    member.getMap("__vertx.nodeInfo").clear();
    member.getMultiMap("__vertx.subs").clear();
    member.getMap("__vertx.haInfo").clear();
    member.getMap("__vertx.clusterInfo").clear();
    assertTrue(member.getMap("__vertx.nodeInfo").isEmpty());
    managers.forEach(manager -> manager.stateChanged(new LifecycleEvent(CLIENT_CHANGED_CLUSTER)));
    assertRestoredState();
    assertNotEquals(previousEpoch, member.<String, String>getMap("__vertx.clusterInfo").get("epoch"));
    assertCommunicationAndNewSubscriptions();
  }

  @Test
  public void reconnectingToSameClusterKeepsValidLeasesAndMetadata() throws Exception {
    String originalEpoch = member.<String, String>getMap("__vertx.clusterInfo").get("epoch");
    managers.forEach(manager -> manager.stateChanged(new LifecycleEvent(CLIENT_DISCONNECTED)));
    managers.forEach(manager -> manager.stateChanged(new LifecycleEvent(CLIENT_CONNECTED)));
    assertRestoredState();
    assertEquals(originalEpoch, member.<String, String>getMap("__vertx.clusterInfo").get("epoch"));
    assertCommunicationAndNewSubscriptions();
  }

  @Test
  public void reconnectingToSameClusterDoesNotResurrectAnExpiredClientLease() throws Exception {
    TrackingManager expired = managers.get(0);
    String originalEpoch = member.<String, String>getMap("__vertx.clusterInfo").get("epoch");
    expired.stateChanged(new LifecycleEvent(CLIENT_DISCONNECTED));
    member.getMap("__vertx.nodeInfo").remove(expired.getNodeId());
    expired.stateChanged(new LifecycleEvent(CLIENT_CONNECTED));
    eventually(() -> !expired.getHazelcastInstance().getLifecycleService().isRunning());
    assertFalse(member.getMap("__vertx.nodeInfo").containsKey(expired.getNodeId()));
    assertFalse(expired.isHealthy());
    assertEquals(originalEpoch, member.<String, String>getMap("__vertx.clusterInfo").get("epoch"));
    assertTrue(managers.get(1).isHealthy());
  }

  @Test
  public void delayedRenewalFromOldConnectionCannotFenceRecoveredClient() throws Exception {
    CountDownLatch renewalStarted = new CountDownLatch(1);
    CountDownLatch releaseRenewal = new CountDownLatch(1);
    CountDownLatch oldResponseReturned = new CountDownLatch(1);
    AtomicBoolean firstRenewal = new AtomicBoolean(true);
    ClientMode mode = new ClientMode(clientConfig(), cfg -> {
      HazelcastInstance actual = HazelcastClient.newHazelcastClient(cfg);
      return (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
        new Class<?>[] {HazelcastInstance.class}, (proxy, method, args) -> {
          try {
            Object result = method.invoke(actual, args);
            if (method.getName().equals("getMap") && "__vertx.nodeInfo".equals(args[0])) {
              return Proxy.newProxyInstance(IMap.class.getClassLoader(), new Class<?>[] {IMap.class},
                (mapProxy, mapMethod, mapArgs) -> {
                  if (mapMethod.getName().equals("setTtl") && firstRenewal.compareAndSet(true, false)) {
                    renewalStarted.countDown();
                    assertTrue(releaseRenewal.await(20, TimeUnit.SECONDS));
                    oldResponseReturned.countDown();
                    return false;
                  }
                  try { return mapMethod.invoke(result, mapArgs); }
                  catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
            }
            return result;
          } catch (InvocationTargetException failure) {
            throw failure.getCause();
          }
        });
    });
    TrackingManager delayed = new TrackingManager(mode);
    VertxOptions options = new VertxOptions().setHAEnabled(true).setQuorumSize(1);
    options.getEventBusOptions().setHost("127.0.0.1");
    Vertx node = await(Vertx.builder().with(options).withClusterManager(delayed).buildClustered());
    nodes.add(node);
    managers.add(delayed);
    deployments.add(await(node.deployVerticle(RecoveryVerticle.class.getName(),
      new DeploymentOptions().setHa(true).setConfig(new io.vertx.core.json.JsonObject().put("address", "ha-delayed")))));
    originalHa.put(delayed.getNodeId(), member.<String, String>getMap("__vertx.haInfo").get(delayed.getNodeId()));
    try {
      assertTrue(renewalStarted.await(5, TimeUnit.SECONDS));
      managers.forEach(manager -> manager.stateChanged(new LifecycleEvent(CLIENT_DISCONNECTED)));
      member.getMap("__vertx.nodeInfo").clear();
      member.getMultiMap("__vertx.subs").clear();
      member.getMap("__vertx.haInfo").clear();
      member.getMap("__vertx.clusterInfo").clear();
      managers.forEach(manager -> manager.stateChanged(new LifecycleEvent(CLIENT_CHANGED_CLUSTER)));
      assertRestoredState();
      long restoredExpiry = member.getMap("__vertx.nodeInfo").getEntryView(delayed.getNodeId()).getExpirationTime();
      releaseRenewal.countDown();
      assertTrue(oldResponseReturned.await(5, TimeUnit.SECONDS));
      eventually(() -> {
        var entry = member.getMap("__vertx.nodeInfo").getEntryView(delayed.getNodeId());
        return entry != null && entry.getExpirationTime() > restoredExpiry;
      });
      assertRestoredState();
    } finally {
      releaseRenewal.countDown();
    }
  }

  private ClientConfig clientConfig() {
    ClientConfig config = new ClientConfig().setClusterName(clusterName);
    config.getNetworkConfig().addAddress("127.0.0.1:" + port);
    config.setProperty(ClientMode.LEASE_TTL_SECONDS, "60");
    config.setProperty(ClientMode.HEARTBEAT_INTERVAL_SECONDS, "1");
    config.setProperty("hazelcast.client.invocation.timeout.seconds", "5");
    config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(30000);
    return config;
  }

  private HazelcastInstance startMember(int requestedPort) {
    Config config = new Config().setClusterName(clusterName);
    config.setProperty("hazelcast.shutdownhook.enabled", "false");
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().setPort(requestedPort).setPortAutoIncrement(false);
    config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    return Hazelcast.newHazelcastInstance(config);
  }

  private void assertRestoredState() throws Exception {
    List<String> expected = managers.stream().map(TrackingManager::getNodeId).sorted().collect(Collectors.toList());
    eventually(() -> managers.stream().allMatch(TrackingManager::isHealthy)
      && managers.stream().allMatch(manager -> manager.getNodes().equals(expected))
      && expected.stream().allMatch(id -> originalHa.get(id).equals(member.getMap("__vertx.haInfo").get(id)))
      && member.getMultiMap("__vertx.subs").values().stream()
        .map(value -> ((HazelcastRegistrationInfo) value).unwrap().nodeId()).collect(Collectors.toSet())
        .containsAll(expected));
    for (int i = 0; i < nodes.size(); i++) {
      assertTrue(managers.get(i).getHazelcastInstance().getLifecycleService().isRunning());
      assertTrue(nodes.get(i).deploymentIDs().contains(deployments.get(i)), "The live HA deployment must remain deployed");
    }
  }

  private void assertCommunicationAndNewSubscriptions() throws Exception {
    for (int i = 0; i < nodes.size(); i++) {
      assertEquals(managers.get(i).getNodeId(), await(nodes.get(1 - i).eventBus().request("ha-" + i, "ping")).body());
    }
    await(nodes.get(1).eventBus().consumer("after-reconnect", message -> message.reply("new subscription")).completion());
    assertEquals("new subscription", await(nodes.get(0).eventBus().request("after-reconnect", "ping")).body());
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
  }

  private static void eventually(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) fail("Cluster state did not converge after reconnect");
      Thread.sleep(50);
    }
  }

  private static class TrackingManager extends HazelcastClusterManager {
    final Set<LifecycleEvent.LifecycleState> events = ConcurrentHashMap.newKeySet();
    TrackingManager(ClientConfig config) { this(new ClientMode(config)); }
    TrackingManager(ClientMode mode) { super(mode); }
    @Override
    public void stateChanged(LifecycleEvent event) {
      events.add(event.getState());
      super.stateChanged(event);
    }
  }

  public static class RecoveryVerticle extends AbstractVerticle {
    @Override
    public void start(Promise<Void> started) {
      String nodeId = ((VertxInternal) vertx).clusterManager().getNodeId();
      vertx.eventBus().consumer(config().getString("address"), message -> message.reply(nodeId))
        .completion().onComplete(started);
    }
  }
}
