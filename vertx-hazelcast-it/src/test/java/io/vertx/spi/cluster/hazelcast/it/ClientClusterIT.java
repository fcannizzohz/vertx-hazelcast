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
import com.hazelcast.map.listener.EntryUpdatedListener;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.ext.healthchecks.Status;
import io.vertx.spi.cluster.hazelcast.ClientMode;
import io.vertx.spi.cluster.hazelcast.ClusterHealthCheck;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
@Timeout(90)
public class ClientClusterIT {
  private HazelcastInstance member;
  private final List<Vertx> nodes = new ArrayList<>();

  @BeforeEach
  public void startMember() {
    Config config = new Config().setClusterName("client-cluster-it-" + UUID.randomUUID());
    config.setProperty("hazelcast.shutdownhook.enabled", "false");
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
    member = Hazelcast.newHazelcastInstance(config);
  }

  @AfterEach
  public void stopNodesAndMember() {
    // Close every owned resource even if another close fails.
    List<Throwable> failures = new ArrayList<>();
    for (Vertx node : nodes) {
      try { await(node.close()); } catch (Throwable t) { failures.add(t); }
    }
    if (member != null) {
      try { member.shutdown(); } catch (Throwable t) { failures.add(t); }
    }
    if (!failures.isEmpty()) {
      AssertionError failure = new AssertionError("Integration fixture cleanup failed");
      failures.forEach(failure::addSuppressed);
      throw failure;
    }
  }

  private HazelcastClusterManager startClient() throws Exception {
    return startClient(60, 10);
  }

  private HazelcastClusterManager startClient(long ttlSeconds, long heartbeatSeconds) throws Exception {
    var address = member.getCluster().getLocalMember().getAddress();
    ClientConfig config = new ClientConfig().setClusterName(member.getConfig().getClusterName());
    config.setInstanceName("vertx-it-" + UUID.randomUUID());
    config.setProperty(ClientMode.LEASE_TTL_SECONDS, Long.toString(ttlSeconds));
    config.setProperty(ClientMode.HEARTBEAT_INTERVAL_SECONDS, Long.toString(heartbeatSeconds));
    config.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
    config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(5000);
    HazelcastClusterManager manager = new HazelcastClusterManager(new ClientMode(config));
    VertxOptions options = new VertxOptions();
    options.getEventBusOptions().setHost("127.0.0.1");
    Vertx node = await(Vertx.builder().with(options).withClusterManager(manager).buildClustered());
    nodes.add(node);
    return manager;
  }

  @Test
  public void missingConnectionTargetFailsBeforeCreatingAClient() throws Exception {
    ClientConfig config = new ClientConfig().setInstanceName("unconfigured-it-" + UUID.randomUUID());
    HazelcastClusterManager manager = new HazelcastClusterManager(new ClientMode(config));
    ExecutionException failure = assertThrows(ExecutionException.class,
      () -> await(Vertx.builder().withClusterManager(manager).buildClustered()));
    assertTrue(failure.getCause().getMessage().contains("explicit member addresses"));
    assertFalse(manager.isActive());
    assertNull(manager.getHazelcastInstance());
    assertNull(HazelcastClient.getHazelcastClientByName(config.getInstanceName()));
  }

  @Test
  public void connectionFailureRollsBackAndAllowsRetry() throws Exception {
    int unavailablePort;
    try (ServerSocket socket = new ServerSocket(0)) {
      unavailablePort = socket.getLocalPort();
    }
    ClientConfig config = new ClientConfig().setClusterName(member.getConfig().getClusterName());
    config.setInstanceName("failed-connection-it-" + UUID.randomUUID());
    config.getNetworkConfig().addAddress("127.0.0.1:" + unavailablePort).setConnectionTimeout(200);
    config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(500);
    HazelcastClusterManager manager = new HazelcastClusterManager(new ClientMode(config));
    Vertx node = Vertx.vertx();
    nodes.add(node);
    manager.init(node);
    try {
      Promise<Void> joined = Promise.promise();
      manager.join(joined);
      ExecutionException failure = assertThrows(ExecutionException.class, () -> await(joined.future()));
      assertTrue(failure.getCause().getMessage().contains("Unable to connect"));
      assertFalse(manager.isActive());
      assertFalse(manager.isHealthy());
      assertNull(manager.getHazelcastInstance());
      assertNull(HazelcastClient.getHazelcastClientByName(config.getInstanceName()));
      Promise<Void> left = Promise.promise();
      manager.leave(left);
      await(left.future());

      var address = member.getCluster().getLocalMember().getAddress();
      config.getNetworkConfig().setAddresses(List.of(address.getHost() + ":" + address.getPort()));
      Promise<Void> retried = Promise.promise();
      manager.join(retried);
      await(retried.future());
      assertTrue(manager.isActive());
      assertTrue(manager.getHazelcastInstance().getLifecycleService().isRunning());
    } finally {
      Promise<Void> left = Promise.promise();
      manager.leave(left);
      await(left.future());
    }
    assertNull(HazelcastClient.getHazelcastClientByName(config.getInstanceName()));
    assertTrue(member.getLifecycleService().isRunning());
  }

  @Test
  public void peerDepartureAndMetadataUpdatesPreserveTheSurvivorsLease() throws Exception {
    // No heartbeat can mask a TTL reset during this test.
    HazelcastClusterManager survivor = startClient(3600, 1200);
    startClient();
    var registry = member.<String, Object>getMap("__vertx.nodeInfo");
    var initial = registry.getEntryView(survivor.getNodeId());
    assertEquals(TimeUnit.HOURS.toMillis(1), initial.getTtl());
    CountDownLatch republished = new CountDownLatch(1);
    UUID listener = registry.addEntryListener((EntryUpdatedListener<String, Object>) event -> {
      if (event.getKey().equals(survivor.getNodeId())) republished.countDown();
    }, false);
    try {
      await(nodes.remove(1).close());
      assertTrue(republished.await(5, TimeUnit.SECONDS), "Survivor must handle the peer departure");
      var afterDeparture = registry.getEntryView(survivor.getNodeId());
      assertEquals(initial.getTtl(), afterDeparture.getTtl(), "Departure must not make the lease permanent");
      assertEquals(initial.getExpirationTime(), afterDeparture.getExpirationTime());

      NodeInfo oldInfo = survivor.getNodeInfo();
      NodeInfo updated = new NodeInfo(oldInfo.host(), oldInfo.port(), new JsonObject().put("updated", true));
      Promise<Void> published = Promise.promise();
      survivor.setNodeInfo(updated, published);
      await(published.future());
      var afterUpdate = registry.getEntryView(survivor.getNodeId());
      assertEquals(initial.getTtl(), afterUpdate.getTtl());
      assertEquals(initial.getExpirationTime(), afterUpdate.getExpirationTime());
      Promise<NodeInfo> read = Promise.promise();
      survivor.getNodeInfo(survivor.getNodeId(), read);
      assertEquals(updated.metadata(), await(read.future()).metadata());
    } finally {
      registry.removeEntryListener(listener);
    }
  }

  @Test
  public void twoClientsDiscoverAndCommunicateThenCleanUpTheirRegistrations() throws Exception {
    HazelcastClusterManager first = startClient();
    HazelcastClusterManager second = startClient();
    assertNotEquals(first.getNodeId(), second.getNodeId());
    List<String> expected = new ArrayList<>(List.of(first.getNodeId(), second.getNodeId()));
    Collections.sort(expected);
    assertEquals(expected, first.getNodes());
    assertEquals(expected, second.getNodes());
    assertEquals(1, member.getCluster().getMembers().size(), "Vert.x nodes must remain clients");

    await(nodes.get(0).eventBus().consumer("first", msg -> msg.reply("first reply")).completion());
    await(nodes.get(1).eventBus().consumer("second", msg -> msg.reply("second reply")).completion());
    assertEquals("first reply", await(nodes.get(1).eventBus().request("first", "ping")).body());
    assertEquals("second reply", await(nodes.get(0).eventBus().request("second", "ping")).body());

    Vertx departing = nodes.remove(1);
    await(departing.close());
    assertFalse(second.getHazelcastInstance().getLifecycleService().isRunning());
    assertNull(HazelcastClient.getHazelcastClientByName(second.getClientConfig().getInstanceName()));
    assertEquals(List.of(first.getNodeId()), first.getNodes());
    assertFalse(member.getMap("__vertx.nodeInfo").containsKey(second.getNodeId()));
    assertFalse(member.getMultiMap("__vertx.subs").values().stream()
      .anyMatch(value -> ((io.vertx.spi.cluster.hazelcast.impl.HazelcastRegistrationInfo) value)
        .unwrap().nodeId().equals(second.getNodeId())));
    assertTrue(member.getLifecycleService().isRunning());
    assertEquals("first reply", await(nodes.get(0).eventBus().request("first", "still alive")).body());
  }

  @Test
  public void clientsShareMapsAndAtomicallyUpdateTheSameCounter() throws Exception {
    startClient();
    startClient();
    var firstMap = await(nodes.get(0).sharedData().<String, String>getAsyncMap("shared-client-map"));
    var secondMap = await(nodes.get(1).sharedData().<String, String>getAsyncMap("shared-client-map"));
    await(firstMap.put("key", "first"));
    assertEquals("first", await(secondMap.get("key")));
    assertEquals("first", await(secondMap.replace("key", "second")));
    assertEquals("second", await(firstMap.get("key")));

    var firstCounter = await(nodes.get(0).sharedData().getCounter("shared-counter"));
    var secondCounter = await(nodes.get(1).sharedData().getCounter("shared-counter"));
    List<Future<Long>> updates = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      updates.add(firstCounter.incrementAndGet());
      updates.add(secondCounter.incrementAndGet());
    }
    await(Future.all(updates));
    assertEquals(80L, await(firstCounter.get()));
    assertEquals(80L, await(secondCounter.get()));
    assertFalse(await(firstCounter.compareAndSet(79, 0)));
    assertTrue(await(secondCounter.compareAndSet(80, 100)));
    assertEquals(99L, await(firstCounter.decrementAndGet()));
  }

  @Test
  public void clientsContendForAndReleaseTheSameDistributedLock() throws Exception {
    startClient();
    startClient();
    var lock = await(nodes.get(0).sharedData().getLockWithTimeout("shared-lock", 2000));
    try {
      ExecutionException failure = assertThrows(ExecutionException.class,
        () -> await(nodes.get(1).sharedData().getLockWithTimeout("shared-lock", 100)));
      assertTrue(failure.getCause().getMessage().contains("Timed out"));
    } finally {
      lock.release();
    }
    var next = await(nodes.get(1).sharedData().getLockWithTimeout("shared-lock", 2000));
    next.release();
  }

  @Test
  public void readinessReflectsPresenceOfTheClientRegistration() throws Exception {
    HazelcastClusterManager manager = startClient();
    assertTrue(health(nodes.get(0)).isOk());
    var registry = member.<String, Object>getMap("__vertx.nodeInfo");
    Object registration = registry.remove(manager.getNodeId());
    assertNotNull(registration);
    try {
      assertFalse(health(nodes.get(0)).isOk());
    } finally {
      registry.put(manager.getNodeId(), registration);
    }
    assertTrue(health(nodes.get(0)).isOk());
  }

  private Status health(Vertx node) throws Exception {
    Promise<Status> promise = Promise.promise();
    ClusterHealthCheck.createProcedure(node).handle(promise);
    return await(promise.future());
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
  }
}
