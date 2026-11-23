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
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.core.spi.cluster.RegistrationListener;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastRegistrationInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
@Timeout(90)
public class ClientCrashIT {
  @TempDir
  Path directory;
  private HazelcastInstance member;
  private TrackingManager manager;
  private Vertx survivor;
  private Process child;

  @BeforeEach
  public void startCluster() throws Exception {
    Config config = new Config().setClusterName("client-crash-" + UUID.randomUUID());
    config.setProperty("hazelcast.shutdownhook.enabled", "false");
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.setProperty("hazelcast.heartbeat.interval.seconds", "1");
    config.setProperty("hazelcast.max.no.heartbeat.seconds", "5");
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
    config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
    member = Hazelcast.newHazelcastInstance(config);
    var address = member.getCluster().getLocalMember().getAddress();
    manager = new TrackingManager(clientConfig(config.getClusterName(), address.getHost() + ":" + address.getPort()));
    survivor = await(Vertx.builder().with(options()).withClusterManager(manager).buildClustered());
  }

  @AfterEach
  public void stopCluster() throws Exception {
    try {
      if (child != null && child.isAlive()) {
        child.destroyForcibly();
        assertTrue(child.waitFor(10, TimeUnit.SECONDS));
      }
    } finally {
      try {
        if (survivor != null) await(survivor.close());
      } finally {
        if (member != null) member.getLifecycleService().terminate();
      }
    }
  }

  @Test
  public void killedClientExpiresAndTriggersSubscriptionCleanupAndHaFailover() throws Exception {
    await(survivor.eventBus().consumer("crash-shared", msg -> msg.reply(manager.getNodeId())).completion());
    String deadNodeId = startChild(CrashingClient.class);
    assertCrashIsReaped(deadNodeId);
  }

  @Test
  public void killedMemberIsRemovedWhenOnlyClientsSurvive() throws Exception {
    await(survivor.eventBus().consumer("crash-shared", msg -> msg.reply(manager.getNodeId())).completion());
    String deadNodeId = startChild(CrashingMember.class);
    try {
      eventually(() -> assertEquals(2, member.getCluster().getMembers().size()));
    } catch (AssertionError failure) {
      throw new AssertionError(Files.readString(directory.resolve("child.log")), failure);
    }
    assertNull(member.getCluster().getLocalMember().getAttribute("__vertx.nodeId"),
      "The remaining Hazelcast member must not host a Vert.x node or a member-mode registry listener");
    assertCrashIsReaped(deadNodeId);
    assertEquals(1, member.getCluster().getMembers().size());
  }

  private String startChild(Class<?> mainClass) throws Exception {
    Path ready = directory.resolve("ready");
    Path output = directory.resolve("child.log");
    var address = member.getCluster().getLocalMember().getAddress();
    child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
      "-Djava.net.preferIPv4Stack=true", "-Dhazelcast.test.use.network=true",
      "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
      mainClass.getName(), member.getConfig().getClusterName(),
      address.getHost() + ":" + address.getPort(), ready.toString())
      .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    eventually(() -> assertTrue(Files.exists(ready), "Child did not start; inspect " + output));
    return Files.readString(ready);
  }

  private void assertCrashIsReaped(String deadNodeId) throws Exception {
    eventually(() -> assertTrue(manager.getNodes().contains(deadNodeId)));
    assertTrue(member.getMap("__vertx.haInfo").containsKey(deadNodeId));
    assertEquals(deadNodeId, await(survivor.eventBus().request("crash-ha", "ping")).body());
    assertTrue(hasSubscriptions(deadNodeId));
    int lostBefore = manager.lost.get();

    // Unlike simulateKill(), destroyForcibly() does not invoke ClusterManager.leave() or shutdown hooks.
    child.destroyForcibly();
    assertTrue(child.waitFor(10, TimeUnit.SECONDS));
    assertNotEquals(0, child.exitValue());
    eventually(() -> assertFalse(member.getMap("__vertx.nodeInfo").containsKey(deadNodeId)));
    eventually(() -> assertTrue(manager.removed.contains(deadNodeId), "nodeLeft must be delivered"));
    eventually(() -> assertTrue(manager.lost.get() > lostBefore, "registrationsLost must be delivered"));
    eventually(() -> assertFalse(hasSubscriptions(deadNodeId)));
    eventually(() -> assertFalse(member.getMap("__vertx.haInfo").containsKey(deadNodeId)));
    eventually(() -> assertTrue(survivor.deploymentIDs().size() > 0, "HA deployment must fail over"));
    assertEquals(manager.getNodeId(), await(survivor.eventBus().request("crash-ha", "ping")).body());
    for (int i = 0; i < 10; i++) {
      assertEquals(manager.getNodeId(), await(survivor.eventBus().request("crash-shared", "ping")).body());
    }
    assertTrue(manager.isHealthy());
    assertEquals(1, manager.getNodes().size());
  }

  @Test
  public void heartbeatKeepsHealthyClientAliveAcrossSeveralLeasePeriods() throws Exception {
    long initialExpiry = member.getMap("__vertx.nodeInfo").getEntryView(manager.getNodeId()).getExpirationTime();
    Thread.sleep(13000);
    assertTrue(manager.isHealthy());
    assertTrue(member.getMap("__vertx.nodeInfo").getEntryView(manager.getNodeId()).getExpirationTime() > initialExpiry);
    await(survivor.close());
    assertFalse(member.getMap("__vertx.nodeInfo").containsKey(manager.getNodeId()));
    Thread.sleep(1500);
    assertFalse(member.getMap("__vertx.nodeInfo").containsKey(manager.getNodeId()),
        "A stopped heartbeat must not recreate the entry");
  }

  @Test
  public void lostLeaseClosesTheClientInsteadOfRecreatingItsRegistration() throws Exception {
    member.getMap("__vertx.nodeInfo").remove(manager.getNodeId());
    eventually(() -> assertFalse(manager.getHazelcastInstance().getLifecycleService().isRunning()));
    assertFalse(manager.isHealthy());
    assertFalse(member.getMap("__vertx.nodeInfo").containsKey(manager.getNodeId()));
  }

  @ParameterizedTest(name = "disconnect past safety deadline: {0}")
  @ValueSource(booleans = {false, true})
  public void customLeaseLossHandlerReplacesCloseWhileTheLeaseRemainsFenced(boolean disconnect) throws Exception {
    AtomicInteger calls = new AtomicInteger();
    AtomicReference<Vertx> received = new AtomicReference<>();
    AtomicReference<Boolean> interrupted = new AtomicReference<>();
    CountDownLatch notified = new CountDownLatch(1);
    assertThrows(NullPointerException.class, () -> manager.setClientLeaseLossHandler(null));
    manager.setClientLeaseLossHandler(node -> {
      calls.incrementAndGet();
      received.set(node);
      interrupted.set(Thread.currentThread().isInterrupted());
      notified.countDown();
    });
    var server = await(survivor.createHttpServer().requestHandler(request -> request.response().end("alive"))
      .listen(0, "127.0.0.1"));
    var http = survivor.createHttpClient();
    try {
      if (disconnect) {
        manager.stateChanged(new com.hazelcast.core.LifecycleEvent(
          com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED));
      } else {
        member.getMap("__vertx.nodeInfo").remove(manager.getNodeId());
      }
      assertTrue(notified.await(10, TimeUnit.SECONDS));
      assertSame(survivor, received.get());
      assertFalse(interrupted.get(), "The watchdog must not interrupt the application handler");
      assertFalse(manager.isHealthy());
      assertTrue(manager.getHazelcastInstance().getLifecycleService().isRunning());
      assertEquals("alive", await(http.request(HttpMethod.GET, server.actualPort(), "127.0.0.1", "/")
        .compose(request -> request.send()).compose(response -> response.body())).toString());

      Promise<Void> published = Promise.promise();
      manager.setNodeInfo(manager.getNodeInfo(), published);
      assertThrows(java.util.concurrent.ExecutionException.class, () -> await(published.future()));
      manager.stateChanged(new com.hazelcast.core.LifecycleEvent(
        com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED));
      Thread.sleep(1200);
      assertFalse(member.getMap("__vertx.nodeInfo").containsKey(manager.getNodeId()));
      assertEquals(1, calls.get(), "Lease loss must only notify the application once");
    } finally {
      await(http.close());
      await(server.close());
    }
  }

  @Test
  public void throwingLeaseLossHandlerFallsBackToClosingVertx() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    manager.setClientLeaseLossHandler(node -> {
      calls.incrementAndGet();
      throw new IllegalStateException("Application shutdown hook failed");
    });
    member.getMap("__vertx.nodeInfo").remove(manager.getNodeId());
    eventually(() -> assertFalse(manager.getHazelcastInstance().getLifecycleService().isRunning()));
    assertEquals(1, calls.get());
    assertFalse(manager.isHealthy());
  }

  @Test
  public void saturatedVertxWorkerPoolDoesNotPreventLeaseRenewal() throws Exception {
    var address = member.getCluster().getLocalMember().getAddress();
    HazelcastClusterManager isolatedManager = new HazelcastClusterManager(new ClientMode(
      clientConfig(member.getConfig().getClusterName(), address.getHost() + ":" + address.getPort())));
    AtomicInteger lostLeases = new AtomicInteger();
    isolatedManager.setClientLeaseLossHandler(node -> lostLeases.incrementAndGet());
    Vertx isolated = await(Vertx.builder().with(options().setWorkerPoolSize(1))
      .withClusterManager(isolatedManager).buildClustered());
    CountDownLatch occupied = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Future<Void> blocking = isolated.executeBlocking(() -> {
      occupied.countDown();
      assertTrue(release.await(20, TimeUnit.SECONDS));
      return null;
    }, false);
    try {
      assertTrue(occupied.await(5, TimeUnit.SECONDS));
      var registry = member.getMap("__vertx.nodeInfo");
      long initialExpiry = registry.getEntryView(isolatedManager.getNodeId()).getExpirationTime();
      // Keep the only application worker occupied beyond the six-second lease TTL.
      Thread.sleep(7000);
      assertEquals(0, lostLeases.get(), "Application worker starvation must not cause lease loss");
      assertTrue(isolatedManager.isHealthy());
      var renewed = registry.getEntryView(isolatedManager.getNodeId());
      assertNotNull(renewed);
      assertTrue(renewed.getExpirationTime() > initialExpiry);
    } finally {
      release.countDown();
      try { await(blocking); }
      finally { await(isolated.close()); }
    }
  }

  @Test
  public void blockedRenewalDoesNotBlockTheLeaseWatchdog() throws Exception {
    CountDownLatch renewalStarted = new CountDownLatch(1);
    CountDownLatch releaseRenewal = new CountDownLatch(1);
    AtomicReference<Thread> renewalThread = new AtomicReference<>();
    AtomicReference<Thread> watchdogThread = new AtomicReference<>();
    var address = member.getCluster().getLocalMember().getAddress();
    ClientConfig config = clientConfig(member.getConfig().getClusterName(), address.getHost() + ":"
        + address.getPort());
    ClientMode mode = new ClientMode(config, cfg -> {
      HazelcastInstance actual = HazelcastClient.newHazelcastClient(cfg);
      return (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
        new Class<?>[] {HazelcastInstance.class}, (proxy, method, args) -> {
          try {
            Object result = method.invoke(actual, args);
            if (method.getName().equals("getMap") && "__vertx.nodeInfo".equals(args[0])) {
              return Proxy.newProxyInstance(IMap.class.getClassLoader(), new Class<?>[] {IMap.class},
                (mapProxy, mapMethod, mapArgs) -> {
                  if (mapMethod.getName().equals("setTtl")) {
                    renewalThread.set(Thread.currentThread());
                    renewalStarted.countDown();
                    assertTrue(releaseRenewal.await(20, TimeUnit.SECONDS));
                  }
                  try {
                    return mapMethod.invoke(result, mapArgs);
                  } catch (InvocationTargetException failure) {
                    throw failure.getCause();
                  }
                });
            }
            return result;
          } catch (InvocationTargetException failure) {
            throw failure.getCause();
          }
        });
    });
    HazelcastClusterManager blockedManager = new HazelcastClusterManager(mode);
    CountDownLatch leaseLost = new CountDownLatch(1);
    blockedManager.setClientLeaseLossHandler(node -> {
      watchdogThread.set(Thread.currentThread());
      leaseLost.countDown();
    });
    Vertx blockedNode = await(Vertx.builder().with(options()).withClusterManager(blockedManager).buildClustered());
    try {
      assertTrue(renewalStarted.await(5, TimeUnit.SECONDS));
      assertTrue(leaseLost.await(10, TimeUnit.SECONDS), "Watchdog must invoke the hook while renewal is blocked");
      assertNotSame(renewalThread.get(), watchdogThread.get());
      assertEquals("vertx-hazelcast-client-lease-renewal", renewalThread.get().getName());
      eventually(() -> assertFalse(blockedManager.isHealthy(),
          "Watchdog must fence the node without waiting for setTtl"));
    } finally {
      releaseRenewal.countDown();
      await(blockedNode.close());
    }
    renewalThread.get().join(5000);
    assertFalse(renewalThread.get().isAlive(), "Closing the client must stop its renewal thread");
    assertFalse(member.getMap("__vertx.nodeInfo").containsKey(blockedManager.getNodeId()));
  }

  private boolean hasSubscriptions(String nodeId) {
    return member.getMultiMap("__vertx.subs").values().stream()
      .anyMatch(value -> ((HazelcastRegistrationInfo) value).unwrap().nodeId().equals(nodeId));
  }

  private static ClientConfig clientConfig(String clusterName, String address) {
    ClientConfig config = new ClientConfig().setClusterName(clusterName);
    config.getNetworkConfig().addAddress(address);
    config.setProperty(ClientMode.LEASE_TTL_SECONDS, "6");
    config.setProperty(ClientMode.HEARTBEAT_INTERVAL_SECONDS, "1");
    config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(5000);
    return config;
  }

  private static VertxOptions options() {
    VertxOptions options = new VertxOptions().setHAEnabled(true).setQuorumSize(1);
    options.getEventBusOptions().setHost("127.0.0.1");
    return options;
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
  }

  private static void eventually(Runnable assertion) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    do {
      try {
        assertion.run();
        return;
      } catch (AssertionError failure) {
        if (System.nanoTime() >= deadline) throw failure;
      }
      Thread.sleep(50);
    } while (true);
  }

  private static final class TrackingManager extends HazelcastClusterManager {
    final Set<String> removed = ConcurrentHashMap.newKeySet();
    final AtomicInteger lost = new AtomicInteger();

    TrackingManager(ClientConfig config) { super(new ClientMode(config)); }

    @Override
    public void nodeListener(NodeListener listener) {
      super.nodeListener(new NodeListener() {
        public void nodeAdded(String nodeId) { listener.nodeAdded(nodeId); }
        public void nodeLeft(String nodeId) {
          listener.nodeLeft(nodeId);
          removed.add(nodeId);
        }
      });
    }

    @Override
    public void registrationListener(RegistrationListener listener) {
      super.registrationListener(new RegistrationListener() {
        public void registrationsUpdated(RegistrationUpdateEvent event) { listener.registrationsUpdated(event); }
        public boolean wantsUpdatesFor(String address) { return listener.wantsUpdatesFor(address); }
        public void registrationsLost() {
          listener.registrationsLost();
          lost.incrementAndGet();
        }
      });
    }
  }

  public static class HaVerticle extends AbstractVerticle {
    @Override
    public void start(Promise<Void> started) {
      String nodeId = ((VertxInternal) vertx).clusterManager().getNodeId();
      vertx.eventBus().consumer("crash-ha", msg -> msg.reply(nodeId)).completion().onComplete(started);
    }
  }

  public static class CrashingClient {
    public static void main(String[] args) throws Exception {
      HazelcastClusterManager manager = new HazelcastClusterManager(new ClientMode(clientConfig(args[0], args[1])));
      Vertx node = await(Vertx.builder().with(options()).withClusterManager(manager).buildClustered());
      await(node.eventBus().consumer("crash-shared", msg -> msg.reply(manager.getNodeId())).completion());
      await(node.deployVerticle(HaVerticle.class.getName(), new DeploymentOptions().setHa(true)));
      Files.writeString(Path.of(args[2]), manager.getNodeId());
    }
  }
  public static class CrashingMember {
    public static void main(String[] args) throws Exception {
      Config config = new Config().setClusterName(args[0]);
      config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
      config.setProperty("hazelcast.shutdownhook.enabled", "false");
      config.setProperty("hazelcast.phone.home.enabled", "false");
      config.setProperty("hazelcast.heartbeat.interval.seconds", "1");
      config.setProperty("hazelcast.max.no.heartbeat.seconds", "5");
      config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
      config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
      config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember(args[1]);
      config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
      HazelcastClusterManager manager = new HazelcastClusterManager(new MemberMode(config));
      Vertx node = await(Vertx.builder().with(options()).withClusterManager(manager).buildClustered());
      await(node.eventBus().consumer("crash-shared", msg -> msg.reply(manager.getNodeId())).completion());
      await(node.deployVerticle(HaVerticle.class.getName(), new DeploymentOptions().setHa(true)));
      Files.writeString(Path.of(args[2]), manager.getNodeId());
    }
  }

}
