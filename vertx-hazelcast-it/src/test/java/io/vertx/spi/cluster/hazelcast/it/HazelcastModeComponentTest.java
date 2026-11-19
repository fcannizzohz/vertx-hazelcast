/*
 * Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it)
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 */

package io.vertx.spi.cluster.hazelcast.it;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.map.IMap;
import com.hazelcast.test.TestEnvironment;
import io.vertx.core.VertxException;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.RegistrationListener;
import io.vertx.spi.cluster.hazelcast.ClientMode;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import io.vertx.spi.cluster.hazelcast.MemberMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real Hazelcast engines using the test factory's in-memory network, without Vert.x sockets.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
@Timeout(30)
public class HazelcastModeComponentTest {
  private TestHazelcastFactory factory;

  @BeforeEach
  public void createFactory() {
    assertTrue(TestEnvironment.isMockNetwork(), "Component tests require the Hazelcast mock network");
    factory = new TestHazelcastFactory();
  }

  @AfterEach
  public void stopFactory() {
    if (factory != null) factory.terminateAll();
  }

  private Config memberConfig() {
    return new Config().setClusterName("mode-component-" + UUID.randomUUID())
      .setProperty("hazelcast.phone.home.enabled", "false");
  }

  private ClientConfig clientConfig(HazelcastInstance member) {
    ClientConfig config = new ClientConfig().setClusterName(member.getConfig().getClusterName());
    var address = member.getCluster().getLocalMember().getAddress();
    config.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
    config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(3000);
    return config;
  }

  @Test
  public void ownedMemberUsesInjectedFactoryAndStopsItsInstance() {
    Config config = memberConfig();
    MemberMode mode = new MemberMode(config, factory::newHazelcastInstance);
    HazelcastInstance member = mode.start(new HazelcastClusterManager(mode));
    assertEquals(mode.nodeId(), member.getCluster().getLocalMember().getAttribute("__vertx.nodeId"));
    assertEquals("false", config.getProperty("hazelcast.shutdownhook.enabled"));
    assertTrue(member.getLifecycleService().isRunning());
    mode.stop();
    assertFalse(member.getLifecycleService().isRunning());
    assertFalse(mode.isHealthy());
  }

  @Test
  public void ownedMemberLeaveRetriesRejectedShutdown() throws Exception {
    AtomicInteger shutdownAttempts = new AtomicInteger();
    AtomicReference<HazelcastInstance> actual = new AtomicReference<>();
    MemberMode mode = new MemberMode(memberConfig(), config -> {
      HazelcastInstance member = factory.newHazelcastInstance(config);
      actual.set(member);
      LifecycleService lifecycle = (LifecycleService) Proxy.newProxyInstance(LifecycleService.class.getClassLoader(),
        new Class<?>[] {LifecycleService.class}, (proxy, method, args) -> {
          if (method.getName().equals("shutdown") && shutdownAttempts.incrementAndGet() <= 2) {
            throw new RejectedExecutionException("Transient shutdown rejection");
          }
          try { return method.invoke(member.getLifecycleService(), args); }
          catch (InvocationTargetException failure) { throw failure.getCause(); }
        });
      return (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
        new Class<?>[] {HazelcastInstance.class}, (proxy, method, args) -> {
          if (method.getName().equals("getLifecycleService")) return lifecycle;
          try { return method.invoke(member, args); }
          catch (InvocationTargetException failure) { throw failure.getCause(); }
        });
    });
    Vertx vertx = Vertx.vertx();
    HazelcastClusterManager manager = new HazelcastClusterManager(mode);
    manager.init(vertx);
    manager.registrationListener(new RegistrationListener() { });
    try {
      Promise<Void> joined = Promise.promise();
      manager.join(joined);
      joined.future().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      Promise<Void> left = Promise.promise();
      manager.leave(left);
      left.future().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      assertEquals(3, shutdownAttempts.get());
      assertFalse(actual.get().getLifecycleService().isRunning());
      mode.stop();
      assertEquals(3, shutdownAttempts.get(), "Stopping an already stopped member must be harmless");
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void externalMemberRemainsRunningWhenModeStops() {
    Config config = memberConfig();
    config.getMemberAttributeConfig().setAttribute("__vertx.nodeId", "existing-node");
    HazelcastInstance member = factory.newHazelcastInstance(config);
    MemberMode mode = new MemberMode(member);
    assertSame(member, mode.start(new HazelcastClusterManager(mode)));
    assertEquals("existing-node", mode.nodeId());
    mode.stop();
    assertTrue(member.getLifecycleService().isRunning());
  }

  @Test
  public void ownedClientBootstrapsInternalConfigurationAndStopsOnlyTheClient() {
    HazelcastInstance member = factory.newHazelcastInstance(memberConfig());
    ClientMode mode = new ClientMode(clientConfig(member), factory::newHazelcastClient);
    HazelcastInstance client = mode.start(new HazelcastClusterManager(mode));
    assertEquals(1, member.getConfig().findMapConfig("__vertx.nodeInfo").getBackupCount());
    assertEquals(1, member.getConfig().findMapConfig("__vertx.haInfo").getBackupCount());
    assertEquals(MultiMapConfig.ValueCollectionType.SET,
      member.getConfig().findMultiMapConfig("__vertx.subs").getValueCollectionType());
    client.<String, String>getMap("component-map").put("key", "value");
    assertEquals("value", member.getMap("component-map").get("key"));
    mode.stop();
    assertFalse(client.getLifecycleService().isRunning());
    assertFalse(mode.isHealthy());
    assertTrue(member.getLifecycleService().isRunning());
  }

  @Test
  public void clientReadinessTimesOutAndSkipsReadsWhileDisconnectedOrRecovering() {
    AtomicInteger reads = new AtomicInteger();
    ClientMode mode = healthMode(() -> {
      reads.incrementAndGet();
      return new CompletableFuture<>();
    });
    HazelcastClusterManager manager = new HazelcastClusterManager(mode);
    try {
      assertTimeoutPreemptively(Duration.ofSeconds(3), () -> assertFalse(mode.isHealthy()));
      assertEquals(1, reads.get());
      mode.stateChanged(manager, new LifecycleEvent(LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED));
      assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertFalse(mode.isHealthy()));
      assertEquals(1, reads.get(), "Disconnected clients must not issue registry reads");
      mode.stateChanged(manager, new LifecycleEvent(LifecycleEvent.LifecycleState.CLIENT_CONNECTED));
      // The inactive manager cannot run recovery, so readiness must remain down.
      assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertFalse(mode.isHealthy()));
      assertEquals(1, reads.get(), "Recovering clients must not issue registry reads");
    } finally {
      mode.stop();
    }
  }

  @Test
  public void clientReadinessChecksRegistrationAndRejectsAReadFromBeforeDisconnection() {
    AtomicReference<Supplier<CompletionStage<Object>>> read = new AtomicReference<>(
      () -> CompletableFuture.completedFuture("registration"));
    ClientMode mode = healthMode(() -> read.get().get());
    HazelcastClusterManager manager = new HazelcastClusterManager(mode);
    try {
      assertTrue(mode.isHealthy());
      read.set(() -> CompletableFuture.completedFuture(null));
      assertFalse(mode.isHealthy());
      RuntimeException expected = new IllegalStateException("registry unavailable");
      read.set(() -> CompletableFuture.failedFuture(expected));
      assertSame(expected, assertThrows(VertxException.class, mode::isHealthy).getCause());
      read.set(() -> {
        mode.stateChanged(manager, new LifecycleEvent(LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED));
        return CompletableFuture.completedFuture("old registration");
      });
      assertFalse(mode.isHealthy(), "A successful read from an old connection must not report ready");
    } finally {
      mode.stop();
    }
  }

  private ClientMode healthMode(Supplier<CompletionStage<Object>> read) {
    HazelcastInstance member = factory.newHazelcastInstance(memberConfig());
    ClientMode mode = new ClientMode(clientConfig(member), config -> {
      HazelcastInstance client = factory.newHazelcastClient(config);
      return (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
        new Class<?>[] {HazelcastInstance.class}, (proxy, method, args) -> {
          try {
            Object result = method.invoke(client, args);
            if (method.getName().equals("getMap") && "__vertx.nodeInfo".equals(args[0])) {
              return Proxy.newProxyInstance(IMap.class.getClassLoader(), new Class<?>[] {IMap.class},
                (mapProxy, mapMethod, mapArgs) -> {
                  if (mapMethod.getName().equals("getAsync")) return read.get();
                  if (mapMethod.getName().equals("containsKey")) {
                    throw new AssertionError("Readiness must not perform an unbounded registry read");
                  }
                  try { return mapMethod.invoke(result, mapArgs); }
                  catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
            }
            return result;
          } catch (InvocationTargetException failure) { throw failure.getCause(); }
        });
    });
    mode.start(new HazelcastClusterManager(mode));
    return mode;
  }

  @Test
  public void bootstrapConflictClosesTheFactoryCreatedClient() {
    Config config = memberConfig().addMapConfig(new MapConfig("__vertx.nodeInfo").setBackupCount(2));
    HazelcastInstance member = factory.newHazelcastInstance(config);
    AtomicReference<HazelcastInstance> created = new AtomicReference<>();
    ClientMode mode = new ClientMode(clientConfig(member), clientConfig -> {
      HazelcastInstance client = factory.newHazelcastClient(clientConfig);
      created.set(client);
      return client;
    });
    assertThrows(IllegalStateException.class, () -> mode.start(new HazelcastClusterManager(mode)));
    assertNotNull(created.get());
    assertFalse(created.get().getLifecycleService().isRunning());
    assertTrue(member.getLifecycleService().isRunning());
    assertEquals(2, member.getConfig().findMapConfig("__vertx.nodeInfo").getBackupCount());
  }

  @Test
  public void invalidClientConfigDoesNotInvokeTheFactory() {
    ClientMode mode = new ClientMode(new ClientConfig(), config -> {
      fail("An unconfigured client must be rejected before instance creation");
      return null;
    });
    assertThrows(IllegalArgumentException.class, () -> mode.start(new HazelcastClusterManager(mode)));
  }
  @Test
  public void invalidLeaseSettingsFailBeforeCreatingAClient() {
    for (String[] settings : new String[][] {{"0", "1"}, {"6", "0"}, {"6", "3"}, {"invalid", "1"}}) {
      ClientConfig config = new ClientConfig();
      config.getNetworkConfig().addAddress("127.0.0.1:5701");
      config.setProperty(ClientMode.LEASE_TTL_SECONDS, settings[0]);
      config.setProperty(ClientMode.HEARTBEAT_INTERVAL_SECONDS, settings[1]);
      ClientMode mode = new ClientMode(config, unused -> {
        fail("An invalid lease must be rejected before creating a client");
        return null;
      });
      assertThrows(IllegalArgumentException.class, () -> mode.start(new HazelcastClusterManager(mode)));
    }
  }

}
