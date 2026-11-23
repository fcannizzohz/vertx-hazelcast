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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.map.IMap;
import com.hazelcast.test.TestEnvironment;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.spi.cluster.RegistrationListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Real Hazelcast engines using the test factory's in-memory network, without Vert.x sockets.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
@Timeout(30)
public class HazelcastModeComponentTest {
  enum ConfigurationSource { EXACT, WILDCARD, DYNAMIC }
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
  public void managerJoinAndDirectStartValidateOncePerAttempt() throws Exception {
    HazelcastInstance member = factory.newHazelcastInstance(memberConfig());
    AtomicInteger validations = new AtomicInteger();
    ClientConfig config = new ClientConfig() {
      @Override
      public String getProperty(String name) {
        if (ClientMode.LEASE_TTL_SECONDS.equals(name)) validations.incrementAndGet();
        return super.getProperty(name);
      }
    };
    config.setClusterName(member.getConfig().getClusterName());
    var address = member.getCluster().getLocalMember().getAddress();
    config.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
    ClientMode mode = new ClientMode(config, factory::newHazelcastClient);
    HazelcastClusterManager manager = new HazelcastClusterManager(mode);
    Vertx vertx = Vertx.vertx();
    manager.init(vertx);
    manager.registrationListener(new RegistrationListener() { });
    try {
      for (int attempt = 1; attempt <= 2; attempt++) {
        Promise<Void> joined = Promise.promise();
        manager.join(joined);
        joined.future().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertEquals(attempt, validations.get(), "Each join attempt must validate exactly once");
        String error = assertThrows(IllegalStateException.class, () -> manager.setConfig(new Config())).getMessage();
        assertEquals(error, assertThrows(IllegalStateException.class, () ->
            manager.setClientConfig(new ClientConfig())).getMessage());
        Promise<Void> left = Promise.promise();
        manager.leave(left);
        left.future().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      }
      mode.start(manager);
      assertEquals(3, validations.get(), "Starting a mode directly must still validate once");
    } finally {
      try {
        Promise<Void> left = Promise.promise();
        manager.leave(left);
        left.future().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      } finally {
        try { mode.stop(); }
        finally { vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS); }
      }
    }
  }

  @Test
  public void suppliedClientsAreRejectedAtConstructionWithoutClosingThem() {
    HazelcastInstance member = factory.newHazelcastInstance(memberConfig());
    HazelcastInstance client = factory.newHazelcastClient(clientConfig(member));
    HazelcastInstance wrapped = (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
      new Class<?>[] {HazelcastInstance.class}, (proxy, method, args) -> {
        if (method.getName().equals("getLocalEndpoint")) return client.getLocalEndpoint();
        throw new AssertionError("The client must be rejected before calling " + method.getName());
      });
    for (HazelcastInstance candidate : new HazelcastInstance[] {client, wrapped}) {
      IllegalArgumentException modeFailure = assertThrows(IllegalArgumentException.class, () ->
          new MemberMode(candidate));
      IllegalArgumentException managerFailure = assertThrows(IllegalArgumentException.class,
        () -> new HazelcastClusterManager(candidate));
      assertEquals(modeFailure.getMessage(), managerFailure.getMessage());
      assertTrue(managerFailure.getMessage().contains("Hazelcast member is required"));
      assertTrue(managerFailure.getMessage().contains("HazelcastClusterManager(clientConfig)"));
      assertTrue(client.getLifecycleService().isRunning(), "Rejecting caller-owned input must not close it");
    }
    client.<String, String>getMap("caller-owned").put("key", "still usable");
    assertEquals("still usable", member.getMap("caller-owned").get("key"));
    client.shutdown();
    for (HazelcastInstance candidate : new HazelcastInstance[] {client, wrapped}) {
      assertTrue(assertThrows(IllegalArgumentException.class, () -> new MemberMode(candidate)).getMessage()
        .contains("HazelcastClusterManager(clientConfig)"));
      assertTrue(assertThrows(IllegalArgumentException.class, () -> new HazelcastClusterManager(candidate)).getMessage()
        .contains("HazelcastClusterManager(clientConfig)"));
    }
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
  public void shutdownRetryRestoresTheCallersInterruptFlag() throws Exception {
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicInteger attempts = new AtomicInteger();
    LifecycleService lifecycle = (LifecycleService) Proxy.newProxyInstance(LifecycleService.class.getClassLoader(),
      new Class<?>[] {LifecycleService.class}, (proxy, method, args) -> {
        if (method.getName().equals("isRunning")) return running.get();
        if (method.getName().equals("shutdown")) {
          if (attempts.incrementAndGet() == 1) throw new RejectedExecutionException("Transient shutdown rejection");
          running.set(false);
          return null;
        }
        throw new AssertionError("Unexpected lifecycle call: " + method.getName());
      });
    Config config = memberConfig();
    HazelcastInstance member = (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
      new Class<?>[] {HazelcastInstance.class}, (proxy, method, args) -> {
        if (method.getName().equals("getLifecycleService")) return lifecycle;
        if (method.getName().equals("getConfig")) return config;
        throw new AssertionError("Unexpected member call: " + method.getName());
      });
    MemberMode mode = new MemberMode(config, ignored -> member);
    mode.start(new HazelcastClusterManager(mode));
    CompletableFuture<Boolean> completed = new CompletableFuture<>();
    Thread caller = new Thread(() -> {
      Thread.currentThread().interrupt();
      try {
        mode.stop();
        completed.complete(Thread.currentThread().isInterrupted());
      } catch (Throwable failure) {
        completed.completeExceptionally(failure);
      }
    });
    caller.start();
    try {
      assertTrue(completed.get(5, TimeUnit.SECONDS));
      assertFalse(running.get());
      assertEquals(2, attempts.get());
    } finally {
      caller.join(5000);
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
    Config config = memberConfig()
      .addMapConfig(new MapConfig("default").setBackupCount(0).setStatisticsEnabled(false))
      .addMultiMapConfig(new MultiMapConfig("default").setBackupCount(0)
        .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST));
    HazelcastInstance member = factory.newHazelcastInstance(config);
    ClientMode mode = new ClientMode(clientConfig(member), factory::newHazelcastClient);
    HazelcastInstance client = mode.start(new HazelcastClusterManager(mode));
    assertEquals(1, member.getConfig().findMapConfig("__vertx.nodeInfo").getBackupCount());
    assertEquals(1, member.getConfig().findMapConfig("__vertx.haInfo").getBackupCount());
    assertEquals(MultiMapConfig.ValueCollectionType.SET,
      member.getConfig().findMultiMapConfig("__vertx.subs").getValueCollectionType());
    assertEquals(0, member.getConfig().findMapConfig("application-map").getBackupCount());
    assertFalse(member.getConfig().findMapConfig("application-map").isStatisticsEnabled());
    assertEquals(MultiMapConfig.ValueCollectionType.LIST,
      member.getConfig().findMultiMapConfig("application-multimap").getValueCollectionType());
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
  public void clientReadinessTimeoutUsesSystemPropertyAndClientConfigTakesPrecedence() {
    String property = ClientMode.HEALTH_CHECK_TIMEOUT_MILLIS;
    String previous = System.getProperty(property);
    System.setProperty(property, "40");
    try {
      ClientMode shortTimeout = healthMode(CompletableFuture::new);
      try {
        assertTimeoutPreemptively(Duration.ofMillis(750), () -> assertFalse(shortTimeout.isHealthy()));
      } finally {
        shortTimeout.stop();
      }
      ClientMode longerTimeout = healthMode(() -> {
        CompletableFuture<Object> result = new CompletableFuture<>();
        CompletableFuture.delayedExecutor(1200, TimeUnit.MILLISECONDS).execute(() -> result.complete("registration"));
        return result;
      }, config -> config.setProperty(property, "3000"));
      try {
        assertTrue(longerTimeout.isHealthy(),
            "Configured timeout must allow reads slower than the old one-second limit");
      } finally {
        longerTimeout.stop();
      }
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  @Test
  public void invalidHealthCheckTimeoutFailsBeforeCreatingAClient() {
    for (String timeout : new String[] {"0", "-1", "invalid", "1.5", "9223372036854775808"}) {
      ClientConfig config = new ClientConfig().setProperty(ClientMode.HEALTH_CHECK_TIMEOUT_MILLIS, timeout);
      config.getNetworkConfig().addAddress("127.0.0.1:5701");
      ClientMode mode = new ClientMode(config, ignored -> {
        fail("An invalid health-check timeout must be rejected before instance creation");
        return null;
      });
      assertTrue(assertThrows(IllegalArgumentException.class, () ->
          mode.start(new HazelcastClusterManager(mode))).getMessage()
        .contains(ClientMode.HEALTH_CHECK_TIMEOUT_MILLIS));
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
    return healthMode(read, config -> { });
  }

  private ClientMode healthMode(Supplier<CompletionStage<Object>> read, Consumer<ClientConfig> configure) {
    HazelcastInstance member = factory.newHazelcastInstance(memberConfig());
    ClientConfig clientConfig = clientConfig(member);
    configure.accept(clientConfig);
    ClientMode mode = new ClientMode(clientConfig, config -> {
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
  public void concurrentClientsCanInstallDefaultsOnMultipleMembers() throws Exception {
    Config config = memberConfig();
    HazelcastInstance first = factory.newHazelcastInstance(config);
    HazelcastInstance second = factory.newHazelcastInstance(config);
    ClientMode mode = new ClientMode(clientConfig(first), factory::newHazelcastClient);
    ClientMode another = new ClientMode(clientConfig(first), factory::newHazelcastClient);
    try {
      CompletableFuture.allOf(
        CompletableFuture.runAsync(() -> mode.start(new HazelcastClusterManager(mode))),
        CompletableFuture.runAsync(() -> another.start(new HazelcastClusterManager(another)))
      ).get(10, TimeUnit.SECONDS);
      for (HazelcastInstance member : new HazelcastInstance[] {first, second}) {
        for (String name : java.util.List.of("__vertx.haInfo", "__vertx.nodeInfo", "__vertx.clusterInfo")) {
          assertEquals(name, member.getConfig().findMapConfig(name).getName());
          assertEquals(1, member.getConfig().findMapConfig(name).getBackupCount());
        }
        assertEquals(1, member.getConfig().findMultiMapConfig("__vertx.subs").getBackupCount());
        assertEquals(MultiMapConfig.ValueCollectionType.SET,
          member.getConfig().findMultiMapConfig("__vertx.subs").getValueCollectionType());
      }
    } finally {
      try { mode.stop(); }
      finally { another.stop(); }
    }
  }

  @ParameterizedTest
  @EnumSource(ConfigurationSource.class)
  void bootstrapPreservesCustomConfigurationOnEveryMember(ConfigurationSource source) {
    Config config = memberConfig();
    if (source != ConfigurationSource.DYNAMIC) customizeInternalObjects(config, source == ConfigurationSource.WILDCARD);
    HazelcastInstance first = factory.newHazelcastInstance(config);
    HazelcastInstance second = factory.newHazelcastInstance(config);
    if (source == ConfigurationSource.DYNAMIC) customizeInternalObjects(first.getConfig(), false);
    ClientMode mode = new ClientMode(clientConfig(first), factory::newHazelcastClient);
    ClientMode another = new ClientMode(clientConfig(first), factory::newHazelcastClient);
    try {
      mode.start(new HazelcastClusterManager(mode));
      another.start(new HazelcastClusterManager(another));
      for (HazelcastInstance member : new HazelcastInstance[] {first, second}) {
        for (String name : java.util.List.of("__vertx.haInfo", "__vertx.nodeInfo", "__vertx.clusterInfo")) {
          MapConfig actual = member.getConfig().findMapConfig(name);
          assertEquals(source == ConfigurationSource.WILDCARD ? "__vertx.*" : name, actual.getName());
          assertEquals(2, actual.getBackupCount());
          assertFalse(actual.isStatisticsEnabled());
          assertEquals(EvictionPolicy.LRU, actual.getEvictionConfig().getEvictionPolicy());
          assertEquals(1000, actual.getEvictionConfig().getSize());
        }
        MultiMapConfig actual = member.getConfig().findMultiMapConfig("__vertx.subs");
        assertEquals(source == ConfigurationSource.WILDCARD ? "__vertx.*" : "__vertx.subs", actual.getName());
        assertEquals(2, actual.getBackupCount());
        assertFalse(actual.isStatisticsEnabled());
        assertFalse(actual.isBinary());
        assertEquals(MultiMapConfig.ValueCollectionType.SET, actual.getValueCollectionType());
      }
    } finally {
      try { mode.stop(); }
      finally { another.stop(); }
    }
  }

  enum Expiry { TIME_TO_LIVE, MAX_IDLE }

  @ParameterizedTest
  @EnumSource(ConfigurationSource.class)
  void bootstrapRejectsExpiringInternalMapConfiguration(ConfigurationSource source) {
    for (Expiry expiry : Expiry.values()) {
      Config config = memberConfig();
      // A catch-all wildcard with expiry is common on shared caching clusters.
      MapConfig expiring = new MapConfig(source == ConfigurationSource.WILDCARD ? "*" : "__vertx.clusterInfo");
      if (expiry == Expiry.TIME_TO_LIVE) expiring.setTimeToLiveSeconds(2);
      else expiring.setMaxIdleSeconds(2);
      if (source != ConfigurationSource.DYNAMIC) config.addMapConfig(expiring);
      HazelcastInstance member = factory.newHazelcastInstance(config);
      if (source == ConfigurationSource.DYNAMIC) member.getConfig().addMapConfig(expiring);
      ClientMode mode = new ClientMode(clientConfig(member), factory::newHazelcastClient);
      Throwable failure = assertThrows(IllegalStateException.class, () ->
          mode.start(new HazelcastClusterManager(mode)));
      while (failure.getCause() != null) failure = failure.getCause();
      assertTrue(failure instanceof IllegalArgumentException, String.valueOf(failure));
      assertTrue(failure.getMessage().contains("must not expire entries"), failure.getMessage());
      assertTrue(failure.getMessage().contains("'" + expiring.getName() + "'"), failure.getMessage());
      member.getLifecycleService().terminate();
    }
  }

  @Test
  void bootstrapOverridesExpiryInheritedFromTheDefaultMapConfiguration() {
    Config config = memberConfig().addMapConfig(new MapConfig("default").setTimeToLiveSeconds(2).setMaxIdleSeconds(2));
    HazelcastInstance member = factory.newHazelcastInstance(config);
    ClientMode mode = new ClientMode(clientConfig(member), factory::newHazelcastClient);
    try {
      mode.start(new HazelcastClusterManager(mode));
      for (String name : java.util.List.of("__vertx.haInfo", "__vertx.nodeInfo", "__vertx.clusterInfo")) {
        MapConfig actual = member.getConfig().findMapConfig(name);
        assertEquals(name, actual.getName());
        assertEquals(0, actual.getTimeToLiveSeconds());
        assertEquals(0, actual.getMaxIdleSeconds());
      }
      assertEquals(2, member.getConfig().findMapConfig("application-map").getTimeToLiveSeconds());
    } finally {
      mode.stop();
    }
  }

  private void customizeInternalObjects(Config config, boolean wildcard) {
    for (String name : wildcard ? java.util.List.of("__vertx.*")
      : java.util.List.of("__vertx.haInfo", "__vertx.nodeInfo", "__vertx.clusterInfo")) {
      MapConfig map = new MapConfig(name).setBackupCount(2).setStatisticsEnabled(false);
      map.getEvictionConfig().setEvictionPolicy(EvictionPolicy.LRU).setSize(1000);
      config.addMapConfig(map);
    }
    config.addMultiMapConfig(new MultiMapConfig(wildcard ? "__vertx.*" : "__vertx.subs")
      .setBackupCount(2).setStatisticsEnabled(false).setBinary(false));
  }

  @ParameterizedTest
  @EnumSource(ConfigurationSource.class)
  void bootstrapConflictClosesTheFactoryCreatedClient(ConfigurationSource source) {
    Config config = memberConfig();
    MultiMapConfig subscriptions = new MultiMapConfig(source == ConfigurationSource.WILDCARD ? "__vertx.*"
        : "__vertx.subs")
      .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
    if (source != ConfigurationSource.DYNAMIC) config.addMultiMapConfig(subscriptions);
    HazelcastInstance member = factory.newHazelcastInstance(config);
    if (source == ConfigurationSource.DYNAMIC) member.getConfig().addMultiMapConfig(subscriptions);
    AtomicReference<HazelcastInstance> created = new AtomicReference<>();
    ClientMode mode = new ClientMode(clientConfig(member), clientConfig -> {
      HazelcastInstance client = factory.newHazelcastClient(clientConfig);
      created.set(client);
      return client;
    });
    Throwable failure = assertThrows(IllegalStateException.class, () -> mode.start(new HazelcastClusterManager(mode)));
    while (failure.getCause() != null) failure = failure.getCause();
    assertTrue(failure.getMessage().contains("__vertx.subs must use SET values; found LIST"));
    assertNotNull(created.get());
    assertFalse(created.get().getLifecycleService().isRunning());
    assertTrue(member.getLifecycleService().isRunning());
    assertEquals(MultiMapConfig.ValueCollectionType.LIST,
      member.getConfig().findMultiMapConfig("__vertx.subs").getValueCollectionType());
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
