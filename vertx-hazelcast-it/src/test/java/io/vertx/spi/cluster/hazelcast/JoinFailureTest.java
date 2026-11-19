/*
 * Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it)
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 */

package io.vertx.spi.cluster.hazelcast;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.test.HazelcastTestSupport;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationListener;
import io.vertx.spi.cluster.hazelcast.impl.ConversionUtils;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastObjectProviderImpl;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastRegistrationInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
@Timeout(30)
public class JoinFailureTest {
  enum Kind { MEMBER, CLIENT, EXTERNAL_MEMBER }
  enum Stage { VALIDATION, START_BEFORE_INSTANCE, START_AFTER_INSTANCE, SUBS_LISTENER, NODE_INFO_MAP, REGISTRY_LISTENER, JOINED, PROVIDER }

  private final RuntimeException expected = new IllegalStateException("original join failure");
  private final RuntimeException leavingFailure = new IllegalStateException("listener cleanup failure");
  private final RuntimeException cleanupFailure = new IllegalStateException("registration cleanup failure");
  private final RuntimeException stopFailure = new IllegalStateException("instance shutdown failure");
  private final AtomicBoolean failNext = new AtomicBoolean(true);
  private final List<ExecutorService> executors = new ArrayList<>();
  private final List<HazelcastInstance> ownedInstances = new ArrayList<>();
  private final List<String> addedListeners = new CopyOnWriteArrayList<>();
  private final List<String> removedListeners = new CopyOnWriteArrayList<>();
  private TestHazelcastFactory factory;
  private Vertx vertx;
  private HazelcastInstance backend;
  private HazelcastClusterManager manager;
  private FailingMode mode;
  private Stage stage;
  private boolean failCleanup;
  private final String clusterName = "join-failure-" + UUID.randomUUID();

  @BeforeEach
  public void setUp() {
    factory = new TestHazelcastFactory();
    vertx = Vertx.vertx();
  }

  @AfterEach
  public void tearDown() throws Exception {
    try {
      if (manager != null) await(leave());
    } finally {
      try { await(vertx.close()); }
      finally { factory.terminateAll(); }
    }
  }

  static Stream<Arguments> failures() {
    return Stream.of(Kind.values()).flatMap(kind -> Stream.of(Stage.values()).map(stage -> Arguments.of(kind, stage)));
  }

  @ParameterizedTest(name = "{0}: {1}")
  @MethodSource("failures")
  public void failedJoinRollsBackAndTheSameManagerCanRetry(Kind kind, Stage failureStage) throws Exception {
    initialize(kind, failureStage);
    ExecutionException failure = assertThrows(ExecutionException.class, () -> await(join()));
    assertSame(expected, failure.getCause());
    assertRolledBack();
    await(leave());
    assertTrue(backend.getLifecycleService().isRunning());

    await(join());
    Promise<Void> published = Promise.promise();
    manager.setNodeInfo(new NodeInfo("127.0.0.1", 1234, null), published);
    await(published.future());
    assertTrue(manager.isActive());
    HazelcastTestSupport.assertTrueEventually(() -> assertTrue(manager.isHealthy()), 10);
    assertTrue(manager.getNodes().contains(manager.getNodeId()));
    Promise<Counter> counter = Promise.promise();
    manager.getCounter("retry-counter", counter);
    assertEquals(1L, await(await(counter.future()).incrementAndGet()));
    await(leave());
    assertTrue(backend.getLifecycleService().isRunning());
    assertExecutorsStopped();
  }

  @Test
  public void cleanupErrorsAreSuppressedWithoutReplacingTheJoinFailure() throws Exception {
    failCleanup = true;
    initialize(Kind.CLIENT, Stage.JOINED);
    ExecutionException failure = assertThrows(ExecutionException.class, () -> await(join()));
    assertSame(expected, failure.getCause());
    assertEquals(List.of(leavingFailure, cleanupFailure, stopFailure), List.of(expected.getSuppressed()));
    assertRolledBack();
    await(leave());
  }

  private void initialize(Kind kind, Stage failureStage) throws Exception {
    stage = failureStage;
    Config backendConfig = config();
    if (kind == Kind.EXTERNAL_MEMBER) {
      backendConfig.getMemberAttributeConfig().setAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE, UUID.randomUUID().toString());
    }
    backend = factory.newHazelcastInstance(backendConfig);
    HazelcastMode delegate;
    if (kind == Kind.EXTERNAL_MEMBER) {
      delegate = new MemberMode(wrap(backend));
    } else if (kind == Kind.MEMBER) {
      delegate = new MemberMode(config(), cfg -> {
        HazelcastInstance instance = factory.newHazelcastInstance(cfg);
        ownedInstances.add(instance);
        return wrap(instance);
      });
    } else {
      ClientConfig client = new ClientConfig().setClusterName(clusterName);
      var address = backend.getCluster().getLocalMember().getAddress();
      client.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
      delegate = new ClientMode(client, cfg -> {
        HazelcastInstance instance = factory.newHazelcastClient(cfg);
        ownedInstances.add(instance);
        return wrap(instance);
      });
    }
    mode = new FailingMode(delegate);
    manager = new HazelcastClusterManager(mode);
    manager.init(vertx);
    manager.registrationListener(new RegistrationListener() { });
    manager.nodeListener(new NodeListener() {
      public void nodeAdded(String id) { }
      public void nodeLeft(String id) { }
    });
    setField("objectProvider", new HazelcastObjectProviderImpl() {
      @Override
      public void onJoin(VertxInternal vertex, ConversionUtils conversion, HazelcastInstance instance, ExecutorService releaseExecutor) {
        super.onJoin(vertex, conversion, instance, releaseExecutor);
        failAt(Stage.PROVIDER);
      }
    });
  }

  private Config config() {
    return new Config().setClusterName(clusterName).setProperty("hazelcast.phone.home.enabled", "false")
      .setProperty("hazelcast.shutdownhook.enabled", "false");
  }

  private void assertRolledBack() throws Exception {
    assertFalse(manager.isActive());
    assertFalse(manager.isHealthy());
    assertNull(manager.getHazelcastInstance());
    assertNull(manager.getNodeId());
    assertNull(manager.getNodeInfo());
    for (String field : List.of("eventExecutor", "lockReleaseExec", "subsMapHelper", "nodeInfoMap")) {
      assertNull(getField(field), "Partial join state must be cleared: " + field);
    }
    assertExecutorsStopped();
    assertTrue(ownedInstances.stream().noneMatch(instance -> instance.getLifecycleService().isRunning()));
    assertEquals(new java.util.TreeSet<>(addedListeners), new java.util.TreeSet<>(removedListeners),
      "All successfully registered listeners must be removed");
    assertEquals(addedListeners.size(), removedListeners.size());
    assertTrue(backend.getLifecycleService().isRunning());
    assertTrue(backend.getMap("__vertx.nodeInfo").isEmpty());
    assertEquals(0, backend.getMultiMap("__vertx.subs").size());
  }

  private void assertExecutorsStopped() throws Exception {
    for (ExecutorService executor : executors) {
      assertTrue(executor.isShutdown());
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "A failed join must not retain executor threads");
    }
  }

  private Future<Void> join() {
    Promise<Void> joined = Promise.promise();
    manager.join(joined);
    return joined.future();
  }

  private Future<Void> leave() {
    Promise<Void> left = Promise.promise();
    manager.leave(left);
    return left.future();
  }

  private Object getField(String name) throws Exception {
    Field field = HazelcastClusterManager.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(manager);
  }

  private void setField(String name, Object value) throws Exception {
    Field field = HazelcastClusterManager.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(manager, value);
  }

  private void failAt(Stage candidate) {
    if (stage == candidate && failNext.compareAndSet(true, false)) throw expected;
  }

  private HazelcastInstance wrap(HazelcastInstance actual) {
    return (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
      new Class<?>[] {HazelcastInstance.class}, (proxy, method, args) -> {
        try {
          if (method.getName().equals("getMap") && "__vertx.nodeInfo".equals(args[0])) failAt(Stage.NODE_INFO_MAP);
          Object result = method.invoke(actual, args);
          if (method.getName().equals("getLifecycleService")) return wrapListeners(result, LifecycleService.class, "lifecycle");
          if (method.getName().equals("getCluster")) return wrapListeners(result, Cluster.class, "membership");
          if (method.getName().equals("getMap") && "__vertx.nodeInfo".equals(args[0])) return wrapListeners(result, IMap.class, "registry");
          if (method.getName().equals("getMultiMap") && "__vertx.subs".equals(args[0])) return wrapListeners(result, MultiMap.class, "subscriptions");
          return result;
        } catch (InvocationTargetException failure) { throw failure.getCause(); }
      });
  }

  private Object wrapListeners(Object actual, Class<?> api, String type) {
    return Proxy.newProxyInstance(api.getClassLoader(), new Class<?>[] {api}, (proxy, method, args) -> {
      String name = method.getName();
      if (name.equals("addEntryListener")) {
        if (type.equals("subscriptions")) failAt(Stage.SUBS_LISTENER);
        if (type.equals("registry")) failAt(Stage.REGISTRY_LISTENER);
      }
      try {
        Object result = method.invoke(actual, args);
        if (name.equals("addEntryListener") || name.equals("addMembershipListener") || name.equals("addLifecycleListener")) {
          addedListeners.add(type + ":" + result);
        } else if (name.equals("removeEntryListener") || name.equals("removeMembershipListener") || name.equals("removeLifecycleListener")) {
          removedListeners.add(type + ":" + args[0]);
        }
        return result;
      } catch (InvocationTargetException failure) { throw failure.getCause(); }
    });
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  private class FailingMode implements HazelcastMode {
    final HazelcastMode delegate;
    FailingMode(HazelcastMode delegate) { this.delegate = delegate; }
    public void validateConfiguration() { failAt(Stage.VALIDATION); delegate.validateConfiguration(); }
    public HazelcastInstance start(HazelcastClusterManager owner) {
      try {
        CountDownLatch started = new CountDownLatch(2);
        for (String field : List.of("eventExecutor", "lockReleaseExec")) {
          ExecutorService executor = (ExecutorService) getField(field);
          executors.add(executor);
          executor.execute(started::countDown);
        }
        assertTrue(started.await(5, TimeUnit.SECONDS));
      } catch (Exception failure) { throw new AssertionError(failure); }
      failAt(Stage.START_BEFORE_INSTANCE);
      HazelcastInstance instance = delegate.start(owner);
      failAt(Stage.START_AFTER_INSTANCE);
      return instance;
    }
    public String nodeId() { return delegate.nodeId(); }
    public void joined(HazelcastClusterManager owner) {
      delegate.joined(owner);
      if (stage == Stage.JOINED && failNext.compareAndSet(true, false)) {
        delegate.publishNodeInfo(owner, new NodeInfo("127.0.0.1", 1234, null));
        owner.getHazelcastInstance().getMultiMap("__vertx.subs")
          .put("failed-join", new HazelcastRegistrationInfo(new RegistrationInfo(owner.getNodeId(), 1, false)));
        throw expected;
      }
    }
    public List<String> getNodes(HazelcastClusterManager owner) { return delegate.getNodes(owner); }
    public void leaving(HazelcastClusterManager owner) {
      delegate.leaving(owner);
      if (failCleanup) throw leavingFailure;
    }
    public void cleanup(HazelcastClusterManager owner) {
      delegate.cleanup(owner);
      if (failCleanup) throw cleanupFailure;
    }
    public void stop() {
      delegate.stop();
      if (failCleanup) throw stopFailure;
    }
    public void publishNodeInfo(HazelcastClusterManager owner, NodeInfo info) { delegate.publishNodeInfo(owner, info); }
    public boolean isHealthy() { return delegate.isHealthy(); }
    public boolean isRecovering() { return delegate.isRecovering(); }
    public void stateChanged(HazelcastClusterManager owner, LifecycleEvent event) { delegate.stateChanged(owner, event); }
    public ClientConfig clientConfig() { return delegate.clientConfig(); }
  }
}
