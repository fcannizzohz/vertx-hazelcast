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
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.test.HazelcastTestSupport;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationListener;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastNodeInfo;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastRegistrationInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Registry callbacks use real Hazelcast mock-network objects and a real cluster event executor.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
@Timeout(30)
public class RegistryEventTest {
  enum Mode { MEMBER, CLIENT }
  enum Publication { UPDATE, DEPARTURE, RECOVERY }

  private TestHazelcastFactory factory;
  private Vertx vertx;
  private HazelcastInstance dataMember;
  private RecordingManager manager;
  private Future<Void> leaving;
  private final Capture capture = new Capture();

  @BeforeEach
  public void setUp() {
    factory = new TestHazelcastFactory();
    vertx = Vertx.vertx();
  }

  @AfterEach
  public void tearDown() throws Exception {
    capture.releaseRepublish.countDown();
    capture.releasePublication.countDown();
    try {
      if (manager != null) await(leave());
    } finally {
      try { await(vertx.close()); }
      finally { factory.terminateAll(); }
    }
  }

  @ParameterizedTest
  @MethodSource("memberDepartures")
  public void delayedMemberDeparturePreservesAReusedNodeIdOnlyWhenAReplacementIsCurrent(Mode mode, boolean restarted) throws Exception {
    start(mode);
    String reusedId = "supplied-member";
    Config peerConfig = new Config().setClusterName(dataMember.getConfig().getClusterName())
      .setProperty("hazelcast.phone.home.enabled", "false").setProperty("hazelcast.shutdownhook.enabled", "false");
    peerConfig.getMemberAttributeConfig().setAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE, reusedId);
    HazelcastInstance old = factory.newHazelcastInstance(peerConfig);
    var oldMember = old.getCluster().getLocalMember();
    var registry = dataMember.<String, HazelcastNodeInfo>getMap("__vertx.nodeInfo");
    registry.put(reusedId, new HazelcastNodeInfo(new NodeInfo("127.0.0.1", 12345, null)));
    CountDownLatch occupied = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    manager.executeClusterEvent(() -> {
      occupied.countDown();
      waitFor(release);
    });
    assertTrue(occupied.await(5, TimeUnit.SECONDS));
    try {
      old.shutdown();
      HazelcastNodeInfo replacementInfo = new HazelcastNodeInfo(new NodeInfo("127.0.0.1", 23456, null));
      if (restarted) {
        HazelcastInstance replacement = factory.newHazelcastInstance(peerConfig);
        var replacementId = replacement.getCluster().getLocalMember().getUuid();
        assertNotEquals(oldMember.getUuid(), replacementId);
        HazelcastTestSupport.assertTrueEventually(() -> assertTrue(manager.getHazelcastInstance().getCluster()
          .getMembers().stream().anyMatch(member -> member.getUuid().equals(replacementId))), 5);
        registry.put(reusedId, replacementInfo);
      }
      dataMember.getMultiMap("__vertx.subs").put("member-address",
        new HazelcastRegistrationInfo(new RegistrationInfo(reusedId, 1, false)));
      assertEquals(1, capture.membershipListeners.size());
      var cluster = manager.getHazelcastInstance().getCluster();
      capture.membershipListeners.get(0).memberRemoved(
        new MembershipEvent(cluster, oldMember, MembershipEvent.MEMBER_REMOVED, cluster.getMembers()));
      release.countDown();
      flush();
      if (restarted) {
        assertEquals(replacementInfo.unwrap(), registry.get(reusedId).unwrap());
        assertTrue(manager.getNodes().contains(reusedId));
        assertFalse(capture.removedNodes.contains(reusedId));
        assertEquals(1, dataMember.getMultiMap("__vertx.subs").get("member-address").size());
      } else {
        HazelcastTestSupport.assertTrueEventually(() -> assertTrue(capture.removedNodes.contains(reusedId)), 5);
        assertFalse(registry.containsKey(reusedId));
        assertTrue(dataMember.getMultiMap("__vertx.subs").get("member-address").isEmpty());
      }
    } finally {
      release.countDown();
    }
  }

  static Stream<Arguments> memberDepartures() {
    return Stream.of(Mode.values()).flatMap(mode -> Stream.of(false, true).map(restarted -> Arguments.of(mode, restarted)));
  }

  @ParameterizedTest
  @MethodSource("publications")
  public void blockedPublicationDoesNotBlockLocalNodeInfoAndLeaveWaitsForIt(Mode mode, Publication publication) throws Exception {
    start(mode);
    NodeInfo expected = manager.getNodeInfo();
    capture.blockPublication.set(true);
    Promise<Void> publishing = Promise.promise();
    switch (publication) {
      case UPDATE:
        manager.setNodeInfo(expected, publishing);
        break;
      case DEPARTURE:
        removed(registryListener(), "departed");
        publishing.complete();
        break;
      case RECOVERY:
        manager.stateChanged(new LifecycleEvent(LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED));
        manager.stateChanged(new LifecycleEvent(LifecycleEvent.LifecycleState.CLIENT_CONNECTED));
        publishing.complete();
        break;
    }
    try {
      assertTrue(capture.publicationStarted.await(5, TimeUnit.SECONDS));
      CompletableFuture<NodeInfo> localRead = new CompletableFuture<>();
      vertx.runOnContext(ignored -> localRead.complete(manager.getNodeInfo()));
      assertEquals(expected, localRead.get(1, TimeUnit.SECONDS),
        "An event loop reading local node info must not wait for a remote registration write");
      Future<Void> closed = leave();
      assertFalse(closed.isComplete(), "Leave must wait for the in-flight publication");
      capture.releasePublication.countDown();
      await(publishing.future());
      await(closed);
      assertFalse(dataMember.getMap("__vertx.nodeInfo").containsKey(manager.getNodeId()));
      assertTrue(dataMember.getLifecycleService().isRunning());
    } finally {
      capture.releasePublication.countDown();
    }
  }

  static Stream<Arguments> publications() {
    return Stream.of(Mode.values()).flatMap(mode -> Stream.of(Publication.values())
      .filter(publication -> publication != Publication.RECOVERY || mode == Mode.CLIENT)
      .map(publication -> Arguments.of(mode, publication)));
  }

  @ParameterizedTest
  @EnumSource(Mode.class)
  public void addedRemovedExpiredAndMergedShareOneQueueWithoutBlockingTheCallbackThread(Mode mode) throws Exception {
    start(mode);
    Object listener = registryListener();
    CountDownLatch occupied = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicReference<Thread> eventThread = new AtomicReference<>();
    manager.executeClusterEvent(() -> {
      eventThread.set(Thread.currentThread());
      occupied.countDown();
      waitFor(release);
    });
    assertTrue(occupied.await(5, TimeUnit.SECONDS));
    ExecutorService callbackSource = Executors.newSingleThreadExecutor(task -> new Thread(task, "hz-test-event-source"));
    try {
      manager.recording = true;
      callbackSource.submit(() -> {
        added(listener, "first");
        removed(listener, "first");
        expired(listener, "expired");
        manager.stateChanged(new LifecycleEvent(LifecycleEvent.LifecycleState.MERGED));
      }).get(5, TimeUnit.SECONDS);
      assertTrue(manager.events.isEmpty(), "Hazelcast callbacks must only enqueue work");
      release.countDown();
      flush();
      assertEquals(List.of("added:first", "removed:first", "removed:expired", "merged"), manager.events);
      assertTrue(manager.threads.stream().allMatch(thread -> thread == eventThread.get()));
    } finally {
      release.countDown();
      callbackSource.shutdownNow();
    }
  }

  @ParameterizedTest
  @EnumSource(Mode.class)
  public void leaveDrainsInFlightRepublishingEvenAfterAnExecutorWaitTimesOut(Mode mode) throws Exception {
    start(mode);
    Promise<Void> registered = Promise.promise();
    manager.addRegistration("own-address", new RegistrationInfo(manager.getNodeId(), 1, false), registered);
    await(registered.future());
    Field executorField = HazelcastClusterManager.class.getDeclaredField("eventExecutor");
    executorField.setAccessible(true);
    ExecutorService actual = (ExecutorService) executorField.get(manager);
    executorField.set(manager, new TimeoutOnceExecutor(actual, capture.nextShutdownStep));

    AtomicReference<Thread> eventThread = new AtomicReference<>();
    manager.executeClusterEvent(() -> eventThread.set(Thread.currentThread()));
    flush();
    capture.blockRepublish.set(true);
    try {
      removed(registryListener(), "departed");
      assertTrue(capture.republishStarted.await(5, TimeUnit.SECONDS));
      assertSame(eventThread.get(), capture.republishThread.get(),
        "Subscription republishing must not escape to a Vert.x worker after the event executor drains");
      Future<Void> closed = leave();
      assertEquals("drain", capture.nextShutdownStep.get(5, TimeUnit.SECONDS),
        "A timed-out wait must not allow cleanup to overtake an in-flight callback");
      assertFalse(closed.isComplete());
      assertTrue(dataMember.getMap("__vertx.nodeInfo").containsKey(manager.getNodeId()));
      capture.releaseRepublish.countDown();
      await(closed);
      assertFalse(dataMember.getMap("__vertx.nodeInfo").containsKey(manager.getNodeId()));
      assertFalse(dataMember.getMultiMap("__vertx.subs").values().stream()
        .anyMatch(value -> ((HazelcastRegistrationInfo) value).unwrap().nodeId().equals(manager.getNodeId())));
      assertTrue(dataMember.getLifecycleService().isRunning(), "Leaving must preserve an externally owned member");
      assertEquals(capture.registrations, capture.removals);
    } finally {
      capture.releaseRepublish.countDown();
    }
  }

  @ParameterizedTest
  @EnumSource(Mode.class)
  public void delayedRemovalPreservesAnAlreadyPublishedReplacement(Mode mode) throws Exception {
    start(mode);
    CountDownLatch occupied = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    manager.executeClusterEvent(() -> {
      occupied.countDown();
      waitFor(release);
    });
    assertTrue(occupied.await(5, TimeUnit.SECONDS));
    try {
      manager.recording = true;
      removed(registryListener(), "replacement");
      HazelcastNodeInfo replacement = new HazelcastNodeInfo(new NodeInfo("127.0.0.1", 12345, null));
      dataMember.<String, HazelcastNodeInfo>getMap("__vertx.nodeInfo").put("replacement", replacement);
      release.countDown();
      flush();
      assertEquals(replacement.unwrap(), dataMember.<String, HazelcastNodeInfo>getMap("__vertx.nodeInfo").get("replacement").unwrap());
      assertFalse(manager.events.contains("removed:replacement"));
    } finally {
      release.countDown();
    }
  }

  @ParameterizedTest
  @EnumSource(Mode.class)
  public void mergeReconciliationDoesNotAnnounceADelayedAdditionTwice(Mode mode) throws Exception {
    start(mode);
    CountDownLatch occupied = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    manager.executeClusterEvent(() -> {
      occupied.countDown();
      waitFor(release);
    });
    assertTrue(occupied.await(5, TimeUnit.SECONDS));
    try {
      manager.stateChanged(new LifecycleEvent(LifecycleEvent.LifecycleState.MERGED));
      dataMember.<String, HazelcastNodeInfo>getMap("__vertx.nodeInfo")
        .put("merged-peer", new HazelcastNodeInfo(new NodeInfo("127.0.0.1", 12345, null)));
      added(registryListener(), "merged-peer");
      release.countDown();
      flush();
      assertEquals(1, capture.addedNodes.stream().filter("merged-peer"::equals).count());
    } finally {
      release.countDown();
    }
  }

  private void start(Mode mode) throws Exception {
    Config config = new Config().setClusterName("registry-events-" + UUID.randomUUID());
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.setProperty("hazelcast.shutdownhook.enabled", "false");
    HazelcastMode lifecycle;
    if (mode == Mode.MEMBER) {
      config.getMemberAttributeConfig().setAttribute(HazelcastClusterManager.NODE_ID_ATTRIBUTE, UUID.randomUUID().toString());
      dataMember = factory.newHazelcastInstance(config);
      lifecycle = new MemberMode(wrap(dataMember));
    } else {
      dataMember = factory.newHazelcastInstance(config);
      ClientConfig client = new ClientConfig().setClusterName(config.getClusterName());
      var address = dataMember.getCluster().getLocalMember().getAddress();
      client.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
      lifecycle = new ClientMode(client, cfg -> wrap(factory.newHazelcastClient(cfg)));
    }
    manager = new RecordingManager(lifecycle, capture);
    manager.registrationListener(new RegistrationListener() { });
    manager.nodeListener(new NodeListener() {
      public void nodeAdded(String id) { capture.addedNodes.add(id); }
      public void nodeLeft(String id) { capture.removedNodes.add(id); }
    });
    manager.init(vertx);
    Promise<Void> joined = Promise.promise();
    manager.join(joined);
    await(joined.future());
    Promise<Void> published = Promise.promise();
    manager.setNodeInfo(new NodeInfo("127.0.0.1", 1234, null), published);
    await(published.future());
  }

  private HazelcastInstance wrap(HazelcastInstance actual) {
    return (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
      new Class<?>[] {HazelcastInstance.class}, (proxy, method, args) -> {
        try {
          Object result = method.invoke(actual, args);
          if (method.getName().equals("getCluster")) {
            return Proxy.newProxyInstance(Cluster.class.getClassLoader(), new Class<?>[] {Cluster.class},
              (clusterProxy, clusterMethod, clusterArgs) -> {
                try {
                  Object value = clusterMethod.invoke(result, clusterArgs);
                  if (clusterMethod.getName().equals("addMembershipListener")) {
                    capture.membershipListeners.add((MembershipListener) clusterArgs[0]);
                  }
                  return value;
                } catch (InvocationTargetException failure) { throw failure.getCause(); }
              });
          }
          if (method.getName().equals("getMap") && "__vertx.nodeInfo".equals(args[0])) {
            return Proxy.newProxyInstance(IMap.class.getClassLoader(), new Class<?>[] {IMap.class}, (mapProxy, mapMethod, mapArgs) -> {
              if ((mapMethod.getName().equals("put") || mapMethod.getName().equals("executeOnKey"))
                && capture.blockPublication.compareAndSet(true, false)) {
                capture.publicationStarted.countDown();
                waitFor(capture.releasePublication);
              }
              try {
                Object value = mapMethod.invoke(result, mapArgs);
                if (mapMethod.getName().equals("addEntryListener")) {
                  capture.listeners.add(mapArgs[0]);
                  capture.registrations.add((UUID) value);
                } else if (mapMethod.getName().equals("removeEntryListener")) {
                  capture.removals.add((UUID) mapArgs[0]);
                }
                return value;
              } catch (InvocationTargetException failure) { throw failure.getCause(); }
            });
          }
          if (method.getName().equals("getMultiMap") && "__vertx.subs".equals(args[0])) {
            return Proxy.newProxyInstance(MultiMap.class.getClassLoader(), new Class<?>[] {MultiMap.class}, (mapProxy, mapMethod, mapArgs) -> {
              if (mapMethod.getName().equals("put") && capture.blockRepublish.get()) {
                capture.republishThread.set(Thread.currentThread());
                capture.republishStarted.countDown();
                waitFor(capture.releaseRepublish);
              }
              try { return mapMethod.invoke(result, mapArgs); }
              catch (InvocationTargetException failure) { throw failure.getCause(); }
            });
          }
          return result;
        } catch (InvocationTargetException failure) { throw failure.getCause(); }
      });
  }

  private Object registryListener() {
    assertEquals(1, capture.listeners.size(), "All node registry event types must use one Hazelcast registration");
    return capture.listeners.get(0);
  }

  @SuppressWarnings("unchecked")
  private void added(Object listener, String key) {
    ((EntryAddedListener<String, HazelcastNodeInfo>) listener).entryAdded(event(EntryEventType.ADDED, key));
  }

  @SuppressWarnings("unchecked")
  private void removed(Object listener, String key) {
    ((EntryRemovedListener<String, HazelcastNodeInfo>) listener).entryRemoved(event(EntryEventType.REMOVED, key));
  }

  @SuppressWarnings("unchecked")
  private void expired(Object listener, String key) {
    ((EntryExpiredListener<String, HazelcastNodeInfo>) listener).entryExpired(event(EntryEventType.EXPIRED, key));
  }

  private EntryEvent<String, HazelcastNodeInfo> event(EntryEventType type, String key) {
    return new EntryEvent<>("__vertx.nodeInfo", dataMember.getCluster().getLocalMember(), type.getType(), key, null);
  }

  private void flush() throws Exception {
    CompletableFuture<Void> done = new CompletableFuture<>();
    manager.executeClusterEvent(() -> done.complete(null));
    done.get(5, TimeUnit.SECONDS);
  }

  private Future<Void> leave() {
    if (leaving == null) {
      Promise<Void> left = Promise.promise();
      manager.leave(left);
      leaving = left.future();
    }
    return leaving;
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  private static void waitFor(CountDownLatch latch) {
    try { assertTrue(latch.await(10, TimeUnit.SECONDS)); }
    catch (InterruptedException failure) { throw new AssertionError(failure); }
  }

  private static class Capture {
    final List<Object> listeners = new CopyOnWriteArrayList<>();
    final List<String> addedNodes = new CopyOnWriteArrayList<>();
    final List<String> removedNodes = new CopyOnWriteArrayList<>();
    final List<MembershipListener> membershipListeners = new CopyOnWriteArrayList<>();
    final List<UUID> registrations = new CopyOnWriteArrayList<>();
    final List<UUID> removals = new CopyOnWriteArrayList<>();
    final AtomicBoolean blockRepublish = new AtomicBoolean();
    final AtomicBoolean blockPublication = new AtomicBoolean();
    final CountDownLatch publicationStarted = new CountDownLatch(1);
    final CountDownLatch releasePublication = new CountDownLatch(1);
    final CountDownLatch republishStarted = new CountDownLatch(1);
    final CountDownLatch releaseRepublish = new CountDownLatch(1);
    final AtomicReference<Thread> republishThread = new AtomicReference<>();
    final CompletableFuture<String> nextShutdownStep = new CompletableFuture<>();
  }

  private static class RecordingManager extends HazelcastClusterManager {
    final List<String> events = new CopyOnWriteArrayList<>();
    final List<Thread> threads = new CopyOnWriteArrayList<>();
    final Capture capture;
    volatile boolean recording;
    RecordingManager(HazelcastMode mode, Capture capture) { super(mode); this.capture = capture; }
    private void record(String label) { if (recording) { events.add(label); threads.add(Thread.currentThread()); } }
    @Override void handleMemberAdded(String id) { record("added:" + id); super.handleMemberAdded(id); }
    @Override void handleMembersRemoved(Set<String> ids) {
      ids.forEach(id -> record("removed:" + id));
      super.handleMembersRemoved(ids);
    }
    @Override public List<String> getNodes() { record("merged"); return super.getNodes(); }
    @Override void cleanSubs(Set<String> ids) {
      if (!isActive() && ids.contains(getNodeId())) capture.nextShutdownStep.complete("cleanup");
      super.cleanSubs(ids);
    }
  }

  private static class TimeoutOnceExecutor extends AbstractExecutorService {
    private final ExecutorService delegate;
    private final CompletableFuture<String> nextStep;
    private final AtomicBoolean first = new AtomicBoolean(true);
    TimeoutOnceExecutor(ExecutorService delegate, CompletableFuture<String> nextStep) { this.delegate = delegate; this.nextStep = nextStep; }
    public void execute(Runnable action) { delegate.execute(action); }
    public void shutdown() { delegate.shutdown(); }
    public List<Runnable> shutdownNow() { return delegate.shutdownNow(); }
    public boolean isShutdown() { return delegate.isShutdown(); }
    public boolean isTerminated() { return delegate.isTerminated(); }
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      if (first.compareAndSet(true, false)) return false;
      nextStep.complete("drain");
      return delegate.awaitTermination(timeout, unit);
    }
  }
}
