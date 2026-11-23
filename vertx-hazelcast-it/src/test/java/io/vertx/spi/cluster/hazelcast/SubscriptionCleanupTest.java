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
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.test.HazelcastTestSupport;
import io.vertx.core.VertxException;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationListener;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastRegistrationInfo;
import io.vertx.spi.cluster.hazelcast.impl.SubsMapHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
@Timeout(30)
public class SubscriptionCleanupTest {
  enum Mode { MEMBER, CLIENT }
  private TestHazelcastFactory factory;
  private HazelcastInstance[] members;

  @BeforeEach
  void startCluster() {
    factory = new TestHazelcastFactory();
    Config config = new Config().setClusterName("subscription-cleanup-" + UUID.randomUUID())
      .setProperty("hazelcast.phone.home.enabled", "false").setProperty("hazelcast.shutdownhook.enabled", "false");
    members = new HazelcastInstance[] {factory.newHazelcastInstance(config), factory.newHazelcastInstance(config),
      factory.newHazelcastInstance(config)};
  }

  @AfterEach
  void stopCluster() { factory.terminateAll(); }

  private HazelcastInstance observer(Mode mode) {
    if (mode == Mode.MEMBER) return members[0];
    ClientConfig config = new ClientConfig().setClusterName(members[0].getConfig().getClusterName());
    var address = members[0].getCluster().getLocalMember().getAddress();
    config.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
    return factory.newHazelcastClient(config);
  }

  @ParameterizedTest
  @EnumSource(Mode.class)
  void cleanupFiltersOnMembersWithoutDownloadingRegistrationsAtTheObserver(Mode mode) throws Exception {
    MultiMap<String, HazelcastRegistrationInfo> subscriptions = members[0].getMultiMap("__vertx.subs");
    for (int i = 0; i < 64; i++) {
      subscriptions.put("address-" + i, registration("live", i));
      subscriptions.put("address-" + i, registration("dead-a", i));
      subscriptions.put("address-" + i, registration("dead-b", i));
    }
    HazelcastTestSupport.waitClusterForSafeState(members[0]);
    Set<UUID> owners = new HashSet<>();
    for (String address : subscriptions.keySet()) {
      var owner = members[0].getPartitionService().getPartition(address).getOwner();
      assertNotNull(owner, "Every subscription partition must have an owner");
      owners.add(owner.getUuid());
    }
    assertEquals(3, owners.size(), "Exercise registrations owned by different members");
    CountDownLatch removed = new CountDownLatch(128);
    AtomicBoolean removedLiveRegistration = new AtomicBoolean();
    UUID listener = subscriptions.addEntryListener(new EntryAdapter<>() {
      @Override
      public void entryRemoved(EntryEvent<String, HazelcastRegistrationInfo> event) {
        if (event.getOldValue().unwrap().nodeId().equals("live")) removedLiveRegistration.set(true);
        removed.countDown();
      }
    }, true);
    AtomicInteger submissions = new AtomicInteger();
    SubsMapHelper helper = helper(observer(mode), submissions, null);
    try {
      helper.removeAllForNodes(Set.of());
      assertEquals(0, submissions.get(), "Empty cleanup must not submit a scan");
      helper.removeAllForNodes(Set.of("dead-a", "dead-b"));
      assertEquals(1, submissions.get());
      assertTrue(removed.await(5, TimeUnit.SECONDS), "Normal removal events must still be delivered");
      assertFalse(removedLiveRegistration.get());
      for (int i = 0; i < 64; i++) assertEquals(Set.of(registration("live", i)),
          new HashSet<>(subscriptions.get("address-" + i)));
      helper.removeAllForNodes(Set.of("dead-a", "dead-b"));
      assertEquals(64, subscriptions.size(), "Repeated cleanup must be harmless");
    } finally {
      try { helper.close(); }
      finally { subscriptions.removeEntryListener(listener); }
    }
  }

  @ParameterizedTest
  @EnumSource(Mode.class)
  void cleanupRetriesIfTheTaskMemberLeaves(Mode mode) {
    members[0].getMultiMap("__vertx.subs").put("address", registration("dead", 1));
    AtomicInteger submissions = new AtomicInteger();
    SubsMapHelper helper = helper(observer(mode), submissions,
        new MemberLeftException(members[0].getCluster().getLocalMember()));
    try {
      helper.removeAllForNodes(Set.of("dead"));
      assertEquals(2, submissions.get());
      assertEquals(0, members[0].getMultiMap("__vertx.subs").size());
    } finally { helper.close(); }
  }

  @ParameterizedTest
  @EnumSource(Mode.class)
  void taskFailuresAreReportedWithoutAnUnboundedRetry(Mode mode) {
    RuntimeException expected = new IllegalStateException("Cleanup task failed");
    AtomicInteger submissions = new AtomicInteger();
    SubsMapHelper helper = helper(observer(mode), submissions, expected);
    try {
      assertSame(expected, assertThrows(VertxException.class, () ->
          helper.removeAllForNodes(Set.of("dead"))).getCause());
      assertEquals(1, submissions.get());
    } finally { helper.close(); }
  }

  @ParameterizedTest
  @EnumSource(Mode.class)
  void cleanupWaitIsBoundedEvenWhenTheWaitingThreadIsInterrupted(Mode mode) {
    AtomicInteger submissions = new AtomicInteger();
    SubsMapHelper helper = helper(observer(mode), submissions, null, true);
    try {
      Thread waiting = Thread.currentThread();
      // Interrupt during the wait, not before: a Hazelcast client rejects proxy calls from an interrupted thread.
      CompletableFuture.runAsync(waiting::interrupt, CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS));
      long start = System.nanoTime();
      VertxException failure = assertThrows(VertxException.class, () -> helper.removeAllForNodes(Set.of("dead"), 500));
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      assertTrue(Thread.interrupted(), "The interrupt must be restored");
      assertTrue(failure.getMessage().contains("Timed out"), failure.getMessage());
      assertTrue(elapsedMillis < 5000, "Waited " + elapsedMillis + " ms");
      assertEquals(1, submissions.get());
    } finally { helper.close(); }
  }

  private static HazelcastRegistrationInfo registration(String nodeId, long sequence) {
    return new HazelcastRegistrationInfo(new RegistrationInfo(nodeId, sequence, false));
  }

  private SubsMapHelper helper(HazelcastInstance observer, AtomicInteger submissions, Throwable firstFailure) {
    return helper(observer, submissions, firstFailure, false);
  }

  private SubsMapHelper helper(HazelcastInstance observer, AtomicInteger submissions, Throwable firstFailure,
      boolean hang) {
    HazelcastInstance guarded = (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
      new Class<?>[] {HazelcastInstance.class}, (proxy, method, args) -> {
        try {
          Object result = method.invoke(observer, args);
          if (method.getName().equals("getMultiMap")) {
            return Proxy.newProxyInstance(MultiMap.class.getClassLoader(), new Class<?>[] {MultiMap.class},
              (mapProxy, mapMethod, mapArgs) -> {
                if (Set.of("entrySet", "keySet", "values", "get").contains(mapMethod.getName())) {
                  throw new AssertionError("Cleanup must not read registrations at the observer: "
                      + mapMethod.getName());
                }
                try { return mapMethod.invoke(result, mapArgs); }
                catch (InvocationTargetException failure) { throw failure.getCause(); }
              });
          }
          if (method.getName().equals("getExecutorService")) {
            return Proxy.newProxyInstance(IExecutorService.class.getClassLoader(),
                new Class<?>[] {IExecutorService.class},
              (executorProxy, executorMethod, executorArgs) -> {
                if (executorMethod.getName().equals("submit") && hang) {
                  submissions.incrementAndGet();
                  return new CompletableFuture<Void>();
                }
                if (executorMethod.getName().equals("submit") && submissions.getAndIncrement() == 0
                    && firstFailure != null) {
                  return CompletableFuture.failedFuture(firstFailure);
                }
                try { return executorMethod.invoke(result, executorArgs); }
                catch (InvocationTargetException failure) { throw failure.getCause(); }
              });
          }
          return result;
        } catch (InvocationTargetException failure) { throw failure.getCause(); }
      });
    return new SubsMapHelper(guarded, new RegistrationListener() {
      @Override public boolean wantsUpdatesFor(String address) { return false; }
    });
  }
}
