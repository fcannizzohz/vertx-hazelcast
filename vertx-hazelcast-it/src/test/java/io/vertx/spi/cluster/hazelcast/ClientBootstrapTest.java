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

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MemberLeftException;
import io.vertx.core.VertxException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
@Timeout(10)
public class ClientBootstrapTest {

  @Test
  void aMemberLeavingDuringBootstrapIsSkipped() {
    HazelcastInstance instance = cluster(Map.of(
      member("left"), CompletableFuture.<Object>failedFuture(new MemberLeftException("left during bootstrap")),
      member("surviving"), CompletableFuture.<Object>completedFuture(null)));
    assertDoesNotThrow(() -> ClientMode.installConfiguration(instance, 1000));
  }

  @Test
  void bootstrapFailsWhenEveryMemberLeft() {
    MemberLeftException left = new MemberLeftException("left during bootstrap");
    HazelcastInstance instance = cluster(Map.of(member("left"), CompletableFuture.<Object>failedFuture(left)));
    VertxException failure = assertThrows(VertxException.class, () -> ClientMode.installConfiguration(instance, 1000));
    assertSame(left, failure.getCause());
  }

  @Test
  void configurationConflictsStillFailBootstrap() {
    IllegalArgumentException conflict = new IllegalArgumentException("conflict");
    HazelcastInstance instance = cluster(Map.of(
      member("surviving"), CompletableFuture.<Object>completedFuture(null),
      member("conflicting"), CompletableFuture.<Object>failedFuture(conflict)));
    assertSame(conflict, assertThrows(VertxException.class, () -> ClientMode.installConfiguration(instance,
        1000)).getCause());
  }

  @Test
  void bootstrapWaitIsBounded() {
    HazelcastInstance instance = cluster(Map.of(member("stuck"), new CompletableFuture<Object>()));
    long start = System.nanoTime();
    VertxException failure = assertThrows(VertxException.class, () -> ClientMode.installConfiguration(instance, 300));
    assertTrue(failure.getMessage().contains("Timed out"), failure.getMessage());
    assertInstanceOf(java.util.concurrent.TimeoutException.class, failure.getCause());
    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 5000);
  }

  private static Member member(String name) {
    return (Member) Proxy.newProxyInstance(Member.class.getClassLoader(), new Class<?>[] {Member.class},
      (proxy, method, args) -> {
        switch (method.getName()) {
          case "toString": return name;
          case "hashCode": return name.hashCode();
          case "equals": return proxy == args[0];
          default: throw new UnsupportedOperationException(method.getName());
        }
      });
  }

  private static HazelcastInstance cluster(Map<Member, Future<Object>> results) {
    Map<Member, Future<Object>> ordered = new LinkedHashMap<>(results);
    IExecutorService executor = (IExecutorService) Proxy.newProxyInstance(IExecutorService.class.getClassLoader(),
      new Class<?>[] {IExecutorService.class}, (proxy, method, args) -> {
        if (method.getName().equals("submitToAllMembers")) return ordered;
        throw new UnsupportedOperationException(method.getName());
      });
    return (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
      new Class<?>[] {HazelcastInstance.class}, (proxy, method, args) -> {
        if (method.getName().equals("getExecutorService")) return executor;
        throw new UnsupportedOperationException(method.getName());
      });
  }
}
