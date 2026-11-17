/*
 * Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it)
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 */

package io.vertx.spi.cluster.hazelcast.tests;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import io.vertx.spi.cluster.hazelcast.MemberMode;
import io.vertx.spi.cluster.hazelcast.ClientMode;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public class HazelcastModeIT {

  @Test
  public void memberModeStartsAndStopsOwnedMember() throws Exception {
    HazelcastClusterManager manager = new HazelcastClusterManager(new MemberMode(standaloneMemberConfig("member-unit-" + UUID.randomUUID())));
    Vertx vertx = Vertx.builder()
      .with(new VertxOptions())
      .withClusterManager(manager)
      .buildClustered().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    HazelcastInstance member = manager.getHazelcastInstance();
    try {
      assertNotNull(member);
      assertTrue(manager.getNodes().contains(manager.getNodeId()));
      assertTrue(member.getLifecycleService().isRunning());
      assertTrue(manager.isHealthy());
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }
    assertFalse(member.getLifecycleService().isRunning());
    assertFalse(manager.isHealthy());
  }

  @Test
  public void clientModeStartsAndStopsOwnedClientWithoutStoppingMember() throws Exception {
    String clusterName = "client-unit-" + UUID.randomUUID();
    Config memberConfig = standaloneMemberConfig(clusterName);
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    memberConfig.getNetworkConfig().setPort(port).setPortAutoIncrement(false).setPublicAddress("127.0.0.1");
    HazelcastInstance member = Hazelcast.newHazelcastInstance(memberConfig);
    Vertx vertx = null;
    try {
      ClientConfig clientConfig = new ClientConfig().setClusterName(clusterName);
      clientConfig.getNetworkConfig().addAddress("127.0.0.1:" + port);
      HazelcastClusterManager manager = new HazelcastClusterManager(new ClientMode(clientConfig));
      vertx = Vertx.builder()
        .with(new VertxOptions())
        .withClusterManager(manager)
        .buildClustered().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

      HazelcastInstance client = manager.getHazelcastInstance();
      assertNotNull(client);
      assertTrue(client.getLifecycleService().isRunning());
      assertTrue(manager.getNodes().contains(manager.getNodeId()));
      assertTrue(member.getLifecycleService().isRunning());

      vertx.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
      vertx = null;

      assertFalse(client.getLifecycleService().isRunning());
      assertFalse(manager.isHealthy());
      assertTrue(member.getLifecycleService().isRunning());
    } finally {
      if (vertx != null) vertx.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
      member.shutdown();
    }
  }

  private Config standaloneMemberConfig(String clusterName) {
    Config config = new Config().setClusterName(clusterName);
    JoinConfig join = config.getNetworkConfig().getJoin();
    join.getAutoDetectionConfig().setEnabled(false);
    join.getMulticastConfig().setEnabled(false);
    join.getTcpIpConfig().setEnabled(false);
    return config;
  }
}
