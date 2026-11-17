/*
 * Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it)
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 */

package io.vertx.spi.cluster.hazelcast.it;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.vertx.core.Vertx;
import io.vertx.core.Promise;
import io.vertx.ext.healthchecks.Status;
import io.vertx.spi.cluster.hazelcast.ClusterHealthCheck;
import io.vertx.core.VertxOptions;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public class ClientModeTest {

  private static class TrackingManager extends HazelcastClusterManager {
    final java.util.concurrent.BlockingQueue<String> added = new java.util.concurrent.LinkedBlockingQueue<>();
    final java.util.concurrent.BlockingQueue<String> removed = new java.util.concurrent.LinkedBlockingQueue<>();

    TrackingManager() { trackNodes(); }
    TrackingManager(HazelcastInstance member) { super(member); trackNodes(); }

    private void trackNodes() {
      nodeListener(new io.vertx.core.spi.cluster.NodeListener() {
        public void nodeAdded(String nodeId) { }
        public void nodeLeft(String nodeId) { }
      });
    }

    @Override
    public void nodeListener(io.vertx.core.spi.cluster.NodeListener listener) {
      super.nodeListener(new io.vertx.core.spi.cluster.NodeListener() {
        public void nodeAdded(String nodeId) {
          listener.nodeAdded(nodeId);
          added.add(nodeId);
        }
        public void nodeLeft(String nodeId) {
          listener.nodeLeft(nodeId);
          removed.add(nodeId);
        }
      });
    }
  }

  @Test
  public void mixedModesDiscoverClientDeparture() throws Exception {
    mixedModesDiscoverEachOther(true);
  }

  @Test
  public void mixedModesDiscoverMemberDeparture() throws Exception {
    mixedModesDiscoverEachOther(false);
  }

  private void mixedModesDiscoverEachOther(boolean clientLeavesFirst) throws Exception {
    Config config = new Config().setClusterName("mixed-mode-" + UUID.randomUUID());
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
    config.getMemberAttributeConfig().setAttribute("__vertx.nodeId", UUID.randomUUID().toString());
    HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
    TrackingManager serverManager = new TrackingManager(member);
    Vertx server = null;
    Vertx client = null;
    try {
      server = Vertx.builder().withClusterManager(serverManager).buildClustered()
        .toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
      var address = serverManager.getHazelcastInstance().getCluster().getLocalMember().getAddress();
      ClientConfig clientConfig = new ClientConfig().setClusterName(config.getClusterName());
      clientConfig.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
      TrackingManager clientManager = new TrackingManager();
      clientManager.setClientConfig(clientConfig);
      client = Vertx.builder().withClusterManager(clientManager).buildClustered()
        .toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
      var expected = java.util.Set.of(serverManager.getNodeId(), clientManager.getNodeId());
      assertEquals(expected, new java.util.HashSet<>(serverManager.getNodes()));
      assertEquals(expected, new java.util.HashSet<>(clientManager.getNodes()));
      var ordered = new java.util.ArrayList<>(expected);
      java.util.Collections.sort(ordered);
      assertEquals(ordered, serverManager.getNodes());
      assertEquals(ordered, clientManager.getNodes());
      assertEquals(clientManager.getNodeId(), serverManager.added.poll(10, TimeUnit.SECONDS));
      // Complete a request over the clustered event bus in each direction.
      server.eventBus().consumer("server", message -> message.reply("server reply"))
        .completion().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      client.eventBus().consumer("client", message -> message.reply("client reply"))
        .completion().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      assertEquals("server reply", client.eventBus().request("server", "ping")
        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS).body());
      assertEquals("client reply", server.eventBus().request("client", "ping")
        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS).body());
      if (clientLeavesFirst) {
        client.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        client = null;
        assertEquals(clientManager.getNodeId(), serverManager.removed.poll(10, TimeUnit.SECONDS));
        assertEquals(java.util.List.of(serverManager.getNodeId()), serverManager.getNodes());
      } else {
        server.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        server = null;
        assertEquals(serverManager.getNodeId(), clientManager.removed.poll(10, TimeUnit.SECONDS));
        assertEquals(java.util.List.of(clientManager.getNodeId()), clientManager.getNodes());
      }
    } finally {
      try {
        if (client != null) client.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
      } finally {
        try {
          if (server != null) server.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        } finally {
          member.shutdown();
        }
      }
    }
  }

  @Test
  public void clientConnectsToMemberClusterAndUsesVertxSharedData() throws Exception {
    String clusterName = "vertx-client-it-" + UUID.randomUUID();
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }

    Config memberConfig = new Config().setClusterName(clusterName);
    memberConfig.getNetworkConfig().setPort(port).setPortAutoIncrement(false).setPublicAddress("127.0.0.1");
    JoinConfig join = memberConfig.getNetworkConfig().getJoin();
    join.getAutoDetectionConfig().setEnabled(false);
    join.getMulticastConfig().setEnabled(false);
    join.getTcpIpConfig().setEnabled(false);
    HazelcastInstance member = Hazelcast.newHazelcastInstance(memberConfig);
    Vertx vertx = null;
    try {
      ClientConfig clientConfig = new ClientConfig().setClusterName(clusterName);
      clientConfig.getNetworkConfig().addAddress("127.0.0.1:" + port);

      HazelcastClusterManager clusterManager = new HazelcastClusterManager();
      clusterManager.setClientConfig(clientConfig);
      vertx = Vertx.builder()
        .with(new VertxOptions())
        .withClusterManager(clusterManager)
        .buildClustered()
        .toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

      assertNotNull(clusterManager.getHazelcastInstance());
      assertTrue(clusterManager.getNodes().contains(clusterManager.getNodeId()));

      Promise<Status> health = Promise.promise();
      ClusterHealthCheck.createProcedure(vertx).handle(health);
      assertTrue(health.future().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS).isOk());

      // Member classpath includes vertx-hazelcast and its runtime dependencies, as documented.
      var counter = vertx.sharedData().getCounter("client-mode-counter")
        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      assertEquals(1L, counter.incrementAndGet().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS));
      assertEquals(1L, counter.getAndAdd(4).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS));
      assertTrue(counter.compareAndSet(5, 9).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS));
      assertEquals(9L, counter.get().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS));


      var asyncMap = vertx.sharedData().<String, String>getAsyncMap("client-mode-it")
        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      asyncMap.put("key", "value").toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      assertEquals("value", member.<String, String>getMap("client-mode-it").get("key"));
    } finally {
      if (vertx != null) {
        vertx.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
      }
      member.shutdown();
    }
  }
}
