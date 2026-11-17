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
public class ClientModeIT {

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
  public void clientBootstrapAcceptsMatchingStaticConfigAndRejectsConflicts() {
    for (String conflictingObject : java.util.List.of("none", "__vertx.haInfo", "__vertx.nodeInfo", "__vertx.subs")) {
      boolean conflict = !conflictingObject.equals("none");
      String cluster = "client-config-bootstrap-" + UUID.randomUUID();
      Config config = new Config().setClusterName(cluster);
      config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
      config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
      config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
      config.addMapConfig(new com.hazelcast.config.MapConfig("__vertx.haInfo")
        .setBackupCount(conflictingObject.equals("__vertx.haInfo") ? 2 : 1));
      config.addMapConfig(new com.hazelcast.config.MapConfig("__vertx.nodeInfo")
        .setBackupCount(conflictingObject.equals("__vertx.nodeInfo") ? 2 : 1));
      config.addMultiMapConfig(new com.hazelcast.config.MultiMapConfig("__vertx.subs")
        .setValueCollectionType(conflictingObject.equals("__vertx.subs")
          ? com.hazelcast.config.MultiMapConfig.ValueCollectionType.LIST
          : com.hazelcast.config.MultiMapConfig.ValueCollectionType.SET));
      HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
      var address = member.getCluster().getLocalMember().getAddress();
      ClientConfig clientConfig = new ClientConfig().setClusterName(cluster);
      clientConfig.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
      clientConfig.setInstanceName("bootstrap-client-" + UUID.randomUUID());
      var first = new io.vertx.spi.cluster.hazelcast.ClientMode(clientConfig);
      var second = new io.vertx.spi.cluster.hazelcast.ClientMode(new ClientConfig(clientConfig)
        .setInstanceName("bootstrap-client-" + UUID.randomUUID()));
      try {
        if (conflict) {
          org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
            () -> first.start(new HazelcastClusterManager(first)));
          org.junit.jupiter.api.Assertions.assertFalse(first.isHealthy());
          org.junit.jupiter.api.Assertions.assertNull(
            com.hazelcast.client.HazelcastClient.getHazelcastClientByName(clientConfig.getInstanceName()),
            "A rejected bootstrap must close its owned client");
          if (conflictingObject.equals("__vertx.subs")) {
            assertEquals(com.hazelcast.config.MultiMapConfig.ValueCollectionType.LIST,
              member.getConfig().findMultiMapConfig(conflictingObject).getValueCollectionType());
          } else {
            assertEquals(2, member.getConfig().findMapConfig(conflictingObject).getBackupCount());
          }
        } else {
          first.start(new HazelcastClusterManager(first));
          second.start(new HazelcastClusterManager(second));
          assertEquals(1, member.getConfig().findMapConfig("__vertx.nodeInfo").getBackupCount());
        }
      } finally {
        try {
          first.stop();
          second.stop();
        } finally {
          member.shutdown();
        }
      }
    }
  }

  @Test
  public void selectsMemberAndClientModesFromPropertiesAndCustomXml() throws Exception {
    String[] properties = {"vertx.hazelcast.mode", "vertx.hazelcast.config", "vertx.hazelcast.client.config"};
    java.util.Map<String, String> previous = new java.util.HashMap<>();
    for (String property : properties) {
      previous.put(property, System.getProperty(property));
      System.clearProperty(property);
    }
    var directory = java.nio.file.Files.createTempDirectory("vertx-mode-xml");
    Vertx server = null;
    Vertx client = null;
    try {
      String cluster = "automatic-mode-" + UUID.randomUUID();
      var memberXml = directory.resolve("member.xml");
      java.nio.file.Files.writeString(memberXml,
        "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\"><cluster-name>" + cluster
          + "</cluster-name><network><interfaces enabled=\"true\"><interface>127.0.0.1</interface></interfaces>"
          + "<join><multicast enabled=\"false\"/><auto-detection enabled=\"false\"/></join></network></hazelcast>");
      // No mode selector is required for the backwards-compatible member default.
      System.setProperty(properties[1], memberXml.toString());
      server = Vertx.builder().buildClustered()
        .toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
      HazelcastClusterManager serverManager = (HazelcastClusterManager)
        ((io.vertx.core.internal.VertxInternal) server).clusterManager();
      var address = serverManager.getHazelcastInstance().getCluster().getLocalMember().getAddress();
      System.clearProperty(properties[1]);
      System.setProperty(properties[0], "client");
      var clientXml = directory.resolve("client.xml");
      java.nio.file.Files.writeString(clientXml,
        "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\"><cluster-name>" + cluster
          + "</cluster-name><network><cluster-members><address>" + address.getHost() + ":" + address.getPort()
          + "</address></cluster-members></network></hazelcast-client>");
      System.setProperty(properties[2], clientXml.toString());
      client = Vertx.builder().buildClustered()
        .toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
      HazelcastClusterManager clientManager = (HazelcastClusterManager)
        ((io.vertx.core.internal.VertxInternal) client).clusterManager();
      assertEquals(cluster, clientManager.getClientConfig().getClusterName());
      assertEquals(serverManager.getNodes(), clientManager.getNodes());
      assertEquals(2, clientManager.getNodes().size());
    } finally {
      try {
        if (client != null) client.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
      } finally {
        try {
          if (server != null) server.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        } finally {
          for (String property : properties) {
            if (previous.get(property) == null) System.clearProperty(property);
            else System.setProperty(property, previous.get(property));
          }
          try (var files = java.nio.file.Files.list(directory)) {
            for (var file : (Iterable<java.nio.file.Path>) files::iterator) java.nio.file.Files.delete(file);
          }
          java.nio.file.Files.delete(directory);
        }
      }
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
      // A supplied config object wins over the matching XML property, even if that file is missing.
      String previousConfig = System.getProperty("vertx.hazelcast.client.config");
      try {
        System.setProperty("vertx.hazelcast.client.config", "missing-client-config.xml");
        vertx = Vertx.builder()
          .with(new VertxOptions())
          .withClusterManager(clusterManager)
          .buildClustered()
          .toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
      } finally {
        if (previousConfig == null) System.clearProperty("vertx.hazelcast.client.config");
        else System.setProperty("vertx.hazelcast.client.config", previousConfig);
      }

      assertNotNull(clusterManager.getHazelcastInstance());
      assertEquals(1, member.getConfig().findMapConfig("__vertx.haInfo").getBackupCount());
      assertEquals(1, member.getConfig().findMapConfig("__vertx.nodeInfo").getBackupCount());
      assertEquals(com.hazelcast.config.MultiMapConfig.ValueCollectionType.SET,
        member.getConfig().findMultiMapConfig("__vertx.subs").getValueCollectionType());
      assertEquals(1, member.getConfig().findMultiMapConfig("__vertx.subs").getBackupCount());
      assertTrue(member.getConfig().getMapConfigs().containsKey("__vertx.haInfo"));
      assertTrue(member.getConfig().getMapConfigs().containsKey("__vertx.nodeInfo"));
      assertTrue(member.getConfig().getMultiMapConfigs().containsKey("__vertx.subs"));

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
