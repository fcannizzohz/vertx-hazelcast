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
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.NodeListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A member-side subscription cleanup failure must not suppress departure notification.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
@Timeout(90)
public class SubscriptionCleanupFailureIT {
  private HazelcastInstance member;

  @BeforeEach
  public void startMember() {
    Config config = new Config().setClusterName("cleanup-failure-" + UUID.randomUUID());
    config.setProperty("hazelcast.shutdownhook.enabled", "false");
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
    // Split-brain protection that a single member never satisfies rejects every cleanup task. A missing task class,
    // denied executor permission or executor rejection fail the same way.
    config.addSplitBrainProtectionConfig(new SplitBrainProtectionConfig("needs-two", true, 2));
    config.addExecutorConfig(new ExecutorConfig("__vertx.subs.cleanup").setSplitBrainProtectionName("needs-two"));
    member = Hazelcast.newHazelcastInstance(config);
  }

  @AfterEach
  public void stopMember() {
    member.getLifecycleService().terminate();
  }

  @Test
  public void cleanupFailureStillReportsNodeLeft() throws Exception {
    TrackingManager observer = new TrackingManager(clientConfig());
    Vertx survivor = await(Vertx.builder().withClusterManager(observer).buildClustered());
    try {
      TrackingManager leaver = new TrackingManager(clientConfig());
      Vertx departing = await(Vertx.builder().withClusterManager(leaver).buildClustered());
      String leaverId = leaver.getNodeId();
      try {
        await(departing.close());
      } catch (Exception ignore) {
        // The leaver's own cleanup fails for the same reason; only the observer's notification matters here.
      }
      assertEquals(leaverId, observer.left.poll(30, TimeUnit.SECONDS), "Peer departure was never reported");
    } finally {
      await(survivor.close());
    }
  }

  private ClientConfig clientConfig() {
    var address = member.getCluster().getLocalMember().getAddress();
    ClientConfig config = new ClientConfig().setClusterName(member.getConfig().getClusterName());
    config.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
    return config;
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
  }

  private static final class TrackingManager extends HazelcastClusterManager {
    final BlockingQueue<String> left = new LinkedBlockingQueue<>();

    TrackingManager(ClientConfig config) {
      super(new ClientMode(config));
      // Without HA, Vert.x installs no node listener; install one so departures are observable.
      nodeListener(new NodeListener() {
        @Override public void nodeAdded(String nodeID) { }
        @Override public void nodeLeft(String nodeID) { }
      });
    }

    @Override
    public void nodeListener(NodeListener listener) {
      super.nodeListener(new NodeListener() {
        @Override public void nodeAdded(String nodeID) { listener.nodeAdded(nodeID); }
        @Override public void nodeLeft(String nodeID) {
          left.add(nodeID);
          listener.nodeLeft(nodeID);
        }
      });
    }
  }
}
