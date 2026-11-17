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
import com.hazelcast.test.TestEnvironment;
import io.vertx.spi.cluster.hazelcast.ClientMode;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import io.vertx.spi.cluster.hazelcast.MemberMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

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
}
