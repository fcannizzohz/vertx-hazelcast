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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Configuration tests require no Hazelcast instance or network sockets.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public class ConfigUtilTest {
  private final Map<String, String> original = new HashMap<>();
  private final String[] properties = {"vertx.hazelcast.mode", "vertx.hazelcast.config",
      "vertx.hazelcast.client.config", "hazelcast.local.localAddress", "hazelcast.local.publicAddress"};
  private Path directory;

  @Before
  public void prepare() throws Exception {
    for (String property : properties) {
      original.put(property, System.getProperty(property));
      System.clearProperty(property);
    }
    directory = Files.createTempDirectory("hazelcast-config-test");
  }

  @After
  public void restore() throws Exception {
    for (String property : properties) {
      String value = original.get(property);
      if (value == null) System.clearProperty(property); else System.setProperty(property, value);
    }
    try (var files = Files.list(directory)) {
      for (Path path : (Iterable<Path>) files::iterator) Files.delete(path);
    }
    Files.delete(directory);
  }

  private String xml(boolean client, String cluster) {
    String root = client ? "hazelcast-client" : "hazelcast";
    String schema = client ? "client-config" : "config";
    return "<" + root + " xmlns=\"http://www.hazelcast.com/schema/" + schema + "\"><cluster-name>"
      + cluster + "</cluster-name></" + root + ">";
  }

  @Test
  public void packagedClientDefaultAndAutomaticModeSelection() {
    ClientConfig template = ConfigUtil.loadClientConfig();
    assertTrue(template.getNetworkConfig().getAddresses().isEmpty());
    assertFalse(template.getNetworkConfig().getAutoDetectionConfig().isEnabled());
    System.setProperty("vertx.hazelcast.mode", "client");
    assertNull(new HazelcastClusterManager().getConfig());
    assertNotNull(new HazelcastClusterManager().getClientConfig());
    System.setProperty("vertx.hazelcast.mode", "invalid");
    assertNotNull(new HazelcastClusterManager().getConfig());
    assertNull(new HazelcastClusterManager().getClientConfig());
    assertThrows(IllegalArgumentException.class, () -> new MemberMode().validateConfiguration());
  }

  @Test
  public void configurationGettersDoNotCommitModeSelection() {
    HazelcastClusterManager inspectedMember = new HazelcastClusterManager();
    Config memberPreview = inspectedMember.getConfig();
    assertSame(memberPreview, inspectedMember.getConfig());
    assertNull(inspectedMember.getClientConfig());
    ClientConfig client = new ClientConfig();
    inspectedMember.setClientConfig(client);
    assertSame(client, inspectedMember.getClientConfig());
    assertNull(inspectedMember.getConfig());

    System.setProperty(properties[0], "client");
    HazelcastClusterManager inspectedClient = new HazelcastClusterManager();
    ClientConfig clientPreview = inspectedClient.getClientConfig();
    assertSame(clientPreview, inspectedClient.getClientConfig());
    assertNull(inspectedClient.getConfig());
    Config member = new Config();
    inspectedClient.setConfig(member);
    assertSame(member, inspectedClient.getConfig());
    assertNull(inspectedClient.getClientConfig());
  }

  @Test
  public void xmlClientConfigurationIsAvailableBeforeJoinAndRetained() throws Exception {
    Path clientXml = directory.resolve("client-preview.xml");
    Files.writeString(clientXml, xml(true, "preview-client"));
    System.setProperty(properties[0], "client");
    System.setProperty(properties[2], clientXml.toString());
    HazelcastClusterManager automatic = new HazelcastClusterManager();
    assertEquals("preview-client", automatic.getClientConfig().getClusterName());
    ClientConfig preview = automatic.getClientConfig();
    preview.setInstanceName("customized-before-join");
    assertSame(preview, automatic.getClientConfig());
    assertEquals("customized-before-join", automatic.getClientConfig().getInstanceName());
    HazelcastClusterManager explicit = new HazelcastClusterManager(new ClientMode());
    assertEquals("preview-client", explicit.getClientConfig().getClusterName());
    assertSame(explicit.getClientConfig(), explicit.getClientConfig());
  }

  @Test
  public void settersAllowSameModeUpdatesAndRejectSwitchesWithTheSameError() {
    Config member = new Config();
    ClientConfig client = new ClientConfig();
    HazelcastClusterManager memberManager = new HazelcastClusterManager();
    memberManager.setConfig(new Config());
    memberManager.setConfig(member);
    assertSame(member, memberManager.getConfig());
    String memberError = assertThrows(IllegalStateException.class, () ->
        memberManager.setClientConfig(client)).getMessage();
    assertSame(member, memberManager.getConfig());

    HazelcastClusterManager clientManager = new HazelcastClusterManager();
    clientManager.setClientConfig(new ClientConfig());
    clientManager.setClientConfig(client);
    assertSame(client, clientManager.getClientConfig());
    assertEquals(memberError, assertThrows(IllegalStateException.class, () ->
        clientManager.setConfig(member)).getMessage());
    assertSame(client, clientManager.getClientConfig());
    assertThrows(NullPointerException.class, () -> new HazelcastClusterManager().setConfig(null));
    assertThrows(NullPointerException.class, () -> new HazelcastClusterManager().setClientConfig(null));
  }

  @Test
  public void clientModeRejectsImplicitConnectionDefaults() {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
      () -> new ClientMode().validateConfiguration());
    assertTrue(error.getMessage().contains("explicit member addresses"));
    assertTrue(error.getMessage().contains("vertx.hazelcast.client.config"));
    // Hazelcast's programmatic defaults also have no explicitly selected target.
    assertThrows(IllegalArgumentException.class,
      () -> new ClientMode(new ClientConfig()).validateConfiguration());
  }

  @Test
  public void clientModeAcceptsExplicitLocalhostAndRejectsBlankAddresses() {
    ClientConfig config = new ClientConfig();
    config.getNetworkConfig().addAddress("127.0.0.1:5701");
    new ClientMode(config).validateConfiguration();
    config.getNetworkConfig().setAddresses(java.util.List.of(" "));
    assertThrows(IllegalArgumentException.class, () -> new ClientMode(config).validateConfiguration());
  }

  @Test
  public void clientModeAcceptsExplicitDiscoveryWithoutMemberAddresses() {
    ClientConfig kubernetes = new ClientConfig();
    kubernetes.getNetworkConfig().getKubernetesConfig().setEnabled(true);
    new ClientMode(kubernetes).validateConfiguration();
    ClientConfig custom = new ClientConfig();
    custom.getNetworkConfig().getDiscoveryConfig().addDiscoveryStrategyConfig(
      new com.hazelcast.config.DiscoveryStrategyConfig("example.CustomDiscoveryStrategy"));
    new ClientMode(custom).validateConfiguration();
    ClientConfig cloud = new ClientConfig();
    cloud.getNetworkConfig().getCloudConfig().setEnabled(true).setDiscoveryToken("explicit-token");
    new ClientMode(cloud).validateConfiguration();
  }

  @Test
  public void clientXmlMustSelectConnectionTargetEvenWhenClusterNameIsPresent() throws Exception {
    Path file = directory.resolve("client.xml");
    Files.writeString(file, xml(true, "production"));
    System.setProperty(properties[2], file.toString());
    assertThrows(IllegalArgumentException.class, () -> new ClientMode().validateConfiguration());
    Files.writeString(file, "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\">"
      + "<network><cluster-members><address>members.example:5701</address></cluster-members></network></hazelcast-client>");
    new ClientMode().validateConfiguration();
  }

  @Test
  public void packagedMemberDefaultIsReadable() {
    System.setProperty(properties[1], "classpath:default-cluster.xml");
    assertTrue(ConfigUtil.loadConfig().getMultiMapConfigs().containsKey("__vertx.subs"));
  }

  @Test
  public void loadsExplicitMemberAndClientFiles() throws Exception {
    for (boolean client : new boolean[] {false, true}) {
      Path file = directory.resolve("explicit.xml");
      Files.writeString(file, xml(client, "from-file"));
      System.setProperty(client ? properties[2] : properties[1], file.toString());
      assertEquals("from-file", client ? ConfigUtil.loadClientConfig().getClusterName()
          : ConfigUtil.loadConfig().getClusterName());
      System.clearProperty(client ? properties[2] : properties[1]);
    }
  }

  @Test
  public void loadsClasspathOverrideAndExplicitClasspathResource() throws Exception {
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try (var loader = new URLClassLoader(new java.net.URL[] {directory.toUri().toURL()}, previous) {
      @Override
      public java.net.URL getResource(String name) {
        Path file = directory.resolve(name);
        if (Files.isRegularFile(file)) {
          try { return file.toUri().toURL(); }
          catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
        }
        return super.getResource(name);
      }
    }) {
      Thread.currentThread().setContextClassLoader(loader);
      for (boolean client : new boolean[] {false, true}) {
        String property = client ? properties[2] : properties[1];
        Path override = directory.resolve(client ? "client-cluster.xml" : "cluster.xml");
        Files.writeString(override, xml(client, "override"));
        assertEquals("override", client ? ConfigUtil.loadClientConfig().getClusterName()
            : ConfigUtil.loadConfig().getClusterName());
        Files.writeString(directory.resolve("custom.xml"), xml(client, "explicit"));
        System.setProperty(property, "classpath:custom.xml");
        assertEquals("explicit", client ? ConfigUtil.loadClientConfig().getClusterName()
            : ConfigUtil.loadConfig().getClusterName());
        System.clearProperty(property);
        Files.writeString(override, "<broken");
        assertThrows(IllegalArgumentException.class, () -> {
          if (client) ConfigUtil.loadClientConfig(); else ConfigUtil.loadConfig();
        });
        Files.delete(override);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  @Test
  public void explicitMissingFilesNeverFallBack() {
    for (String location : new String[] {directory.resolve("missing.xml").toString(), "classpath:missing-config.xml",
        ""}) {
      System.setProperty(properties[2], location);
      IllegalArgumentException error = assertThrows(IllegalArgumentException.class, ConfigUtil::loadClientConfig);
      assertTrue(error.getMessage().contains(properties[2]));
    }
  }

  @Test
  public void rejectsMalformedAndWrongXmlTypes() throws Exception {
    Path file = directory.resolve("wrong.xml");
    System.setProperty(properties[2], file.toString());
    for (String content : new String[] {"<broken", xml(false, "member")}) {
      Files.writeString(file, content);
      assertThrows(IllegalArgumentException.class, ConfigUtil::loadClientConfig);
    }
    System.clearProperty(properties[2]);
    System.setProperty(properties[1], file.toString());
    Files.writeString(file, xml(true, "client"));
    assertThrows(IllegalArgumentException.class, ConfigUtil::loadConfig);
  }

  @Test
  public void rejectsConflictingModeAndXmlProperties() {
    System.setProperty(properties[0], "member");
    assertThrows(IllegalArgumentException.class, () -> new ClientMode().validateConfiguration());
    System.setProperty(properties[0], "client");
    assertThrows(IllegalArgumentException.class, () -> new MemberMode().validateConfiguration());
    System.clearProperty(properties[0]);
    System.setProperty(properties[1], "member.xml");
    assertThrows(IllegalArgumentException.class, () -> new ClientMode().validateConfiguration());
    System.setProperty(properties[2], "client.xml");
    assertThrows(IllegalArgumentException.class, () -> new MemberMode().validateConfiguration());
    System.clearProperty(properties[1]);
    assertThrows(IllegalArgumentException.class, () -> new MemberMode().validateConfiguration());
  }

  @Test
  public void rejectsMemberAddressPropertiesInClientMode() {
    System.setProperty(properties[3], "127.0.0.1");
    assertThrows(IllegalArgumentException.class, () -> new ClientMode().validateConfiguration());
    System.clearProperty(properties[3]);
    System.setProperty(properties[4], "127.0.0.1");
    assertThrows(IllegalArgumentException.class, () -> new ClientMode().validateConfiguration());
  }

  @Test
  public void externalMembersRejectXmlAndReplacementConfig() {
    Config config = new Config();
    var member = java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
      new Class<?>[] {com.hazelcast.cluster.Member.class}, (proxy, method, args) -> {
        throw new AssertionError("Unexpected member call: " + method.getName());
      });
    var instance = (com.hazelcast.core.HazelcastInstance) java.lang.reflect.Proxy.newProxyInstance(
      getClass().getClassLoader(), new Class<?>[] {com.hazelcast.core.HazelcastInstance.class},
      (proxy, method, args) -> {
        if (method.getName().equals("getConfig")) return config;
        if (method.getName().equals("getLocalEndpoint")) return member;
        throw new AssertionError("Unexpected call: " + method.getName());
      });
    MemberMode mode = new MemberMode(instance);
    mode.validateConfiguration();
    assertThrows(IllegalStateException.class, () -> mode.configureMember(new Config()));
    System.setProperty(properties[1], "member.xml");
    assertThrows(IllegalArgumentException.class, mode::validateConfiguration);
  }

  @Test
  public void programmaticConfigurationIsRetainedAndWrongModeSetterFails() {
    ClientConfig client = new ClientConfig().setClusterName("programmatic");
    HazelcastClusterManager manager = new HazelcastClusterManager(new ClientMode(client));
    System.setProperty(properties[2], "missing-file.xml");
    assertSame(client, manager.getClientConfig());
    assertThrows(IllegalStateException.class, () -> manager.setConfig(new Config()));
    Config member = new Config();
    assertThrows(IllegalStateException.class, () -> new HazelcastClusterManager(member).setClientConfig(client));
    assertSame(member, new HazelcastClusterManager(member).getConfig());
  }
}
