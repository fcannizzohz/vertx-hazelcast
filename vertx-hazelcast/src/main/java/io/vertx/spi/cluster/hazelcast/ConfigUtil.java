/*
 * Copyright 2018 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.vertx.spi.cluster.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Function;

/**
 * XML configuration loading for Hazelcast members and clients.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public final class ConfigUtil {

  /** Load member XML from the explicit property, classpath override, or packaged default. */
  public static Config loadConfig() {
    return load("vertx.hazelcast.config", "cluster.xml", "default-cluster.xml",
      stream -> new XmlConfigBuilder(stream).build());
  }

  /**
   * Load client XML from the explicit property, classpath override, or packaged template.
   * The template has no connection target; configure addresses or discovery before starting client mode.
   */
  public static ClientConfig loadClientConfig() {
    return load("vertx.hazelcast.client.config", "client-cluster.xml", "default-client-cluster.xml",
      stream -> new XmlClientConfigBuilder(stream).build());
  }

  static String selectedMode() {
    String mode = System.getProperty("vertx.hazelcast.mode");
    if (mode != null && !mode.equals("member") && !mode.equals("client")) {
      throw new IllegalArgumentException("Invalid vertx.hazelcast.mode '" + mode + "': expected member or client");
    }
    return mode;
  }

  static HazelcastMode defaultMode() {
    return "client".equals(selectedMode()) ? new ClientMode() : new MemberMode();
  }

  static void validateMode(String mode, boolean externalInstance) {
    String selected = selectedMode();
    if (selected != null && !mode.equals(selected)) {
      throw new IllegalArgumentException("Injected " + mode + " mode conflicts with vertx.hazelcast.mode=" + selected);
    }
    String member = System.getProperty("vertx.hazelcast.config");
    String client = System.getProperty("vertx.hazelcast.client.config");
    if (member != null && client != null) {
      throw new IllegalArgumentException("Set only one of vertx.hazelcast.config and vertx.hazelcast.client.config");
    }
    String incompatible = mode.equals("client") ? "vertx.hazelcast.config" : "vertx.hazelcast.client.config";
    if (System.getProperty(incompatible) != null) {
      throw new IllegalArgumentException(incompatible + " is incompatible with " + mode + " mode");
    }
    if (mode.equals("client")) {
      for (String property : new String[] {"hazelcast.local.localAddress", "hazelcast.local.publicAddress"}) {
        if (System.getProperty(property) != null) {
          throw new IllegalArgumentException(property + " is a member address setting and is incompatible with client mode; configure Vert.x event-bus addresses separately");
        }
      }
    }
    if (externalInstance && (member != null || client != null)) {
      throw new IllegalArgumentException("XML configuration properties cannot configure an externally supplied Hazelcast instance");
    }
  }

  private static <T> T load(String property, String override, String fallback, Function<InputStream, T> parser) {
    String explicit = System.getProperty(property);
    String source = explicit != null ? property + "=" + explicit : "classpath:" + override;
    try {
      InputStream stream;
      if (explicit != null) {
        if (explicit.isEmpty()) throw new IOException("Configuration location is empty");
        if (explicit.startsWith("classpath:")) {
          java.net.URL location = resource(explicit.substring("classpath:".length()));
          stream = location != null ? location.openStream() : null;
        } else {
          stream = Files.newInputStream(Paths.get(explicit));
        }
      } else {
        java.net.URL location = resource(override);
        if (location == null) {
          source = "packaged " + fallback;
          location = ConfigUtil.class.getClassLoader().getResource(fallback);
        }
        stream = location != null ? location.openStream() : null;
      }
      if (stream == null) throw new IOException("Configuration resource not found");
      try (InputStream input = stream) {
        return parser.apply(input);
      }
    } catch (IOException | RuntimeException e) {
      throw new IllegalArgumentException("Failed to load Hazelcast configuration from " + source, e);
    }
  }

  private static java.net.URL resource(String name) {
    ClassLoader context = Thread.currentThread().getContextClassLoader();
    java.net.URL location = context != null ? context.getResource(name) : null;
    return location != null ? location : ConfigUtil.class.getClassLoader().getResource(name);
  }

  private ConfigUtil() { }
}
