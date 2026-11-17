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

package io.vertx.spi.cluster.hazelcast.it;

import com.hazelcast.config.ConfigXmlGenerator;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.internal.VertxInternal;
import io.vertx.spi.cluster.hazelcast.ConfigUtil;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import io.vertx.spi.cluster.hazelcast.tests.TestClusterManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public class ServiceProviderIT {

  private String previousConfig;
  private Path configFile;

  @Before
  public void configureDiscovery() throws Exception {
    previousConfig = System.getProperty("vertx.hazelcast.config");
    var config = TestClusterManager.getConf(ConfigUtil.loadConfig());
    config.setClusterName("service-provider-it-" + java.util.UUID.randomUUID());
    configFile = Files.createTempFile("vertx-service-provider", ".xml");
    Files.writeString(configFile, new ConfigXmlGenerator().generate(config));
    System.setProperty("vertx.hazelcast.config", configFile.toString());
  }

  @After
  public void restoreConfiguration() throws Exception {
    if (previousConfig == null) System.clearProperty("vertx.hazelcast.config");
    else System.setProperty("vertx.hazelcast.config", previousConfig);
    if (configFile != null) Files.deleteIfExists(configFile);
  }

  @Test
  public void testDiscovery() throws Exception {
    CompletableFuture<Void> result = new CompletableFuture<>();
    Vertx v1 = Vertx.clusteredVertx(new VertxOptions()).toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    try {
      Vertx v2 = Vertx.clusteredVertx(new VertxOptions()).toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
      try {
        assertTrue(((VertxInternal) v1).clusterManager() instanceof HazelcastClusterManager);
        assertTrue(((VertxInternal) v2).clusterManager() instanceof HazelcastClusterManager);
        v1.eventBus().consumer("the-address", msg -> {
          if ("ping".equals(msg.body())) {
            result.complete(null);
          } else {
            result.completeExceptionally(new Exception());
          }
        }).completion().onComplete(ar -> {
          if (ar.succeeded()) {
            v2.eventBus().send("the-address", "ping");
          } else {
            result.completeExceptionally(ar.cause());
          }
        });
        result.get(30, TimeUnit.SECONDS);
      } finally {
        v2.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
      }
    } finally {
      v1.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }
  }
}
