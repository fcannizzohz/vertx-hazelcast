/*
 * Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it)
 *
 * Fabrizio Cannizzo licenses this file to you under the Apache License, version 2.0
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
package io.vertx.spi.cluster.hazelcast.tests;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.ClientMode;
import io.vertx.spi.cluster.hazelcast.ConfigUtil;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import io.vertx.spi.cluster.hazelcast.MemberMode;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Isolates each invocation and keeps the client backing member alive until Vert.x teardown finishes.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public class ClusterModeTestRule implements TestRule, AutoCloseable {

  public enum Mode { MEMBER, CLIENT }

  private final String clusterName = "vertx-mode-test-" + UUID.randomUUID();
  private final List<HazelcastInstance> instances = Collections.synchronizedList(new ArrayList<>());
  private HazelcastInstance backingMember;
  private TestHazelcastInstanceFactory memberFactory;

  private Config memberConfig() {
    Config config = ConfigUtil.loadConfig().setClusterName(clusterName);
    config.setProperty("hazelcast.shutdownhook.enabled", "false");
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
    return config;
  }

  public synchronized ClusterManager getClusterManager(Mode mode) {
    java.util.Objects.requireNonNull(mode, "Test mode must be supplied by the parameterized runner");
    if (mode == Mode.MEMBER) {
      if (memberFactory == null) {
        memberFactory = new TestHazelcastInstanceFactory();
      }
      return new HazelcastClusterManager(new MemberMode(memberConfig(), config -> {
        HazelcastInstance instance = memberFactory.newHazelcastInstance(config);
        instances.add(instance);
        return instance;
      }));
    }
    if (backingMember == null) {
      backingMember = Hazelcast.newHazelcastInstance(memberConfig());
      instances.add(backingMember);
    }
    var address = backingMember.getCluster().getLocalMember().getAddress();
    ClientConfig config = new ClientConfig().setClusterName(clusterName);
    config.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
    config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(5000);
    return new HazelcastClusterManager(new ClientMode(config, clientConfig -> {
      HazelcastInstance instance = com.hazelcast.client.HazelcastClient.newHazelcastClient(clientConfig);
      instances.add(instance);
      return instance;
    }));
  }

  public void closeClustered(List<Vertx> clustered) throws Exception {
    Lifecycle.closeClustered(clustered, memberFactory == null || !TestEnvironment.isMockNetwork());
  }

  @Override
  public void close() throws Exception {
    List<Throwable> failures = new ArrayList<>();
    // Clients must stop before the member they depend on, including after failed setup.
    for (int i = instances.size() - 1; i >= 0; i--) {
      try {
        instances.get(i).getLifecycleService().terminate();
      } catch (Throwable failure) {
        failures.add(failure);
      }
    }
    if (memberFactory != null) {
      try {
        memberFactory.terminateAll();
      } catch (Throwable failure) {
        failures.add(failure);
      }
    }
    instances.clear();
    backingMember = null;
    memberFactory = null;
    MultipleFailureException.assertEmpty(failures);
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        List<Throwable> failures = new ArrayList<>();
        try {
          base.evaluate();
        } catch (Throwable failure) {
          failures.add(failure);
        } finally {
          try {
            close();
          } catch (Throwable failure) {
            failures.add(failure);
          }
        }
        MultipleFailureException.assertEmpty(failures);
      }
    };
  }
}
