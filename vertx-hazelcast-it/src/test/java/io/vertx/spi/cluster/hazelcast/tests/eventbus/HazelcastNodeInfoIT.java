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

package io.vertx.spi.cluster.hazelcast.tests.eventbus;

import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.tests.ClusterModeTestRule;
import io.vertx.spi.cluster.hazelcast.tests.ClusterModeTestRule.Mode;
import io.vertx.spi.cluster.hazelcast.tests.LoggingTestWatcher;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
@RunWith(Parameterized.class)
public class HazelcastNodeInfoIT extends io.vertx.tests.eventbus.NodeInfoTest {

  @Parameterized.Parameters(name = "{0}")
  public static Mode[] modes() {
    return Mode.values();
  }

  @Parameterized.Parameter
  public Mode mode;

  @Rule
  public ClusterModeTestRule cluster = new ClusterModeTestRule();

  @Rule
  public LoggingTestWatcher watchman = new LoggingTestWatcher();

  @Override
  protected ClusterManager getClusterManager() {
    return cluster.getClusterManager(mode);
  }

  @Override
  protected void close(List<Vertx> clustered) throws Exception {
    cluster.closeClustered(clustered);
  }
}
