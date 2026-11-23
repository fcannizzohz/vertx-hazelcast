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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.spi.cluster.hazelcast.impl.HazelcastNodeInfo;

import java.util.List;

/**
 * Mode-specific Hazelcast lifecycle and Vert.x node discovery.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
interface HazelcastMode {

  /** Validate configuration before allocating cluster resources. */
  default void validateConfiguration() { }

  HazelcastInstance start(HazelcastClusterManager manager);

  /** Start after the manager has validated configuration; built-in modes avoid validating it again. */
  default HazelcastInstance startValidated(HazelcastClusterManager manager) { return start(manager); }

  String nodeId();

  void joined(HazelcastClusterManager manager);

  /** Return a local discovery snapshot without blocking or making remote calls. */
  List<String> getNodes(HazelcastClusterManager manager);

  /** Refresh discovery after recovery or merge; called on the cluster event executor and may block. */
  default void refreshNodes(HazelcastClusterManager manager) { }

  void leaving(HazelcastClusterManager manager);

  void cleanup(HazelcastClusterManager manager);

  default void stateChanged(HazelcastClusterManager manager, LifecycleEvent event) { }

  default boolean isRecovering() { return false; }

  /** Publish this node's discovery information using the mode's liveness policy. */
  default void publishNodeInfo(HazelcastClusterManager manager, NodeInfo nodeInfo) {
    manager.nodeInfoMap().put(nodeId(), new HazelcastNodeInfo(nodeInfo));
  }

  void stop();

  /** Check readiness; implementations may perform blocking Hazelcast operations. */
  boolean isHealthy();

  default String clusterHost() { return null; }

  default String clusterPublicHost() { return null; }

  default com.hazelcast.config.Config memberConfig() { return null; }

  default void configureMember(com.hazelcast.config.Config config) {
    throw new IllegalStateException("Cannot switch the selected Hazelcast mode; supply configuration matching "
        + "that mode");
  }

  default com.hazelcast.client.config.ClientConfig clientConfig() { return null; }

  default void configureClient(com.hazelcast.client.config.ClientConfig config) {
    throw new IllegalStateException("Cannot switch the selected Hazelcast mode; supply configuration matching "
        + "that mode");
  }
}
