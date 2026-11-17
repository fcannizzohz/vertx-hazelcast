/*
 * Copyright (c) 2026 Fabrizio Cannizzo (https://smartrics.it)
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 */

package io.vertx.spi.cluster.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import java.util.List;

/**
 * Mode-specific Hazelcast lifecycle and Vert.x node discovery.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public interface HazelcastMode {

  /** Validate configuration before allocating cluster resources. */
  default void validateConfiguration() { }

  HazelcastInstance start(HazelcastClusterManager manager);

  String nodeId();

  void joined(HazelcastClusterManager manager);

  List<String> getNodes(HazelcastClusterManager manager);

  void leaving(HazelcastClusterManager manager);

  void cleanup(HazelcastClusterManager manager);

  void stop();

  /** Check readiness; implementations may perform blocking Hazelcast operations. */
  boolean isHealthy();

  default String clusterHost() { return null; }

  default String clusterPublicHost() { return null; }

  default com.hazelcast.config.Config memberConfig() { return null; }

  default void configureMember(com.hazelcast.config.Config config) {
    throw new IllegalStateException("This mode does not accept member configuration");
  }

  default com.hazelcast.client.config.ClientConfig clientConfig() { return null; }
}
