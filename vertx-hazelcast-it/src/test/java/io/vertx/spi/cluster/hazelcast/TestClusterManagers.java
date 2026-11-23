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
import com.hazelcast.core.HazelcastInstance;

import java.util.function.Function;

/**
 * Test access to the package-private lifecycle modes, for suites outside this package that must control how the
 * Hazelcast instance is created.
 *
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public final class TestClusterManagers {
  private TestClusterManagers() { }

  public static HazelcastClusterManager member(Config config, Function<Config, HazelcastInstance> instanceFactory) {
    return new HazelcastClusterManager(new MemberMode(config, instanceFactory));
  }

  public static HazelcastClusterManager client(ClientConfig config, Function<ClientConfig,
      HazelcastInstance> instanceFactory) {
    return new HazelcastClusterManager(new ClientMode(config, instanceFactory));
  }
}
