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

import io.vertx.core.VertxException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
@Timeout(10)
public class ClusterEventTest {

  @Test
  void waitingForAClusterEventFailsInsteadOfHangingWhenTheEventCannotRun() {
    HazelcastClusterManager manager = new HazelcastClusterManager();
    AtomicBoolean ran = new AtomicBoolean();
    // executeClusterEvent drops actions on an inactive manager; a caller waiting for completion must not block forever.
    assertThrows(VertxException.class, () -> manager.runClusterEvent(() -> ran.set(true)));
    assertFalse(ran.get());
  }
}
