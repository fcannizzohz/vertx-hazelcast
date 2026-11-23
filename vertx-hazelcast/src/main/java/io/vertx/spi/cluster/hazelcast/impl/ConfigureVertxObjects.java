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

package io.vertx.spi.cluster.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Runs on every member during client bootstrap: inspects the effective member configuration and installs defaults for
 * the Vert.x internal objects. It runs member-side because a client cannot read member configuration.
 */
public final class ConfigureVertxObjects implements Callable<Void>, HazelcastInstanceAware, DataSerializable {
  private transient HazelcastInstance hazelcast;

  public ConfigureVertxObjects() { }

  @Override
  public void setHazelcastInstance(HazelcastInstance hazelcast) { this.hazelcast = hazelcast; }

  @Override
  public Void call() {
    Config config = hazelcast.getConfig();
    for (String name : List.of("__vertx.haInfo", "__vertx.nodeInfo", "__vertx.clusterInfo")) {
      MapConfig effective = config.findMapConfig(name);
      if (effective.getName().equals("default")) {
        config.addMapConfig(new MapConfig(name).setBackupCount(1));
      } else if (effective.getTimeToLiveSeconds() > 0 || effective.getMaxIdleSeconds() > 0) {
        // Map-level expiry would silently drop HA descriptions, the cluster epoch, departure markers and node leases.
        throw new IllegalArgumentException("Hazelcast map " + name + " must not expire entries; configuration '"
          + effective.getName() + "' sets time-to-live-seconds=" + effective.getTimeToLiveSeconds()
          + " and max-idle-seconds=" + effective.getMaxIdleSeconds()
          + ". Add an exact-name map configuration for " + name + " without expiry.");
      }
    }
    MultiMapConfig subscriptions = config.findMultiMapConfig("__vertx.subs");
    if (subscriptions.getName().equals("default")) {
      config.addMultiMapConfig(new MultiMapConfig("__vertx.subs")
        .setBackupCount(1).setValueCollectionType(MultiMapConfig.ValueCollectionType.SET));
    } else if (subscriptions.getValueCollectionType() != MultiMapConfig.ValueCollectionType.SET) {
      throw new IllegalArgumentException("Hazelcast multimap __vertx.subs must use SET values; found "
        + subscriptions.getValueCollectionType());
    }
    return null;
  }

  @Override
  public void writeData(ObjectDataOutput out) { }

  @Override
  public void readData(ObjectDataInput in) { }
}

