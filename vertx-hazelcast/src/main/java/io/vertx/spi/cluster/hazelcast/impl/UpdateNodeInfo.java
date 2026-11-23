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

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.ExtendedMapEntry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Map;

/**
 * Runs on the partition owner: updates an existing node registration without resetting its lease. {@code IMap.put}
 * and {@code IMap.replace} would reset the entry's TTL to the map default.
 */
public final class UpdateNodeInfo implements EntryProcessor<String, HazelcastNodeInfo, Boolean>, DataSerializable {
  private HazelcastNodeInfo value;

  public UpdateNodeInfo() { }

  public UpdateNodeInfo(HazelcastNodeInfo value) { this.value = value; }

  @Override
  public Boolean process(Map.Entry<String, HazelcastNodeInfo> entry) {
    if (entry.getValue() == null) return false;
    ((ExtendedMapEntry<String, HazelcastNodeInfo>) entry).setValueWithoutChangingExpiryTime(value);
    return true;
  }

  @Override
  public void writeData(ObjectDataOutput out) throws IOException { out.writeObject(value); }

  @Override
  public void readData(ObjectDataInput in) throws IOException { value = in.readObject(); }
}

