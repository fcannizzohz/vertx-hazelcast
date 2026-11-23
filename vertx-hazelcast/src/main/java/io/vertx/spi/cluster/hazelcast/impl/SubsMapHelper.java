/*
 * Copyright 2020 Red Hat, Inc.
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

package io.vertx.spi.cluster.hazelcast.impl;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.map.MapEvent;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import io.vertx.core.VertxException;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationListener;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Thomas Segismont
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public class SubsMapHelper implements EntryListener<String, HazelcastRegistrationInfo> {

  private static final Logger log = LoggerFactory.getLogger(SubsMapHelper.class);
  private static final String CLEANUP_EXECUTOR = "__vertx.subs.cleanup";
  private static final long CLEANUP_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);

  private final HazelcastInstance hazelcast;
  private final Throttling throttling;
  private final MultiMap<String, HazelcastRegistrationInfo> map;
  private final RegistrationListener registrationListener;
  private final UUID listenerId;

  private final ConcurrentMap<String, Set<RegistrationInfo>> ownSubs = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<RegistrationInfo>> localSubs = new ConcurrentHashMap<>();
  private final ReadWriteLock republishLock = new ReentrantReadWriteLock();

  public SubsMapHelper(HazelcastInstance hazelcast, RegistrationListener registrationListener) {
    this.hazelcast = hazelcast;
    throttling = new Throttling(this::getAndUpdate);
    this.registrationListener = registrationListener;
    try {
      map = hazelcast.getMultiMap("__vertx.subs");
      listenerId = map.addEntryListener(this, false);
    } catch (RuntimeException | Error failure) {
      try {
        throttling.close();
      } catch (RuntimeException | Error cleanupFailure) {
        if (failure != cleanupFailure) failure.addSuppressed(cleanupFailure);
      }
      throw failure;
    }
  }

  public List<RegistrationInfo> get(String address) {
    Lock readLock = republishLock.readLock();
    readLock.lock();
    try {
      List<RegistrationInfo> list;
      int size;
      Collection<HazelcastRegistrationInfo> remote = map.get(address);
      size = remote.size();
      Set<RegistrationInfo> local = localSubs.get(address);
      if (local != null) {
        synchronized (local) {
          size += local.size();
          if (size == 0) {
            return Collections.emptyList();
          }
          list = new ArrayList<>(size);
          list.addAll(local);
        }
      } else if (size == 0) {
        return Collections.emptyList();
      } else {
        list = new ArrayList<>(size);
      }
      for (HazelcastRegistrationInfo hazelcastRegistrationInfo : remote) {
        RegistrationInfo unwrap = hazelcastRegistrationInfo.unwrap();
        list.add(unwrap);
      }
      return list;
    } finally {
      readLock.unlock();
    }
  }

  public void put(String address, RegistrationInfo registrationInfo) {
    Lock readLock = republishLock.readLock();
    readLock.lock();
    try {
      if (registrationInfo.localOnly()) {
        localSubs.compute(address, (add, curr) -> addToSet(registrationInfo, curr));
        fireRegistrationUpdateEvent(address);
      } else {
        ownSubs.compute(address, (add, curr) -> addToSet(registrationInfo, curr));
        map.put(address, wrapRegistrationInfo(registrationInfo));
      }
    } finally {
      readLock.unlock();
    }
  }

  private HazelcastRegistrationInfo wrapRegistrationInfo(RegistrationInfo registrationInfo) {
    return new HazelcastRegistrationInfo(registrationInfo);
  }

  private Set<RegistrationInfo> addToSet(RegistrationInfo registrationInfo, Set<RegistrationInfo> curr) {
    Set<RegistrationInfo> res = curr != null ? curr : Collections.synchronizedSet(new LinkedHashSet<>());
    res.add(registrationInfo);
    return res;
  }

  public void remove(String address, RegistrationInfo registrationInfo) {
    Lock readLock = republishLock.readLock();
    readLock.lock();
    try {
      if (registrationInfo.localOnly()) {
        localSubs.computeIfPresent(address, (add, curr) -> removeFromSet(registrationInfo, curr));
        fireRegistrationUpdateEvent(address);
      } else {
        ownSubs.computeIfPresent(address, (add, curr) -> removeFromSet(registrationInfo, curr));
        map.remove(address, wrapRegistrationInfo(registrationInfo));
      }
    } finally {
      readLock.unlock();
    }
  }

  private Set<RegistrationInfo> removeFromSet(RegistrationInfo registrationInfo, Set<RegistrationInfo> curr) {
    curr.remove(registrationInfo);
    return curr.isEmpty() ? null : curr;
  }

  public void removeAllForNodes(Set<String> nodeIds) {
    removeAllForNodes(nodeIds, CLEANUP_TIMEOUT_MILLIS);
  }

  /**
   * Remove the registrations of departed nodes on a member and wait at most {@code timeoutMillis} for completion,
   * retries included. On timeout the task may still finish on the member; the caller is told it did not complete.
   */
  public void removeAllForNodes(Set<String> nodeIds, long timeoutMillis) {
    if (nodeIds.isEmpty()) return;
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    boolean interrupted = false;
    try {
      while (true) {
        Future<Void> cleanup = hazelcast.getExecutorService(CLEANUP_EXECUTOR).submit(new RemoveSubscriptions(nodeIds));
        while (true) {
          long remaining = deadline - System.nanoTime();
          try {
            if (remaining <= 0) throw new TimeoutException();
            cleanup.get(remaining, TimeUnit.NANOSECONDS);
            return;
          } catch (InterruptedException ignore) {
            // Keep waiting within the deadline: departure notification and leave should follow a finished cleanup.
            interrupted = true;
          } catch (TimeoutException timeout) {
            throw new VertxException("Timed out after " + timeoutMillis
                + " ms removing subscriptions for departed nodes "
              + nodeIds, true);
          } catch (ExecutionException failure) {
            if ((failure instanceof MemberLeftException || failure.getCause() instanceof MemberLeftException)
              && hazelcast.getLifecycleService().isRunning() && !hazelcast.getCluster().getMembers().isEmpty()) {
              break; // The task is idempotent; retry it on a surviving member.
            }
            Throwable cause = failure.getCause() != null ? failure.getCause() : failure;
            throw new VertxException("Failed to remove subscriptions for departed nodes", cause);
          }
        }
      }
    } finally {
      if (interrupted) Thread.currentThread().interrupt();
    }
  }

  /**
   * Runs on a member: removes the registrations of departed nodes there and returns only completion, so that each
   * observer does not download the subscription table. Members therefore need this library on their classpath.
   */
  public static final class RemoveSubscriptions implements Callable<Void>, HazelcastInstanceAware, DataSerializable {
    private Set<String> nodeIds;
    private transient HazelcastInstance hazelcast;

    public RemoveSubscriptions() { }

    private RemoveSubscriptions(Set<String> nodeIds) { this.nodeIds = new HashSet<>(nodeIds); }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcast) { this.hazelcast = hazelcast; }

    @Override
    public Void call() {
      MultiMap<String, HazelcastRegistrationInfo> subscriptions = hazelcast.getMultiMap("__vertx.subs");
      // Scan one address at a time to avoid materializing the complete registration table, even on the member.
      for (String address : subscriptions.keySet()) {
        for (HazelcastRegistrationInfo registration : subscriptions.get(address)) {
          if (nodeIds.contains(registration.unwrap().nodeId())) subscriptions.remove(address, registration);
        }
      }
      return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
      out.writeInt(nodeIds.size());
      for (String nodeId : nodeIds) out.writeString(nodeId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
      int size = in.readInt();
      nodeIds = new HashSet<>();
      for (int i = 0; i < size; i++) nodeIds.add(in.readString());
    }
  }

  public void republishOwnSubs() {
    Lock writeLock = republishLock.writeLock();
    writeLock.lock();
    try {
      for (Map.Entry<String, Set<RegistrationInfo>> entry : ownSubs.entrySet()) {
        String address = entry.getKey();
        for (RegistrationInfo registrationInfo : entry.getValue()) {
          map.put(address, wrapRegistrationInfo(registrationInfo));
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void entryAdded(EntryEvent<String, HazelcastRegistrationInfo> event) {
    fireRegistrationUpdateEvent(event.getKey());
  }

  private void fireRegistrationUpdateEvent(String address) {
    throttling.onEvent(address);
  }

  private void getAndUpdate(String address) {
    if (registrationListener.wantsUpdatesFor(address)) {
      List<RegistrationInfo> registrationInfos;
      try {
        registrationInfos = get(address);
      } catch (Exception e) {
        log.trace("A failure occurred while retrieving the updated registrations", e);
        registrationInfos = Collections.emptyList();
      }
      registrationListener.registrationsUpdated(new RegistrationUpdateEvent(address, registrationInfos));
    }
  }

  @Override
  public void entryEvicted(EntryEvent<String, HazelcastRegistrationInfo> event) {
  }

  @Override
  public void entryRemoved(EntryEvent<String, HazelcastRegistrationInfo> event) {
    fireRegistrationUpdateEvent(event.getKey());
  }

  @Override
  public void entryUpdated(EntryEvent<String, HazelcastRegistrationInfo> event) {
    fireRegistrationUpdateEvent(event.getKey());
  }

  @Override
  public void mapCleared(MapEvent event) {
  }

  @Override
  public void mapEvicted(MapEvent event) {
  }

  @Override
  public void entryExpired(EntryEvent<String, HazelcastRegistrationInfo> event) {
  }

  public void close() {
    try {
      map.removeEntryListener(listenerId);
    } finally {
      throttling.close();
    }
  }
}
