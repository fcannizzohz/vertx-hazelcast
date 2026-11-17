/*
 * Copyright (c) 2011-2026 The original author or authors
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
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.map.IMap;
import io.vertx.core.Completable;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.*;
import io.vertx.spi.cluster.hazelcast.impl.*;
import io.vertx.spi.cluster.hazelcast.spi.HazelcastObjectProvider;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A cluster manager that uses Hazelcast
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author <a href="https://smartrics.it">Fabrizio Cannizzo</a>
 */
public class HazelcastClusterManager implements ClusterManager, LifecycleListener {

  private static final Logger log = LoggerFactory.getLogger(HazelcastClusterManager.class);

  static final String NODE_ID_ATTRIBUTE = "__vertx.nodeId";

  private VertxInternal vertx;

  private HazelcastInstance hazelcast;
  private HazelcastObjectProvider objectProvider;
  private volatile String nodeId;
  private NodeInfo nodeInfo;
  private SubsMapHelper subsMapHelper;
  private IMap<String, HazelcastNodeInfo> nodeInfoMap;
  private final Set<String> nodeIds = new HashSet<>();

  private NodeListener nodeListener;
  private RegistrationListener registrationListener;
  private volatile boolean active;

  private HazelcastMode mode;
  private boolean explicitMode;

  private ExecutorService lockReleaseExec;
  private ExecutorService eventExecutor;

  private HazelcastMode mode() {
    if (mode == null) mode = ConfigUtil.defaultMode();
    return mode;
  }

  /**
   * Constructor - gets config from classpath
   */
  public HazelcastClusterManager() {
    ServiceLoader<HazelcastObjectProvider> loader = ServiceLoader.load(HazelcastObjectProvider.class);
    for (HazelcastObjectProvider hazelcastObjectProvider : loader) {
      objectProvider = hazelcastObjectProvider;
    }
    if (objectProvider == null) {
      objectProvider = new HazelcastObjectProviderImpl();
    }
  }

  /**
   * Constructor - config supplied
   *
   * @param conf Hazelcast config, not null
   */
  public HazelcastClusterManager(Config conf) {
    this();
    Objects.requireNonNull(conf, "The Hazelcast config cannot be null.");
    this.mode = new MemberMode(conf);
    explicitMode = true;
  }

  public HazelcastClusterManager(HazelcastInstance instance) {
    this();
    Objects.requireNonNull(instance, "The Hazelcast instance cannot be null.");
    hazelcast = instance;
    mode = new MemberMode(instance);
    explicitMode = true;
  }

  /** Create a manager with an explicitly selected Hazelcast lifecycle mode. */
  public HazelcastClusterManager(HazelcastMode mode) {
    this();
    this.mode = Objects.requireNonNull(mode, "The Hazelcast mode cannot be null.");
    explicitMode = true;
  }

  @Override
  public void init(Vertx vertx) {
    this.vertx = (VertxInternal) vertx;
  }

  @Override
  public void join(Completable<Void> promise) {
    vertx.<Void>executeBlocking(() -> {
      if (!active) {
        mode().validateConfiguration();
        active = true;

        lockReleaseExec = Executors.newCachedThreadPool(r -> new Thread(r, "vertx-hazelcast-service-release-lock-thread"));

        eventExecutor = Executors.newSingleThreadExecutor(r -> {
          Thread thread = new Thread(r, "vertx-hazelcast-service-event-thread");
          thread.setDaemon(true);
          return thread;
        });

        hazelcast = mode.start(this);
        nodeId = mode.nodeId();


        subsMapHelper = new SubsMapHelper(hazelcast, registrationListener);


        nodeInfoMap = hazelcast.getMap("__vertx.nodeInfo");
        mode.joined(this);

        objectProvider.onJoin(vertx, new ConversionUtils(), hazelcast, lockReleaseExec);

      }
      return null;
    }).onComplete(promise);
  }

  @Override
  public String getNodeId() {
    return nodeId;
  }

  @Override
  public List<String> getNodes() {
    return mode.getNodes(this);
  }

  @Override
  public void registrationListener(RegistrationListener listener) {
    this.registrationListener = listener;
  }

  @Override
  public void nodeListener(NodeListener listener) {
    this.nodeListener = listener;
  }

  @Override
  public void setNodeInfo(NodeInfo nodeInfo, Completable<Void> promise) {
    synchronized (this) {
      this.nodeInfo = nodeInfo;
    }
    HazelcastNodeInfo value = wrapNodeInfo(nodeInfo);
    vertx.<Void>executeBlocking(() -> {
      nodeInfoMap.put(nodeId, value);
      return null;
    }, false).onComplete(promise);
  }

  private HazelcastNodeInfo wrapNodeInfo(NodeInfo nodeInfo) {
    return new HazelcastNodeInfo(nodeInfo);
  }

  @Override
  public synchronized NodeInfo getNodeInfo() {
    return nodeInfo;
  }

  @Override
  public void getNodeInfo(String nodeId, Completable<NodeInfo> promise) {
    vertx.executeBlocking(() -> {
      HazelcastNodeInfo value = nodeInfoMap.get(nodeId);
      if (value != null) {
        return value.unwrap();
      } else {
        throw new VertxException("Not a member of the cluster", true);
      }
    }, false).onComplete(promise);
  }

  @Override
  public <K, V> void getAsyncMap(String name, Completable<AsyncMap<K, V>> promise) {
    promise.succeed(objectProvider.getAsyncMap(name));
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return objectProvider.getSyncMap(name);
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Completable<Lock> promise) {
    vertx.executeBlocking(() -> objectProvider.getLockWithTimeout(name, timeout), false)
      .onComplete(promise);
  }

  @Override
  public void getCounter(String name, Completable<Counter> promise) {
    promise.succeed(objectProvider.createCounter(name));
  }

  @Override
  public void leave(Completable<Void> promise) {
    vertx.<Void>executeBlocking(() -> {
      synchronized (HazelcastClusterManager.this) {
        if (!active) {
          return null;
        }
        active = false;
      }

      try {
        mode.leaving(this);

        eventExecutor.shutdownNow();
        try {
          if (!eventExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            log.warn("Event executor did not terminate within 10 seconds");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        lockReleaseExec.shutdown();
        subsMapHelper.close();

        mode.cleanup(this);

      } finally {
        mode.stop();
      }
      return null;
    }).onComplete(promise);
  }

  void handleMemberAdded(String nid) {
    try {
      synchronized (this) {
        if (!active) {
          return;
        }
        if (nodeListener != null) {
          nodeIds.add(nid);
        }
      }
      if (nodeListener != null) {
        nodeListener.nodeAdded(nid);
      }
    } catch (Throwable t) {
      log.error("Failed to handle memberAdded", t);
    }
  }

  void executeClusterEvent(Runnable action) {
    try {
      eventExecutor.execute(action);
    } catch (RejectedExecutionException ignore) {
    }
  }

  void handleMembersRemoved(Set<String> ids) {
    try {
      NodeInfo currentNodeInfo;
      synchronized (this) {
        if (!active) {
          return;
        }
        currentNodeInfo = this.nodeInfo;
      }

      cleanSubs(ids);
      cleanNodeInfos(ids);
      nodeInfoMap.put(nodeId, wrapNodeInfo(currentNodeInfo));

      registrationListener.registrationsLost();
      republishOwnSubs();

      synchronized (this) {
        if (nodeListener != null) {
          nodeIds.removeAll(ids);
        }
      }
      if (nodeListener != null) {
        ids.forEach(nodeListener::nodeLeft);
      }
    } catch (Throwable t) {
      log.error("Failed to handle membersRemoved", t);
    }
  }

  void cleanSubs(Set<String> ids) {
    subsMapHelper.removeAllForNodes(ids);
  }

  private void cleanNodeInfos(Set<String> ids) {
    ids.forEach(nodeInfoMap::remove);
  }

  private void republishOwnSubs() {
    vertx.executeBlocking(() -> {
      subsMapHelper.republishOwnSubs();
      return null;
    }, false);
  }

  @Override
  public void stateChanged(LifecycleEvent lifecycleEvent) {
    if (lifecycleEvent.getState() == LifecycleEvent.LifecycleState.MERGED) {
      try {
        eventExecutor.execute(this::handleMerged);
      } catch (RejectedExecutionException ignore) {
      }
    }
  }

  private void handleMerged() {
    try {
      Set<String> nodeIdsCopy;
      synchronized (this) {
        if (!active) {
          return;
        }
        nodeIdsCopy = new HashSet<>(nodeIds);
      }

      final Set<String> currentNodes = new HashSet<>(getNodes());

      Set<String> newNodes = new HashSet<>(currentNodes);
      newNodes.removeAll(nodeIdsCopy);

      Set<String> removedMembers = new HashSet<>(nodeIdsCopy);
      removedMembers.removeAll(currentNodes);

      if (nodeListener != null) {
        for (String nodeId : newNodes) {
          nodeListener.nodeAdded(nodeId);
        }
      }

      handleMembersRemoved(removedMembers);

      synchronized (this) {
        nodeIds.retainAll(currentNodes);
      }
    } catch (Throwable t) {
      log.error("Failed to handle cluster merge", t);
    }
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public void addRegistration(String address, RegistrationInfo registrationInfo, Completable<Void> promise) {
    SubsOpSerializer serializer = SubsOpSerializer.get(vertx.getOrCreateContext());
    serializer.execute(subsMapHelper::put, address, registrationInfo, promise);
  }

  @Override
  public void removeRegistration(String address, RegistrationInfo registrationInfo, Completable<Void> promise) {
    SubsOpSerializer serializer = SubsOpSerializer.get(vertx.getOrCreateContext());
    serializer.execute(subsMapHelper::remove, address, registrationInfo, promise);
  }

  @Override
  public void getRegistrations(String address, Completable<List<RegistrationInfo>> promise) {
    vertx
      .executeBlocking(() -> subsMapHelper.get(address), false)
      .onComplete(promise);
  }

  @Override
  public String clusterHost() {
    return mode.clusterHost();
  }

  @Override
  public String clusterPublicHost() {
    return mode.clusterPublicHost();
  }

  /**
   * Get the Hazelcast config.
   *
   * @return a config object
   */
  public Config getConfig() {
    return mode().memberConfig();
  }

  /**
   * Set the Hazelcast config.
   *
   * @param config a config object
   */
  public void setConfig(Config config) {
    if (active) throw new IllegalStateException("The Hazelcast mode cannot be changed after joining");
    if (mode == null) mode = new MemberMode();
    mode.configureMember(config);
    explicitMode = true;
  }

  /** Configure this manager to connect as a Hazelcast client. */
  public void setClientConfig(ClientConfig config) {
    if (active) throw new IllegalStateException("The Hazelcast mode cannot be changed after joining");
    if (explicitMode) throw new IllegalStateException("A Hazelcast mode was already configured; inject one mode with its configuration");
    this.mode = new ClientMode(Objects.requireNonNull(config, "The Hazelcast client config cannot be null."));
    explicitMode = true;
  }

  /** @return the Hazelcast client config, or {@code null} when member mode is used */
  public ClientConfig getClientConfig() {
    return mode().clientConfig();
  }

  String nodeIdValue() { return nodeId; }
  IMap<String, HazelcastNodeInfo> nodeInfoMap() { return nodeInfoMap; }

  /** Load client XML for customization before injecting a {@link ClientMode}. */
  public ClientConfig loadClientConfig() {
    return ConfigUtil.loadClientConfig();
  }

  /**
   * Load Hazelcast config XML and transform it into a {@link Config} object.
   * The content is read from:
   * <ol>
   * <li>the location denoted by the {@code vertx.hazelcast.config} sysprop, if present, or</li>
   * <li>the {@code cluster.xml} file on the classpath, if present, or</li>
   * <li>the default config file</li>
   * </ol>
   * <p>
   * The cluster manager uses this method to load the config when the node joins the cluster, if no config was provided upon creation.
   * </p>
   * <p>
   * You may use this method to get a base config and customize it before the node joins the cluster.
   * In this case, don't forget to invoke {@link #setConfig(Config)} after you applied your changes.
   * </p>
   *
   * @return a config object
   */
  public Config loadConfig() {
    return ConfigUtil.loadConfig();
  }

  /**
   * Check this node's readiness using its Hazelcast mode.
   * This method may block and should be called on a worker thread.
   *
   * @return whether the node is active and the mode is healthy
   */
  public boolean isHealthy() {
    return active && mode.isHealthy();
  }

  public HazelcastInstance getHazelcastInstance() {
    return hazelcast;
  }
}
