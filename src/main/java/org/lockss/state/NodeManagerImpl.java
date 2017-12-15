/*
 * $Id$
 */

/*
 Copyright (c) 2000-2008 Board of Trustees of Leland Stanford Jr. University,
 all rights reserved.
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 Except as contained in this notice, the name of Stanford University shall not
 be used in advertising or otherwise to promote the sale, use or other dealings
 in this Software without prior written authorization from Stanford University.
 */

package org.lockss.state;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import org.lockss.app.BaseLockssDaemonManager;
import org.lockss.app.LockssAuManager;
import org.lockss.app.LockssDaemon;
import org.lockss.config.Configuration;
import org.lockss.daemon.ActivityRegulator;
import org.lockss.daemon.Crawler;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.CachedUrlSet;
import org.lockss.poller.PollManager;
import org.lockss.repository.LockssRepository;
import org.lockss.util.KeyPair;
import org.lockss.util.Logger;
import org.lockss.util.UniqueRefLruCache;

/**
 * Implementation of the NodeManager.
 */
public class NodeManagerImpl extends BaseLockssDaemonManager implements NodeManager {

  public static final String PARAM_ENABLE_V3_POLLER =
    org.lockss.poller.v3.V3PollFactory.PARAM_ENABLE_V3_POLLER;
  public static final boolean DEFAULT_ENABLE_V3_POLLER =
    org.lockss.poller.v3.V3PollFactory.DEFAULT_ENABLE_V3_POLLER;

  // the various necessary managers
  LockssDaemon theDaemon;
  HistoryRepository historyRepo;
  private LockssRepository lockssRepo;
  // state and caches for this AU
  ArchivalUnit managedAu;
  AuState auState;
  UniqueRefLruCache nodeCache;
  HashMap activeNodes;

   //the set of nodes marked damaged (these are nodes which have lost content
   // poll).
  DamagedNodeSet damagedNodes;

  private static Logger logger = Logger.getLogger("NodeManager");

  NodeManagerImpl(ArchivalUnit au) {
    managedAu = au;
  }

  public void startService() {
    super.startService();
    // gets all the managers
    if (logger.isDebug2()) logger.debug2("Starting: " + managedAu);
    theDaemon = getDaemon();
    historyRepo = theDaemon.getHistoryRepository(managedAu);
    lockssRepo = theDaemon.getLockssRepository(managedAu);
    // initializes the state info
    if (getNodeManagerManager().isGlobalNodeCache()) {
      nodeCache = getNodeManagerManager().getGlobalNodeCache();
    } else {
      nodeCache = new UniqueRefLruCache(getNodeManagerManager().paramNodeStateCacheSize);
    }

    auState = historyRepo.loadAuState();

    // damagedNodes not used for V3, avoid file lookup per AU
//     damagedNodes = historyRepo.loadDamagedNodeSet();
    damagedNodes = new DamagedNodeSet(managedAu, historyRepo);

    logger.debug2("NodeManager successfully started");
  }

  public void stopService() {
    if (logger.isDebug()) logger.debug("Stopping: " + managedAu);
    if (activeNodes != null) {
      activeNodes.clear();
    }
    if (damagedNodes != null) {
      damagedNodes.clear();
    }
    if (nodeCache != null) {
      nodeCache.clear();
    }

    super.stopService();
    logger.debug2("NodeManager successfully stopped");
  }

  NodeManagerManager getNodeManagerManager() {
    return theDaemon.getNodeManagerManager();
  }

  PollManager getPollManager() {
    return theDaemon.getPollManager();
  }

  ActivityRegulator getActivityRegulator() {
    return theDaemon.getActivityRegulator(managedAu);
  }

  boolean isGlobalNodeCache() {
    return nodeCache == getNodeManagerManager().getGlobalNodeCache();
  }

  public void setNodeStateCacheSize(int size) {
    if (nodeCache != null && !isGlobalNodeCache() &&
	nodeCache.getMaxSize() != size) {
      nodeCache.setMaxSize(size);
    }
  }

  public void setAuConfig(Configuration auConfig) {
  }

  public DamagedNodeSet getDamagedNodes() {
    return damagedNodes;
  }

  public synchronized NodeState getNodeState(CachedUrlSet cus) {
    String url = cus.getUrl();
    NodeState node = (NodeState)nodeCache.get(nodeCacheKey(url));
    if (node == null) {
      // if in repository, add to our state list
      try {
        if (lockssRepo.getNode(url) != null) {
          node = createNodeState(cus);
        } else {
          logger.debug("URL '"+cus.getUrl()+"' not found in cache.");
        }
      }
      catch (MalformedURLException mue) {
        logger.error("Can't get NodeState due to bad CUS '" + cus.getUrl()+"'");
      }
    }
    return node;
  }

  /**
   * Creates or loads a new NodeState instance (not in cache) and runs a
   * dead poll check against it.
   * @param cus CachedUrlSet
   * @return NodeState
   */
  NodeState createNodeState(CachedUrlSet cus) {
    // load from file cache, or get a new one
    NodeState state = historyRepo.loadNodeState(cus);

    nodeCache.put(nodeCacheKey(cus.getUrl()), state);
    return state;
  }

  Object nodeCacheKey(String canonUrl) {
    if (isGlobalNodeCache()) {
      return new KeyPair(this, canonUrl);
    }
    return canonUrl;
  }

  public AuState getAuState() {
    return auState;
  }

  /**
   * Returns a list of cached NodeStates.  This only returns nodes currently in
   * the cache, rather than a full list, but it's only used by the UI so that's
   * not a problem.
   * @return Iterator the NodeStates
   */
  Iterator getCacheEntries() {
    if (isGlobalNodeCache()) {
      // must return just our entries from global cache
      Collection auEntries = new ArrayList();
      for (Iterator iter = nodeCache.snapshot().iterator(); iter.hasNext(); ) {
        NodeState state = (NodeState)iter.next();
	if (managedAu == state.getCachedUrlSet().getArchivalUnit()) {
	  auEntries.add(state);
	}
      }
      return auEntries.iterator();
    } else {
      return nodeCache.snapshot().iterator();
    }
  }

  // Callers should call AuState directly when NodeManager goes.
  public void newContentCrawlFinished() {
    newContentCrawlFinished(Crawler.STATUS_SUCCESSFUL, null);
  }

  // Callers should call AuState directly when NodeManager goes.
  public void newContentCrawlFinished(int result, String msg) {
    // notify and checkpoint the austate (it writes through)
    AuState aus = getAuState();
    if (aus == null) {
      // Can happen in testing
      logger.warning("newContentCrawlFinished with null AU state");
      return;
    }
    aus.newCrawlFinished(result, msg);

    if (result == Crawler.STATUS_SUCCESSFUL) {
      // checkpoint the top-level nodestate
      NodeState topState = getNodeState(managedAu.getAuCachedUrlSet());
      CrawlState crawl = topState.getCrawlState();
      crawl.status = CrawlState.FINISHED;
      crawl.type = CrawlState.NEW_CONTENT_CRAWL;
      crawl.startTime = getAuState().getLastCrawlTime();
      historyRepo.storeNodeState(topState);
    }
  }

  public void hashFinished(CachedUrlSet cus, long hashDuration) {
    if (hashDuration < 0) {
      logger.warning("Tried to update hash with negative duration.");
      return;
    }
    NodeState state = getNodeState(cus);
    if (state == null) {
      logger.error("Updating state on non-existant node: " + cus.getUrl());
      throw new IllegalArgumentException(
          "Updating state on non-existant node.");
    } else {
      logger.debug3("Hash finished for CUS '" + cus.getUrl() + "'");
      ((NodeStateImpl)state).setLastHashDuration(hashDuration);
    }
  }

  /**
   * Mark the given CachedUrlSet deleted.
   *
   * @param cus The CUS to delete.
   */
  public void deleteNode(CachedUrlSet cus) throws IOException {
    LockssRepository repository = getDaemon().getLockssRepository(managedAu);
    repository.deleteNode(cus.getUrl());
    NodeState extraState = getNodeState(cus);
    extraState.getCrawlState().type = CrawlState.NODE_DELETED;
  }

  /**
   * Returns true if the CUS has damage.
   * @param cus CachedUrlSet
   * @return boolean true iff has damage.
   */
  boolean hasDamage(CachedUrlSet cus) {
    return damagedNodes.hasDamage(cus);
  }


  /**
   * Factory to create new NodeManager instances.
   */
  public static class Factory implements LockssAuManager.Factory {
    public LockssAuManager createAuManager(ArchivalUnit au) {
      return new NodeManagerImpl(au);
    }
  }

}
