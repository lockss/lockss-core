/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.*;
import java.util.*;
import org.apache.activemq.broker.*;
import org.apache.activemq.store.*;

import org.lockss.app.*;
import org.lockss.daemon.*;
import org.lockss.log.*;
import org.lockss.util.*;
import org.lockss.config.*;
import org.lockss.plugin.*;
import org.lockss.protocol.*;

/** Contains the basic logic for all StateManagers.  Exact behavior
 * implemented and modified subclasses.
 *
 * For AuState, guarantees and enforces a single AuState per AU.  Supports
 * operations on either AuState or AuStateBean, to support both clients
 * accessing AuState in the course of their work with an AU, and the state
 * service, where not all AUs will necessarily exist (so AuState cannot
 * exist) but still want to cache bean data to avoid extra DB accesses.
 */
public abstract class CachingStateManager extends BaseStateManager {

  protected static L4JLogger log = L4JLogger.getLogger();

  /**
   * The max size of the LRU cache from AuId to agreement map.
   */
  public static final String PARAM_AGREE_MAPS_CACHE_SIZE
    = PREFIX + "agreeMapsCacheSize";
  public static final int DEFAULT_AGREE_MAPS_CACHE_SIZE = 50;


  protected AuEventHandler auEventHandler;

  @Override
  public void initService(LockssDaemon daemon) throws LockssAppException {
    super.initService(daemon);
    auStates = newAuStateMap();
    auStateBeans = newAuStateBeanMap();
    agmnts = newAuAgreementsMap();
  }

  public void startService() {
    super.startService();

    // register our AU event handler
    auEventHandler = new AuEventHandler.Base() {
      @Override
      public void auDeleted(AuEvent event, ArchivalUnit au) {
	handleAuDeleted(au);
      }
    };
    if (pluginMgr != null) {
      pluginMgr.registerAuEventHandler(auEventHandler);
    }
  }

  public void stopService() {
    if (auEventHandler != null) {
      if (pluginMgr != null) {
	pluginMgr.unregisterAuEventHandler(auEventHandler);
      }
      auEventHandler = null;
    }
    super.stopService();
  }

  protected void handleAuDeleted(ArchivalUnit au) {
    handleAuDeletedAuState(au);
    handleAuDeletedAuAgreements(au);
  }


  // /////////////////////////////////////////////////////////////////
  // AuState
  // /////////////////////////////////////////////////////////////////

  /** Cache of extant AuState instances */
  protected Map<String,AuState> auStates;

  /** Cache of extant AuStateBean instances.  AuStateBean stored here only
   * when corresponding AuState does not exist. */
  protected Map<String,AuStateBean> auStateBeans;

  /** Return the current singleton AuState for the AU, creating one if
   * necessary. */
  @Override
  public synchronized AuState getAuState(ArchivalUnit au) {
    String key = auKey(au);
    AuState aus = auStates.get(key);
    if (aus == null) {
      AuStateBean ausb = auStateBeans.get(key);
      if (ausb != null) {
	// Create an AuState, move the item from bean to main cache.  No
	// store needed here has been exists and has been stored
	aus = new AuState(au, this, ausb);
	putAuState(key, aus);
      }
    }
    log.debug2("getAuState({}) [{}] = {}", au, key, aus);
    if (aus == null) {
      aus = handleAuStateCacheMiss(au);
    }
    return aus;
  }

  /** Return the current singleton AuStateBean for the auid, creating one
   * if necessary. */
  @Override
  public synchronized AuStateBean getAuStateBean(String key) {
    // first look for a cached AuState, return its bean
    AuState aus = auStates.get(key);
    if (aus != null) {
      log.debug2("getAuStateBean({}) = {}", key, aus);
      return aus.getBean();
    }

    AuStateBean ausb = auStateBeans.get(key);
    log.debug2("getAuStateBean({}) = {}", key, ausb);
    if (ausb == null) {
      ausb = handleAuStateBeanCacheMiss(key);
    }
    return ausb;
  }

  /** Update the stored AuState with the values of the listed fields.
   * @param aus The source of the new values.
   */
  @Override
  public synchronized void updateAuState(AuState aus, Set<String> fields) {
    String key = auKey(aus.getArchivalUnit());
    log.debug2("updateAuState: {}: {}", key, fields);
    AuState cur = auStates.get(key);
    try {
      if (cur != null) {
	if (cur != aus) {
	  throw new IllegalStateException("Attempt to store from wrong AuState instance");
	}
	String jsonChange = aus.toJson(fields);
	doStoreAuStateBean(key, aus.getBean(), fields);
	doNotifyAuStateChanged(key, jsonChange);
      } else if (isStoreOfMissingAuStateAllowed(fields)) {
	AuStateBean curbean = auStateBeans.get(key);
	if (curbean != null) { // FIXME
	  throw new IllegalStateException("AuStateBean but no AuState exists.  Do we need to support this?");
	}

	// XXX log?
	putAuState(key, aus);
	doStoreAuStateBean(key, aus.getBean(), null);
      } else {
	throw new IllegalStateException("Attempt to apply partial update to AuState not in cache");
      }
    } catch (IOException e) {
      log.error("Couldn't serialize AuState: {}", aus, e);
      throw new StateLoadStoreException("Couldn't serialize AuState: " + aus);
    }
  }

  /** Update the stored AuState with the values of the listed fields.
   * @param aus The source of the new values.
   */
  @Override
  public synchronized void updateAuStateBean(String key,
					     AuStateBean ausb,
					     Set<String> fields) {
    log.debug2("Updating: {}: {}", key, fields);
    AuState curaus = auStates.get(key);
    AuStateBean curausb;
    if (curaus != null) {
      curausb = curaus.getBean();
    } else {
      curausb = auStateBeans.get(key);
    }
    try {
      if (curausb != null) {
	if (curausb != ausb) {
	  throw new IllegalStateException("Attempt to store from wrong AuStateBean instance");
	}
	String jsonChange = ausb.toJson(fields);
	doStoreAuStateBean(key, ausb, fields);
	doNotifyAuStateChanged(key, jsonChange);
      } else if (isStoreOfMissingAuStateAllowed(fields)) {
	// XXX log?
	auStateBeans.put(key, ausb);
	doStoreAuStateBean(key, ausb, null);
      } else {
	throw new IllegalStateException("Attempt to apply partial update to AuStateBean not in cache: " + key);
      }
    } catch (IOException e) {
      log.error("Couldn't serialize AuStateBean: {}", ausb, e);
      throw new StateLoadStoreException("Couldn't serialize AuStateBean: " +
					ausb);
    }
  }

  /** Update AuState from a json string
   * @param auid
   * @param json the serialized set of changes
   * @throws IOException if json conversion throws
   */
  @Override
  public void updateAuStateFromJson(String auid, String json)
      throws IOException {
    AuStateBean ausb = getAuStateBean(auid);
    ausb.updateFromJson(json, daemon);
    updateAuStateBean(auid, ausb, AuUtil.jsonToMap(json).keySet());
  }

  /** Store an AuState from a json string
   * @param key the auid
   * @param json the serialized AuStateBean
   * @throws IOException if json conversion throws
   */
  @Override
  public void storeAuStateFromJson(String auid, String json)
      throws IOException {
    AuStateBean ausb = newDefaultAuStateBean(auid);
    ausb.updateFromJson(json, daemon);
    storeAuStateBean(auid, ausb);
  }

  /** Store an AuState not obtained from StateManager.  Useful in tests.
   * Can only be called once per AU. */
  @Override
  public synchronized void storeAuState(AuState aus) {
    String key = auKey(aus.getArchivalUnit());
    if (auStates.containsKey(key)) {
      throw new IllegalStateException("Storing 2nd AuState: " + key);
    }
    putAuState(key, aus);
    doStoreAuStateBean(key, aus.getBean(), null);
  }

  /** Store an AuStateBean not obtained from StateManager.  Useful in
   * tests.  Can only be called once per AU. */
  @Override
  public synchronized void storeAuStateBean(String key, AuStateBean ausb) {
    if (auStateExists(key)) {
      throw new IllegalStateException("Storing 2nd AuState: " + key);
    }
    auStateBeans.put(key, ausb);
    doStoreAuStateBean(key, ausb, null);
  }

  /** Return true if an AuState(Bean) exists for the given auid
   * @param key the auid
   */
  @Override
  public boolean auStateExists(String key) {
    return auStates.containsKey(key) || auStateBeans.containsKey(key);
  }

  /** Default behavior when AU is deleted/deactivated is to remove AuState
   * from cache.  Persistent implementations should not remove it from
   * storage. */
  protected synchronized void handleAuDeletedAuState(ArchivalUnit au) {
    auStates.remove(auKey(au));
    auStateBeans.remove(auKey(au));
  }

  /** Handle a cache miss.  Call hooks to load an object from backing
   * store, if any, or to create and store new default object. */
  protected AuState handleAuStateCacheMiss(ArchivalUnit au) {
    String key = auKey(au);
    AuStateBean ausb = doLoadAuStateBean(key);
    if (ausb != null) {
      // If there's already a bean in the db, just create the corresponding
      // AuState
      AuState aus = new AuState(au, this, ausb);
      putAuState(key, aus);
      log.debug2("handleAuStateCacheMiss: loaded bean for {}", key);
      return aus;
    }
    // Else create a new default object and store in the DB
    AuState aus = newDefaultAuState(au);
    putAuState(key, aus);
    log.debug2("handleAuStateCacheMiss: new bean for {}", key);
    doStoreAuStateBean(key, aus.getBean(), null);
    return aus;
  }

  /** Handle a cache miss.  Call hooks to load an object from backing
   * store, if any, or to create and store new default object. */
  protected AuStateBean handleAuStateBeanCacheMiss(String key) {
    AuStateBean ausb = doLoadAuStateBean(key);
    if (ausb != null) {
      log.debug2("handleAuStateBeanCacheMiss: loaded bean for {}", key);
      auStateBeans.put(key, ausb);
    } else {
      ausb = newDefaultAuStateBean(key);
      auStateBeans.put(key, ausb);
      log.debug2("handleAuStateBeanCacheMiss: new bean for {}", key);
      doStoreAuStateBean(key, ausb, null);
    }
    return ausb;
  }

  /** Put an AuState in the auStates map, and remove any AuStateBean entry
   * from auStateBeans. */
  protected void putAuState(String key, AuState aus) {
    auStates.put(key, aus);
    auStateBeans.remove(key);
  }

  /** @return a Map suitable for an AuState cache.  By default a HashMap,
   * for a complete cache. */
  protected Map<String,AuState> newAuStateMap() {
    return new HashMap<>();
  }

  /** @return a Map suitable for an AuStateBean cache.  By default a
   * HashMap, for a complete cache. */
  protected Map<String,AuStateBean> newAuStateBeanMap() {
    return new HashMap<>();
  }

  /** Return true if an update call for an unknown AuState should be
   * allowed (and treated as a store).  By default it's allowed iff it's a
   * complete update (all fields).  Overridable because of the many tests
   * that were written when this was permissible */
  protected boolean isStoreOfMissingAuStateAllowed(Set<String> fields) {
    return fields == null || fields.isEmpty();
  }

  // /////////////////////////////////////////////////////////////////
  // AuAgreements
  // /////////////////////////////////////////////////////////////////

  /** Cache of extant AuAgreements instances */
  protected Map<String,AuAgreements> agmnts;

  /** Return the current singleton AuAgreements for the auid, creating one
   * if necessary. */
  public synchronized AuAgreements getAuAgreements(String key) {
    AuAgreements aua = agmnts.get(key);
    log.debug2("getAuAgreements({}) = {}", key, aua);
    if (aua == null) {
      aua = handleAuAgreementsCacheMiss(key);
    }
    return aua;
  }

  public synchronized void updateAuAgreements(String key,
					     AuAgreements aua,
					     Set<PeerIdentity> peers) {
    log.debug2("Updating: {}: {}", key, peers);
    AuAgreements curaua = agmnts.get(key);
    try {
      if (curaua != null) {
	if (curaua != aua) {
	  throw new IllegalStateException("Attempt to store from wrong AuAgreements instance");
	}
	String json = aua.toJson(peers);
	doStoreAuAgreementsUpdate(key, aua, peers);
	doNotifyAuAgreementsChanged(key, json);
      } else if (isStoreOfMissingAuAgreementsAllowed(peers)) {
	// XXX log?
	agmnts.put(key, aua);
	String json = aua.toJson(peers);
	doStoreAuAgreementsNew(key, aua);
      } else {
	throw new IllegalStateException("Attempt to apply partial update to AuAgreements not in cache: " + key);
      }
    } catch (IOException e) {
      log.error("Couldn't serialize AuAgreements: {}", aua, e);
      throw new StateLoadStoreException("Couldn't serialize AuAgreements: " +
					aua);
    }
  }

  /** Entry point from state service to store changes to an AuAgreements.  Write
   * to DB, call hook to notify clients if appropriate
   * @param key the auid
   * @param json the serialized set of changes
   * @param map Map representation of change fields.
   * @throws IOException if json conversion throws
   */
  public void updateAuAgreementsFromJson(String auid, String json)
      throws IOException {
    AuAgreements aua = getAuAgreements(auid);
    Set<PeerIdentity> changedPids = aua.updateFromJson(json, daemon);
    updateAuAgreements(auid, aua, changedPids);
  }

  /** Store an AuAgreements not obtained from StateManager.  Useful in tests.
   * Can only be called once per AU. */
  public synchronized void storeAuAgreements(String key, AuAgreements aua) {
    updateAuAgreements(key, aua, null);
  }

  /** Default behavior when AU is deleted/deactivated is to remove
   * AuAgreements from cache.  Persistent implementations should not remove
   * it from storage. */
  protected synchronized void handleAuDeletedAuAgreements(ArchivalUnit au) {
    agmnts.remove(auKey(au));
  }

  /** Handle a cache miss.  Call hooks to load an object from backing
   * store, if any, or to create and store new default object. */
  protected AuAgreements handleAuAgreementsCacheMiss(String key) {
    AuAgreements aua = doLoadAuAgreements(key);
    if (aua != null) {
      agmnts.put(key, aua);
    } else {
      aua = newDefaultAuAgreements(key);
      agmnts.put(key, aua);
      try {
	String json = aua.toJson();
	doStoreAuAgreementsNew(key, aua);
      } catch (IOException e) {
	log.error("Couldn't serialize AuAgreements: {}", aua, e);
	throw new StateLoadStoreException("Couldn't serialize AuAgreements: " + aua);
      }
    }
    return aua;
  }

  /** Return true if an AuAgreements exists for the given auid
   * @param key the auid
   */
  @Override
  public boolean auAgreementsExists(String key) {
    return agmnts.containsKey(key);
  }

  /** Put an AuAgreements in the agmnts map. */
  protected void putAuAgreements(String key, AuAgreements aua) {
    agmnts.put(key, aua);
  }

  /** @return a Map suitable for an AuAgreements cache.  By default a
   * UniqueRefLruCache. */
  protected Map<String,AuAgreements> newAuAgreementsMap() {
    return new UniqueRefLruCache<>(DEFAULT_AGREE_MAPS_CACHE_SIZE);
  }

  /** Return true if an update call for an unknown AuAgreements should be
   * allowed (and treated as a store).  By default it's allowed iff it's a
   * complete update (all peers).  Overridable becauae of the many tests
   * that were written when this was permissiable */
  protected boolean isStoreOfMissingAuAgreementsAllowed(Set<PeerIdentity> peers) {
    return peers == null || peers.isEmpty();
  }

}
