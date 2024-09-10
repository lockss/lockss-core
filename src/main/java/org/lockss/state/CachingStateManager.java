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
import java.util.concurrent.ConcurrentHashMap;

import org.lockss.account.AccountManager;
import org.lockss.account.UserAccount;
import org.lockss.app.*;
import org.lockss.log.*;
import org.lockss.util.*;
import org.lockss.plugin.*;
import org.lockss.protocol.*;
import org.lockss.state.AuSuspectUrlVersions.SuspectUrlVersion;

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

  /**
   * The max size of the LRU cache from AuId to suspect URL versions map.
   */
  public static final String PARAM_SUSPECT_VERSIONS_MAPS_CACHE_SIZE
    = PREFIX + "suspectVersionsMapsCacheSize";
  public static final int DEFAULT_SUSPECT_VERSIONS_MAPS_CACHE_SIZE = 50;

  /**
   * The max size of the LRU cache from AuId to no peer set map.
   */
  public static final String PARAM_NO_PEER_SET_MAPS_CACHE_SIZE
    = PREFIX + "noPeerSetMapsCacheSize";
  public static final int DEFAULT_NO_PEER_SET_MAPS_CACHE_SIZE = 50;

  protected AuEventHandler auEventHandler;

  @Override
  public void initService(LockssDaemon daemon) throws LockssAppException {
    super.initService(daemon);
    auStates = newAuStateMap();
    auStateBeans = newAuStateBeanMap();
    agmnts = newAuAgreementsMap();
    suspectVers = newAuSuspectUrlVersionsMap();
    noAuPeerSets = newNoAuPeerSetMap();
    userAccounts = newUserAccountsMap();
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
    stopJms();
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
    handleAuDeletedAuSuspectUrlVersions(au);
    handleAuDeletedNoAuPeerSet(au);
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
	doNotifyAuStateChanged(key, jsonChange, null);
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
   * @param ausb The source of the new values.
   */
  @Override
  public synchronized void updateAuStateBean(String key,
					     AuStateBean ausb,
					     Set<String> fields) {
    updateAuStateBean(key, ausb, fields, null);
  }

  public synchronized void updateAuStateBean(String key,
					     AuStateBean ausb,
					     Set<String> fields,
					     String cookie) {
    log.debug2("Updating AuState: {}: {}", key, fields);
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
	doNotifyAuStateChanged(key, jsonChange, cookie);
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
   * @param cookie propagated to JMS change notifications (if non-null)
   * @throws IOException if json conversion throws
   */
  @Override
  public void updateAuStateFromJson(String auid, String json, String cookie)
      throws IOException {
    AuStateBean ausb = getAuStateBean(auid);
    ausb.updateFromJson(json, daemon);
    updateAuStateBean(auid, ausb, AuUtil.jsonToMap(json).keySet(), cookie);
  }

  /** Store an AuState from a json string
   * @param auid the auid
   * @param json the serialized AuStateBean
   * @throws IOException if json conversion throws
   */
  protected void storeAuStateFromJson(String auid, String json)
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
    if (hasAuState(key)) {
      throw new IllegalStateException("Storing 2nd AuState: " + key);
    }
    auStateBeans.put(key, ausb);
    doStoreAuStateBean(key, ausb, null);
  }

  /** Return true if an AuState(Bean) exists for the given auid
   * @param key the auid
   */
  @Override
  public boolean hasAuState(String key) {
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
    updateAuAgreements(key, aua, peers, null);
  }

  public synchronized void updateAuAgreements(String key,
					      AuAgreements aua,
					      Set<PeerIdentity> peers,
					      String cookie) {
    log.debug2("Updating AuAgreements: {}: {}", key, peers);
    AuAgreements curaua = agmnts.get(key);
    try {
      if (curaua != null) {
	if (curaua != aua) {
	  throw new IllegalStateException("Attempt to store from wrong AuAgreements instance");
	}
	String json = aua.toJson(peers);
	doStoreAuAgreementsUpdate(key, aua, peers);
	doNotifyAuAgreementsChanged(key, json, cookie);
      } else if (isStoreOfMissingAuAgreementsAllowed(peers)) {
	// XXX log?
	agmnts.put(key, aua);
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
   * @param auid the auid
   * @param json the serialized set of changes
   * @param cookie propagated to JMS change notifications (if non-null)
   * @throws IOException if json conversion throws
   */
  public void updateAuAgreementsFromJson(String auid, String json,
					 String cookie)
      throws IOException {
    AuAgreements aua = getAuAgreements(auid);
    Set<PeerIdentity> changedPids = aua.updateFromJson(json, daemon);
    updateAuAgreements(auid, aua, changedPids, cookie);
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
    }
    return aua;
  }

  /** Return true if an AuAgreements exists for the given auid
   * @param key the auid
   */
  @Override
  public boolean hasAuAgreements(String key) {
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
   * complete update (all peers).  Overridable because of the many tests
   * that were written when this was permissible */
  protected boolean isStoreOfMissingAuAgreementsAllowed(Set<PeerIdentity> peers) {
    return peers == null || peers.isEmpty();
  }

  // /////////////////////////////////////////////////////////////////
  // AuSuspectUrlVersions
  // /////////////////////////////////////////////////////////////////

  /** Cache of extant AuSuspectUrlVersions instances */
  protected Map<String,AuSuspectUrlVersions> suspectVers;

  /** Return the current singleton AuSuspectUrlVersions for the auid,
   * creating one if necessary. */
  public synchronized AuSuspectUrlVersions getAuSuspectUrlVersions(String key) {
    AuSuspectUrlVersions asuv = suspectVers.get(key);
    log.debug2("getAuSuspectUrlVersions({}) = {}", key, asuv);
    if (asuv == null) {
      asuv = handleAuSuspectUrlVersionsCacheMiss(key);
    }
    return asuv;
  }

  /** Completely replace the stored AuSuspectUrlVersions with the data from
   * this one.
   */
  public synchronized void updateAuSuspectUrlVersions(String key,
					     AuSuspectUrlVersions asuv) {
    updateAuSuspectUrlVersions(key, asuv, null);
  }

  /** Completely replace the stored AuSuspectUrlVersions with the data from
   * this one.  The versions arg is intended for future use, to support
   * incremental udpate.  It's currently always null.
   */
  public synchronized void updateAuSuspectUrlVersions(String key,
					      AuSuspectUrlVersions asuv,
					      Set<SuspectUrlVersion> versions) {
    updateAuSuspectUrlVersions(key, asuv, versions, null);
  }

  public synchronized void updateAuSuspectUrlVersions(String key,
					      AuSuspectUrlVersions asuv,
					      Set<SuspectUrlVersion> versions,
					      String cookie) {
    log.debug2("Updating suspectUrlVersions: {}: {}", key, asuv);
    AuSuspectUrlVersions curasuv = suspectVers.get(key);
    try {
      if (curasuv != null) {
	if (curasuv != asuv) {
	  throw new IllegalStateException("Attempt to store from wrong AuSuspectUrlVersions instance");
	}
	String json = asuv.toJson(versions);
	doStoreAuSuspectUrlVersionsUpdate(key, asuv, versions);
	doNotifyAuSuspectUrlVersionsChanged(key, json, cookie);
      } else if (isStoreOfMissingAuSuspectUrlVersionsAllowed(versions)) {
	// XXX log?
 	suspectVers.put(key, asuv);
      } else {
	throw new IllegalStateException("Attempt to apply partial update to AuSuspectUrlVersions not in cache: " + key);
      }
    } catch (IOException e) {
      log.error("Couldn't serialize AuSuspectUrlVersions: {}", asuv, e);
      throw new StateLoadStoreException("Couldn't serialize AuSuspectUrlVersions: " +
	  asuv);
    }
  }

  /** Entry point from state service to store changes to an AuSuspectUrlVersions.  Write
   * to DB, call hook to notify clients if appropriate
   * @param auid the auid
   * @param json the serialized set of changes
   * @param cookie propagated to JMS change notifications (if non-null)
   * @throws IOException if json conversion throws
   */
  public void updateAuSuspectUrlVersionsFromJson(String auid, String json,
						 String cookie)
      throws IOException {
    AuSuspectUrlVersions asuv = getAuSuspectUrlVersions(auid);
    Set<SuspectUrlVersion> changedVersions = asuv.updateFromJson(json, daemon);
    updateAuSuspectUrlVersions(auid, asuv, changedVersions);
  }

  /** Store an AuSuspectUrlVersions not obtained from StateManager.  Useful in tests.
   * Can only be called once per AU. */
  public synchronized void storeAuSuspectUrlVersions(String key,
      AuSuspectUrlVersions asuv) {
    updateAuSuspectUrlVersions(key, asuv, null);
  }

  /** Default behavior when AU is deleted/deactivated is to remove
   * AuSuspectUrlVersions from cache.  Persistent implementations should not remove
   * it from storage. */
  protected synchronized void handleAuDeletedAuSuspectUrlVersions(ArchivalUnit au) {
    suspectVers.remove(auKey(au));
  }

  /** Handle a cache miss.  Call hooks to load an object from backing
   * store, if any, or to create and store new default object. */
  protected AuSuspectUrlVersions handleAuSuspectUrlVersionsCacheMiss(String key) {
    AuSuspectUrlVersions asuv = doLoadAuSuspectUrlVersions(key);
    log.debug2("handleAuSuspectUrlVersionsCacheMiss: {}", asuv);
    if (asuv != null) {
      suspectVers.put(key, asuv);
    } else {
      asuv = newDefaultAuSuspectUrlVersions(key);
      suspectVers.put(key, asuv);
    }
    return asuv;
  }

  /** Return true if an AuSuspectUrlVersions exists for the given auid
   * @param key the auid
   */
  @Override
  public boolean hasAuSuspectUrlVersions(String key) {
    return suspectVers.containsKey(key);
  }

  /** Put an AuSuspectUrlVersions in the suspectVers map. */
  protected void putAuSuspectUrlVersions(String key, AuSuspectUrlVersions asuv) {
    suspectVers.put(key, asuv);
  }

  /** @return a Map suitable for an AuSuspectUrlVersions cache.  By default a
   * UniqueRefLruCache. */
  protected Map<String,AuSuspectUrlVersions> newAuSuspectUrlVersionsMap() {
    return new UniqueRefLruCache<>(DEFAULT_SUSPECT_VERSIONS_MAPS_CACHE_SIZE);
  }

  /** Return true if an update call for an unknown AuSuspectUrlVersions should be
   * allowed (and treated as a store).  By default it's allowed iff it's a
   * complete update (all peers).  Overridable because of the many tests
   * that were written when this was permissible */
  protected boolean isStoreOfMissingAuSuspectUrlVersionsAllowed(Set<SuspectUrlVersion> versions) {
    return versions == null || versions.isEmpty();
  }

  // /////////////////////////////////////////////////////////////////
  // NoAuPeerSet
  // /////////////////////////////////////////////////////////////////

  /** Cache of extant NoAuPeerSet instances */
  protected Map<String,DatedPeerIdSet> noAuPeerSets;

  /** Return the current singleton NoAuPeerSet for the auid,
   * creating one if necessary. */
  public synchronized DatedPeerIdSet getNoAuPeerSet(String key) {
    DatedPeerIdSet naps = noAuPeerSets.get(key);
    log.debug2("getNoAuPeerSet({}) = {}", key, naps);
    if (naps == null) {
      naps = handleNoAuPeerSetCacheMiss(key);
    }
    return naps;
  }

  public synchronized void updateNoAuPeerSet(String key,
					     DatedPeerIdSet naps) {
    updateNoAuPeerSet(key, naps, null);
  }

  public synchronized void updateNoAuPeerSet(String key,
					     DatedPeerIdSet naps,
					     Set<PeerIdentity> peers) {
    updateNoAuPeerSet(key, naps, peers, null);
  }

  public synchronized void updateNoAuPeerSet(String key,
					     DatedPeerIdSet naps,
					     Set<PeerIdentity> peers,
					     String cookie) {
    log.debug2("Updating NoAuPeerSet: {}: {})", key, naps);
    DatedPeerIdSet curnaps = noAuPeerSets.get(key);
    try {
      if (curnaps != null) {
	if (curnaps != naps) {
	  throw new IllegalStateException("Attempt to store from wrong NoAuPeerSet instance");
	}
	String json = naps.toJson(peers);
	doStoreNoAuPeerSetUpdate(key, naps, peers);
	doNotifyNoAuPeerSetChanged(key, json, cookie);
      } else if (isStoreOfMissingNoAuPeerSetAllowed(peers)) {
	// XXX log?
	noAuPeerSets.put(key, naps);
      } else {
	throw new IllegalStateException("Attempt to apply partial update to NoAuPeerSet not in cache: " + key);
      }
    } catch (IOException e) {
      log.error("Couldn't serialize NoAuPeerSet: {}", naps, e);
      throw new StateLoadStoreException("Couldn't serialize NoAuPeerSet: " +
					naps);
    }
  }

  /** Entry point from state service to store changes to an NoAuPeerSet.  Write
   * to DB, call hook to notify clients if appropriate
   * @param auid the auid
   * @param json the serialized set of changes
   * @param cookie propagated to JMS change notifications (if non-null)
   * @throws IOException if json conversion throws
   */
  public void updateNoAuPeerSetFromJson(String auid, String json, String cookie)
      throws IOException {
    DatedPeerIdSet naps = getNoAuPeerSet(auid);
    Set<PeerIdentity> changedPids = naps.updateFromJson(json, daemon);
    updateNoAuPeerSet(auid, naps, changedPids, cookie);
  }

  /** Store an NoAuPeerSet not obtained from StateManager.  Useful in tests.
   * Can only be called once per AU. */
  public synchronized void storeNoAuPeerSet(String key, DatedPeerIdSet naps) {
    updateNoAuPeerSet(key, naps, null);
  }

  /** Default behavior when AU is deleted/deactivated is to remove
   * NoAuPeerSet from cache.  Persistent implementations should not remove
   * it from storage. */
  protected synchronized void handleAuDeletedNoAuPeerSet(ArchivalUnit au) {
    noAuPeerSets.remove(auKey(au));
  }

  /** Handle a cache miss.  Call hooks to load an object from backing
   * store, if any, or to create and store new default object. */
  protected DatedPeerIdSet handleNoAuPeerSetCacheMiss(String key) {
    DatedPeerIdSet naps = doLoadNoAuPeerSet(key);
    if (naps != null) {
      noAuPeerSets.put(key, naps);
    } else {
      naps = newDefaultNoAuPeerSet(key);
      noAuPeerSets.put(key, naps);
    }
    return naps;
  }

  /** Return true if an NoAuPeerSet exists for the given auid
   * @param key the auid
   */
  @Override
  public boolean hasNoAuPeerSet(String key) {
    return noAuPeerSets.containsKey(key);
  }

  /** Put an NoAuPeerSet in the noAuPeerSets map. */
  protected void putNoAuPeerSet(String key, DatedPeerIdSet naps) {
    noAuPeerSets.put(key, naps);
  }

  /** @return a Map suitable for an NoAuPeerSet cache.  By default a
   * UniqueRefLruCache. */
  protected Map<String,DatedPeerIdSet> newNoAuPeerSetMap() {
    return new UniqueRefLruCache<>(DEFAULT_NO_PEER_SET_MAPS_CACHE_SIZE);
  }

  /** Return true if an update call for an unknown NoAuPeerSet should be
   * allowed (and treated as a store).  By default it's allowed iff it's a
   * complete update (all peers).  Overridable because of the many tests
   * that were written when this was permissible */
  protected boolean isStoreOfMissingNoAuPeerSetAllowed(Set<PeerIdentity> peers) {
    return peers == null || peers.isEmpty();
  }

  // /////////////////////////////////////////////////////////////////
  // UserAccount
  // /////////////////////////////////////////////////////////////////

  protected Map<String, UserAccount> userAccounts;

  protected Map<String,UserAccount> newUserAccountsMap() {
    return new ConcurrentHashMap<>();
  }

  @Override
  public Iterable<String> getUserAccountNames() throws IOException {
    return doLoadUserAccountNames();
  }

  @Override
  public synchronized UserAccount getUserAccount(String name) throws IOException {
    UserAccount acct = userAccounts.get(name);
    if (acct == null) {
      acct = handleUserAccountCacheMiss(name);
    }
    return acct;
  }

  protected UserAccount handleUserAccountCacheMiss(String name) throws IOException {
    UserAccount acct = doLoadUserAccount(name);
    if (acct != null) {
      userAccounts.put(name, acct);
    }
    return acct;
  }

  @Override
  public synchronized void storeUserAccount(UserAccount acct) throws IOException {
    if (acct == null) {
      throw new IllegalArgumentException("Cannot store null UserAccount");
    }

    String name = acct.getName();

    // Check whether the UserAccount already exists in this cache (if loaded
    // or added already) or the user exists in the StateStore:
    if (userAccounts.containsKey(name) || hasUserAccount(name)) {
      throw new IllegalStateException("Storing 2nd UserAccount: " + name);
    }

    userAccounts.put(name, acct);

    /*
    ClientA:
    storeUserAccount -> Store UserAccount to clientA's StateManager
    doStoreUserAccount -> send POST to REST server
                       -> (server) storeUserAccount
                       -> (server) doStoreUserAccount: Add UserAccount to server's StateManager
                       -> (server) doUserAccountChangedCallbacks: Add UserAccount to server's AccountManager
                       -> (server) doNotifyUserAccountChanged: Notify clients of new UserAccount
                       -> (clientB) receives JMS message
                       -> (clientB) doReceiveUserAccountChanged: Add to clientB's StateManager and AccountManager

    doUserAccountChangedCallbacks -> update clientA's AccountManager
    doNotifyUserAccountChanged -> no-op
     */

    doUserAccountChangedCallbacks(UserAccount.UserAccountChange.ADD, name, acct);
    doStoreUserAccount(name, acct, null);
    doNotifyUserAccountChanged(UserAccount.UserAccountChange.ADD, name, acct.toJson(), null);
  }

  @Override
  public synchronized UserAccount updateUserAccountFromJson(String username, String json, String cookie)
      throws IOException {
    UserAccount userAccount = getUserAccount(username);
    if (userAccount != null) {
      userAccount.updateFromJson(json);
      updateUserAccount(userAccount, AuUtil.jsonToMap(json).keySet(), cookie);
      return userAccount;
    } else {
      log.debug("Attempted to update non-existent UserAccount");
      return null;
    }
  }

  @Override
  public synchronized UserAccount updateUserAccount(UserAccount acct,
                                                    Set<String> fields) throws IOException {
    return updateUserAccount(acct, fields, null);
  }

  /** Update UserAccount in cache */
  public synchronized UserAccount updateUserAccount(UserAccount acct,
                                                    Set<String> fields,
                                                    String cookie) throws IOException {
    String username = acct.getName();
    log.debug2("Updating user account: {}", username);
    UserAccount curAcct = getUserAccount(username);
    try {
      if (curAcct != null) {
        if (curAcct != acct) {
          throw new IllegalStateException("Attempted to update a different UserAccount instance");
        }
        String json = UserAccount.jsonFromUserAccount(acct, fields);

        /*
        ClientA:
        doStoreUserAccount -> (clientA) send UserAccount to REST server
                           -> (server) REST server calls updateUserAccountFromJson
                           -> (server) -> updateUserAccount (this method)
                           -> (server) send JMS notifications to client
                           -> (clientB) receives JMS notification
                           -> (clientB) calls doReceiveUserAccountChanged
                           -> (clientB) updates its AccountManager and StateManager
        doUserAccountChangedCallbacks
                           -> (clientA) update client's AccountManager
        doNotifyUserAccountChanged
                           -> (clientA) do nothing
         */

        doUserAccountChangedCallbacks(UserAccount.UserAccountChange.UPDATE, username, acct);
        doStoreUserAccount(username, acct, fields);
        doNotifyUserAccountChanged(UserAccount.UserAccountChange.UPDATE, username, json, cookie);
      }
      else if (isStoreOfMissingUserAccountAllowed(fields)) {
        storeUserAccount(acct);
//        userAccounts.put(username, acct);
//        doStoreUserAccount(username, acct, fields);
//        doUserAccountChangedCallbacks(UserAccount.UserAccountChange.ADD, username, acct);
//        doNotifyUserAccountChanged(UserAccount.UserAccountChange.ADD, username, acct.toJson(), cookie);
      } else {
        throw new IllegalStateException("Attempted to update a missing user account: " + username);
      }
      return acct;
    } catch (IOException e) {
      log.error("Could not update user account", e);
      throw e;
    }
  }

  private boolean isStoreOfMissingUserAccountAllowed(Set<String> fields) {
    return fields == null || fields.isEmpty();
  }

  @Override
  public synchronized void removeUserAccount(UserAccount acct) throws IOException {
    if (acct == null) {
      // This may occur under normal operation because DELETE messages are
      // sent without a cookie. I.e., the originating client may call this
      // method twice; second time with null.
      if (!(this instanceof ClientStateManager)) {
        log.warn("Attempted to remove null UserAccount from a non-client StateManager");
      }
      return;
    }

    String username = acct.getName();
    userAccounts.remove(username);
    /*
    ClientA:
    doRemoveUserAccount -> send DELETE to REST server
                        -> (server) removeUserAccount
                        -> (server) doRemoveUserAccount: Remove UserAccount from server's StateStore
                        -> (server) doUserAccountChangedCallbacks: Remove UserAccount from server's AccountManager
                        -> (server) doNotifyUserAccountChanged: Notify clients of UserAccount removal
                        -> (clientB) receives JMS message
                        -> (clientB) doReceiveUserAccountChanged
     */
    doRemoveUserAccount(acct);
    doUserAccountChangedCallbacks(UserAccount.UserAccountChange.DELETE, username, acct);
    doNotifyUserAccountChanged(UserAccount.UserAccountChange.DELETE, username, null, null);
  }

  @Override
  public boolean hasUserAccount(String name) throws IOException {
    // Match first in Iterable
    for (String currentName : doLoadUserAccountNames()) {
      if (currentName.equals(name)) {
        return true;
      }
    }

    return false;
  }
}
