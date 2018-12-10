/*

Copyright (c) 2000-2018 Board of Trustees of Leland Stanford Jr. University,
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

/** Contains the basic logic for all StateManagers.  Exact behavior
 * implemented and modified subclasses.
 *
 * For AuState, guarantees and enforces a single AuState per AU.
 */
public abstract class CachingStateManager extends BaseStateManager {

  protected static L4JLogger log = L4JLogger.getLogger();

  /** Cache of extant AuState instances */
  protected Map<String,AuState> auStates;

  protected AuEventHandler auEventHandler;

  @Override
  public void initService(LockssDaemon daemon) throws LockssAppException {
    super.initService(daemon);
    auStates = newAuStateMap();
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

  /** Return the current singleton AuState for the AU, creating one if
   * necessary. */
  public synchronized AuState getAuState(ArchivalUnit au) {
    AuState aus = auStates.get(auKey(au));
    log.debug("getAuState({}) [{}] = {}", au, auKey(au), aus);
    if (aus == null) {
      aus = handleCacheMiss(au);
    }
    return aus;
  }

  /** Update the stored AuState with the values of the listed fields.
   * @param aus The  source of the new values.  (Normally this will be
 */
  public synchronized void updateAuState(AuState aus, Set<String> fields) {
    String key = auKey(aus.getArchivalUnit());
    log.error("Updating: {}: {}", key, fields);
    AuState cur = auStates.get(key);
    try {
      if (cur != null) {
	if (cur != aus) {
	  throw new IllegalStateException("Attempt to store from wrong AuState instance");
	}
	String json = aus.toJson(fields);
	doStoreAuStateUpdate(key, aus, json, AuUtil.jsonToMap(json));
	doNotifyAuStateChanged(key, json);
      } else if (isStoreOfMissingAuStateAllowed(fields)) {
	// XXX log?
	auStates.put(key, aus);
	String json = aus.toJson(fields);
	doStoreAuStateNew(key, aus, json, AuUtil.jsonToMap(json));
      } else {
	throw new IllegalStateException("Attempt to apply partial update to AuState not in cache");
      }
    } catch (IOException e) {
      log.error("Couldn't serialize AuState: {}", aus, e);
      throw new StateLoadStoreException("Couldn't serialize AuState: " + aus);
    }
  }

  /** Store an AuState not obtained from StateManager.  Useful in tests.
   * Can only be called once per AU. */
  public synchronized void storeAuState(AuState aus) {
    String key = auKey(aus.getArchivalUnit());
    if (auStates.containsKey(key)) {
      throw new IllegalStateException("Storing 2nd AuState: " + key);
    }
    auStates.put(key, aus);
    try {
      String json = aus.toJson();
      doStoreAuStateNew(key, aus, json, AuUtil.jsonToMap(json));
    } catch (IOException e) {
      log.error("Couldn't serialize AuState: {}", aus, e);
      throw new StateLoadStoreException("Couldn't deserialize AuState: " + aus);
    }
  }

  /** Default behavior when AU is deleted/deactivated is to remove AuState
   * from cache.  Persistent implementations should not remove it from
   * storage. */
  protected synchronized void handleAuDeleted(ArchivalUnit au) {
    auStates.remove(auKey(au));
  }

  /** Handle a cache miss.  Call hooks to load an object from backing
   * store, if any, or to create and store new default object. */
  protected AuState handleCacheMiss(ArchivalUnit au) {
    String key = auKey(au);
    AuState aus = doLoadAuState(au);
    if (aus == null) {
      aus = newDefaultAuState(au);
      auStates.put(key, aus);
      try {
	String json = aus.toJson();
	doStoreAuStateNew(key, aus, json, AuUtil.jsonToMap(json));
      } catch (IOException e) {
	log.error("Couldn't serialize AuState: {}", aus, e);
	throw new StateLoadStoreException("Couldn't serialize AuState: " + aus);
      }
    }
    return aus;
  }

  /** @return a Map suitable for an AuState cache.  By default a HashMap,
   * for a complete cache. */
  protected Map<String,AuState> newAuStateMap() {
    return new HashMap<>();
  }

  /** Return true if an update call for an unknown AuState should be
   * allowed (and treated as a store).  By default it's allowed iff it's a
   * complete update (all fields).  Overridable because of the many tests
   * that were written when this was permissiable */
  protected boolean isStoreOfMissingAuStateAllowed(Set<String> fields) {
    return fields == null || fields.isEmpty();
  }

}
