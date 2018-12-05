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

/** StateManager that caches instances in memory.  Persistence may be
 * implemented by subclasses.
 *
 * For AuState, guarantees and enforces a single AuState per AU.
 */
public class CachingStateManager extends BaseStateManager {

  protected static L4JLogger log = L4JLogger.getLogger();

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
      auEventHandler = null;
    }
    if (pluginMgr != null) {
      pluginMgr.unregisterAuEventHandler(auEventHandler);
    }
    super.stopService();
  }

//   public void setConfig(Configuration config, Configuration oldConfig,
// 			Configuration.Differences changedKeys) {
//     super.setConfig(config, oldConfig, changedKeys);
//     if (changedKeys.contains(PREFIX)) {
//     }
//   }

  /** Return the AuState for the AU.  Each AU has a singleton AuState
   * instance */
  public AuState getAuState(ArchivalUnit au) {
    AuState aus = auStates.get(auKey(au));
    log.debug("getAuState({}) [{}] = {}", au, auKey(au), aus);
    if (aus == null) {
      aus = handleCacheMiss(au);
    }
    return aus;
  }

  /** Update the stored AuState with the values of the listed fields */
  public void updateAuState(AuState aus, Set<String> fields) {
    log.error("Updating: {}: {}", auKey(aus.getArchivalUnit()), fields);
    AuState cur = auStates.get(auKey(aus.getArchivalUnit()));
    if (cur != null) {
      if (cur != aus) {
	throw new IllegalStateException("Attempt to store from wrong AuState instance");
      }
      try {
	String json = aus.toJson(fields);
	log.debug2("Updating: {}", json);
	updateStoredObject(cur, json, daemon);
      } catch (IOException e) {
	log.error("Couldn't de/serialize AuState: {}", aus, e);
      }
    } else if (isStoreOfMissingAuStateAllowed(fields)) {
      storeAuState(aus);
    } else {
      throw new IllegalStateException("Attempt to apply partial update to AuState not in cache");
    }
  }

  protected boolean isStoreOfMissingAuStateAllowed(Set<String> fields) {
    return fields == null || fields.isEmpty();
  }

  protected void updateStoredObject(AuState cur, String json,
				    LockssDaemon daemon) throws IOException {
    cur.updateFromJson(json, daemon);
  }


  /** Store the AuState for the AU.  Can only be used once per AU. */
  public void storeAuState(AuState aus) {
    String key = auKey(aus.getArchivalUnit());
    if (auStates.containsKey(key)) {
      throw new IllegalStateException("Storing 2nd AuState: " + key);
    }
    auStates.put(key, aus);
  }

  private synchronized void handleAuDeleted(ArchivalUnit au) {
    // XXX If AU recreated need to ensure new AU is store in AuState.
    // Could do that with a handleAuCreated() which either stores the new
    // AU or (better) restores the AuState from a "deleted" map

//     auStates.remove(auKey(au));

  }

  /** Handle a cache miss.  No-persistence here, just create a new
   * AuState and put it in the cache. */
  protected AuState handleCacheMiss(ArchivalUnit au) {
    AuState aus = newDefaultAuState(au);
    auStates.put(auKey(au), aus);
    return aus;
  }

  protected Map<String,AuState> newAuStateMap() {
    return new HashMap<>();
  }

}
