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
import org.apache.commons.collections4.map.*;

import org.lockss.app.*;
import org.lockss.daemon.*;
import org.lockss.log.*;
import org.lockss.util.*;
import org.lockss.config.*;
import org.lockss.plugin.*;

/** StateManager that saves and loads state from persistent storage. */
public class DbStateManager extends CachingStateManager {

  protected static L4JLogger log = L4JLogger.getLogger();


  /** Entry point from state service to store changes to an AuState.  Write
   * to DB, call hook to notify clients if appropriate
   * @param key the auid
   * @param json the serialized set of changes
   * @param map Map representation of change fields.
   */
  // XXXFGL
  public void updateAuStateFromService(String auid, String json,
				       Map<String,Object> map) {

    doStoreAuStateUpdate(auid, null, json, map);
    doNotifyAuStateChanged(auid, json);
  }

  /** Hook to store a new AuState in the DB.
   * @param key the auid
   * @param aus the AuState object, may be null
   * @param json the serialized set of changes
   * @throws IllegalStateException if this key is already present in the DB
   */
  @Override
  protected void doStoreAuStateNew(String key, AuState aus,
			      String json, Map<String,Object> map) {

    // XXXFGL store new, complete object in DB

  }

  /** Hook to store the changes to the AuState in the DB.
   * @param key the auid
   * @param aus the AuState object, may be null
   * @param json the serialized set of changes
   * @param map Map representation of change fields.
   */
  @Override
  protected void doStoreAuStateUpdate(String key, AuState aus,
				 String json, Map<String,Object> map) {

    // XXXFGL store changes in DB

  }

  /** Hook to load an AuState from the DB.
   * @param au the AU
   * @return the AuState reflecting the current contents of the DB, or null
   * if there's no AuState for the AU in the DB.
   */
  @Override
  protected AuState doLoadAuState(ArchivalUnit au) {
    String key = auKey(au);
    AuState res = null;

    // XXXFGL load from DB, return null if not found

    return res;
  }

  // A reference map provides minimum acceptable amount of caching, given
  // the contract for getAuState().  But this would cause worse performance
  // due to more DB accesses.  The default full map provides the best
  // performance; if memory becomes an issue, an LRU reference map would
  // provide better performance than just a reference map.
//   @Override
//   protected Map<String,AuState> newAuStateMap() {
//     return new ReferenceMap<>(AbstractReferenceMap.ReferenceStrength.HARD,
// 			      AbstractReferenceMap.ReferenceStrength.WEAK);
//   }
}
