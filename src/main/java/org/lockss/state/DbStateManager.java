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


  /** XXXFGL Entry point from service */
  public void updateFromService(String auid, String json,
				Map<String,Object> map) {

    doPersistUpdate(auid, null, json, map);
  }

  /** Save the changes to the DB.
   * @param key the auid
   * @param aus the AuState object, may be null
   * @param json the serialized set of changes
   * @param fields set of changed fields.  If null, all fields changed
   * [Fernando - tell me if that makes it more difficult]
   */
  @Override
  protected void doPersistUpdate(String key, AuState aus,
				 String json, Map<String,Object> map) {

    // XXXFGL store changes in DB

  }

  /** Save a new object to the DB.
   * @param key the auid
   * @param aus the AuState object, may be null
   * @param json the serialized set of changes
   * @throws IllegalStateException if this key is already present in the DB
   */
  @Override
  protected void doPersistNew(String key, AuState aus,
			      String json, Map<String,Object> map) {

    // XXXFGL store new, complete object in DB

  }

  @Override
  protected AuState fetchPersistentAuState(ArchivalUnit au) {
    String key = auKey(au);
    AuState res = null;

    // load from DB, return null if not found

    return res;
  }

  /** With DB, don't need to keep all AuState objects in memory, can allow
   * them to be GCed if not referenced.  (Though, it might still be better
   * to keep them all as it would improve server response, and probably not
   * increase the peak memory needed. */
  @Override
  protected Map<String,AuState> newAuStateMap() {
    return new ReferenceMap<>(AbstractReferenceMap.ReferenceStrength.HARD,
			      AbstractReferenceMap.ReferenceStrength.WEAK);
  }
}
