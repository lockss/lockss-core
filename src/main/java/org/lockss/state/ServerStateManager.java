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

/** StateManager that saves and loads state from persistent storage,
 * intended for use in REST service */
public class ServerStateManager extends CachingStateManager {

  protected static L4JLogger log = L4JLogger.getLogger();


//   @Override
//   public void initService(LockssDaemon daemon) throws LockssAppException {
//     super.initService(daemon);
//   }

  public void startService() {
    super.startService();
    setUpJmsSend();
  }

  public void stopService() {
    stopJms();
    super.stopService();
  }

//   public void setConfig(Configuration config, Configuration oldConfig,
// 			Configuration.Differences changedKeys) {
//     if (changedKeys.contains(PREFIX)) {
//     }
//   }

  /** Save the changes in the DB, send notification to clients */
  @Override
  protected void updateStoredObject(AuState cur, String json,
				    LockssDaemon daemon) throws IOException {
    super.updateStoredObject(cur, json, daemon);

    // XXX store json diffs in DB

    sendAuStateChangedEvent(auKey(cur.getArchivalUnit()), json, false);

  }

  /** Handle a cache miss.  Retrieve AuState from DB. */
  @Override
  protected AuState handleCacheMiss(ArchivalUnit au) {

    AuState aus = null;

    // XXX fetch AuState from database

    if (aus != null) {
      auStates.put(auKey(au), aus);
    }
    return aus;
  }

  @Override
  protected Map<String,AuState> newAuStateMap() {
    return new ReferenceMap<>(AbstractReferenceMap.ReferenceStrength.HARD,
			      AbstractReferenceMap.ReferenceStrength.WEAK);
  }
}
