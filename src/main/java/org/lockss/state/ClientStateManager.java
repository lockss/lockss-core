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

/** Caching StateManager that accesses state objects from a REST
 * StateService.  Receives notifications of changes sent by service. */
public class ClientStateManager extends CachingStateManager {

  protected static L4JLogger log = L4JLogger.getLogger();

  @Override
  public void initService(LockssDaemon daemon) throws LockssAppException {
    super.initService(daemon);
  }

  public void startService() {
    super.startService();
    setUpJmsReceive();
  }

  public void stopService() {
    stopJms();
    super.stopService();
  }

  /** Send the changes to the StateService */
  @Override
  protected void updateStoredObject(AuState cur, String json,
				    LockssDaemon daemon) throws IOException {
    super.updateStoredObject(cur, json, daemon);

    // XXX send json diffs to service

  }


  // XXX FIX RACE - sequence: fetch AuState, receive state changed msg (due
  // to other client sending patch) it, discard it because we don't have
  // the complete object to udpate, then receive response to original
  // request, which may now be out of date.

  /** Handle incoming AuState changed msg */
  @Override
  public synchronized void receiveAuState(String auid, String json,
					  boolean complete) {
    AuState cur = auStates.get(auid);
    String msg = "Updating";
    if (cur == null) {
      msg = "Storing";
      if (!complete) {
	log.debug2("Ignoring partial update for AuState we don't have: {}", auid);
	return;
      }
      ArchivalUnit au = pluginMgr.getAuFromIdIfExists(auid);
      if (au != null) {
	cur = newDefaultAuState(au);
      } else {	
	log.error("Can't create AuState for non-existent AU: {}", auid);
      }
    }
    try {
      log.debug2("{}: {} from {}", msg, cur, json);
      cur.updateFromJson(json, daemon);
    } catch (IOException e) {
      log.error("Couldn't deserialize AuState: {}", json, e);
    }
  }    

  // XXX See RACE above

  /** Handle a cache miss.  Retrieve AuState from service. */
  @Override
  protected AuState handleCacheMiss(ArchivalUnit au) {

    AuState aus = null;

    // XXX fetch AuState from service

    if (aus != null) {
      auStates.put(auKey(au), aus);
    }
    return aus;
  }
}
