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
import org.lockss.rs.exception.LockssRestException;

/** Caching StateManager that accesses state objects from a REST
 * StateService.  Receives notifications of changes sent by service. */

public class ClientStateManager extends CachingStateManager {

  protected static L4JLogger log = L4JLogger.getLogger();

  // Unique client identifier
  private String cliendId =
    org.apache.commons.lang3.RandomStringUtils.randomAlphabetic(8);

  // Request counter
  private long reqId = 0;

  // Make a unique cookie for a request
  private String makeCookie() {
    return cliendId + "-" + ++reqId;
  }

  @Override
  public void startService() {
    super.startService();
    setUpJmsReceive();
  }

  @Override
  public void stopService() {
    stopJms();
    super.stopService();
  }


  // /////////////////////////////////////////////////////////////////
  // AuState
  // /////////////////////////////////////////////////////////////////

  // Records changes we send so we can avoid (re)applying them when
  // received.  Changeset is included along with request cookie to guard
  // against non-unique cookie.
  private Map<String,Map<String,Object>> myChanges = new HashMap<>();

  /** Handle incoming AuState changed msg */
  @Override
  public synchronized void doReceiveAuStateChanged(String auid, String json,
						   String cookie) {
    AuState cur = auStates.get(auid);
    if (cur == null) {
      log.debug2("Ignoring partial update for AuState we don't have: {}", auid);
      return;
    }
    try {
      log.debug2("Updating: {} from {}", cur, json);
      if (isMyUpdate(cookie, json)) {
	log.debug2("Ignoring my AuState change: {}: {}", cookie, json);
      } else {
	cur.updateFromJson(json, daemon);
      }
    } catch (IOException e) {
      log.error("Couldn't deserialize AuState: {}", json, e);
    }
  }    

  private synchronized boolean isMyUpdate(String cookie, String json)
      throws IOException {
//     if (myChanges.containsKey(cookie)) {
//       myChanges.remove(cookie);
//       return true;
//     }
    if (myChanges.containsKey(cookie)) {
      Map jmap = AuUtil.jsonToMap(json);
      if (myChanges.remove(cookie, jmap)) {
	return true;
      } else {
	log.error("Apparently non-unique cookie {} received with change: {}, expected {}",
		  cookie, jmap, myChanges.get(cookie));
	return false;
      }
    }
    return false;
  }

  private synchronized void recordMyUpdate(String cookie, String json)
      throws IOException {
    log.debug2("mychanges.add: {}", cookie);
    myChanges.put(cookie, AuUtil.jsonToMap(json));
  }

  /** Send the changes to the StateService */
  @Override
  protected void doStoreAuStateBean(String key,
				    AuStateBean ausb,
				    Set<String> fields) {
    String auState = null;

    try {
      auState = ausb.toJson(fields);
      String cookie = makeCookie();
      recordMyUpdate(cookie, auState);
      configMgr.getRestConfigClient().patchArchivalUnitState(key, auState,
							     cookie);
    } catch (IOException e) {
      log.error("Couldn't serialize AuState: {}", ausb, e);
    } catch (LockssRestException lre) {
      log.error("Couldn't store AuState: {}", ausb, lre);
    }
  }

  @Override
  protected AuStateBean doLoadAuStateBean(String key) {
    AuStateBean res = null;
    String auState = null;

    try {
      auState = configMgr.getRestConfigClient().getArchivalUnitState(key);
    } catch (LockssRestException lre) {
      log.error("Couldn't get AuState: {}", auState, lre);
    }

    log.debug2("austate = {}", auState);

    if (auState != null) {
      try {
	res = new AuStateBean().updateFromJson(auState, daemon);
      } catch (IOException e) {
	log.error("Couldn't deserialize AuState: {}", auState, e);
      }
    }

    return res;
  }

  // /////////////////////////////////////////////////////////////////
  // AuAgreements
  // /////////////////////////////////////////////////////////////////

  /** Handle incoming AuAgreements changed msg */
  @Override
  public synchronized void doReceiveAuAgreementsChanged(String auid,
							String json,
							String cookie) {
    AuAgreements cur = agmnts.get(auid);
    if (cur == null) {
      log.debug2("Ignoring partial update for AuAgreements we don't have: {}", auid);
      return;
    }
    try {
      log.debug2("Updating: {} from {}", cur, json);
      if (isMyUpdate(cookie, json)) {
	log.debug2("Ignoring my AuAgreements change: {}: {}", cookie, json);
      } else {
	cur.updateFromJson(json, daemon);
      }
    } catch (IOException e) {
      log.error("Couldn't deserialize AuAgreements: {}", json, e);
    }
  }

  /** Send the changes to the StateService */
  @Override
  protected void doStoreAuAgreementsUpdate(String key,
					   AuAgreements aua,
					   Set<PeerIdentity> peers) {
    String auAgreementsJson = null;

    try {
      auAgreementsJson = aua.toJson(peers);
      String cookie = makeCookie();
      recordMyUpdate(cookie, auAgreementsJson);
      configMgr.getRestConfigClient().patchArchivalUnitAgreements(key,
	  auAgreementsJson, cookie);
    } catch (IOException e) {
      log.error("Couldn't serialize AuAgreements: {}", aua, e);
    } catch (LockssRestException lre) {
      log.error("Couldn't store AuAgreements: {}", aua, lre);
    }
  }

  @Override
  protected AuAgreements doLoadAuAgreements(String key) {
    AuAgreements res = null;
    String auAgreementsJson = null;

    try {
      auAgreementsJson =
	  configMgr.getRestConfigClient().getArchivalUnitAgreements(key);
      log.debug2("auAgreementsJson = {}", auAgreementsJson);
    } catch (LockssRestException lre) {
      log.error("Couldn't get AuAgreements: {}", key, lre);
    }

    if (auAgreementsJson != null) {
      try {
	res = newDefaultAuAgreements(key);
	res.updateFromJson(auAgreementsJson, daemon);
      } catch (IOException e) {
	log.error("Couldn't deserialize AuAgreements: {}", auAgreementsJson, e);
      }
    }

    return res;
  }
}
