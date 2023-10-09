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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.lockss.account.UserAccount;
import org.lockss.log.*;
import org.lockss.config.*;
import org.lockss.plugin.*;
import org.lockss.protocol.*;
import org.lockss.util.rest.exception.LockssRestException;
import org.lockss.state.AuSuspectUrlVersions.SuspectUrlVersion;

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
  public void doReceiveAuStateChanged(String auid, String json,
				      String cookie) {
    try {
      boolean doit = false;
      AuState cur;
      synchronized (this) {
	cur = auStates.get(auid);
	if (cur == null) {
	  log.debug2("Ignoring partial update for AuState we don't have: {}", auid);
	  return;
	}
	log.debug2("Updating: {} from {}", cur, json);
	if (isMyUpdate(cookie, json)) {
	  log.debug2("Ignoring my AuState change: {}: {}", cookie, json);
	} else {
	  doit = true;
	}
      }
      // Must not udpate object while holding StateManager lock
      if (doit) {
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

  protected synchronized void recordMyUpdate(String cookie, String json)
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
      log.debug2("patchArchivalUnitState({}, {}, {}, {}", key, auState, cookie, fields);
      configMgr.getRestConfigClient().patchArchivalUnitState(key, auState,
							     cookie);
    } catch (LockssRestException lre) {
      log.error("Couldn't store AuState: {}", ausb, lre);
    } catch (IOException e) {
      log.error("Couldn't serialize AuState: {}", ausb, e);
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
  public void doReceiveAuAgreementsChanged(String auid,
					   String json,
					   String cookie) {
    try {
      boolean doit = false;
      AuAgreements cur;
      synchronized (this) {
	cur = agmnts.get(auid);
	if (cur == null) {
	  log.debug2("Ignoring partial update for AuAgreements we don't have: {}", auid);
	  return;
	}
	log.debug2("Updating: {} from {}", cur, json);
	if (isMyUpdate(cookie, json)) {
	  log.debug2("Ignoring my AuAgreements change: {}: {}", cookie, json);
	} else {
	  doit = true;
	}
      }
      // Must not udpate object while holding StateManager lock
      if (doit) {
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
    } catch (LockssRestException lre) {
      log.error("Couldn't store AuAgreements: {}", aua, lre);
    } catch (IOException e) {
      log.error("Couldn't serialize AuAgreements: {}", aua, e);
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

  // /////////////////////////////////////////////////////////////////
  // AuSuspectUrlVersions
  // /////////////////////////////////////////////////////////////////

  /** Handle incoming AuSuspectUrlVersions changed msg */
  @Override
  public void doReceiveAuSuspectUrlVersionsChanged(String auid,
						   String json,
						   String cookie) {
    try {
      boolean doit = false;
      AuSuspectUrlVersions cur;
      synchronized (this) {
	cur = suspectVers.get(auid);
	if (cur == null) {
	  log.debug2("Ignoring partial update for AuSuspectUrlVersions we don't have: {}", auid);
	  return;
	}
	log.debug2("Updating: {} from {}", cur, json);
	if (isMyUpdate(cookie, json)) {
	  log.debug2("Ignoring my AuSuspectUrlVersions change: {}: {}", cookie, json);
	} else {
	  doit = true;
	}
      }
      // Must not udpate object while holding StateManager lock
      if (doit) {
	cur.updateFromJson(json, daemon);
      }
    } catch (IOException e) {
      log.error("Couldn't deserialize AuSuspectUrlVersions: {}", json, e);
    }
  }

  /** Send the changes to the StateService */
  @Override
  protected void doStoreAuSuspectUrlVersionsUpdate(String key,
					   AuSuspectUrlVersions asuv,
					   Set<SuspectUrlVersion> versions) {
    String json = null;

    try {
      json = asuv.toJson(versions);
      String cookie = makeCookie();
      recordMyUpdate(cookie, json);
      configMgr.getRestConfigClient().putArchivalUnitSuspectUrlVersions(key,
	  json, cookie);
    } catch (LockssRestException lre) {
      log.error("Couldn't store AuSuspectUrlVersions: {}", asuv, lre);
    } catch (IOException e) {
      log.error("Couldn't serialize AuSuspectUrlVersions: {}", asuv, e);
    }
  }

  @Override
  protected AuSuspectUrlVersions doLoadAuSuspectUrlVersions(String key) {
    AuSuspectUrlVersions res = null;
    String json = null;

    try {
      json = configMgr.getRestConfigClient()
	  .getArchivalUnitSuspectUrlVersions(key);
      log.debug2("json = {}", json);
    } catch (LockssRestException lre) {
      log.error("Couldn't get AuSuspectUrlVersions: {}", key, lre);
    }

    if (json != null) {
      try {
	res = newDefaultAuSuspectUrlVersions(key);
	res.updateFromJson(json, daemon);
      } catch (IOException e) {
	log.error("Couldn't deserialize AuSuspectUrlVersions: {}", json, e);
      }
    }

    return res;
  }

  // /////////////////////////////////////////////////////////////////
  // NoAuPeerSet
  // /////////////////////////////////////////////////////////////////

  /** Handle incoming NoAuPeerSet changed msg */
  @Override
  public void doReceiveNoAuPeerSetChanged(String auid,
					  String json,
					  String cookie) {
    try {
      boolean doit = false;
      DatedPeerIdSet cur;
      synchronized (this) {
	cur = noAuPeerSets.get(auid);
	if (cur == null) {
	  log.debug2("Ignoring partial update for NoAuPeerSet we don't have: {}",
		     auid);
	  return;
	}
	log.debug2("Updating: {} from {}", cur, json);
	if (isMyUpdate(cookie, json)) {
	  log.debug2("Ignoring my NoAuPeerSet change: {}: {}", cookie, json);
	} else {
	  doit = true;
	}
      }
      // Must not udpate object while holding StateManager lock
      if (doit) {
	cur.updateFromJson(json, daemon);
      }
    } catch (IOException e) {
      log.error("Couldn't deserialize NoAuPeerSet: {}", json, e);
    }
  }

  /** Send the changes to the StateService */
  @Override
  protected void doStoreNoAuPeerSetUpdate(String key,
					   DatedPeerIdSet naps,
					   Set<PeerIdentity> peers) {
    String json = null;

    try {
      json = naps.toJson(peers);
      String cookie = makeCookie();
      recordMyUpdate(cookie, json);
      configMgr.getRestConfigClient().putNoAuPeers(key, json, cookie);
    } catch (LockssRestException lre) {
      log.error("Couldn't store NoAuPeerSet: {}", naps, lre);
    } catch (IOException e) {
      log.error("Couldn't serialize NoAuPeerSet: {}", naps, e);
    }
  }

  @Override
  protected DatedPeerIdSet doLoadNoAuPeerSet(String key) {
    DatedPeerIdSet res = null;
    String json = null;

    try {
      json = configMgr.getRestConfigClient().getNoAuPeers(key);
      log.debug2("json = {}", json);
    } catch (LockssRestException lre) {
      log.error("Couldn't get NoAuPeerSet: {}", key, lre);
    }

    if (json != null) {
      try {
	res = newDefaultNoAuPeerSet(key);
	res.updateFromJson(json, daemon);
      } catch (IOException e) {
	log.error("Couldn't deserialize NoAuPeerSet: {}", json, e);
      }
    }

    return res;
  }

  @Override
  public void doReceiveUserAccountChanged(UserAccount.UserAccountChange op,
                                          String username, String json,
                                          String cookie) {
    try {
      boolean applyChange = false;
      UserAccount cur;

      synchronized (this) {
        cur = userAccounts.get(username);
        if (cur == null && op == UserAccount.UserAccountChange.UPDATE) {
          log.debug2("Ignoring partial update for UserAccount we don't have: {}", username);
          return;
        }
        if (isMyUpdate(cookie, json)) {
          log.debug2("Ignoring my UserAccount change: {}: {}", cookie, json);
        } else {
          applyChange = true;
        }
      }

      if (applyChange) {
        // Handle change in this client's CachingStateManager
        switch (op) {
          case ADD:
            log.debug2("Adding: {} from {}", username, json);
            ObjectMapper objMapper = new ObjectMapper();
            AuUtil.setFieldsOnly(objMapper);
            UserAccount acct = objMapper
                .readerFor(UserAccount.class)
                .readValue(json);

            userAccounts.put(username, acct);
            break;

          case UPDATE:
            log.debug2("Updating: {} from {}", cur, json);
            cur.updateFromJson(json);
            break;

          case DELETE:
            log.debug2("Removing: {}", username);
            userAccounts.remove(username);
            break;

          default:
            log.error("Unknown operation on UserAccount: {}", op);
            throw new IllegalArgumentException("Unknown operation on UserAccount");
        }

        // Handle change in this client's AccountManager
        doUserAccountChangedCallbacks(op, username, cur);
      }
    } catch (IOException e) {
      log.error("Couldn't deserialize AuState: {}", json, e);
    }
  }

  @Override
  protected Iterable<String> doLoadUserAccountNames() {
    try {
      return configMgr.getRestConfigClient().getUserAccountNames();
    } catch (LockssRestException lre) {
      log.error("Could not get user account names", lre);
      return null;
    }
  }

  @Override
  protected UserAccount doLoadUserAccount(String name) {
    try {
      return configMgr.getRestConfigClient().getUserAccount(name);
    } catch (LockssRestException e) {
      log.error("Could not get user account", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doStoreUserAccount(String username, UserAccount acct, Set<String> fields) {
    try {
      RestConfigClient cfgSvcClient = configMgr.getRestConfigClient();
      if (fields == null || fields.isEmpty()) {
        cfgSvcClient.postUserAccount(acct);
      } else {
        String json = UserAccount.jsonFromUserAccount(acct, fields);
        String cookie = makeCookie();
        recordMyUpdate(cookie, json);
        cfgSvcClient.patchUserAccount(username, json, cookie);
      }
    } catch (LockssRestException e) {
      log.error("Could not store user account", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      log.error("Error serializing to JSON", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doRemoveUserAccount(UserAccount acct) {
    try {
      configMgr.getRestConfigClient().deleteUserAccount(acct.getName());
    } catch (LockssRestException e) {
      log.error("Could not remove user account", e);
      throw new RuntimeException(e);
    }
  }
}
