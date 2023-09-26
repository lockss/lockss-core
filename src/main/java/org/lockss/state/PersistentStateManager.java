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

import java.io.IOException;
import java.util.*;

import org.lockss.account.UserAccount;
import org.lockss.app.*;
import org.lockss.log.*;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.protocol.*;
import org.lockss.state.AuSuspectUrlVersions.SuspectUrlVersion;

/** StateManager that saves and loads state from persistent storage. */
public class PersistentStateManager extends CachingStateManager {

  protected static L4JLogger log = L4JLogger.getLogger();

  // The database state manager SQL executor.
  private StateStore stateStore = null;


  // /////////////////////////////////////////////////////////////////
  // AuState
  // /////////////////////////////////////////////////////////////////

  /** Hook to store the changes to the AuState in the DB.
   * @param key the auid
   * @param aus the AuState object, may be null
   * @param json the serialized set of changes
   * @param map Map representation of change fields.
   */
  @Override
  protected void doStoreAuStateBean(String key,
				    AuStateBean ausb,
				    Set<String> fields)
      throws StateLoadStoreException {
    log.debug2("key = {}", key);
    log.debug2("ausb = {}", ausb);
    log.debug2("fields = {}", fields);

    try {
      Long auSeq =
	getStateStore().updateArchivalUnitState(key, ausb, fields);
      log.trace("auSeq = {}", auSeq);
    } catch (StoreException se) {
      String message = "Exception caught persisting new AuState: " +
        se.toString();
      log.error("key = {}", key);
      log.error("ausb = {}", ausb);
      log.error("fields = {}", fields);
      throw new StateLoadStoreException(message, se);
    }

    log.debug2("Done");
  }

  /** Hook to load an AuState from the DB.
   * @param au the AU
   * @return the AuState reflecting the current contents of the DB, or null
   * if there's no AuState for the AU in the DB.
   */
  @Override
  protected AuStateBean doLoadAuStateBean(String key) 
      throws StateLoadStoreException {
    AuStateBean res = null;

    try {
      res = getStateStore().findArchivalUnitState(key);

    } catch (IOException ioe) {
      String message = "Exception caught composing AuState";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, ioe);
    } catch (StoreException se) {
      String message = "Exception caught finding AuState";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, se);
    }

    log.debug2("res = {}", res);
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


  // /////////////////////////////////////////////////////////////////
  // AuAgreements
  // /////////////////////////////////////////////////////////////////

  /** Hook to load an AuAgreements from the DB.
   * @param au the AU
   * @return the AuAgreements reflecting the current contents of the DB, or null
   * if there's no AuAgreements for the AU in the DB.
   */
  @Override
  protected AuAgreements doLoadAuAgreements(String key) {
    AuAgreements res = null;

    try {
      res = getStateStore().findAuAgreements(key);
    } catch (IOException ioe) {
      String message = "Exception caught composing AuAgreements";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, ioe);
    } catch (StoreException se) {
      String message = "Exception caught finding AuAgreements";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, se);
    }

    log.debug2("res = {}", res);
    return res;
  }

  /** Hook to store the changes to the AuAgreements in the DB.
   * @param key the auid
   * @param aua the AuAgreements object, may be null
   * @param peers the set of PeerIdentity whose PeerAgreements should be
   * stored
   */
  @Override
  protected void doStoreAuAgreementsUpdate(String key, AuAgreements aua,
					   Set<PeerIdentity> peers) {

    log.debug2("key = {}", key);
    log.debug2("aua = {}", aua);
    log.debug2("peers = {}", peers);

    try {
      Long auSeq = getStateStore().updateAuAgreements(key, aua, peers);
      log.trace("auSeq = {}", auSeq);
    } catch (StoreException se) {
      String message = "Exception caught persisting AuAgreements";
      log.error("key = {}", key);
      log.error("aua = {}", aua);
      log.error("peers = {}", peers);
      throw new StateLoadStoreException(message, se);
    }

    log.debug2("Done");
  }

  // /////////////////////////////////////////////////////////////////
  // AuSuspectUrlVersions
  // /////////////////////////////////////////////////////////////////

  /**
   * Loads an AuSuspectUrlVersions object from the DB.
   * 
   * @param key
   *          A String with the key under which the AuSuspectUrlVersions is
   *          stored.
   * @return an AuSuspectUrlVersions object reflecting the current contents of
   *         the DB, or null if there's no AuSuspectUrlVersions for the AU in
   *         the DB.
   */
  @Override
  protected AuSuspectUrlVersions doLoadAuSuspectUrlVersions(String key) {
    AuSuspectUrlVersions res = null;

    try {
      res = getStateStore().findAuSuspectUrlVersions(key);
    } catch (IOException ioe) {
      String message = "Exception caught composing AuSuspectUrlVersions";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, ioe);
    } catch (StoreException se) {
      String message = "Exception caught finding AuSuspectUrlVersions";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, se);
    }

    log.debug2("res = {}", res);
    return res;
  }

  /**
   * Stores in the DB the changes to an AuSuspectUrlVersions object.
   * 
   * @param key
   *          A String with the key under which the AuSuspectUrlVersions is
   *          stored.
   * @param ausuv
   *          An AuSuspectUrlVersions with the object to be stored, which may be
   *          null.
   * @param versions
   *          A Set<SuspectUrlVersion> with the suspect URL versions to be
   *          stored.
   */
  @Override
  protected void doStoreAuSuspectUrlVersionsUpdate(String key,
      AuSuspectUrlVersions ausuv, Set<SuspectUrlVersion> versions) {

    log.debug2("key = {}", key);
    log.debug2("ausuv = {}", ausuv);
    log.debug2("versions = {}", versions);

    try {
      Long auSeq =
	  getStateStore().updateAuSuspectUrlVersions(key, ausuv, versions);
      log.trace("auSeq = {}", auSeq);
    } catch (StoreException se) {
      String message = "Exception caught persisting AuSuspectUrlVersions";
      log.error("key = {}", key);
      log.error("ausuv = {}", ausuv);
      log.error("versions = {}", versions);
      throw new StateLoadStoreException(message, se);
    }

    log.debug2("Done");
  }

  // /////////////////////////////////////////////////////////////////
  // NoAuPeerSet
  // /////////////////////////////////////////////////////////////////

  /**
   * Loads a NoAuPeerSet object from the DB.
   * 
   * @param key
   *          A String with the key under which the NoAuPeerSet is stored.
   * @return a DatedPeerIdSet object reflecting the current contents of the DB,
   *         or null if there's no NoAuPeerSet for the AU in the DB.
   */
  @Override
  protected DatedPeerIdSet doLoadNoAuPeerSet(String key) {
    DatedPeerIdSet res = null;

    try {
      res = getStateStore().findNoAuPeerSet(key);
    } catch (IOException ioe) {
      String message = "Exception caught composing NoAuPeerSet";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, ioe);
    } catch (StoreException se) {
      String message = "Exception caught finding NoAuPeerSet";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, se);
    }

    log.debug2("res = {}", res);
    return res;
  }

  /**
   * Stores in the DB the changes to a NoAuPeerSet object.
   * 
   * @param key
   *          A String with the key under which the NoAuPeerSet is stored.
   * @param dpis
   *          A DatedPeerIdSet with the object to be stored, which may be null.
   * @param peers
   *          A Set<PeerIdentity> with the peers whose PeerAgreements should be
   *          stored.
   */
  @Override
  protected void doStoreNoAuPeerSetUpdate(String key, DatedPeerIdSet dpis,
					  Set<PeerIdentity> peers) {

    log.debug2("key = {}", key);
    log.debug2("dpis = {}", dpis);
    log.debug2("peers = {}", peers);

    try {
      Long auSeq = getStateStore().updateNoAuPeerSet(key, dpis, peers);
      log.trace("auSeq = {}", auSeq);
    } catch (StoreException se) {
      String message = "Exception caught persisting NoAuPeerSet";
      log.error("key = {}", key);
      log.error("dpis = {}", dpis);
      log.error("peers = {}", peers);
      throw new StateLoadStoreException(message, se);
    }

    log.debug2("Done");
  }

  /**
   * Provides the StateStore to use to load and store state objects in
   * persistent store.  Currently creates a DbStateManagerSql, overriden in
   * tests.
   * 
   * @return a StateStore implementation
   * @throws StoreException
   *           if any problem occurred accessing the database.
   */
  protected StateStore getStateStore() throws StoreException {
    if (stateStore == null) {
      ConfigDbManager cfgDbMgr = theApp.getManagerByType(ConfigDbManager.class);
      assert cfgDbMgr.isReady();
      stateStore = new PersistentStateManagerStateStore(cfgDbMgr);
    }

    return stateStore;
  }

  // /////////////////////////////////////////////////////////////////
  // UserAccount
  // /////////////////////////////////////////////////////////////////

  @Override
  protected Iterable<String> doLoadUserAccountNames()
      throws StateLoadStoreException {
    Iterable<String> res = null;

    try {
      res = getStateStore().findUserAccountNames();
    } catch (IOException ioe) {
      String message = "Exception caught loading usernames";
      throw new StateLoadStoreException(message, ioe);
    } catch (StoreException se) {
      String message = "Exception caught finding usernames";
      throw new StateLoadStoreException(message, se);
    }

    log.debug2("res = {}", res);
    return res;
  }

  @Override
  protected Iterable<UserAccount> doLoadUserAccounts()
      throws StateLoadStoreException {
    Iterable<UserAccount> res = null;

    try {
      res = getStateStore().findUserAccounts();
    } catch (IOException ioe) {
      String message = "Exception caught loading user accounts";
      throw new StateLoadStoreException(message, ioe);
    } catch (StoreException se) {
      String message = "Exception caught finding user accounts";
      throw new StateLoadStoreException(message, se);
    }

    log.debug2("res = {}", res);
    return res;
  }

  /** Hook to load a {@link UserAccount} from the DB.
   * @param key the name of the user account.
   * @return the {@link UserAccount} reflecting the current contents of the DB, or null
   * if there's no {@link UserAccount} by that name in the DB.
   */
  @Override
  protected UserAccount doLoadUserAccount(String key)
      throws StateLoadStoreException {
    UserAccount res = null;

    try {
      res = getStateStore().findUserAccount(key);
    } catch (IOException ioe) {
      String message = "Exception caught loading user account";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, ioe);
    } catch (StoreException se) {
      String message = "Exception caught finding user account";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, se);
    }

    log.debug2("res = {}", res);
    return res;
  }

  @Override
  protected void doStoreUserAccount(String key, UserAccount acct, Set<String> fields) {
    try {
      getStateStore().updateUserAccount(key, acct, fields);
    } catch (StoreException e) {
      String message = "Exception caught persisting UserAccount";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, e);
    }
  }

  @Override
  protected void doRemoveUserAccount(UserAccount acct) {
    try {
      getStateStore().removeUserAccount(acct);
    } catch (StoreException e) {
      String message = "Exception caught removing user account";
      log.error("userName = {}", acct.getName());
      throw new StateLoadStoreException(message, e);
    }
  }
}
