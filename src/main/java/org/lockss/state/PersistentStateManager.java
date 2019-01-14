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

import java.io.IOException;
import java.util.*;
import org.lockss.app.*;
import org.lockss.db.DbException;
import org.lockss.log.*;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.plugin.*;
import org.lockss.protocol.*;

/** StateManager that saves and loads state from persistent storage. */
public class PersistentStateManager extends CachingStateManager {

  protected static L4JLogger log = L4JLogger.getLogger();

  // The database state manager SQL executor.
  private StateStore stateStore = null;


  // /////////////////////////////////////////////////////////////////
  // AuState
  // /////////////////////////////////////////////////////////////////

  /** Hook to store a new AuState in the DB.
   * @param key the auid
   * @param aus the AuState object, may be null
   * @param json the serialized set of changes
   * @throws IllegalStateException if this key is already present in the DB
   */
  @Override
  protected void doStoreAuStateBeanNew(String key,
                                       AuStateBean ausb)
      throws StateLoadStoreException {
    log.debug2("key = {}", key);
    log.debug2("ausb = {}", ausb);

    try {
      Long auSeq =
	  getStateStore().addArchivalUnitState(key, ausb);
      log.trace("auSeq = {}", auSeq);
    } catch (DbException dbe) {
      String message = "Exception caught persisting new AuState";
      log.error("key = {}", key);
      log.error("ausb = {}", ausb);
      throw new StateLoadStoreException(message, dbe);
    }

    log.debug2("Done");
  }

  /** Hook to store the changes to the AuState in the DB.
   * @param key the auid
   * @param aus the AuState object, may be null
   * @param json the serialized set of changes
   * @param map Map representation of change fields.
   */
  @Override
  protected void doStoreAuStateBeanUpdate(String key,
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
    } catch (DbException dbe) {
      String message = "Exception caught persisting new AuState";
      log.error("key = {}", key);
      log.error("ausb = {}", ausb);
      log.error("fields = {}", fields);
      throw new StateLoadStoreException(message, dbe);
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
    } catch (DbException dbe) {
      String message = "Exception caught finding AuState";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, dbe);
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
    } catch (DbException dbe) {
      String message = "Exception caught finding AuAgreements";
      log.error("key = {}", key);
      throw new StateLoadStoreException(message, dbe);
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
    } catch (DbException dbe) {
      String message = "Exception caught persisting AuAgreements";
      log.error("key = {}", key);
      log.error("aua = {}", aua);
      log.error("peers = {}", peers);
      throw new StateLoadStoreException(message, dbe);
    }

    log.debug2("Done");
  }

  /**
   * Provides the StateStore to use to load and store state objects in
   * persistent store.  Currently creates a DbStateManagerSql, overriden in
   * tests.
   * 
   * @return a StateStore implementation
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  protected StateStore getStateStore() throws DbException {
    if (stateStore == null) {
      if (theApp == null) {
	stateStore = new DbStateManagerSql(
	    LockssApp.getManagerByTypeStatic(ConfigDbManager.class));
      } else {
	stateStore = new DbStateManagerSql(
	    theApp.getManagerByType(ConfigDbManager.class));
      }
    }

    return stateStore;
  }
}
