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

import java.util.*;
import org.lockss.app.*;
import org.lockss.db.DbException;
import org.lockss.log.*;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.plugin.*;

/** StateManager that saves and loads state from persistent storage. */
public class DbStateManager extends CachingStateManager {

  protected static L4JLogger log = L4JLogger.getLogger();

  // The database state manager SQL executor.
  private DbStateManagerSql dbStateManagerSql = null;

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
    log.debug2("key = {}", key);
    log.debug2("aus = {}", aus);
    log.debug2("json = {}", json);
    log.debug2("map = {}", map);

    String pluginId = PluginManager.pluginIdFromAuId(key);
    log.trace("pluginId = {}", pluginId);

    String auKey = PluginManager.auKeyFromAuId(key);
    log.trace("auKey = {}", auKey);

    try {
      Long auSeq =
	  getDbStateManagerSql().addArchivalUnitState(pluginId, auKey, map);
      log.trace("auSeq = {}", auSeq);
    } catch (DbException dbe) {
      String message = "Exception caught persisting new AuState";
      log.error("key = {}", key);
      log.error("aus = {}", aus);
      log.error("json = {}", json);
      log.error("map = {}", map);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new RuntimeException(message, dbe);
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
    log.debug2("au = {}", au);

    String key = auKey(au);
    AuState res = null;
    String pluginId = PluginManager.pluginIdFromAuId(key);
    log.trace("pluginId = {}", pluginId);

    String auKey = PluginManager.auKeyFromAuId(key);
    log.trace("auKey = {}", auKey);

    try {
      Map<String, Object> auStateProps =
	  getDbStateManagerSql().findArchivalUnitState(pluginId, auKey);
      log.trace("auStateProps = {}", auStateProps);

      if (auStateProps != null) {
        res = populateAuStateFromMap(au, auStateProps);
      }
    } catch (DbException dbe) {
      String message = "Exception caught finding AuState";
      log.error("key = {}", key);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new RuntimeException(message, dbe);
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

  /**
   * Provides the database state manager SQL executor.
   * 
   * @return a DbStateManagerSql with the database state manager SQL executor.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private DbStateManagerSql getDbStateManagerSql() throws DbException {
    if (dbStateManagerSql == null) {
      if (theApp == null) {
	dbStateManagerSql = new DbStateManagerSql(
	    LockssApp.getManagerByTypeStatic(ConfigDbManager.class));
      } else {
	dbStateManagerSql = new DbStateManagerSql(
	    theApp.getManagerByType(ConfigDbManager.class));
      }
    }

    return dbStateManagerSql;
  }

  /**
   * Provides an AuState populated with the passed contents.
   * 
   * @param au
   *          An ArchivalUnit with the Archival Unit.
   * @param auStateProps
   *          A Map<String, Object> with the Archival Unit state properties.
   * @return an AuState populated with the passed contents.
   */
  private AuState populateAuStateFromMap(ArchivalUnit au,
      Map<String, Object> auStateProps) {
    log.debug2("au = {}", au);
    log.debug2("auStateProps = {}", auStateProps);

    AuState auState = new AuState(au,
	(Long)auStateProps.get("lastCrawlTime"),
	(Long)auStateProps.get("lastCrawlAttempt"),
	(Integer)auStateProps.get("lastCrawlResult"),
	(String)auStateProps.get("lastCrawlResultMsg"),
	(Long)auStateProps.get("lastTopLevelPollTime"),
	(Long)auStateProps.get("lastPollStart"),
	(Integer)auStateProps.get("lastPollResult"),
	(String)auStateProps.get("lastPollResultMsg"),
	(Long)auStateProps.get("pollDuration"),
	(Long)auStateProps.get("averageHashDuration"),
	0,
	null,
	(AuState.AccessType)auStateProps.get("accessType"),
	0,
	(Double)auStateProps.get("v3Agreement"),
	(Double)auStateProps.get("highestV3Agreement"),
	(SubstanceChecker.State)auStateProps.get("hasSubstance"),
	(String)auStateProps.get("substanceVersion"),
	(String)auStateProps.get("metadataVersion"),
	(Long)auStateProps.get("lastMetadataIndex"),
	(Long)auStateProps.get("lastContentChange"),
	(Long)auStateProps.get("lastPoPPoll"),
	(Integer)auStateProps.get("lastPoPPollResult"),
	(Long)auStateProps.get("lastLocalHashScan"),
	(Integer)auStateProps.get("numAgreePeersLastPoR"),
	(Integer)auStateProps.get("numWillingRepairers"),
	(Integer)auStateProps.get("numCurrentSuspectVersions"),
	(List<String>)auStateProps.get("cdnStems"),
	this
	);

    log.debug2("auState = {}", auState);
    return auState;
  }
}
