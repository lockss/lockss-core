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
   * @throws IOException if json conversion throws
   */
  public void updateAuStateFromService(String auid, String json,
				       Map<String,Object> map)
      throws IOException {
    AuStateBean ausb = getAuStateBean(auid);
    ausb.updateFromJson(json, daemon);
    updateAuStateBean(auid, ausb, map.keySet());
  }

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

    String pluginId = PluginManager.pluginIdFromAuId(key);
    log.trace("pluginId = {}", pluginId);

    String auKey = PluginManager.auKeyFromAuId(key);
    log.trace("auKey = {}", auKey);

    try {
      Long auSeq =
	  getDbStateManagerSql().addArchivalUnitState(pluginId, auKey, ausb);
      log.trace("auSeq = {}", auSeq);
    } catch (DbException dbe) {
      String message = "Exception caught persisting new AuState";
      log.error("key = {}", key);
      log.error("ausb = {}", ausb);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
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

    String pluginId = PluginManager.pluginIdFromAuId(key);
    log.trace("pluginId = {}", pluginId);

    String auKey = PluginManager.auKeyFromAuId(key);
    log.trace("auKey = {}", auKey);

    try {
      Long auSeq =
          getDbStateManagerSql().updateArchivalUnitState(pluginId, auKey, ausb);
      log.trace("auSeq = {}", auSeq);
    } catch (DbException dbe) {
      String message = "Exception caught persisting new AuState";
      log.error("key = {}", key);
      log.error("ausb = {}", ausb);
      log.error("fields = {}", fields);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
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
    String pluginId = PluginManager.pluginIdFromAuId(key);
    log.trace("pluginId = {}", pluginId);

    String auKey = PluginManager.auKeyFromAuId(key);
    log.trace("auKey = {}", auKey);

    try {
      String stateString =
	  getDbStateManagerSql().findArchivalUnitState(pluginId, auKey);
      log.trace("auStateProps = {}", stateString);

      if (stateString != null) {
        res = newDefaultAuStateBean(key)
            .updateFromJson(stateString, daemon);
      }
    } catch (IOException ioe) {
      String message = "Exception caught composing AuState";
      log.error("key = {}", key);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new StateLoadStoreException(message, ioe);
    } catch (DbException dbe) {
      String message = "Exception caught finding AuState";
      log.error("key = {}", key);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new StateLoadStoreException(message, dbe);
    }

    log.debug2("res = {}", res);
    return res;
  }

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
}
