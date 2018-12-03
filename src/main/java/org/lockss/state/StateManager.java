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

/** Manages loading and storing state objects.  This is a temporary
 * implementation using an in-memory store. */
public class StateManager extends BaseLockssManager
  implements ConfigurableManager  {

  protected static L4JLogger log = L4JLogger.getLogger();

  public static final String PREFIX = Configuration.PREFIX + "state.";

  private LockssDaemon daemon;
  private ConfigManager cmgr;
  private Map<String,AuState> auStates = new HashMap<>();

  public void initService(LockssDaemon daemon) throws LockssAppException {
    this.daemon = daemon;
    cmgr = daemon.getConfigManager();
    super.initService(daemon);
  }

  public void startService() {
    super.startService();
  }

  public void stopService() {
    super.stopService();
  }

  public void setConfig(Configuration config, Configuration oldConfig,
			Configuration.Differences changedKeys) {
    if (changedKeys.contains(PREFIX)) {
    }
  }

  /** Return the AuState for the AU.  Each AU has a singleton AuState
   * instance */
  public AuState getAuState(ArchivalUnit au) {
    AuState aus = auStates.get(auKey(au));
    log.debug("getAuState({}) [{}] = {}", au, auKey(au), aus);
    if (aus == null) {
      aus = newDefaultAuState(au);
      storeAuState(aus);
    }
    return aus;
  }

  /** Update the stored AuState with the values of the listed fields */
  public void updateAuState(AuState aus, Set<String> fields) {
    log.error("Updating: {}: xxx {}", auKey(aus.getArchivalUnit()), fields,
	      new Throwable());
    AuState cur = auStates.get(auKey(aus.getArchivalUnit()));
    if (cur != null) {
      if (cur != aus) {
	throw new IllegalStateException("Attempt to store from wrong AuState instance");
      }
      try {
	String json = aus.toJson(fields);
	log.debug2("Updating: {}", json);
	cur.updateFromJson(json, daemon);
      } catch (IOException e) {
	log.error("Couldn't de/serialize AuState: {}", aus, e);
      }
    } else {
      auStates.put(auKey(aus.getArchivalUnit()), aus);
    }
    // XXX send notifications
  }

  /** Store the AuState for the AU.  Can only been used once per AU. */
  public void storeAuState(AuState aus) {
    log.fatal("storeAuState()");
    String key = auKey(aus.getArchivalUnit());
    if (auStates.containsKey(key)) {
      throw new IllegalStateException("Storing 2nd AuState: " + key);
    }
    log.fatal("Storing: " + key, new Throwable());
    auStates.put(key, aus);
    // XXX send notifications
  }

  /** Create a default AuState */
  AuState newDefaultAuState(ArchivalUnit au) {
    return new AuState(au, null);
  }

  /** return the string to use as a key for the AU's AuState.  Normally the
   * auid, this is necessary right now because in testing some AUs have no
   * auid */
  private String auKey(ArchivalUnit au) {
    try {
      return au.getAuId();
    } catch (RuntimeException e) {
      return "" + System.identityHashCode(au);
    }
  }
}
