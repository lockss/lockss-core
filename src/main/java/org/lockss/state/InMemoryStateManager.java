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

/** StateManager that keeps objects in memory, persists AuState objects (in
 * memory) across AU deletion/creation, but not across java invocations.
 * Suitable for use in unit tests and other situations where on-disk
 * persistence is not needed.  */
public class InMemoryStateManager extends CachingStateManager {

  protected static L4JLogger log = L4JLogger.getLogger();

  // Serialized AuState instances for AUs that have been
  // deleted/deactivated.  Serializing is a more realistic way to
  // save/restore the state, plus it provides a convenient way to restore
  // the data into an AuState object with a different AU.
  protected Map<String,String> deletedAuStates = new HashMap<>();

  /** When AU deleted, backup any existing AuState to the deletedAuStates
   * map and delete from cache */
  protected synchronized void handleAuDeleted(ArchivalUnit au) {
    // Serialize the AU's AuState to "backing" store so it can be restored
    // if the AU is reactivated.

    String key = auKey(au);
    AuState aus = auStates.get(key);
    log.debug("Remembering: {}: {}", au, aus);
    if (aus != null) {
      try {
	String json = aus.toJson();
	deletedAuStates.put(key, json);
      } catch (IOException e) {
	log.error("Couldn't serialize AuState to \"backing\" store: {}",
		  aus, e);
	throw new StateLoadStoreException("Couldn't serialize AuState to \"backing\" store",
					  e);
      }
    }
    super.handleAuDeleted(au);
  }

  /** "Load" an AuState from the deletedAuStates if it's there */
  protected AuState doLoadAuState(ArchivalUnit au) {
    String key = auKey(au);
    String json = deletedAuStates.get(key);
    if (json != null) {
      AuState aus = newDefaultAuState(au);
      try {
	return aus.updateFromJson(json, daemon);
      } catch (IOException e) {
	log.error("Couldn't deserialize AuState from \"backing\" store: {}",
		  json, e);
	throw new StateLoadStoreException("Couldn't deserialize AuState from \"backing\" store",
					  e);
      }
    }
    return null;
  }

}
