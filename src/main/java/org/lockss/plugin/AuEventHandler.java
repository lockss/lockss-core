/*

Copyright (c) 2012-2019 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.plugin;

import java.util.*;
import org.lockss.config.*;
import org.lockss.log.*;
import org.lockss.plugin.AuEvent;

/**
 * Handler for AU transition events such as created, deleted, reconfigured.
 * Classes that wish to be notified of a subset of these events will
 * probably want to extend {@link AuEventHandler.Base}, which provides a
 * null implementation, and define only the handlers they need.
 */
public interface AuEventHandler {
  /** Called after the AU is created (either by user action or at daemon
   * start time.
   * @param event the AuEvent
   * @param auid the auid
   * @param au the AU, if present in this component, else null.
   */
  void auCreated(AuEvent event, String auid, ArchivalUnit au);
  /** Called before the AU is deleted.
   * @param event the AuEvent
   * @param auid the auid
   * @param au the AU, if present in this component, else null.
   */
  void auDeleted(AuEvent event, String auid, ArchivalUnit au);
  /** Called after an existing AU's configuration is changed.
   * @param event the AuEvent
   * @param auid the auid
   * @param au the AU, if present in this component, else null.
   * @param oldAuConf the AU's previous configuration.
   */
  void auReconfigured(AuEvent event, String auid, ArchivalUnit au,
		      Configuration oldAuConf);
  /** Called after a change to the AU's content.
   * @param event the AuEvent
   * @param auid the auid
   * @param au the AU, if present in this component, else null.
   * @param info describes the change to the AU's content.
   */
  void auContentChanged(AuEvent event, String auid, ArchivalUnit au,
			AuEvent.ContentChangeInfo info);

  /** Convenience class with null handlers for all AuEventHandler events.
   * Specialize this and override the events of interest.  If you are only
   * interested in events pertaining to AUs that are present, override
   * methods that take an AU but not the auid.  If you are interested in
   * events pertaining to AUs that may not be present, override methods
   * that take also take an auid; the AU will be null if the AU is not
   * present.
   */
  public class Base implements AuEventHandler {
    private static L4JLogger log = L4JLogger.getLogger();
    public void auCreated(AuEvent event, String auid, ArchivalUnit au) {
      if (au != null) {
	auCreated(event, au);
      } else {
	log.debug("Ignoring received auCreated event for absent AU: " + auid);
      }
    }
    public void auDeleted(AuEvent event, String auid, ArchivalUnit au) {
      if (au != null) {
	auDeleted(event, au);
      } else {
	log.debug("Ignoring received auDeleted event for absent AU: " + auid);
      }
    }
    public void auReconfigured(AuEvent event, String auid, ArchivalUnit au,
			       Configuration oldAuConf) {
      if (au != null) {
	auReconfigured(event, au, oldAuConf);
      } else {
	log.debug("Ignoring received auReconfigured event for absent AU: "
		  + auid);
      }
    }
    public void auContentChanged(AuEvent event, String auid, ArchivalUnit au,
				 AuEvent.ContentChangeInfo info) {
      if (au != null) {
	auContentChanged(event, au, info);
      } else {
	log.debug("Ignoring received auContentChanged event for absent AU: "
		  + auid);
      }
    }

    public void auCreated(AuEvent event, ArchivalUnit au) {}
    public void auDeleted(AuEvent event, ArchivalUnit au) {}
    public void auReconfigured(AuEvent event, ArchivalUnit au,
			       Configuration oldAuConf) {}
    public void auContentChanged(AuEvent event, ArchivalUnit au,
				 AuEvent.ContentChangeInfo info) {}
  }
}
