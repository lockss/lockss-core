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
import org.lockss.protocol.*;

/** Manages loading and storing state objects.  */
public interface StateManager extends LockssManager {

  public static final String PREFIX = Configuration.PREFIX + "state.";


  // /////////////////////////////////////////////////////////////////
  // AuState
  // /////////////////////////////////////////////////////////////////

  /** Return the current singleton AuState for the AU, creating one if
   * necessary.  There is only one AuState instance in existence for any AU
   * at any time, though that instance may change over time.  As long as
   * anyone has a pointer to an instance, this method must return the same
   * instance on each call.  If all references to the instance are deleted,
   * this method may return a new instance on the next call.  If the AU is
   * deleted or deactivated, the next call (after the AU is reactivated)
   * may return a new instance.  */
  public AuState getAuState(ArchivalUnit au);

  /** Return the current singleton AuStateBean for the auid, creating one
   * if necessary. */
  public AuStateBean getAuStateBean(String key);

  /** Update the stored AuState with the values of the listed fields */
  public void updateAuState(AuState aus, Set<String> fields);

  /** Update the stored AuStateBean with the values of the listed fields */
  public void updateAuStateBean(String key, AuStateBean ausb, Set<String> fields);

  /** Store the AuState for the AU.  Can only be used once per AU. */
  public void storeAuState(AuState aus);

  /** Store the AuStateBean for the AU.  Can only be used once per AU. */
  public void storeAuStateBean(String key, AuStateBean ausb);

  /** Entry point from state service to store changes to an AuState.
   * @param key the auid
   * @param json the serialized set of changes
   * @throws IOException if json conversion throws
   */
  default public void updateAuStateFromJson(String auid, String json)
      throws IOException {
    throw new UnsupportedOperationException("updateAuStateFromJson() available only in Server implementation");
  }

  /** Entry point from state service to store an AuState.
   * @param key the auid
   * @param json the serialized AuStateBean
   * @throws IOException if json conversion throws
   */
  default public void storeAuStateFromJson(String auid, String json)
      throws IOException {
    throw new UnsupportedOperationException("storeAuStateFromJson() available only in Server implementation");
  }

  /** Return true if an AuState(Bean) exists for the given auid
   * @param key the auid
   */
  public boolean auStateExists(String key);


  // /////////////////////////////////////////////////////////////////
  // AuAgreements
  // /////////////////////////////////////////////////////////////////

  /** Return the current singleton AuAgreements for the AU, creating one if
   * necessary.  There is only one AuAgreements instance in existence for any AU
   * at any time, though that instance may change over time.  As long as
   * anyone has a pointer to an instance, this method must return the same
   * instance on each call.  If all references to the instance are deleted,
   * this method may return a new instance on the next call.  If the AU is
   * deleted or deactivated, the next call (after the AU is reactivated)
   * may return a new instance.  */
  public AuAgreements getAuAgreements(String key);

  /** Update the stored AuAgreements with the values of the listed peers */
  public void updateAuAgreements(String key, AuAgreements aua, Set<PeerIdentity> peers);

  /** Store the AuAgreements for the AU.  Can only be used once per AU. */
  public void storeAuAgreements(String key, AuAgreements aua);

  public void updateAuAgreementsFromJson(String auid, String json)
      throws IOException;

  /** Return true if an AuAgreements exists for the given auid
   * @param key the auid
   */
  public boolean auAgreementsExists(String key);

  /** Load/store exception.  Clients of AuState aren't prepared for checked
   * exceptions; this is used to turn them into RuntimeExceptions */
  public static class StateLoadStoreException extends RuntimeException {

    public StateLoadStoreException() {
      super();
    }

    public StateLoadStoreException(String message) {
      super(message);
    }

    public StateLoadStoreException(Throwable cause) {
      super(cause);
    }

    public StateLoadStoreException(String message, Throwable cause) {
      super(message, cause);
    }
  }


}