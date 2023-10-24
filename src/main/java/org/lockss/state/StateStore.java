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

import org.lockss.account.UserAccount;
import org.lockss.app.StoreException;
import org.lockss.protocol.AuAgreements;
import org.lockss.protocol.DatedPeerIdSet;
import org.lockss.protocol.PeerIdentity;
import org.lockss.state.AuSuspectUrlVersions.SuspectUrlVersion;

import java.io.IOException;
import java.util.Set;

/** interface between StateManager and persistent state store
 * implementations.
 */

public interface StateStore {

  /** Return the AuStateBean associated with the key (an AUID)
   * @param key the key under which the AuStateBean is stored
   * @return the AuStateBean, or null if not present in the store
   */
  public AuStateBean findArchivalUnitState(String key)
      throws StoreException, IOException;

  /** Update an AuStateBean in the store, creating it if not already
   * present.  If already present, only those fields listed in
   * <code>fields</code> must be stored, but it it permissible to ignore
   * <code>fields</code> and store the entire object.
   * @param key the key under which the AuStateBean should be stored
   * @param ausb the AuStateBean with the values to be written
   * @param fields names of fields whose values must be written
   * @return 
   */
  public Long updateArchivalUnitState(String key, AuStateBean ausb,
				      Set<String> fields)
      throws StoreException;

  /** Return the AuAgreements associated with the key (an AUID)
   * @param key the key under which the AuAgreements is stored
   * @return the AuAgreements, or null if not present in the store
   */
  public AuAgreements findAuAgreements(String key)
      throws StoreException, IOException;

  /** Update an AuAgreements in the store, creating it if not already
   * present.  If already present, only those peers listed in
   * <code>peers</code> must be stored, but it it permissible to ignore
   * <code>peers</code> and store the entire object.
   * @param key the key under which the AuAgreements should be stored
   * @param aua the AuAgreements to be written
   * @param peers peers whose PeerAgreements must be written
   * @return 
   */
  public Long updateAuAgreements(String key, AuAgreements aua,
				 Set<PeerIdentity> peers)
      throws StoreException;

  /**
   * Provides the AuSuspectUrlVersions associated with the key (an AUID).
   * 
   * @param key
   *          A String with the key under which the AuSuspectUrlVersions is
   *          stored.
   * @return an AuSuspectUrlVersions, or null if not present in the store.
   * @throws StoreException
   *           if any problem occurred accessing the data.
   * @throws IOException
   *           if any problem occurred accessing the data.
   */
  public AuSuspectUrlVersions findAuSuspectUrlVersions(String key)
      throws StoreException, IOException;

  /**
   * Updates an AuSuspectUrlVersions in the store, creating it if not already
   * present. If already present, only those fields listed in <code>ausuv</code>
   * must be stored, but it it permissible to ignore <code>ausuv</code> and
   * store the entire object.
   * 
   * @param key
   *          A String with the key under which the AuSuspectUrlVersions is
   *          stored.
   * @param ausuv
   *          An AuSuspectUrlVersions with the object to be updated.
   * @param versions
   *          A Set<SuspectUrlVersion> with the fields that must be written.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws StoreException
   *           if any problem occurred accessing the data.
   */
  public Long updateAuSuspectUrlVersions(String key, AuSuspectUrlVersions ausuv,
				         Set<SuspectUrlVersion> versions)
      throws StoreException;

  /**
   * Provides the NoAuPeerSet associated with the key (an AUID).
   * 
   * @param key
   *          A String with the key under which the NoAuPeerSet is stored.
   * @return a DatedPeerIdSet, or null if not present in the store.
   * @throws StoreException
   *           if any problem occurred accessing the data.
   * @throws IOException
   *           if any problem occurred accessing the data.
   */
  public DatedPeerIdSet findNoAuPeerSet(String key)
      throws StoreException, IOException;

  /**
   * Updates a NoAuPeerSet in the store, creating it if not already present.
   * If already present, only those peers listed in <code>peers</code> must be
   * stored, but it it permissible to ignore <code>peers</code> and store the
   * entire object.
   * 
   * @param key
   *          A String with the key under which the NoAuPeerSet is stored.
   * @param dpis
   *          A DatedPeerIdSet wth the object to be updated.
   * @param peers
   *          A Set<PeerIdentity> with the peers that must be written.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws StoreException
   *           if any problem occurred accessing the data.
   */
  public Long updateNoAuPeerSet(String key, DatedPeerIdSet dpis,
				Set<PeerIdentity> peers)
      throws StoreException;

  // /////////////////////////////////////////////////////////////////
  // UserAccount
  // /////////////////////////////////////////////////////////////////

  default Iterable<String> findUserAccountNames() throws StoreException, IOException {
    throw new UnsupportedOperationException();
  }

  default UserAccount findUserAccount(String key) throws StoreException, IOException {
    throw new UnsupportedOperationException();
  }

  default void updateUserAccount(String key, UserAccount acct, Set<String> fields) throws StoreException {
    throw new UnsupportedOperationException();
  }

  default void removeUserAccount(UserAccount acct) {
    throw new UnsupportedOperationException();
  }
}
