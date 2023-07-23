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

package org.lockss.protocol;

import java.io.*;
import java.util.*;
import com.fasterxml.jackson.annotation.*;

import org.lockss.app.*;
import org.lockss.plugin.AuUtil;


public class DatedPeerIdSetImpl extends PersistentPeerIdSetImpl implements
    DatedPeerIdSet {

  private long date = -1;
  
  /**
   * @param identityManager
   */
  public DatedPeerIdSetImpl(IdentityManager identityManager) {
    super(identityManager);
  }

  /**
   * Constructor.
   * 
   * @param auid
   *          A String with the Archival Unit identifier.
   * @param identityManager
   *          An {@link IdentityManager} to translate {@link String}s to
   *          {@link PeerIdentity} instances.
   */
  public DatedPeerIdSetImpl(String auid, IdentityManager identityManager) {
    super(auid, identityManager);
  }

  /**
   * Factory method for json/Jackson.  This creates a "bean" instance which
   * is used only as a source from which to copy data, then it is
   * discarded.  Hence, idMgr is not needed.
   * @param auid The AUID
   */
  @JsonCreator
  public static DatedPeerIdSetImpl make(@JsonProperty("auid") String auid) {
    DatedPeerIdSetImpl res = new DatedPeerIdSetImpl(auid, null);
    return res;
  }

  public static DatedPeerIdSetImpl make(String auid, IdentityManager idMgr) {
    DatedPeerIdSetImpl res = new DatedPeerIdSetImpl(auid, idMgr);
    return res;
  }

  /** (non-Javadoc)
   * @see org.lockss.protocol.DatedPeerIdSet#getDate()
   */
  public long getDate() {
    return date;
  }

  /* (non-Javadoc)
   * @see org.lockss.protocol.DatedPeerIdSet#setDate(java.lang.Long)
   */
  public void setDate(long l) {
    if (date != l) {
      date = l;
      m_changed = true;
    }
  }

  public boolean equals(Object o) {
    if (o instanceof DatedPeerIdSetImpl) {
      DatedPeerIdSetImpl dpis = (DatedPeerIdSetImpl)o;
      return date == dpis.date && peerSet.equals(dpis.peerSet);
    } else {
      return false;
    }
  }

  /* A hash code must always return a value; it cannot throw an IOException. */
  public int hashCode() {
    return peerSet.hashCode() + (int)date;
  }

  @Override
  public synchronized String toJson(Set<PeerIdentity> peers)
      throws IOException {
    return AuUtil.jsonFromDatedPeerIdSetImpl(makeBean(peers));
  }

  /**
   * Creates serializable bean, with peerids replaces by their string
   *
   * @param peers
   *          A Set<PeerIdentity> with the peers to be included.
   * @return a DatedPeerIdSetImpl with the newly created object.
   */
  DatedPeerIdSetImpl makeBean(Set<PeerIdentity> peers) {
    DatedPeerIdSetImpl res = new DatedPeerIdSetImpl(auid, m_identityManager);
    res.rawSet = makeRawSet(peers);
    res.date = date;
    return res;
  }

  /**
   * Update this object from the serialized data in the json string
   *
   * @param json
   *          A String with the JSON text.
   * @param app
   *          A LockssApp with the LOCKSS context.
   * @return a Set<PeerIdentity> that was updated from the JSON source.
   * @throws IOException
   *           if any problem occurred during the deserialization.
   */
  public synchronized Set<PeerIdentity> updateFromJson(String json,
						       LockssApp app)
      throws IOException {
    // Deserialize the JSON text into a new, scratch instance.
    DatedPeerIdSetImpl srcSet = AuUtil.datedPeerIdSetImplFromJson(json);
    // Get the peer identities.
    peerSet = internPeerIdSet(m_identityManager, srcSet.rawSet);
    date = srcSet.date;
    postUnmarshal(app);
    return peerSet;
  }

  /**
   * Deserializes a JSON string into a new DatedPeerIdSetImpl object.
   *
   * @param key
   *          A String with the Archival Unit identifier.
   * @param json
   *          A String with the JSON text.
   * @param daemon
   *          A LockssDaemon with the LOCKSS daemon.
   * @return a DatedPeerIdSetImpl with the newly created object.
   * @throws IOException
   *           if any problem occurred during the deserialization.
   */
  public static DatedPeerIdSetImpl fromJson(String key, String json,
					    LockssDaemon daemon)
      throws IOException {
    DatedPeerIdSetImpl res =
	DatedPeerIdSetImpl.make(key, daemon.getIdentityManager());
    res.updateFromJson(json, daemon);
    return res;
  }


  /** Store the set
   */
  @Override
  public void store() {
    if (m_changed) {
      getStateMgr().updateNoAuPeerSet(auid, this);
      m_changed = false;
    }
  }

  @Override
  public String toString() {
    return "[DPIDSet: " + auid + ": " + date + ": " + peerSet + "]";
  }

}
