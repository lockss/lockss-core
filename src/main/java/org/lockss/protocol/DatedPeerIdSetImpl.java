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

public class DatedPeerIdSetImpl extends PersistentPeerIdSetImpl implements
    DatedPeerIdSet {

  private long m_date = -1;
  
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

  /** (non-Javadoc)
   * @see org.lockss.protocol.DatedPeerIdSet#getDate()
   */
  public long getDate() {
    return m_date;
  }

  /* (non-Javadoc)
   * @see org.lockss.protocol.DatedPeerIdSet#setDate(java.lang.Long)
   */
  public void setDate(long l) {
    if (m_date != l) {
      m_date = l;
      m_changed = true;
    }
  }

  /** Store the set
   */
  @Override
  public void store() {
    getStateMgr().updateNoAuPeerSet(auid, this);
  }

}
