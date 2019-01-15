/*
 * $Id$
 */

/*

Copyright (c) 2000-2003 Board of Trustees of Leland Stanford Jr. University,
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
import java.util.List;
import org.lockss.plugin.*;
import org.lockss.protocol.DatedPeerIdSet;
import org.lockss.repository.*;
import org.lockss.app.LockssAuManager;

/**
 * HistoryRepository is an inner layer of the NodeManager which handles the actual
 * storage of NodeStates.
 */
public interface HistoryRepository extends LockssAuManager {

  /**
   * Return the date/time the au was created.
   */
  public long getAuCreationTime() throws LockssRepositoryException;

  /**
   * Return the associated DatedPeerIdSet
   */
  public DatedPeerIdSet getNoAuPeerSet();
  
  /**
   * return the AuState.  Temporarily left here for a couple callers that
   * use other HistoryRepository methods
   * @return the loaded AuState.
   * @deprecated
   */
  @Deprecated
  public AuState getAuState();

  /**
   * Stores the DamagedNodeSet for this ArchivalUnit
   * @param nodeSet the set to store
   */
  public void storeDamagedNodeSet(DamagedNodeSet nodeSet);

  /**
   * Loads the DamagedNodeSet for this ArchivalUnit
   * @return the {@link DamagedNodeSet}
   */
  public DamagedNodeSet loadDamagedNodeSet();
  public boolean hasDamage(CachedUrlSet cus);
}
