/*
 * $Id$
 */

/*
Copyright (c) 2000-2008 Board of Trustees of Leland Stanford Jr. University,
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

import org.lockss.util.*;
import org.lockss.util.os.PlatformUtil;

public class PersistentPeerIdSetImpl implements PersistentPeerIdSet {
  // Static constants 
  protected static final String TEMP_EXTENSION = ".temp";

  // Internal variables
  private IdentityManager m_identityManager;
  private static Logger m_logger = Logger.getLogger();
  protected Set<PeerIdentity> m_setPeerId = new HashSet<PeerIdentity>();
  protected boolean m_changed = false;


  public PersistentPeerIdSetImpl(IdentityManager identityManager) {
    m_identityManager = identityManager;
  }

  /** Store the set and retain in memory
   */
  public void store() throws IOException {
  }

  /** Store the set and optionally remove from memory
   * @param release if true the set will be released
   */
  public void store(boolean release) throws IOException {
  }

  /** Release resources without saving */
  public void release() {
  }

  public boolean add(PeerIdentity pi) {
    boolean result;

    result = m_setPeerId.add(pi);
    m_changed |= result;
      
    return result;
  }

  public boolean addAll(Collection<? extends PeerIdentity> cpi) {
    boolean result;

    result = m_setPeerId.addAll(cpi);
    m_changed |= result;

    return result;
  }


  public void clear() {
    if (!m_setPeerId.isEmpty()) {
      m_setPeerId.clear();
      m_changed = true;
    }
  }


  public boolean contains(Object o) {
    return m_setPeerId.contains(o);
  }


  public boolean containsAll(Collection<?> co) {
    return m_setPeerId.containsAll(co);
  }


  // One exception is equals.
  public boolean equals(Object o) {
    if (o instanceof PersistentPeerIdSetImpl) {
      PersistentPeerIdSetImpl ppis = (PersistentPeerIdSetImpl) o;

      return m_setPeerId.equals(ppis.m_setPeerId);
    } else {
      return false;
    }
  }


  /* A hash code must always return a value; it cannot throw an IOException. */
  public int hashCode() {
    return m_setPeerId.hashCode();
  }


  public boolean isEmpty() {
    return m_setPeerId.isEmpty();
  }


  public Iterator<PeerIdentity> iterator() {
    return m_setPeerId.iterator();
  }


  public boolean remove(Object o) {
    boolean result;

    result = m_setPeerId.remove(o);
    m_changed |= result;

    return result;
  }


  public boolean removeAll(Collection<?> c) {
    boolean result;

    result = m_setPeerId.removeAll(c);
    m_changed |= result;

    return result;
  }


  public boolean retainAll(Collection<?> c) {
    boolean result;

    result = m_setPeerId.retainAll(c);
    m_changed |= result;

    return result;
  }


  public int size() {
    return m_setPeerId.size();
  }


  public Object[] toArray() {
    return m_setPeerId.toArray();
  }


}


