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
import org.lockss.app.LockssApp;
import org.lockss.app.LockssDaemon;
import org.lockss.plugin.AuUtil;
import org.lockss.util.*;
import org.lockss.util.os.PlatformUtil;

public class PersistentPeerIdSetImpl implements PersistentPeerIdSet {
  // Static constants 
  protected static final String TEMP_EXTENSION = ".temp";

  // Internal variables
  private String auid;
  private IdentityManager m_identityManager;
  private static Logger m_logger = Logger.getLogger();
  protected Set<PeerIdentity> m_setPeerId = new HashSet<PeerIdentity>();
  protected boolean m_changed = false;


  public PersistentPeerIdSetImpl(IdentityManager identityManager) {
    m_identityManager = identityManager;
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
  public PersistentPeerIdSetImpl(String auid, IdentityManager identityManager) {
    this.auid = auid;
    m_identityManager = identityManager;
  }

  /**
   * Creates and provides a new instance.
   * 
   * @param auid
   *          A String with the Archival Unit identifier.
   * @param idMgr
   *          An {@link IdentityManager} to translate {@link String}s to
   *          {@link PeerIdentity} instances.
   * @return a PersistentPeerIdSetImpl with the newly created object.
   */
  public static PersistentPeerIdSetImpl make(String auid,
      IdentityManager idMgr) {
    PersistentPeerIdSetImpl persistentPeerIdSetImpl =
	new PersistentPeerIdSetImpl(auid, idMgr);
    return persistentPeerIdSetImpl;
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

  /**
   * Provides a serialized version of this entire object as a JSON string.
   * 
   * @return a String with this object serialized as a JSON string.
   * @throws IOException
   *           if any problem occurred during the serialization.
   */
  @Override
  public String toJson() throws IOException {
    return toJson((Set<PeerIdentity>)null);
  }

  /**
   * Provides a serialized version of this object with the named peers as a JSON
   * string.
   * 
   * @param peers
   *          A Set<PeerIdentity> with the peers to be included.
   * 
   * @return a String with this object serialized as a JSON string.
   * @throws IOException
   *           if any problem occurred during the serialization.
   */
  @Override
  public synchronized String toJson(Set<PeerIdentity> peers)
      throws IOException {
    return AuUtil.jsonFromPersistentPeerIdSetImpl(makeBean(peers), peers);
  }

  /**
   * Creates and provides a new instance with the named peers.
   * 
   * @param peers
   *          A Set<PeerIdentity> with the peers to be included.
   * @return a PersistentPeerIdSetImpl with the newly created object.
   */
  PersistentPeerIdSetImpl makeBean(Set<PeerIdentity> peers) {
    PersistentPeerIdSetImpl res =
	new PersistentPeerIdSetImpl(auid, m_identityManager);
    res.addAll(peers);
    return res;
  }

  /**
   * Provides the PeerIdentitys that are present in a serialized JSON string.
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
    PersistentPeerIdSetImpl srcPpisis =
	AuUtil.persistentPeerIdSetImplFromJson(json);
    // Get the peer identities.
    Set<PeerIdentity> res = srcPpisis.m_setPeerId;
    postUnmarshal(app);
    return res;
  }

  /**
   * Deserializes a JSON string into a new PersistentPeerIdSetImpl object.
   * 
   * @param key
   *          A String with the Archival Unit identifier.
   * @param json
   *          A String with the JSON text.
   * @param daemon
   *          A LockssDaemon with the LOCKSS daemon.
   * @return a PersistentPeerIdSetImpl with the newly created object.
   * @throws IOException
   *           if any problem occurred during the deserialization.
   */
  public static PersistentPeerIdSetImpl fromJson(String key, String json,
						 LockssDaemon daemon)
						     throws IOException {
    PersistentPeerIdSetImpl res =
	PersistentPeerIdSetImpl.make(key, daemon.getIdentityManager());
    res.updateFromJson(json, daemon);
    return res;
  }

  /**
   * Avoids duplicating common strings.
   */
  protected void postUnmarshal(LockssApp lockssContext) {
    auid = StringPool.AUIDS.intern(auid);
  }
}


