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

import org.lockss.app.LockssApp;
import org.lockss.app.LockssDaemon;
import org.lockss.plugin.AuUtil;
import org.lockss.util.*;
import org.lockss.state.*;
import org.lockss.util.os.PlatformUtil;

public class PersistentPeerIdSetImpl implements PersistentPeerIdSet {
  private static Logger log = Logger.getLogger();

  // Static constants 
  protected static final String TEMP_EXTENSION = ".temp";

  // Internal variables
  protected String auid;
  @JsonIgnore
  protected Set<PeerIdentity> peerSet = new HashSet<>();
  protected Set<String> rawSet = new HashSet<>();

  @JsonIgnore
  protected IdentityManager m_identityManager;
  @JsonIgnore
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
    PersistentPeerIdSetImpl ppisi = new PersistentPeerIdSetImpl(auid, idMgr);
    return ppisi;
  }

  /**
   * Factory method for json/Jackson.  This creates a "bean" instance which
   * is used only as a source from which to copy data, then it is
   * discarded.  Hence, idMgr is not needed.
   * @param auid The AUID
   */
  @JsonCreator
  public static PersistentPeerIdSetImpl make(@JsonProperty("auid") String auid) {
    PersistentPeerIdSetImpl res = new PersistentPeerIdSetImpl(auid, null);
    return res;
  }

  /** Store the set and retain in memory
   */
  public void store() throws IOException {
    throw new UnsupportedOperationException("Storing PersistentPeerIdSet is currently only implemented for DatedPeerIdSet");
  }

  protected StateManager getStateMgr() {
    return LockssDaemon.getManagerByTypeStatic(StateManager.class);
  }

  /** Release resources without saving */
  public void release() {
  }

  public boolean add(PeerIdentity pi) {
    boolean result;

    result = peerSet.add(pi);
    m_changed |= result;
      
    return result;
  }

  public boolean addAll(Collection<? extends PeerIdentity> cpi) {
    boolean result;

    result = peerSet.addAll(cpi);
    m_changed |= result;

    return result;
  }


  public void clear() {
    if (!peerSet.isEmpty()) {
      peerSet.clear();
      m_changed = true;
    }
  }


  public boolean contains(Object o) {
    return peerSet.contains(o);
  }


  public boolean containsAll(Collection<?> co) {
    return peerSet.containsAll(co);
  }


  // Using class.equals() because this has a subclass
  public boolean equals(Object o) {
    if (getClass().equals(o.getClass())) {
      PersistentPeerIdSetImpl ppis = (PersistentPeerIdSetImpl) o;

      return peerSet.equals(ppis.peerSet);
    } else {
      return false;
    }
  }


  /* A hash code must always return a value; it cannot throw an IOException. */
  public int hashCode() {
    return peerSet.hashCode();
  }


  public boolean isEmpty() {
    return peerSet.isEmpty();
  }


  public Iterator<PeerIdentity> iterator() {
    return peerSet.iterator();
  }


  public boolean remove(Object o) {
    boolean result;

    result = peerSet.remove(o);
    m_changed |= result;

    return result;
  }


  public boolean removeAll(Collection<?> c) {
    boolean result;

    result = peerSet.removeAll(c);
    m_changed |= result;

    return result;
  }


  public boolean retainAll(Collection<?> c) {
    boolean result;

    result = peerSet.retainAll(c);
    m_changed |= result;

    return result;
  }


  public int size() {
    return peerSet.size();
  }


  public Object[] toArray() {
    return peerSet.toArray();
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
    return AuUtil.jsonFromPersistentPeerIdSetImpl(makeBean(peers));
  }

  /** Return a set of Strings of the keys of the pids in our peerSet that
   * are contained in filterPeers */
  protected Set<String> makeRawSet(Set<PeerIdentity> filterPeers) {
    Set<String> res = new HashSet<>();
    for (PeerIdentity pid : peerSet) {
      if (filterPeers == null || filterPeers.contains(pid)) {
	res.add(pid.getKey());
      }
    }
    return res;
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
    res.rawSet = makeRawSet(peers);
    return res;
  }

  /** Return a set of PeerIdentity created from the supplied id strings */
  protected Set<PeerIdentity> internPeerIdSet(IdentityManager idMgr,
					      Set<String> ids) {
    Set<PeerIdentity> res = new HashSet<>();
    for (String s : ids) {
      try {
	res.add(idMgr.findPeerIdentity(s));
      } catch (IdentityManager.MalformedIdentityKeyException e) {
	throw new IllegalArgumentException("Illegal pid " + s +
					   " in " + ids, e);
      }
    }
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
    PersistentPeerIdSetImpl srcSet =
	AuUtil.persistentPeerIdSetImplFromJson(json);
    // Get the peer identities.
    peerSet = internPeerIdSet(m_identityManager, srcSet.rawSet);
    postUnmarshal(app);
    return peerSet;
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


