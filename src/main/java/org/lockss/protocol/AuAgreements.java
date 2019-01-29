/*

Copyright (c) 2013-2019 Board of Trustees of Leland Stanford Jr. University,
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
import java.util.stream.*;
import com.fasterxml.jackson.annotation.*;

import org.lockss.app.*;
import org.lockss.protocol.IdentityManager;
import org.lockss.plugin.*;
import org.lockss.repository.LockssRepositoryException;
import org.lockss.state.*;
import org.lockss.util.*;
import org.lockss.util.io.LockssSerializable;


/**
 * The saved information for a single {@link ArchivalUnit} about poll
 * agreements between this cache and all the peers it has agreements
 * with.
 */
public class AuAgreements implements LockssSerializable {
  protected static Logger log = Logger.getLogger();

  private String auid;

  // Raw map for serialization
  private Map<String, PeerAgreements> rawMap;

  // Caches PeerIdentity -> PeerAgreements mapping once loaded
  @JsonIgnore
  private Map<PeerIdentity, PeerAgreements> map;

  @JsonIgnore
  private final IdentityManager idMgr;

  private AuAgreements(String auid, IdentityManager idMgr) {
    this.auid = auid;
    this.idMgr = idMgr;
    this.map = new HashMap();
  }

  /**
   * Create a new instance.
   * @param auid The AUID
   * @param idMgr A {@link IdentityManager} to translate {@link
   * String}s to {@link PeerIdentity} instances.
   */
  public static AuAgreements make(String auid, IdentityManager idMgr) {
    AuAgreements auAgreements = new AuAgreements(auid, idMgr);
    return auAgreements;
  }

  /**
   * Factory method for json/Jackson.  This creates a "bean" instance which
   * is used only as a source from which to copy PeerAgreements, then it is
   * discarded.  Hence, idMgr is not needed.
   * @param auid The AUID
   */
  @JsonCreator
  public static AuAgreements make(@JsonProperty("auid") String auid) {
    AuAgreements auAgreements = new AuAgreements(auid, null);
    return auAgreements;
  }

  /**
   * @return the AUID
   */
  public String getAuid() {
    return auid;
  }

  /** Serialize entire object to json string */
  public String toJson() throws IOException {
    return toJson((Set<PeerIdentity>)null);
  }

  /** Serialize a single field to json string */
  public String toJson(PeerIdentity pid) throws IOException {
    return toJson(SetUtil.set(pid));
  }

  /** Serialize PeerAgreements for named peers to json string */
  public synchronized String toJson(Set<PeerIdentity> peers) throws IOException {
    return AuUtil.jsonFromAuAgreements(makeBean(peers), peers);
  }

  AuAgreements makeBean(Set<PeerIdentity> peers) {
    AuAgreements res = new AuAgreements(auid, idMgr);
    res.rawMap = new HashMap<>();
    for (Map.Entry<PeerIdentity,PeerAgreements> ent : map.entrySet()) {
      if (peers == null || peers.contains(ent.getKey())) {
	res.rawMap.put(ent.getKey().getKey(), ent.getValue());
      }
    }
    return res;
  }

  /** Deserialize a json string into this AuAgreements, replacing only
   * those PeerAgreements that are present in the json string
   * @param json json string
   * @param app
   * @return set of the PeerIdentity of each PeerAgreements that was
   * updated from the json source
   */
  public synchronized Set<PeerIdentity> updateFromJson(String json,
						       LockssApp app)
      throws IOException {
    // Deserialize json into a new, scratch instance
    AuAgreements srcAgmnts = AuUtil.auAgreementsFromJson(json);
    // Copy the PeerAgreements from its rawMap into our map
    Set<PeerIdentity> res = new HashSet<>();
    for (PeerAgreements pas : srcAgmnts.rawMap.values()) {
      res.add(setPeerAgreements(pas));
    }
    postUnmarshal(app);
    return res;
  }

  /** Deserialize a json string into a new AuAgreements
   * @param json json string
   * @param app
   */
  public static AuAgreements fromJson(String key, String json,
				      LockssDaemon daemon)
      throws IOException {
    AuAgreements res = AuAgreements.make(key, daemon.getIdentityManager());
    res.updateFromJson(json, daemon);
    return res;
  }

  /** Update the saved state to reflect the changed made to the named
   * fields.
   * @param fields fields to store.  If null, all fields are stored
   */
  public synchronized void storeAuAgreements(Set<PeerIdentity> peers) {
    getStateMgr().updateAuAgreements(auid, this, peers);
  }

  private StateManager getStateMgr() {
//     if (stateMgr == null) {
//       // XXX very handy for test.  alternative?
      return LockssDaemon.getManagerByTypeStatic(StateManager.class);
//     }
//     return stateMgr;
  }

  /**
   * Avoid duplicating common strings
   */
  protected void postUnmarshal(LockssApp lockssContext) {
    auid = StringPool.AUIDS.intern(auid);
  }

  @Override
  public String toString() {
    return "[AuAgreements: " + map + "]";
  }

  /**
   * @return true iff we have some data.
   */
  public synchronized boolean haveAgreements() {
    return !map.isEmpty();
  }

  public PeerIdentity setPeerAgreements(PeerAgreements peerAgreements)
      throws IllegalArgumentException {
    String id = peerAgreements.getId();
    try {
      PeerIdentity pid = idMgr.findPeerIdentity(id);
      if (pid != null) {
	map.put(pid, peerAgreements);
      } else {
	throw new IllegalArgumentException("Null pid in " + peerAgreements);
      }
      return pid;
    } catch (IdentityManager.MalformedIdentityKeyException e) {
	throw new IllegalArgumentException("Illegal pid " + id +
					   " in " + peerAgreements, e);
    }
  }

  /**
   * @param pid a {@link PeerIdentity}.
   * @return The {@link PeerAgreements} or {@code null} if no agreement
   * exists.
   */
  public PeerAgreements getPeerAgreements(PeerIdentity pid) {
    return map.get(pid);
  }

  /**
   * @return All the {@link PeerAgreements}
   */
  public Set<PeerAgreements> getAllPeerAgreements() {
    // Don't return HashMap.values() directly - it doesn't implement
    // equals(), etc.
    return new HashSet(map.values());
  }

  /**
   * @param pid a {@link PeerIdentity}.
   * @return The {@link PeerAgreements} -- perhaps newly-created -- for the
   * pid.
   */
  private PeerAgreements findPeerAgreements(PeerIdentity pid) {
    PeerAgreements peerAgreements = getPeerAgreements(pid);
    if (peerAgreements == null) {
      peerAgreements = new PeerAgreements(pid);
      map.put(pid, peerAgreements);
    }
    return peerAgreements;
  }

  /**
   * Find or create a PeerAgreement for the {@link PeerIdentity}.
   *
   * @param pid A {@link PeerIdentity}.
   * @param type The {@link AgreementType} to record.
   * @return A {@link PeerAgreement}, either the one already existing
   * or {@link PeerAgreement#NO_AGREEMENT}. Never {@code null}.
   */
  public synchronized PeerAgreement findPeerAgreement(PeerIdentity pid,
						      AgreementType type) {
    return findPeerAgreement0(pid, type);
  }

  /**
   * Find or create a PeerAgreement for the {@link PeerIdentity}.
   * Should only be called with this instance synchronized.
   * @param pid A PeerIdentity.
   * @param type The {@link AgreementType} to record.
   * @return A {@link PeerAgreement}, either the one already existing
   * or {@link PeerAgreement#NO_AGREEMENT}. Never {@code null}.
   */
  private PeerAgreement findPeerAgreement0(PeerIdentity pid,
					   AgreementType type) {
    PeerAgreements peerAgreements = getPeerAgreements(pid);
    if (peerAgreements == null) {
      return PeerAgreement.NO_AGREEMENT;
    }
    return peerAgreements.getPeerAgreement(type);
  }

  /**
   * Record the agreement hint we received from one of our votes in a
   * V3 poll on this AU.
   *
   * @param pid
   * @param type The {@link AgreementType} to record.
   * @param percent
   */
  public synchronized void signalPartialAgreement(PeerIdentity pid,
						  AgreementType type,
						  float percent, long time) {
    PeerAgreements peerAgreements = findPeerAgreements(pid);
    peerAgreements.signalAgreement(type, percent, time);
  }

  /**
   * @param pid A {@link PeerIdentity} instance.
   * @param minPercentPartialAgreement The threshold below which we
   * will return {@code false}.
   * @return {@code true} iff the peer has agreed with us at or above
   * the requested threshold on a POR poll.
   */
  public synchronized boolean hasAgreed(PeerIdentity pid,
					float threshold) {
    PeerAgreements peerAgreements = findPeerAgreements(pid);
    PeerAgreement peerAgreement =
      peerAgreements.getPeerAgreement(AgreementType.POR);
    return peerAgreement.getHighestPercentAgreement() >= threshold;
  }

  /**
   * Return a mapping for each peer for which we have an agreement of
   * the requested type to the {@link PeerAgreement} record for that
   * peer.
   *
   * @param type The {@link AgreementType} to look for.
   * @return A Map mapping each {@link PeerIdentity} which has an
   * agreement of the requested type to the {@link PeerAgreement} for
   * that type. A peer with some agreements but none of the requested
   * type will not have an entry in the returned map.
   */
  public synchronized Map<PeerIdentity, PeerAgreement>
    getAgreements(AgreementType type) {
    Map<PeerIdentity, PeerAgreement> agreements = new HashMap();
    for (Map.Entry<PeerIdentity, PeerAgreements> ent: map.entrySet()) {
      PeerAgreements peerAgreements = ent.getValue();
      PeerAgreement peerAgreement = peerAgreements.getPeerAgreement(type);
      if (peerAgreement != PeerAgreement.NO_AGREEMENT) {
	PeerIdentity pid = ent.getKey();
	agreements.put(pid, peerAgreement);
      }
    }
    return agreements;
  }

  /**
   * Count the number of peers with at least threshold agreement of the
   * specified type.
   * @param type The {@link AgreementType} to look for.
   * @param threshold The minimum agreement value to count
   * @return count of peers with at least specified agreement of specified
   * type.
   */
  public synchronized int countAgreements(AgreementType type,
					  float threshold) {
    int res = 0;
    for (Map.Entry<PeerIdentity, PeerAgreements> ent: map.entrySet()) {
      PeerAgreements peerAgreements = ent.getValue();
      PeerAgreement peerAgreement = peerAgreements.getPeerAgreement(type);
      if (peerAgreement != PeerAgreement.NO_AGREEMENT &&
	  (peerAgreement.getHighestPercentAgreement() >= threshold)) {
	res++;
      }
    }
    return res;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof AuAgreements) {
      AuAgreements other = (AuAgreements)o;
      return map.equals(other.map);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return map.hashCode();
  }

}
