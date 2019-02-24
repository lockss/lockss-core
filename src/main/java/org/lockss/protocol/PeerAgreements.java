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

import java.util.*;
import com.fasterxml.jackson.annotation.*;

import org.lockss.util.io.LockssSerializable;

/* NOTE: Instances of this class are not public. {@link
 * IdentityManagerImpl} uses this class internally.
 */

/**
 * The saved information for a single {@link ArchivalUnit} about
 * poll agreements between this cache and another peer.
 */
public class PeerAgreements implements LockssSerializable {
  // The String representing the other peer.
  private final String id;
  // A Map detailing the agreements with the other peer.
  private final EnumMap<AgreementType, PeerAgreement> map;

  @JsonCreator
  private PeerAgreements(@JsonProperty("id") String id,
			 @JsonProperty("map") EnumMap<AgreementType, PeerAgreement> map) {
    if (id == null) {
      throw new IllegalArgumentException("id may not be null");
    }
    if (map == null) {
      throw new IllegalArgumentException("map may not be null");
    }
    this.id = id;
    this.map = map;
  }

  /**
   * Create a new instance with no known agreements of any {@link
   * AgreementType}.
   *
   * @param id The string representing the peer.
   */
  public PeerAgreements(String id) {
    this(id, new EnumMap<AgreementType, PeerAgreement>(AgreementType.class));
  }

  /**
   * Create a new instance with no known agreements of any {@link
   * AgreementType}.
   *
   * @param pid The {@link PeerIdentity} of the peer.
   */
  public PeerAgreements(PeerIdentity pid) {
    this(pid.getIdString());
  }

  @Override
  public String toString() {
    return "PeerAgreements[id="+id+
      ", map="+map+
      "]";
  }

  /**
   * @return the {@link String} representing the other peer.
   */
  public String getId() {
    return id;
  }

  /**
   * @param type An {@link AgreementType}.
   * @return the {@link PeerAgreement} containing the agreement
   * information of the requested type between us and the other peer
   * or {@link PeerAgreement.NO_AGREEMENT} if none is available. Never
   * returns {@code null}.
   */
  public PeerAgreement getPeerAgreement(AgreementType type) {
    PeerAgreement agreement = map.get(type);
    return agreement == null ? PeerAgreement.NO_AGREEMENT : agreement;
  }

  /**
   * Update the agreements between us and the other peer to include
   * the new information.
   * @param type An {@link AgreementType}.
   * @param percent A {@code float} between {@code 0.0} and {@code 1.0}.
   * @param time The time the signal should be recorded as occuring.
   */
  public void signalAgreement(AgreementType type,
			      float percent, long time) {
    PeerAgreement peerAgreement =
      getPeerAgreement(type).signalAgreement(percent, time);
    map.put(type, peerAgreement);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PeerAgreements) {
      PeerAgreements other = (PeerAgreements)o;
      return id.equals(other.id) && map.equals(other.map);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id.hashCode() * 17 + map.hashCode();
  }

}
