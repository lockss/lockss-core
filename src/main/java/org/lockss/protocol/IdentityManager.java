/*
 * $Id$
 */

/*

Copyright (c) 2000-2005 Board of Trustees of Leland Stanford Jr. University,
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

import org.apache.commons.collections4.Predicate;

import org.lockss.app.*;
import org.lockss.config.Configuration;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.util.*;
import org.lockss.util.io.LockssSerializable;
import org.lockss.util.net.IPAddr;
import org.lockss.hasher.*;

/**
 * <p>Abstraction for identity of a LOCKSS cache. Currently wraps an
 * IP address.<p>
 * @author Claire Griffin
 * @version 1.0
 */
public interface IdentityManager extends LockssManager {
  /**
   * <p>A prefix common to all parameters defined by this class.</p>
   */
  public static final String PREFIX = Configuration.PREFIX + "id.";

  /**
   * <p>The LOCAL_IP parameter.</p>
   */
  public static final String PARAM_LOCAL_IP =
    Configuration.PREFIX + "localIPAddress";

  /**
   * <p>The TCP port for the local V3 identity
   * (at org.lockss.localIPAddress). Can be overridden by
   * org.lockss.platform.v3.port.</p>
   */
  public static final String PARAM_LOCAL_V3_PORT =
    Configuration.PREFIX + "localV3Port";

  /**
   * <p>Local V3 identity string. If this is set it will take
   * precedence over org.lockss.platform.v3.identity.</p> */
  public static final String PARAM_LOCAL_V3_IDENTITY =
    Configuration.PREFIX + "localV3Identity";

  /**
   * <p>The IDDB_DIR parameter.</p>
   */
  public static final String PARAM_IDDB_DIR = PREFIX + "database.dir";

  /**
   * <p>The name of the IDDB file.</p>
   */
  public static final String IDDB_FILENAME = "iddb_v3.xml";

  /**
   * <p>The mapping file for this class.</p>
   */
  public static final String MAPPING_FILE_NAME =
    "/org/lockss/protocol/idmapping.xml";
  // CASTOR: Remove the field above when Castor is phased out.

  /** The minimum percent agreement required before we are
   * willing to serve a repair to a peer.
   */
  public static final String PARAM_MIN_PERCENT_AGREEMENT =
    PREFIX + "minPercentAgreement";

  /** The default percent agreement required to signal agreement
   * with a peer.
   */
  public static final float DEFAULT_MIN_PERCENT_AGREEMENT =
    0.5f;

  /**
   * <p>Currently the only allowed V3 protocol.</p>
   */
  public static final String V3_ID_PROTOCOL_TCP = "TCP";

  /**
   * <p>The V3 protocol separator.</p>
   */
  public static final String V3_ID_PROTOCOL_SUFFIX = ":";

  /**
   * <p>The V3 TCP IP addr prefix.</p>
   */
  public static final String V3_ID_TCP_ADDR_PREFIX = "[";

  /**
   * <p>The V3 TCP IP addr suffix.</p>
   */
  public static final String V3_ID_TCP_ADDR_SUFFIX = "]";

  /**
   * <p>The V3 TCP IP / port separator.</p>
   */
  public static final String V3_ID_TCP_IP_PORT_SEPARATOR = ":";

  /**
  /**
   * <p>Finds or creates unique instances of PeerIdentity.</p>
   */
  public PeerIdentity findPeerIdentity(String key)
      throws MalformedIdentityKeyException;

  /**
   * <p>Returns the peer identity matching the IP address and port;
   * An instance is created if necesary.</p>
   * <p>Used only by LcapDatagramRouter (and soon by its stream
   * analog).</p>
   * @param addr The IPAddr of the peer, null for the local peer.
   * @param port The port of the peer.
   * @return The PeerIdentity representing the peer.
   */
  public PeerIdentity ipAddrToPeerIdentity(IPAddr addr, int port)
      throws MalformedIdentityKeyException;

  public PeerIdentity ipAddrToPeerIdentity(IPAddr addr)
      throws MalformedIdentityKeyException;


  /**
   * <p>Returns the peer identity matching the String IP address and
   * port. An instance is created if necesary. Used only by
   * LcapMessage (and soon by its stream analog).
   * @param idKey the ip addr and port of the peer, null for the local
   *              peer.
   * @return The PeerIdentity representing the peer.
   */
  public PeerIdentity stringToPeerIdentity(String idKey)
      throws IdentityManager.MalformedIdentityKeyException;

  /**
   * <p>Returns the local peer identity.</p>
   * @param pollVersion The poll protocol version.
   * @return The local peer identity associated with the poll version.
   * @throws IllegalArgumentException if the pollVersion is not
   *                                  configured or is outside the
   *                                  legal range.
   */
  public PeerIdentity getLocalPeerIdentity(int pollVersion);

  /**
   * @return a list of all local peer identities.
   */
  public List<PeerIdentity> getLocalPeerIdentities();

  /**
   * <p>Returns the IPAddr of the local peer.</p>
   * @return The IPAddr of the local peer.
   */
  public IPAddr getLocalIPAddr();

  /**
   * <p>Determines if this PeerIdentity is the same as the local
   * host.</p>
   * @param id The PeerIdentity.
   * @return true if is the local identity, false otherwise.
   */
  public boolean isLocalIdentity(PeerIdentity id);

  /**
   * <p>Determines if this PeerIdentity is the same as the local
   * host.</p>
   * @param idStr The string representation of the voter's
   *        PeerIdentity.
   * @return true if is the local identity, false otherwise.
   */
  public boolean isLocalIdentity(String idStr);

  /**
   * <p>Used by the PollManager to record the result of tallying a
   * poll.</p>
   * @see #storeIdentities(ObjectSerializer)
   */
  public void storeIdentities() throws ProtocolException;

  /**
   * <p>Records the result of tallying a poll using the given
   * serializer.</p>
   */
  public void storeIdentities(ObjectSerializer serializer)
      throws ProtocolException;

  /**
   * <p>Copies the identity database file to the stream.</p>
   * @param out An OutputStream instance.
   */
  public void writeIdentityDbTo(OutputStream out) throws IOException;

  /**
   * Return a list of all known TCP (V3) peer identities.
   */
  public Collection getTcpPeerIdentities();

  /**
   * Return a filtered list of all known TCP (V3) peer identities.
   */
  public Collection getTcpPeerIdentities(Predicate peerPredicate);

  /**
   * <p>Signals that we've agreed with pid on a top level poll on
   * au.</p>
   * <p>Only called if we're both on the winning side.</p>
   * @param pid The PeerIdentity of the agreeing peer.
   * @param au  The {@link ArchivalUnit}.
   */
  public void signalAgreed(PeerIdentity pid, ArchivalUnit au);

  /**
   * <p>Signals that we've disagreed with pid on any level poll on
   * au.</p>
   * <p>Only called if we're on the winning side.</p>
   * @param pid The PeerIdentity of the disagreeing peer.
   * @param au  The {@link ArchivalUnit}.
   */
  public void signalDisagreed(PeerIdentity pid, ArchivalUnit au);

  /**
   * Signal partial agreement with a peer on a given archival unit following
   * a V3 poll at the poller.
   * 
   * @param pid  The PeerIdentity of the agreeing peer.
   * @param au  The {@link ArchivalUnit}.
   * @param agreement  A number between 0.0 and 1.0 representing the percentage
   *                   of agreement on the total AU.
   */
  public void signalPartialAgreement(PeerIdentity pid, ArchivalUnit au,
                                     float agreement);
  
  /**
   * Signal partial agreement with a peer on a given archival unit following
   * a V3 poll at the voter based on the hint in the receipt.
   * 
   * @param pid  The PeerIdentity of the agreeing peer.
   * @param au  The {@link ArchivalUnit}.
   * @param agreement  A number between 0.0 and 1.0 representing the percentage
   *                   of agreement on the total AU.
   */
  public void signalPartialAgreementHint(PeerIdentity pid, ArchivalUnit au,
                                         float agreement);

  /**
   * Signal partial agreement with a peer on a given archival unit following
   * a V3 poll.
   *
   * @param agreementType The {@link AgreementType} to be recorded.
   * @param pid The {@link PeerIdentity} of the agreeing peer.
   * @param au The {@link ArchivalUnit}.
   * @param agreement A number between {@code 0.0} and {@code 1.0}
   *                   representing the percentage of agreement on the
   *                   portion of the AU polled.
   */
  public void signalPartialAgreement(AgreementType agreementType, 
				     PeerIdentity pid, ArchivalUnit au,
                                     float agreement);

  /**
   * Signal the completion of a local hash check.
   *
   * @param filesCount The number of files checked.
   * @param urlCount The number of URLs checked.
   * @param agreeCount The number of files which agreed with their
   * previous hash value.
   * @param disagreeCount The number of files which disagreed with
   * their previous hash value.
   * @param missingCount The number of files which had no previous
   * hash value.
   */
  public void signalLocalHashComplete(LocalHashResult lhr);
  
  /**
   * Return the percent agreement for a given peer on a given
   * {@link ArchivalUnit}.  Used only by V3 Polls.
   * 
   * @param pid The {@link PeerIdentity}.
   * @param au The {@link ArchivalUnit}.
   * @return The percent agreement for the peer on the au.
   */
  public float getPercentAgreement(PeerIdentity pid, ArchivalUnit au);
  
  /** Return the highest percent agreement recorded for the given peer
   * on a given {@link ArchivalUnit}.
   * 
   * @param pid The {@link PeerIdentity}.
   * @param au The {@link ArchivalUnit}.
   * @return The highest percent agreement for the peer on the au.
   */
  public float getHighestPercentAgreement(PeerIdentity pid, ArchivalUnit au);
  
  /** Return agreement peer has most recently seen from us.
   * @param pid The {@link PeerIdentity}.
   * @param au The {@link ArchivalUnit}.
   * @return agreement, -1.0 if not known */
  public float getPercentAgreementHint(PeerIdentity pid, ArchivalUnit au);
  
  /** Return highest agreement peer has seen from us.
   * @param pid The {@link PeerIdentity}.
   * @param au The {@link ArchivalUnit}.
   * @return agreement, -1.0 if not known */
  public float getHighestPercentAgreementHint(PeerIdentity pid,
					      ArchivalUnit au);

  /**
   * A list of peers with whom we have had a POR poll and a result
   * above the minimum threshold for repair.
   *
   * NOTE: No particular order should be assumed.
   * NOTE: This does NOT use the "hint", which would be more reasonable.
   *
   * @param au ArchivalUnit to look up PeerIdentities for.
   * @return List of peers from which to try to fetch repairs for the
   *         AU.
   */
  public List<PeerIdentity> getCachesToRepairFrom(ArchivalUnit au);

  /**
   * Count the peers with whom we have had a POR poll and a result
   * above the minimum threshold for repair.
   *
   * NOTE: This does NOT use the "hint", which would be more reasonable.
   *
   * @param au ArchivalUnit to look up PeerIdentities for.
   * @return Count of peers we believe are willing to send us repairs for
   * this AU.
   */
  public int countCachesToRepairFrom(ArchivalUnit au);

  /**
   * Return a mapping for each peer for which we have an agreement of
   * the requested type, to the {@link PeerAgreement} record for that
   * peer.
   *
   * @param au The {@link ArchivalUnit} in question.
   * @param type The {@link AgreementType} to look for.
   * @return A Map mapping each {@link PeerIdentity} which has an
   * agreement of the requested type to the {@link PeerAgreement} for
   * that type.
   */
  public Map<PeerIdentity, PeerAgreement> getAgreements(ArchivalUnit au,
							AgreementType type);

  public boolean hasAgreed(String ip, ArchivalUnit au)
      throws MalformedIdentityKeyException;

  public boolean hasAgreed(PeerIdentity pid, ArchivalUnit au);

  /** Convenience methods returns agreement on AU au, of AgreementType type
   * with peer pid */
  public float getPercentAgreement(PeerIdentity pid,
				   ArchivalUnit au,
				   AgreementType type);

  /** Convenience methods returns agreement on AU au, of AgreementType type
   * with peer pid */
  public float getHighestPercentAgreement(PeerIdentity pid,
					  ArchivalUnit au,
					  AgreementType type);

  /**
   * <p>Return map peer -> last agree time. Used for logging and
   * debugging.</p>
   */
  public Map getAgreed(ArchivalUnit au);

  /**
   * @return {@code true} iff there are no data on agreements.
   */
  public boolean hasAgreeMap(ArchivalUnit au);
  
  /**
   * <p>Copies the identity agreement file for the AU to the given
   * stream.</p>
   * @param au  An archival unit.
   * @param out An output stream.
   * @throws IOException if input or output fails.
   */
  public void writeIdentityAgreementTo(ArchivalUnit au, OutputStream out)
      throws IOException;

  /**
   * <p>Installs the contents of the stream as the identity agreement
   * file for the AU.</p>
   * @param au An archival unit.
   * @param in An input stream to read from.
   */
  public void readIdentityAgreementFrom(ArchivalUnit au, InputStream in)
      throws IOException;
  
  /**
   * @return List of  PeerIdentityStatus for all PeerIdentity.
   */
  public List<PeerIdentityStatus> getPeerIdentityStatusList();

  /**
   * @param pid The PeerIdentity.
   * @return The PeerIdentityStatus associated with the given PeerIdentity.
   */
  public PeerIdentityStatus getPeerIdentityStatus(PeerIdentity pid);
  
  /**
   * @param key The Identity Key
   * @return The PeerIdentityStatus associated with the given PeerIdentity.
   */
  public PeerIdentityStatus getPeerIdentityStatus(String key);

  public String getUiUrlStem(PeerIdentity pid);

  /**
   * <p>Exception thrown for illegal identity keys.</p>
   */
  public static class MalformedIdentityKeyException extends IOException {
    public MalformedIdentityKeyException(String message) {
      super(message);
    }
  }
}
