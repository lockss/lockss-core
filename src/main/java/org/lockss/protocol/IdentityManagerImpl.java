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
import java.net.UnknownHostException;
import java.util.*;

import org.apache.commons.collections.*;

import org.lockss.app.*;
import org.lockss.config.*;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.poller.*;
import org.lockss.protocol.IdentityManager.MalformedIdentityKeyException;
import org.lockss.repository.*;
import org.lockss.state.*;
import org.lockss.plugin.AuUtil;
import org.lockss.util.*;
import org.lockss.util.SerializationException.FileNotFound;
import org.lockss.util.lang.LockssRandom;
import org.lockss.util.net.IPAddr;
import org.lockss.util.time.TimeBase;
import org.lockss.hasher.*;

/**
 * <p>Abstraction for identity of a LOCKSS cache. Currently wraps an
 * IP address.<p>
 * @author Claire Griffin
 * @version 1.0
 */
public class IdentityManagerImpl extends BaseLockssDaemonManager
  implements IdentityManager, ConfigurableManager {

  /**
   * <p>A logger for this class.</p>
   */
  protected static Logger log = Logger.getLogger();

  /**
   * <p>The number of update events between IDDB serializations.
   * This parameter is a maximum, and does not alter the fact that
   * the IDDB table is serialized at the end of every poll, and
   * whenever a peer is deleted (for example, due to polling group
   * mismatch).</p>
   */
  public static final String PARAM_UPDATES_BEFORE_STORING =
    PREFIX + "updatesBeforeStoring";
  public static final long DEFAULT_UPDATES_BEFORE_STORING = 100;

  /**
   * <p>The initial list of V3 peers for this cache.</p>
   */
  public static final String PARAM_INITIAL_PEERS = PREFIX + "initialV3PeerList";
  public static final List DEFAULT_INITIAL_PEERS = Collections.EMPTY_LIST;

  /**
   * True to enable V1 identities
   */
  public static final String PARAM_ENABLE_V1 = PREFIX + "v1Enabled";
  public static final boolean DEFAULT_ENABLE_V1 = true;

  /** Maps PeerId to UI URL stem.  Useful for testing frameworks to point
   * nonstandard ports.  List of PeerId,URL-stem;,...*/
  public static final String PARAM_UI_STEM_MAP =
    PREFIX + "pidUiStemMap";

  /** Maps PeerId to PeerAddress.  Useful to allow a node behind NAT to
   * reach others nodes behind the same NAT using the internal address.
   * List of PeerId,Peer;,...  Daemon restart required to remove mappings. */
  public static final String PARAM_PEER_ADDRESS_MAP = PREFIX + "peerAddressMap";

  /**
   * <p>An instance of {@link LockssRandom} for use by this class.</p>
   */
  static LockssRandom theRandom = new LockssRandom();

  /**
   * <p>IP address of our identity (not necessarily this machine's IP
   * if behind NAT).<p>
   * <p>All current identities are IP-based; future ones may not
   * be.</p>
   */
  protected IPAddr theLocalIPAddr = null;

  /**
   * <p>Array of PeerIdentity for each of our local identities,
   * (potentially) one per protocol version.
   */
  protected PeerIdentity localPeerIdentities[];

  /**
   * <p>A mapping of PeerIdentity objects to status objects.  This is
   * the map that is actually serialized onto disk for persistence</p>
   */
  protected Map<PeerIdentity,PeerIdentityStatus> pidStatusMap;

  /**
   * Maps PeerIdentity key (<i>eg</i>, <tt>TCP:[192.168.0.1]:9729</tt>) to
   * unique PeerIdentity object.  Multiple unnormalized keys, in addition
   * to the single normalized key, may map to the same PeerIdentity.
   */
  private Map<String,PeerIdentity> pidMap;

  /**
   * Set of all PeerIdentity objects.  Separate set required when
   * enumerating pids as map may contain mappings from unnormalized keys.
   */
  // Currently duplicates pidStatusMap.keySet(), but that will be
  // going away.
  private Set<PeerIdentity> pidSet;

  /**
   * <p>The IDDB file.</p>
   */
  File iddbFile = null;

  /**
   * Maps auid to AuAgreements. The LRU cache allows objects to be
   * collected when they are no longer in use.
   */
  private float minPercentPartialAgreement = DEFAULT_MIN_PERCENT_AGREEMENT;

  private long updatesBeforeStoring = DEFAULT_UPDATES_BEFORE_STORING;
  private long updates = 0;

  private Map<PeerIdentity,String> pidUiStemMap;

  /**
   * <p>Builds a new IdentityManager instance.</p>
   */
  public IdentityManagerImpl() {
    pidStatusMap = new HashMap();
    pidMap = new HashMap();
    pidSet = new HashSet();
  }

  public void initService(LockssDaemon daemon) throws LockssAppException {
    // Set up local identities *before* processing rest of config.  (Else
    // any reference to to our ID string will create a non-local identity,
    // which will later be replaced by the local identity
    setupLocalIdentities();
    super.initService(daemon);
    // Don't prefetch StateManager here as it uses IdentityManager
  }

  /**
   * <p>Sets up the local identities.</p>
   * <p>This is protected only so it can be overridden in a mock
   * subclass in another package (TestRemoteApi), which won't be
   * necessary when there's an interface for the mock class to
   * implement instead.</p>
   */
  protected void setupLocalIdentities() {
    localPeerIdentities = new PeerIdentity[Poll.MAX_PROTOCOL + 1];
    boolean hasLocalIdentity = false;

    // setConfig() has not yet run.  All references to the config must be
    // explicit.
    Configuration config = ConfigManager.getCurrentConfig();

    String localV1IdentityStr = getLocalIpParam(config);
    if (config.getBoolean(PARAM_ENABLE_V1, DEFAULT_ENABLE_V1)) {
      // Find local IP addr and create V1 identity if configured
      if (localV1IdentityStr != null) {
	try {
	  theLocalIPAddr = IPAddr.getByName(localV1IdentityStr);
	} catch (UnknownHostException uhe) {
	  String msg = "Cannot start: Can't lookup \"" + localV1IdentityStr + "\"";
	  log.critical(msg);
	  throw new LockssAppException("IdentityManager: " + msg);
	}
	try {
	  localPeerIdentities[Poll.V1_PROTOCOL] =
	    findLocalPeerIdentity(localV1IdentityStr);
	} catch (MalformedIdentityKeyException e) {
	  String msg = "Cannot start: Can't create local identity:" +
	    localV1IdentityStr;
	  log.critical(msg, e);
	  throw new LockssAppException("IdentityManager: " + msg);
	}

	hasLocalIdentity = true;
      }
    }
    // Create V3 identity if configured
    String v3idstr = config.get(PARAM_LOCAL_V3_IDENTITY);
    if (StringUtil.isNullString(v3idstr) &&
        config.containsKey(PARAM_LOCAL_V3_PORT)) {
      int localV3Port = config.getInt(PARAM_LOCAL_V3_PORT, -1);
      if (localV3Port > 0 && localV1IdentityStr != null) {
        v3idstr = IDUtil.ipAddrToKey(localV1IdentityStr, localV3Port);
      }
    }
    if (v3idstr != null) {
      try {
        localPeerIdentities[Poll.V3_PROTOCOL] = findLocalPeerIdentity(v3idstr);
      } catch (MalformedIdentityKeyException e) {
        String msg = "Cannot start: Cannot create local V3 identity: " +
	  v3idstr;
        log.critical(msg, e);
        throw new LockssAppException("IdentityManager: " + msg);
      }

      hasLocalIdentity = true;
    }
    
    // Make sure we have configured at least one local identity.
    if (!hasLocalIdentity) {
      String msg = "Cannot start: Must configure at least one local V1 or "
                   + "local V3 identity!";
      log.critical(msg);
      throw new LockssAppException("IdentityManager: " + msg);
    }
  }

  /**
   * <p>Starts the identity manager.</p>
   * @see LockssManager#startService()
   */
  public void startService() {
    super.startService();
    
    // Register a message handler with LcapRouter to peek at incoming
    // messages.
    LcapRouter router = getDaemon().getRouterManager();
    router.registerMessageHandler(new LcapRouter.MessageHandler() {
      public void handleMessage(LcapMessage msg) {
        try {
          PeerIdentityStatus status = findPeerIdentityStatus(msg.m_originatorID);
          if (status != null) {
            status.messageReceived(msg);
            if (++updates > updatesBeforeStoring) {
              storeIdentities();
              updates = 0;
            }
          }
        } catch (Exception ex) {
          log.error("Unable to checkpoint iddb file!", ex);
        }
      }

      public String toString() {
        return "[IdentityManager Message Handler]";
      }
    });

    reloadIdentities();

    if (localPeerIdentities[Poll.V1_PROTOCOL] != null)
      log.info("Local V1 identity: " + getLocalPeerIdentity(Poll.V1_PROTOCOL));
    if (localPeerIdentities[Poll.V3_PROTOCOL] != null)
      log.info("Local V3 identity: " + getLocalPeerIdentity(Poll.V3_PROTOCOL));

    IdentityManagerStatus status = makeStatusAccessor();
    getDaemon().getStatusService().registerStatusAccessor("Identities",
							  status);
    Vote.setIdentityManager(this);
    LcapMessage.setIdentityManager(this);
  }

  protected IdentityManagerStatus makeStatusAccessor() {
    return new IdentityManagerStatus(this);
  }

  /**
   * <p>Stops the identity manager.</p>
   * @see LockssManager#stopService()
   */
  public void stopService() {
    try {
      storeIdentities();
    }
    catch (ProtocolException ex) {}
    super.stopService();
    getDaemon().getStatusService().unregisterStatusAccessor("Identities");
    Vote.setIdentityManager(null);
    LcapMessage.setIdentityManager(null);
  }

  public List<PeerIdentityStatus> getPeerIdentityStatusList() {
    synchronized (pidStatusMap) {
      return new ArrayList(pidStatusMap.values());
    }
  }

  /**
   * <p>Finds or creates unique instances of both PeerIdentity and
   * PeerIdentityStatus</p>
   * 
   * @param id
   * @return
   * @throws MalformedIdentityKeyException 
   */
  private PeerIdentityStatus findPeerIdentityStatus(PeerIdentity id) {
    synchronized (pidStatusMap) {
      PeerIdentityStatus status = pidStatusMap.get(id);
      if (status == null) {
        status = new PeerIdentityStatus(id);
	pidStatusMap.put(id, status);
      }
      return status;
    }
  }
  
  
  /**
   * @param pid The PeerIdentity.
   * @return The PeerIdentityStatus associated with the given PeerIdentity.
   */
  public PeerIdentityStatus getPeerIdentityStatus(PeerIdentity pid) {
    return findPeerIdentityStatus(pid);
  }
  
  /**
   * @param key The Identity Key
   * @return The PeerIdentityStatus associated with the given PeerIdentity.
   */
  public PeerIdentityStatus getPeerIdentityStatus(String key) {
    synchronized (pidMap) {
      PeerIdentity pid = pidMap.get(key);
      if (pid != null) {
        return getPeerIdentityStatus(pid);
      } else {
        return null;
      }
    }
  }

  /**
   * <p>Finds or creates unique instance of PeerIdentity.</p>
   */
  private PeerIdentity findLocalPeerIdentity(String key)
      throws MalformedIdentityKeyException {
    PeerIdentity pid;
    synchronized (pidMap) {
      pid = pidMap.get(key);
      if (pid == null || !pid.isLocalIdentity()) {
        pid = ensureNormalizedPid(key, new PeerIdentity.LocalIdentity(key));
        pidMap.put(key, pid);
	pidSet.add(pid);
      }
    }
    return pid;
  }

  /**
   * <p>Finds or creates unique instances of PeerIdentity.</p>
   */
  public PeerIdentity findPeerIdentity(String key)
      throws MalformedIdentityKeyException {
    synchronized (pidMap) {
      PeerIdentity pid = pidMap.get(key);
      if (pid == null) {
        pid = ensureNormalizedPid(key, new PeerIdentity(key));
	pidMap.put(key, pid);
	pidSet.add(pid);
      }
      return pid;
    }
  }

  private PeerIdentity ensureNormalizedPid(String key, PeerIdentity pid) {
    String normKey = pid.getKey();
    if (!key.equals(normKey)) {
      // The key we were given is unnormalized, see if we have already
      // have a pid for the normalized key
      PeerIdentity normPid = pidMap.get(normKey);
      if (normPid == null) {
	// this is a new pid, store under normalized key and continue using
	// it
	log.debug("Unnormalized key (new): " + key + " != " + normKey);
	pidMap.put(normKey, pid);
      } else {
	// use existing pid
	log.debug("Unnormalized key: " + key + " != " + normKey);
	return normPid;
      }
    }
    return pid;
  }

  /**
   * <p>Finds or creates unique instances of PeerIdentity.</p>
   */
  private PeerIdentity findPeerIdentityAndData(String key)
      throws MalformedIdentityKeyException {
    PeerIdentity pid = findPeerIdentity(key);
    return pid;
  }

  /**
   * <p>Finds or creates unique instances of PeerIdentity.</p>
   */
  private PeerIdentity findPeerIdentityAndData(IPAddr addr, int port)
      throws MalformedIdentityKeyException {
    String key = IDUtil.ipAddrToKey(addr, port);
    PeerIdentity pid = findPeerIdentity(key);
    return pid;
  }

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
      throws MalformedIdentityKeyException {
    if (addr == null) {
      log.warning("ipAddrToPeerIdentity(null) is deprecated.");
      log.warning("  Use getLocalPeerIdentity() to get a local identity");
      // XXX return V1 identity until all callers fixed
      return localPeerIdentities[Poll.V1_PROTOCOL];
    }
    else {
      return findPeerIdentityAndData(addr, port);
    }
  }

  public PeerIdentity ipAddrToPeerIdentity(IPAddr addr)
      throws MalformedIdentityKeyException {
    return ipAddrToPeerIdentity(addr, 0);
  }

  /**
   * <p>Returns the peer identity matching the String IP address and
   * port. An instance is created if necesary. Used only by
   * LcapMessage (and soon by its stream analog).
   * @param idKey the ip addr and port of the peer, null for the local
   *              peer.
   * @return The PeerIdentity representing the peer.
   */
  public PeerIdentity stringToPeerIdentity(String idKey)
      throws IdentityManager.MalformedIdentityKeyException {
    if (idKey == null) {
      log.warning("stringToPeerIdentity(null) is deprecated.");
      log.warning("  Use getLocalPeerIdentity() to get a local identity");
      // XXX return V1 identity until all callers fixed
      return localPeerIdentities[Poll.V1_PROTOCOL];
    }
    else {
      return findPeerIdentityAndData(idKey);
    }
  }

  /**
   * Returns the local peer identity.
   * @param pollVersion The poll protocol version.
   * @return The local peer identity associated with the poll version.
   * @throws IllegalArgumentException if the pollVersion is not
   *                                  configured or is outside the
   *                                  legal range.
   */
  public PeerIdentity getLocalPeerIdentity(int pollVersion) {
    PeerIdentity pid = null;
    try {
      pid = localPeerIdentities[pollVersion];
    } catch (ArrayIndexOutOfBoundsException e) {
      // fall through
    }
    if (pid == null) {
      throw new IllegalArgumentException("Illegal poll version: " +
					 pollVersion);
    }
    return pid;
  }

  /**
   * @return a list of all local peer identities.
   */
  public List<PeerIdentity> getLocalPeerIdentities() {
    List<PeerIdentity> res = new ArrayList();
    for (PeerIdentity pid : localPeerIdentities) {
      if (pid != null) {
	res.add(pid);
      }
    }
    return res;
  }

  /**
   * <p>Returns the IPAddr of the local peer.</p>
   * @return The IPAddr of the local peer.
   */
  public IPAddr getLocalIPAddr() {
    return theLocalIPAddr;
  }

  /**
   * <p>Determines if this PeerIdentity is the same as the local
   * host.</p>
   * @param id The PeerIdentity.
   * @return true if is the local identity, false otherwise.
   */
  public boolean isLocalIdentity(PeerIdentity id) {
    return id.isLocalIdentity();
  }

  /**
   * <p>Determines if this PeerIdentity is the same as the local
   * host.</p>
   * @param idStr The string representation of the voter's
   *        PeerIdentity.
   * @return true if is the local identity, false otherwise.
   */
  public boolean isLocalIdentity(String idStr) {
    try {
      return isLocalIdentity(stringToPeerIdentity(idStr));
    } catch (IdentityManager.MalformedIdentityKeyException e) {
      return false;
    }
  }

  File setupIddbFile() {
    if (iddbFile == null) {
      String iddbDir = CurrentConfig.getParam(PARAM_IDDB_DIR);
      if (iddbDir != null) {
	iddbFile = new File(iddbDir, IDDB_FILENAME);
      }
    }
    return iddbFile;
  }

  /**
   * <p>Reloads the peer data from the identity database.</p>
   * <p>This may overwrite the PeerIdentityStatus instance for local
   * identity(s). That may not be appropriate if this is ever called
   * other than at startup.</p>
   * @see #reloadIdentities(ObjectSerializer)
   */
  void reloadIdentities() {
    reloadIdentities(makeIdentityListSerializer());
  }

  /**
   * <p>Reloads the peer data from the identity database using the
   * given deserializer.</p>
   * @param deserializer An ObjectSerializer instance.
   * @see #reloadIdentities()
   */
  void reloadIdentities(ObjectSerializer deserializer) {
    if (setupIddbFile() == null) {
      log.warning("Cannot load identities; no value for '"
          + PARAM_IDDB_DIR + "'.");
      return;
    }

    synchronized (iddbFile) {
      try {
        HashMap map = 
          (HashMap<PeerIdentity,PeerIdentityStatus>) deserializer.
          deserialize(iddbFile);
        synchronized (pidStatusMap) {
          pidStatusMap.putAll(map);
        }
      }
      catch (SerializationException.FileNotFound e) {
        log.warning("No identity database");
      }
      catch (Exception e) {
        log.warning("Could not load identity database", e);
      }
    }
  }

  /**
   * <p>Used by the PollManager to record the result of tallying a
   * poll.</p>
   * @see #storeIdentities(ObjectSerializer)
   */
  public void storeIdentities()
      throws ProtocolException {
    storeIdentities(makeIdentityListSerializer());
  }

  /**
   * <p>Records the result of tallying a poll using the given
   * serializer.</p>
   */
  public void storeIdentities(ObjectSerializer serializer)
      throws ProtocolException {
    if (setupIddbFile() == null) {
      log.warning("Cannot store identities; no value for '"
          + PARAM_IDDB_DIR + "'.");
      return;
    }

    synchronized (iddbFile) {
      try {
        File dir = iddbFile.getParentFile();
        if (dir != null) {
	  FileUtil.ensureDirExists(dir);
	}
        serializer.serialize(iddbFile, wrap(pidStatusMap));
      }
      catch (Exception e) {
        log.error("Could not store identity database", e);
        throw new ProtocolException("Unable to store identity database.");
      }
    }
  }

  /**
   * <p>Builds an ObjectSerializer suitable for storing identity
   * maps.</p>
   * @return An initialized ObjectSerializer instance.
   */
  private ObjectSerializer makeIdentityListSerializer() {
    XStreamSerializer serializer = new XStreamSerializer(getDaemon());
    return serializer;
  }

  /**
   * <p>Copies the identity database file to the stream.</p>
   * @param out OutputStream instance.
   */
  public void writeIdentityDbTo(OutputStream out) throws IOException {
    // XXX hokey way to have the acceess performed by the object that has the
    // appropriate lock
    if (setupIddbFile() == null) {
      return;
    }
    if (iddbFile.exists()) {
      synchronized (iddbFile) {
        InputStream in =
          new BufferedInputStream(new FileInputStream(iddbFile));
        try {
          StreamUtil.copy(in, out);
        } finally {
          IOUtil.safeClose(in);
        }
      }
    }
  }

  /**
   * <p>Return a collection of all V3-style PeerIdentities.</p>
   */
  public Collection getTcpPeerIdentities() {
    return getTcpPeerIdentities(PredicateUtils.truePredicate());
  }

  /**
   * Return a filtered collection of V3-style PeerIdentities.
   */
  public Collection getTcpPeerIdentities(Predicate peerPredicate) {
    Collection retVal = new ArrayList();
    for (PeerIdentity id : pidSet) {
      if (id.getPeerAddress() instanceof PeerAddress.Tcp
	  && !id.isLocalIdentity()
	  && peerPredicate.evaluate(id)) {
	retVal.add(id);
      }
    }
    return retVal;
  }

  /**
   * <p>Castor+XStream transition helper method, that wraps the
   * identity map into the object expected by serialization code.</p>
   * @param theIdentities The {@link #pidStatusMap} map.
   * @return An object suitable for serialization.
   */
  private Serializable wrap(Map theIdentities) {
    return (Serializable)theIdentities;
  }

  /**
   * <p>Castor+XStream transition helper method, that unwraps the
   * identity map when it returns from serialized state.</p>
   * @param obj The object returned by deserialization code.
   * @return An unwrapped identity map.
   */
//  private HashMap unwrap(Object obj) {
//    return (HashMap)obj;
//  }

  /**
   * <p>Signals that we've agreed with pid on a top level poll on
   * au.</p>
   * <p>Only called if we're both on the winning side.</p>
   * @param pid The PeerIdentity of the agreeing peer.
   * @param au  The {@link ArchivalUnit}.
   */
  public void signalAgreed(PeerIdentity pid, ArchivalUnit au) {
    signalPartialAgreement(pid, au, 1.0f);
  }

  /**
   * <p>Signals that we've disagreed with pid on any level poll on
   * au.</p>
   * <p>Only called if we're on the winning side.</p>
   * @param pid The PeerIdentity of the disagreeing peer.
   * @param au  The {@link ArchivalUnit}.
   */
  public void signalDisagreed(PeerIdentity pid, ArchivalUnit au) {
    signalPartialAgreement(pid, au, 0.0f);
  }

  /**
   * Signal that we've reached partial agreement with a peer during a
   * V3 poll on au.
   *
   * @param pid
   * @param au
   * @param percent
   */
  public void signalPartialAgreement(PeerIdentity pid, ArchivalUnit au,
                                     float percent) {
    signalPartialAgreement(AgreementType.POR, pid, au, percent);
  }

  /**
   * Get the percent agreement for a V3 poll on a given AU.
   *
   * @param pid The {@link PeerIdentity}.
   * @param au The {@link ArchivalUnit}.
   *
   * @return The percent agreement for this AU and peer.
   */
  public float getPercentAgreement(PeerIdentity pid, ArchivalUnit au) {
    AuAgreements auAgreements = findAuAgreements(au);
    float percentAgreement = auAgreements.
      findPeerAgreement(pid, AgreementType.POR). getPercentAgreement();
    return percentAgreement;
  }
  
  public float getHighestPercentAgreement(PeerIdentity pid, ArchivalUnit au) {
    AuAgreements auAgreements = findAuAgreements(au);
    float highestPercentAgreement = auAgreements.
      findPeerAgreement(pid, AgreementType.POR). getHighestPercentAgreement();
    return highestPercentAgreement;
  }

  /**
   * Record the agreement hint we received from one of our votes in a
   * V3 poll on au.
   *
   * @param pid
   * @param au
   * @param percent
   */
  public void signalPartialAgreementHint(PeerIdentity pid, ArchivalUnit au,
					 float percent) {
    signalPartialAgreement(AgreementType.POR_HINT, pid, au, percent);
  }

  /**
   * Signal partial agreement with a peer on a given archival unit following
   * a V3 poll.
   *
   * @param agreementType The {@link AgreementType} to be recorded.
   * @param pid The {@link PeerIdentity} of the agreeing peer.
   * @param au The {@link ArchivalUnit}.
   * @param agreement A number between {@code 0.0} and {@code
   *                   1.0} representing the percentage of agreement
   *                   on the portion of the AU polled.
   */
  public void signalPartialAgreement(AgreementType agreementType, 
				     PeerIdentity pid, ArchivalUnit au,
                                     float agreement) {
    if (log.isDebug3()) {
      log.debug3("called signalPartialAgreement("+
		 "agreementType="+agreementType+
		 ", pid="+pid+
		 ", au="+au+
		 ", agreement"+agreement+
		 ")");
    }
    if (au == null) {
      throw new IllegalArgumentException("Called with null au");
    }
    if (pid == null) {
      throw new IllegalArgumentException("Called with null pid");
    }
    if (agreement < 0.0f || agreement > 1.0f) {
      throw new IllegalArgumentException("pecentAgreement must be between "+
					 "0.0 and 1.0. It was: "+agreement);
    }
    AuAgreements auAgreements = findAuAgreements(au);
    if (auAgreements == null) {
      log.error("No auAgreements: " + au.getName());
    } else {
      auAgreements.signalPartialAgreement(pid, agreementType, agreement,
					  TimeBase.nowMs());
      auAgreements.storeAuAgreements(SetUtil.set(pid));

      AuState aus = AuUtil.getAuState(au);
      // XXX ER/EE AuState s.b. updated with repairer count, not repairee.
      int willingRepairers =
	auAgreements.countAgreements(AgreementType.POR,
				     minPercentPartialAgreement);
      aus.setNumWillingRepairers(willingRepairers);
    }
  }

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
  public void signalLocalHashComplete(LocalHashResult lhr) {
    log.debug("called signalLocalHashComplete("+ lhr + ")");
  }

  /**
   * Get the percent agreement hint for a V3 poll on a given AU.
   *
   * @param pid The {@link PeerIdentity}.
   * @param au The {@link ArchivalUnit}.
   *
   * @return The percent agreement hint for this AU and peer.
   */
  public float getPercentAgreementHint(PeerIdentity pid, ArchivalUnit au) {
    AuAgreements auAgreements = findAuAgreements(au);
    return auAgreements.findPeerAgreement(pid, AgreementType.POR_HINT).
      getPercentAgreement();
  }
  
  public float getHighestPercentAgreementHint(PeerIdentity pid, ArchivalUnit au) {
    AuAgreements auAgreements = findAuAgreements(au);
    return auAgreements.findPeerAgreement(pid, AgreementType.POR_HINT).
      getHighestPercentAgreement();
  }

  /**
   * A list of peers with whom we have had a POR poll and a result
   * above the minimum threshold for repair.
   *
   * NOTE: No particular order should be assumed.
   * NOTE: This does NOT use the "hint", which would be more reasonable.
   *
   * @param au ArchivalUnit to look up PeerIdentities for.
   * @return List of peers from which to try to fetch repairs for the
   *         AU. Never {@code null}.
   */
  public List<PeerIdentity> getCachesToRepairFrom(ArchivalUnit au) {
    if (au == null) {
      throw new IllegalArgumentException("Called with null au");
    }
    // NOTE: some tests rely on the MockIdentityManager changing
    // getAgreed() and having that change getCachesToRepairFrom
    return new ArrayList(getAgreed(au).keySet());
  }

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
  public int countCachesToRepairFrom(ArchivalUnit au) {
    if (au == null) {
      throw new IllegalArgumentException("Called with null au");
    }
    AuAgreements auAgreements = findAuAgreements(au);
    return auAgreements.countAgreements(AgreementType.POR,
					minPercentPartialAgreement);
  }

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
							AgreementType type) {
    AuAgreements auAgreements = findAuAgreements(au);
    return auAgreements.getAgreements(type);
  }

  public boolean hasAgreed(String ip, ArchivalUnit au)
      throws IdentityManager.MalformedIdentityKeyException {
    return hasAgreed(stringToPeerIdentity(ip), au);
  }

  public boolean hasAgreed(PeerIdentity pid, ArchivalUnit au) {
    AuAgreements auAgreements = findAuAgreements(au);
    return auAgreements.hasAgreed(pid, minPercentPartialAgreement);
  }

  /** Convenience method returns agreement on AU au, of AgreementType type
   * with peer pid.  Returns -1.0 if no agreement of the specified type has
   * been recorded. */
  public float getPercentAgreement(PeerIdentity pid,
				   ArchivalUnit au,
				   AgreementType type) {
    PeerAgreement pa = findAuAgreements(au).findPeerAgreement(pid, type);
    return pa.getPercentAgreement();
  }

  /** Convenience method returns highest agreement on AU au, of
   * AgreementType type with peer pid.  Returns -1.0 if no agreement of the
   * specified type has been recorded. */
  public float getHighestPercentAgreement(PeerIdentity pid,
					  ArchivalUnit au,
					  AgreementType type) {
    PeerAgreement pa = findAuAgreements(au).findPeerAgreement(pid, type);
    return pa.getHighestPercentAgreement();
  }

  /**
   * <p>Return map peer -> last agree time. Used for logging and
   * debugging.</p>
   */
  public Map<PeerIdentity, Long> getAgreed(ArchivalUnit au) {
    if (au == null) {
      throw new IllegalArgumentException("Called with null au");
    }
    AuAgreements auAgreements = findAuAgreements(au);
    Map<PeerIdentity, PeerAgreement> agreements =
      auAgreements.getAgreements(AgreementType.POR);
    Map<PeerIdentity, Long> result = new HashMap();
    for (Map.Entry<PeerIdentity, PeerAgreement> ent: agreements.entrySet()) {
      PeerAgreement agreement = ent.getValue();
      long percentAgreementTime = ent.getValue().getPercentAgreementTime();
      if (agreement.getHighestPercentAgreement() 
	  >= minPercentPartialAgreement) {
	PeerIdentity pid = ent.getKey();
	result.put(pid, Long.valueOf(percentAgreementTime));
      }
    }
    return result;
  }

  StateManager getStateManager() {
    return getDaemon().getManagerByType(StateManager.class);
  }

  protected AuAgreements findAuAgreements(ArchivalUnit au) {
    String auId = au.getAuId();
    AuAgreements auAgreements = getStateManager().getAuAgreements(auId);
    return auAgreements;
  }

  public boolean hasAgreeMap(ArchivalUnit au) {
    if (getStateManager().hasAuAgreements(au.getAuId())) {
      return findAuAgreements(au).haveAgreements();
    } else {
      return false;
    }
  }
  

  /**
   * <p>Copies the identity agreement file for the AU to the given
   * stream.</p>
   * @param au  An archival unit.
   * @param out An output stream.
   * @throws IOException if input or output fails.
   */
  public void writeIdentityAgreementTo(ArchivalUnit au, OutputStream out)
      throws IOException {
    // have the file access performed by the AuAgreements instance,
    // since it has the appropriate lock
    AuAgreements auAgreements = findAuAgreements(au);
    OutputStreamWriter wrtr =
      new OutputStreamWriter(out, Constants.ENCODING_UTF_8);
    wrtr.write(auAgreements.toJson());
    wrtr.flush();
  }

  /**
   * <p>Installs the contents of the stream as the identity agreement
   * file for the AU.</p>
   * @param au An archival unit.
   * @param in An input stream to read from.
   * @return {@code true} if the copy was successful and the au's
   * {@link AuAgreements} instance now reflects the new content.
   */
  public void readIdentityAgreementFrom(ArchivalUnit au, InputStream in)
      throws IOException {
    // have the file access performed by the AuAgreements instance,
    // since it has the appropriate lock
    String json = StringUtil.fromInputStream(in);
    AuAgreements auAgreements = findAuAgreements(au);
    auAgreements.updateFromJson(json, getDaemon());
    auAgreements.storeAuAgreements(null);
  }

  public void setConfig(Configuration config, Configuration oldConfig,
			Configuration.Differences changedKeys) {
    if (changedKeys.contains(PREFIX)) {
      updatesBeforeStoring =
        config.getLong(PARAM_UPDATES_BEFORE_STORING,
                       DEFAULT_UPDATES_BEFORE_STORING);
      minPercentPartialAgreement =
        config.getPercentage(PARAM_MIN_PERCENT_AGREEMENT,
                             DEFAULT_MIN_PERCENT_AGREEMENT);
      if (changedKeys.contains(PARAM_UI_STEM_MAP)) {
	pidUiStemMap = makePidUiStemMap(config.getList(PARAM_UI_STEM_MAP));
      }
      setPeerAddresses(config.getList(PARAM_PEER_ADDRESS_MAP));
      configV3Identities();
    }
  }

  /**
   * Configure initial list of V3 peers.
   */
  private void configV3Identities() {
    List ids = CurrentConfig.getList(PARAM_INITIAL_PEERS,
                                     DEFAULT_INITIAL_PEERS);
    for (Iterator iter = ids.iterator(); iter.hasNext(); ) {
      try {
	// Just ensure the peer is in the ID map.
	findPeerIdentity((String)iter.next());
      } catch (MalformedIdentityKeyException e) {
	log.error("Malformed initial peer", e);
      }
    }
  }

  /** Set up any explicit mappings from PeerIdentity to PeerAddress.
   * Allows different peers to address the same peer differently, e,g, when
   * multiple peers are behind the same NAT. */
  void setPeerAddresses(Collection<String> peerAddressPairs) {
    if (peerAddressPairs != null) {
      for (String pair : peerAddressPairs) {
	List<String> lst = StringUtil.breakAt(pair, ',', -1, true, true);
	if (lst.size() == 2) {
	  String peer = lst.get(0);
	  String addr = lst.get(1);
	  log.debug("Setting address of " + peer + " to " + addr);
	  try {
	    PeerIdentity pid = stringToPeerIdentity(peer);
	    pid.setPeerAddress(PeerAddress.makePeerAddress(addr));
	  } catch (IdentityManager.MalformedIdentityKeyException e) {
	    log.error("Couldn't set address of " + peer + " to " + addr, e);
	  }
	} else {
	  log.error("Malformed peer,address pair: " + pair);
	}
      }
    }
  }

  Map<PeerIdentity,String> makePidUiStemMap(List<String> pidStemList) {
    if (pidStemList == null) {
      return null;
    }
    Map<PeerIdentity,String> res = new HashMap<PeerIdentity,String>();
    for (String one : pidStemList) {
      List<String> lst = StringUtil.breakAt(one, ',', -1, true, true);
      if (lst.size() == 2) {
	try {
	  PeerIdentity pid = stringToPeerIdentity(lst.get(0));
	  res.put(pid, lst.get(1));
	  if (log.isDebug3()) {
	    log.debug3("pidUiStemMap.put(" + pid + ", " + lst.get(1) + ")");
	  }
	} catch (IdentityManager.MalformedIdentityKeyException e) {
	  log.warning("Bad peer in pidUiStemMap: " +lst.get(0), e);
	}
      }
    }
    return res;
  }

  protected String getLocalIpParam(Configuration config) {
    // overridable for testing
    return config.get(PARAM_LOCAL_IP);
  }

  public String getUiUrlStem(PeerIdentity pid) {
    if (pidUiStemMap != null) {
      if (log.isDebug3()) {
	log.debug3("getUiUrlStem(" + pid + "): " + pidUiStemMap.get(pid));
      }
      return pidUiStemMap.get(pid);
    }
    return null;
  }

}
