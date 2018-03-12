/*
 * $Id$
 */

/*

Copyright (c) 2000-2015 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.File;
import java.io.InterruptedIOException;

import org.lockss.app.BaseLockssDaemonManager;
import org.lockss.app.LockssAuManager;
import org.lockss.app.LockssDaemon;
import org.lockss.config.Configuration;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.CachedUrlSet;
import org.lockss.protocol.AuAgreements;
import org.lockss.protocol.DatedPeerIdSet;
import org.lockss.protocol.DatedPeerIdSetImpl;
import org.lockss.protocol.IdentityManager;
import org.lockss.repository.OldLockssRepository;
import org.lockss.repository.OldLockssRepository.RepositoryStateException;
import org.lockss.repository.OldLockssRepositoryImpl;
import org.lockss.util.*;

/**
 * HistoryRepository is an inner layer of the NodeManager which handles the
 * actual storage of NodeStates.
 */
public class HistoryRepositoryImpl
  extends BaseLockssDaemonManager
  implements HistoryRepository {

  /**
   * <p>Name of top directory in which the histories are stored.</p>
   */
  public static final String HISTORY_ROOT_NAME = "cache";

  /**
   * <p>Configuration parameter name for Lockss history location.</p>
   */
  public static final String PARAM_HISTORY_LOCATION = Configuration.PREFIX + "history.location";

  /**
   * <p>The AU state file name.</p>
   */
  public static final String AU_FILE_NAME = "#au_state.xml";

  /**
   * <p>The damaged nodes file name.</p>
   */
  static final String DAMAGED_NODES_FILE_NAME = "#damaged_nodes.xml";

  /**
   * <p>File name of the dated peer id set of peers who have said they
   * don't have the AU</p>
   */
  static final String NO_AU_PEER_ID_SET_FILE_NAME = "#no_au_peers";
  
  /**
   * <p>The history file name.</p>
   */
  static final String HISTORY_FILE_NAME = "#history.xml";

  /**
   * <p>The identity agreement list file name.</p>
   */
  static final String IDENTITY_AGREEMENT_FILE_NAME = "#id_agreement.xml";

  /**
   * <p>Mapping file for polls.</p>
   */
  static final String MAPPING_FILE_NAME = "/org/lockss/state/pollmapping.xml";

  /**
   * <p>All relevant mapping files used by this class.</p>
   */
  static final String[] MAPPING_FILES = {
      MAPPING_FILE_NAME,
      ExternalizableMap.MAPPING_FILE_NAME,
      IdentityManager.MAPPING_FILE_NAME
  };

  /**
   * <p>The node state file name.</p>
   */
  static final String NODE_FILE_NAME = "#nodestate.xml";

  
  /**
   * <p>A logger for use by this class.</p>
   */
  private static Logger logger = Logger.getLogger("HistoryRepository");


  /**
   * <p>Factory class to create HistoryRepository instances.</p>
   */
  public static class Factory implements LockssAuManager.Factory {
    public LockssAuManager createAuManager(ArchivalUnit au) {
      return createNewHistoryRepository(au);
    }
  }

  private String rootLocation;

  private ArchivalUnit storedAu;
  LockssDaemon theDaemon;
  DamagedNodeSet damagedNodes;
  AuState auState;
  OldLockssRepository lockssRepo;

  protected HistoryRepositoryImpl(ArchivalUnit au, String rootPath) {
    storedAu = au;
    rootLocation = rootPath;
    if (rootLocation==null) {
      throw new NullPointerException();
    }
    if (!rootLocation.endsWith(File.separator)) {
      // this shouldn't happen
      rootLocation += File.separator;
    }
  }

  public File getIdentityAgreementFile() {
    return new File(rootLocation, IDENTITY_AGREEMENT_FILE_NAME);
  }

  public File getAuStateFile() {
    return new File(rootLocation, AU_FILE_NAME);
  }

  public long getAuCreationTime() {
    File auidfile = new File(rootLocation, OldLockssRepositoryImpl.AU_ID_FILE);
    return auidfile.lastModified();
  }
  
  private DatedPeerIdSet m_noAuDpis = null;
  
  /**
   * Return the associated NoAuPeerIdSet
   */
  public DatedPeerIdSet getNoAuPeerSet()
  {
    IdentityManager idman;
    
    if (m_noAuDpis == null) {
      File file = prepareFile(rootLocation, NO_AU_PEER_ID_SET_FILE_NAME);
      LockssDaemon ld = getDaemon();
      if (ld != null) {
        idman = ld.getIdentityManager();
      } else {
        logger.error("When attempting to get a dated Peer ID set, I could not find the daemon.  Aborting.");
        throw new NullPointerException();
      }
      
      m_noAuDpis = new DatedPeerIdSetImpl(file, idman);
    }
    
    return m_noAuDpis;
  }

  /**
   * <p>Loads the state of an AU.</p>
   * @return An AuState instance loaded from file.
   * @throws RepositoryStateException if an error condition arises
   *                                  that is neither a file not found
   *                                  exception nor a serialization
   *                                  exception.
   */
  public AuState loadAuState() {
    logger.debug3("Loading state for AU '" + storedAu.getName() + "'");
    File auFile = getAuStateFile();
    String errorString = "Could not load AU state for AU '" + storedAu.getName() + "'";
    ObjectSerializer deserializer = makeObjectSerializer();
    try {
      AuState auState = (AuState)deserializer.deserialize(auFile);
      return  new AuState(auState, storedAu, this);
    } catch (SerializationException.FileNotFound fnf) {
      logger.debug2(errorString + ": " + fnf);
    } catch (SerializationException se) {
      logger.error(errorString, se);
    } catch (InterruptedIOException iioe) {
      logger.error(errorString, iioe);
      throw new RepositoryStateException(errorString, iioe);
    }
    // Default: return default
    return new AuState(storedAu, this);
  }

  /**
   * <p>Loads a damaged node set from file.</p>
   * @return A damaged node set retrieved from file.
   * @throws RepositoryStateException if an error condition arises
   *                                  that is neither a file not found
   *                                  exception nor a serialization
   *                                  exception.
   */
  public DamagedNodeSet loadDamagedNodeSet() {
    logger.debug3("Loading damaged nodes for AU '" + storedAu.getName() + "'");
    File damFile = new File(rootLocation, DAMAGED_NODES_FILE_NAME);
    ObjectSerializer deserializer = makeObjectSerializer();
    String errorString = "Could not load damaged nodes '" + storedAu.getName() + "'";

    try {
      DamagedNodeSet damNodes = (DamagedNodeSet)deserializer.deserialize(damFile);
      // set these fields manually // post-deserialization method?
      damNodes.theAu = storedAu;
      damNodes.repository = this;
      return damNodes;
    }
    catch (SerializationException.FileNotFound fnf) {
      logger.debug2("No damaged node file for AU '" + storedAu.getName() + "'");
      // drop down to return empty set
    }
    catch (SerializationException se) {
      logger.error(errorString, se);
      // drop down to return empty set
    }
    catch (InterruptedIOException exc) {
      logger.error(errorString, exc);
      throw new RepositoryStateException(errorString, exc);
    }

    // Default: return empty set
    return new DamagedNodeSet(storedAu, this);
  }

  /**
   * <p>Loads an identity agreement.</p>
   * @return The saved {@link Object}, or {@code null} if no instance
   * is found.
   * @throws RepositoryStateException if an error condition arises
   *                                  that is neither a file not found
   *                                  exception nor a serialization
   *                                  exception.
   *
   */
  public Object loadIdentityAgreements() {
    logger.debug3("Loading identity agreements for AU '" + storedAu.getName() + "'");
    File idFile = getIdentityAgreementFile();
    ObjectSerializer deserializer = makeObjectSerializer();
    String errorString = "Could not load identity agreements for AU '" + storedAu.getName() + "'";
    try {
      return deserializer.deserialize(idFile);
    }
    catch (SerializationException.FileNotFound fnf) {
      logger.debug2("No identities file for AU '" + storedAu.getName() + "'");
      // drop down to return null
    }
    catch (SerializationException se) {
      logger.error(errorString, se);
      // drop down to return null
    }
    catch (InterruptedIOException exc) {
      logger.error(errorString, exc);
      throw new RepositoryStateException(errorString, exc);
    }
    return null;
  }

  public void setAuConfig(Configuration auConfig) {

  }

  public void startService() {
    super.startService();
    // gets all the managers
    if (logger.isDebug2()) logger.debug2("Starting: " + storedAu);
    theDaemon = getDaemon();
    auState = loadAuState();
    damagedNodes = new DamagedNodeSet(storedAu, this);

    lockssRepo = theDaemon.getLockssRepository(storedAu);
    logger.debug2("HistoryRepository successfully started");
  }

  public void stopService() {
    if (logger.isDebug()) logger.debug("Stopping: " + storedAu);
    if (damagedNodes != null) {
      damagedNodes.clear();
    }
    super.stopService();
    logger.debug2("HistoryRepository successfully stopped");
  }

  public AuState getAuState() {
    return auState;
  }
  public void setAuState(AuState auState) {this.auState = auState;}

  @Override
  public boolean hasDamage(CachedUrlSet cus) {
    return damagedNodes.hasDamage(cus);
  }

  /**
   * <p>Stores the state of an AU.</p>
   * @param auState    A AU state instance.
   * @see #storeAuState(ObjectSerializer, AuState)
   */
  public void storeAuState(AuState auState) {
    logger.debug3("Storing state for AU '" + auState.getArchivalUnit().getName() + "'");
    this.auState = auState;
    File file = prepareFile(rootLocation, AU_FILE_NAME);
    ObjectSerializer serializer = makeObjectSerializer();
    try {
      serializer.serialize(file, auState);
    }
    catch (Exception exc) {
      String errorString = "Could not store AU state for AU '" + auState.getArchivalUnit().getName() + "'";
      logger.error(errorString, exc);
      throw new RepositoryStateException(errorString, exc);
    }
  }

  /**
   * <p>Stores a damaged node set.</p>
   * @param nodeSet    A damaged node set.
   * @see #storeDamagedNodeSet(ObjectSerializer, DamagedNodeSet)
   */
  public void storeDamagedNodeSet(DamagedNodeSet nodeSet) {
    logger.debug3("Storing damaged nodes for AU '" + nodeSet.theAu.getName() + "'");
    File file = prepareFile(rootLocation, DAMAGED_NODES_FILE_NAME);
    ObjectSerializer serializer = makeObjectSerializer();

    try {
      serializer.serialize(file, nodeSet);
    }
    catch (Exception exc) {
      String errorString = "Could not store damaged nodes for AU '" + nodeSet.theAu.getName() + "'";
      logger.error(errorString, exc);
      throw new RepositoryStateException(errorString, exc);
    }
  }

  /**
   * <p>Stores an identity agreement instance.</p>
   * @param auAgreements A {@link AuAgreements} instance.
   */
  public void storeIdentityAgreements(AuAgreements auAgreements) {
    logger.debug3("Storing identity agreements for AU '" + storedAu.getName() + "'");
    File file = prepareFile(rootLocation, IDENTITY_AGREEMENT_FILE_NAME);
    ObjectSerializer serializer = makeObjectSerializer();
    try {
      serializer.serialize(file, auAgreements);
    }
    catch (Exception exc) {
      String errorString = "Could not store identity agreements for AU '" + storedAu.getName() + "'";
      logger.error(errorString, exc);
      throw new RepositoryStateException(errorString, exc);
    }
  }


  /**
   * <p>Factory method to create new HistoryRepository instances.</p>
   * @param au The {@link ArchivalUnit}.
   * @return A new HistoryRepository instance.
   */
  public static HistoryRepository createNewHistoryRepository(ArchivalUnit au) {
    String root = OldLockssRepositoryImpl.getRepositoryRoot(au);
    return
      new HistoryRepositoryImpl(au,
                                OldLockssRepositoryImpl.mapAuToFileLocation(root,
                                                                         au));
  }

  /**
   * <p>Builds a new object serializer, suitable for the given class,
   * using the various mapping files referenced by
   * {@link #MAPPING_FILES}.</p>
   * @param cla The class of objects being serialized/deserialized.
   * @return A new object serializer ready to process objects of type
   *         <code>cla</code>.
   */
  protected ObjectSerializer makeObjectSerializer() {
    return new XStreamSerializer(theDaemon);
  }


  /**
   * <p>Instantiates a {@link File} instance with the given prefix and
   * suffix, creating the path of directories denoted by the prefix if
   * needed by calling {@link File#mkdirs}.</p>
   * @param parent The path prefix.
   * @param child  The file name.
   * @return A new file instance with the prefix appropriately
   *         created.
   */
  private static File prepareFile(String parent, String child) {
    File parentFile = new File(parent);
    if (!parentFile.exists()) {
      parentFile.mkdirs();
    }
    return new File(parentFile, child);
  }
}
