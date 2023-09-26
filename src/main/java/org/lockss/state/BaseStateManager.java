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

package org.lockss.state;

import java.util.*;
import javax.jms.*;

import org.lockss.account.UserAccount;
import org.lockss.app.*;
import org.lockss.log.*;
import org.lockss.config.*;
import org.lockss.plugin.*;
import org.lockss.protocol.*;
import org.lockss.state.AuSuspectUrlVersions.SuspectUrlVersion;

/** Building blocks for {@link StateManager}s.
*/
public abstract class BaseStateManager extends BaseLockssDaemonManager
  implements StateManager, ConfigurableManager {

  protected static L4JLogger log = L4JLogger.getLogger();

  public static final String PREFIX = Configuration.PREFIX + "state.";

  protected LockssDaemon daemon;
  protected ConfigManager configMgr;
  protected PluginManager pluginMgr;

  @Override
  public void initService(LockssDaemon daemon) throws LockssAppException {
    super.initService(daemon);
    this.daemon = daemon;
    configMgr = daemon.getConfigManager();
    pluginMgr = daemon.getPluginManager();
    // Don't prefetch IdentityManager here as it uses StateManager
  }

  public void setConfig(Configuration config, Configuration oldConfig,
			Configuration.Differences changedKeys) {
    if (changedKeys.contains(PREFIX)) {
      notificationTopic = config.get(PARAM_JMS_NOTIFICATION_TOPIC,
				     DEFAULT_JMS_NOTIFICATION_TOPIC);
      clientId = config.get(PARAM_JMS_CLIENT_ID, DEFAULT_JMS_CLIENT_ID);
    }
  }

  /** return the string to use as a key for the AU's AuState.  Normally the
   * auid, this is necessary right now because in testing some AUs have no
   * auid */
  public String auKey(ArchivalUnit au) {
    try {
      return au.getAuId();
    } catch (RuntimeException e) {
      return "" + System.identityHashCode(au);
    }
  }

  protected void putNotNull(Map map, String key, String val) {
    if (val != null) {
      map.put(key, val);
    }
  }

  // JMS notification support

  // Notification message is a map:
  // name - Name of state class (e.g., AuState)
  // auid - if object is per-AU
  // username - if object is UserAccount
  // userAccountChange - type of change to the user account
  // json - serialized json of whole object or diffs

  public static final String JMS_MAP_NAME = "name";
  public static final String JMS_MAP_AUID = "auid";
  public static final String JMS_MAP_USERNAME = "username";
  public static final String JMS_MAP_USERACCOUNT_CHANGE = "userAccountChange";
  public static final String JMS_MAP_JSON = "json";
  public static final String JMS_MAP_COOKIE = "cookie";
  

  public static final String JMS_PREFIX = PREFIX + "jms.";

  /** The jms topic at which state change notifications are sent
   * @ParamRelevance Rare
   */
  public static final String PARAM_JMS_NOTIFICATION_TOPIC =
    JMS_PREFIX + "topic";
  public static final String DEFAULT_JMS_NOTIFICATION_TOPIC =
    "StateChangedTopic";

  /** The jms clientid of the StateManager.
   * @ParamRelevance Rare
   */
  public static final String PARAM_JMS_CLIENT_ID = JMS_PREFIX + "clientId";
  public static final String DEFAULT_JMS_CLIENT_ID = "StateMgr";

  private String notificationTopic = DEFAULT_JMS_NOTIFICATION_TOPIC;
  private String clientId = DEFAULT_JMS_CLIENT_ID;

  void setUpJmsReceive() {
    super.setUpJmsReceive(clientId,
			  notificationTopic,
			  new MapMessageListener("State Listener"));
  }

  void setUpJmsSend() {
    super.setUpJmsSend(clientId, notificationTopic);
  }

  /** Incoming StateChange message */
  @Override
  protected void receiveMessage(Map map) {
    log.debug2("Received notification: " + map);
    try {
      String name = (String)map.get(JMS_MAP_NAME);
      String json = (String)map.get(JMS_MAP_JSON);
      String cookie = (String)map.get(JMS_MAP_COOKIE);

      // AUID may be null if we're handling a non-AU message:
      String auid = (String)map.get(JMS_MAP_AUID);

      switch (name) {
      case "AuState":
	doReceiveAuStateChanged(auid, json, cookie);
	break;
      case "AuAgreements":
	doReceiveAuAgreementsChanged(auid, json, cookie);
	break;
      case "AuSuspectUrlVersions":
	doReceiveAuSuspectUrlVersionsChanged(auid, json, cookie);
	break;
      case "NoAuPeerSet":
	doReceiveNoAuPeerSetChanged(auid, json, cookie);
	break;
      case "UserAccount":
        String username = (String)map.get(JMS_MAP_USERNAME);
        String userAccountChange = (String)map.get(JMS_MAP_USERACCOUNT_CHANGE);
        doReceiveUserAccountChanged(UserAccount.UserAccountChange.valueOf(userAccountChange), username, json,
            cookie);
        break;
      default:
	log.warn("Receive state update for unknown object: {}", name);
      }
    } catch (ClassCastException e) {
      log.error("Wrong type field in message: {}", map, e);
    }
  }

  // /////////////////////////////////////////////////////////////////
  // AuState
  // /////////////////////////////////////////////////////////////////

  /** Send JMS notification of AuState change.  Should be called only from
   * a hook in a server StateManager.
   * @param key auid
   * @param json string containing only the changed fields.
   */
  protected void sendAuStateChangedEvent(String key, String json,
					 String cookie) {
    if (jmsProducer != null) {
      Map<String,Object> map = new HashMap<>();
      map.put(JMS_MAP_NAME, "AuState");
      map.put(JMS_MAP_AUID, key);
      map.put(JMS_MAP_JSON, json);
      putNotNull(map, JMS_MAP_COOKIE, cookie);
      try {
	jmsProducer.sendMap(map);
      } catch (JMSException e) {
	log.error("Couldn't send StateChanged notification", e);
      }
    }
  }

  /** Create a default AuState */
  protected AuState newDefaultAuState(ArchivalUnit au) {
    AuState aus = new AuState(au, this);
    aus.setAuId(auKey(au));
    return aus;
  }

  /** Create a default AuStateBean */
  protected AuStateBean newDefaultAuStateBean(String key) {
    AuStateBean ausb = new AuStateBean();
    ausb.setAuId(key);
    return ausb;
  }

  // Hooks to be implemented by subclasses

  /** Hook for subclass to store or update an AuStateBean in persistent
   * storage.  If <code>fields</code> is a non-empty set, Only those fields
   * named in it should be updated.
   * @param key AUID or other key for AU
   * @param aus AuStateBean data source
   * @param fields The fields to store, or null to store all fields
   */
  protected void doStoreAuStateBean(String key,
				    AuStateBean ausb,
				    Set<String> fields) {
  }

  /** Hook for subclass to read an AuStateBean instance from persistent
   * storage. */
  protected AuStateBean doLoadAuStateBean(String key) {
    return null;
  }

  /** Hook for subclass to send AuState changed notifications */
  protected void doNotifyAuStateChanged(String auid, String json,
					String cookie) {
  }

  /** Hook for subclass to receive AuState changed notifications */
  protected void doReceiveAuStateChanged(String auid, String json,
					 String cookie) {
  }


  // /////////////////////////////////////////////////////////////////
  // AuAgreements
  // /////////////////////////////////////////////////////////////////

  /** Send JMS notification of AuState change.  Should be called only from
   * a hook in a server StateManager.
   * @param key auid
   * @param json string containing only the changed fields.
   */
  protected void sendAuAgreementsChangedEvent(String key, String json,
					      String cookie) {
    if (jmsProducer != null) {
      Map<String,Object> map = new HashMap<>();
      map.put(JMS_MAP_NAME, "AuAgreements");
      map.put(JMS_MAP_AUID, key);
      map.put(JMS_MAP_JSON, json);
      putNotNull(map, JMS_MAP_COOKIE, cookie);
      try {
	jmsProducer.sendMap(map);
      } catch (JMSException e) {
	log.error("Couldn't send StateChanged notification", e);
      }
    }
  }

  /** Create a default AuAgreementsBean */
  protected AuAgreements newDefaultAuAgreements(String key) {
    return AuAgreements.make(key, daemon.getIdentityManager());
  }

  // Hooks to be implemented by subclasses

  /** Hook for subclass to update an existing AuAgreements in persistent
   * storage.  Any of the three data sources may be used.  Only those
   * fields present in the Map or the json string should be saved.
   * @param key AUID or other key for AU
   * @param aus AuAgreements data source
   * @param aus json data source
   * @param aus Map data source
   */
  protected void doStoreAuAgreementsUpdate(String key, AuAgreements aua,
					   Set<PeerIdentity> peers) {
  }

  /** Hook for subclass to read an AuAgreements instance from persistent
   * storage. */
  protected AuAgreements doLoadAuAgreements(String key) {
    return null;
  }

  /** Hook for subclass to send AuAgreements changed notifications */
  protected void doNotifyAuAgreementsChanged(String auid, String json,
					     String cookie) {
  }

  /** Hook for subclass to receive AuAgreements changed notifications */
  protected void doReceiveAuAgreementsChanged(String auid, String json,
					      String cookie) {
  }


  // /////////////////////////////////////////////////////////////////
  // AuSuspectUrlVersions
  // /////////////////////////////////////////////////////////////////

  /** Send JMS notification of AuState change.  Should be called only from
   * a hook in a server StateManager.
   * @param key auid
   * @param json string containing only the changed fields.
   */
  protected void sendAuSuspectUrlVersionsChangedEvent(String key, String json,
						      String cookie) {
    if (jmsProducer != null) {
      Map<String,Object> map = new HashMap<>();
      map.put(JMS_MAP_NAME, "AuSuspectUrlVersions");
      map.put(JMS_MAP_AUID, key);
      map.put(JMS_MAP_JSON, json);
      putNotNull(map, JMS_MAP_COOKIE, cookie);
      try {
 	jmsProducer.sendMap(map);
      } catch (JMSException e) {
 	log.error("Couldn't send StateChanged notification", e);
      }
    }
  }

  /** Create a default AuSuspectUrlVersionsBean */
  protected AuSuspectUrlVersions newDefaultAuSuspectUrlVersions(String key) {
    return new AuSuspectUrlVersions(key);
  }

  // Hooks to be implemented by subclasses

  /** Hook for subclass to update an existing AuSuspectUrlVersions in persistent
   * storage.  Any of the three data sources may be used.  Only those
   * fields present in the Map or the json string should be saved.
   * @param key AUID or other key for AU
   * @param ausuv AuSuspectUrlVersions data source
   * @param versions Map data source
   */
  protected void doStoreAuSuspectUrlVersionsUpdate(String key,
						   AuSuspectUrlVersions ausuv,
					      Set<SuspectUrlVersion> versions) {
  }

  /** Hook for subclass to read an AuSuspectUrlVersions instance from persistent
   * storage. */
  protected AuSuspectUrlVersions doLoadAuSuspectUrlVersions(String key) {
    return null;
  }

  /** Hook for subclass to send AuSuspectUrlVersions changed notifications */
  protected void doNotifyAuSuspectUrlVersionsChanged(String auid, String json,
						     String cookie) {
  }

  /** Hook for subclass to receive AuSuspectUrlVersions changed notifications */
  protected void doReceiveAuSuspectUrlVersionsChanged(String auid, String json,
						      String cookie) {
  }

  // /////////////////////////////////////////////////////////////////
  // NoAuPeerSet
  // /////////////////////////////////////////////////////////////////

  /** Send JMS notification of AuState change.  Should be called only from
   * a hook in a server StateManager.
   * @param key auid
   * @param json string containing only the changed fields.
   */
  protected void sendNoAuPeerSetChangedEvent(String key, String json,
					     String cookie) {
    if (jmsProducer != null) {
      Map<String,Object> map = new HashMap<>();
      map.put(JMS_MAP_NAME, "NoAuPeerSet");
      map.put(JMS_MAP_AUID, key);
      map.put(JMS_MAP_JSON, json);
      putNotNull(map, JMS_MAP_COOKIE, cookie);
      map.put(JMS_MAP_COOKIE, cookie);
      try {
 	jmsProducer.sendMap(map);
      } catch (JMSException e) {
 	log.error("Couldn't send StateChanged notification", e);
      }
    }
  }

  /** Create a default NoAuPeerSetBean */
  protected DatedPeerIdSet newDefaultNoAuPeerSet(String key) {
    return new DatedPeerIdSetImpl(key, daemon.getIdentityManager());
  }

  // Hooks to be implemented by subclasses

  /** Hook for subclass to update an existing NoAuPeerSet in persistent
   * storage.  Any of the three data sources may be used.  Only those
   * fields present in the Map or the json string should be saved.
   * @param key AUID or other key for AU
   * @param dpis NoAuPeerSet data source
   * @param peers Map data source
   */
  protected void doStoreNoAuPeerSetUpdate(String key, DatedPeerIdSet dpis,
					  Set<PeerIdentity> peers) {
  }

  /** Hook for subclass to read an NoAuPeerSet instance from persistent
   * storage. */
  protected DatedPeerIdSet doLoadNoAuPeerSet(String key) {
    return null;
  }

  /** Hook for subclass to send NoAuPeerSet changed notifications */
  protected void doNotifyNoAuPeerSetChanged(String auid, String json,
					    String cookie) {
  }

  /** Hook for subclass to receive NoAuPeerSet changed notifications */
  protected void doReceiveNoAuPeerSetChanged(String auid, String json,
					     String cookie) {
  }

  // /////////////////////////////////////////////////////////////////
  // UserAccount
  // /////////////////////////////////////////////////////////////////

  protected void sendUserAccountChangedEvent(String key, String json,
                                         UserAccount.UserAccountChange op,
                                         String cookie) {
    if (jmsProducer != null) {
      Map<String,Object> map = new HashMap<>();
      map.put(JMS_MAP_NAME, "UserAccount");
      map.put(JMS_MAP_USERNAME, key);
      map.put(JMS_MAP_USERACCOUNT_CHANGE, op);
      map.put(JMS_MAP_JSON, json);
      putNotNull(map, JMS_MAP_COOKIE, cookie);
      try {
        jmsProducer.sendMap(map);
      } catch (JMSException e) {
        log.error("Couldn't send StateChanged notification", e);
      }
    }
  }

  /** Hook for subclass to send UserAccount changed notifications */
  protected void doNotifyUserAccountChanged(UserAccount.UserAccountChange op, String username, String json,
                                            String cookie) {
    // Intentionally left blank
  }

  /** Hook for subclass to receive UserAccount changed notifications */
  protected void doReceiveUserAccountChanged(UserAccount.UserAccountChange op, String username, String json,
                                             String cookie) {
    // Intentionally left blank
  }

  /** Hook for subclass to read the set of {@link UserAccount} names
   * from persistent storage. */
  protected Iterable<String> doLoadUserAccountNames() {
    return null;
  }

  /** Hook for subclass to read the set of {@link UserAccount}s from
   * persistent storage. */
  protected Iterable<UserAccount> doLoadUserAccounts() {
    return null;
  }

  /** Hook for subclass to read a {@link UserAccount} from persistent
   * storage. */
  protected UserAccount doLoadUserAccount(String name) {
    return null;
  }

  /** Hook for subclass to store or update a {@link UserAccount} in
   * persistent storage.
   * @param key
   * @param acct
   */
  protected void doStoreUserAccount(String key,
                                    UserAccount acct,
                                    Set<String> fields) {
    // Intentionally left blank
  }

  /** Hook for subclass to remove a {@link UserAccount} from persistent
   * storage.
   * @param acct
   */
  protected void doRemoveUserAccount(UserAccount acct) {
    // Intentionally left blank
  }
}
