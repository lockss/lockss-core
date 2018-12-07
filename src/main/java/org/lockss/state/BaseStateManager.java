/*

Copyright (c) 2000-2018 Board of Trustees of Leland Stanford Jr. University,
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
import java.util.*;
import javax.jms.*;
import org.apache.activemq.broker.*;
import org.apache.activemq.store.*;

import org.lockss.app.*;
import org.lockss.daemon.*;
import org.lockss.log.*;
import org.lockss.jms.*;
import org.lockss.util.*;
import org.lockss.config.*;
import org.lockss.plugin.*;

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
  }

  public void setConfig(Configuration config, Configuration oldConfig,
			Configuration.Differences changedKeys) {
    if (changedKeys.contains(PREFIX)) {
      notificationTopic = config.get(PARAM_JMS_NOTIFICATION_TOPIC,
				     DEFAULT_JMS_NOTIFICATION_TOPIC);
      enableJmsSend = config.getBoolean(PARAM_ENABLE_JMS_SEND,
					DEFAULT_ENABLE_JMS_SEND);
      enableJmsReceive = config.getBoolean(PARAM_ENABLE_JMS_RECEIVE,
					   DEFAULT_ENABLE_JMS_RECEIVE);
      clientId = config.get(PARAM_JMS_CLIENT_ID, DEFAULT_JMS_CLIENT_ID);
    }
  }

  /** Create a default AuState */
  protected AuState newDefaultAuState(ArchivalUnit au) {
    return new AuState(au, null);
  }

  /** return the string to use as a key for the AU's AuState.  Normally the
   * auid, this is necessary right now because in testing some AUs have no
   * auid */
  protected String auKey(ArchivalUnit au) {
    try {
      return au.getAuId();
    } catch (RuntimeException e) {
      return "" + System.identityHashCode(au);
    }
  }

  // JMS notification support

  // Notification message is a map:
  // name - Name of state class (e.g., AuState)
  // auid - if object is per-AU
  // complete - true iff the entire object is represented, false if diffs
  // json - serialized json of whole object or diffs

  public static final String JMS_MAP_NAME = "name";
  public static final String JMS_MAP_AUID = "auid";
  public static final String JMS_MAP_COMPLETE = "complete";
  public static final String JMS_MAP_JSON = "json";
  

  public static final String JMS_PREFIX = PREFIX + "jms.";

  /** Enable notification sending by server StateManager
   * @ParamRelevance Rare
   */
  public static final String PARAM_ENABLE_JMS_SEND = JMS_PREFIX + "enableSend";
  public static final boolean DEFAULT_ENABLE_JMS_SEND = true;

  /** Enable notification receipt by client StateManager
   * @ParamRelevance Rare
   */
  public static final String PARAM_ENABLE_JMS_RECEIVE =
    JMS_PREFIX + "enableReceive";
  public static final boolean DEFAULT_ENABLE_JMS_RECEIVE = true;

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
  public static final String DEFAULT_JMS_CLIENT_ID = null;

  private Consumer jmsConsumer;
  private Producer jmsProducer;
  private String notificationTopic = DEFAULT_JMS_NOTIFICATION_TOPIC;
  private boolean enableJmsSend = DEFAULT_ENABLE_JMS_SEND;
  private boolean enableJmsReceive = DEFAULT_ENABLE_JMS_RECEIVE;
  private String clientId = DEFAULT_JMS_CLIENT_ID;

  void setUpJmsReceive() {
    if (!enableJmsReceive) {
      log.info("JMS receive manually disabled, not processing incoming state changed notifications");
      return;
    }
    log.debug("Creating consumer");
    try {
      jmsConsumer =
	Consumer.createTopicConsumer(clientId,
				     notificationTopic,
				     new MyMessageListener("State Listener"));
    } catch (JMSException e) {
      log.error("Couldn't create jms consumer", e);
    }
  }

  void setUpJmsSend() {
    if (!enableJmsSend) {
      log.info("JMS send manually disabled, not sending state changed notifications");
      return;
    }
    log.debug("Creating producer");
    try {
      jmsProducer = Producer.createTopicProducer(clientId, notificationTopic);
    } catch (JMSException e) {
      log.error("Couldn't create jms producer", e);
    }
  }

  void stopJms() {
    Producer p = jmsProducer;
    if (p != null) {
      try {
	jmsProducer = null;
	p.closeConnection();
      } catch (JMSException e) {
	log.error("Couldn't stop jms producer", e);
      }
    }
    Consumer c = jmsConsumer;
    if (c != null) {
      try {
	jmsConsumer = null;
	c.closeConnection();
      } catch (JMSException e) {
	log.error("Couldn't stop jms consumer", e);
      }
    }
  }

  void sendAuStateChangedEvent(String auid, String json, boolean complete) {
    if (jmsProducer != null) {
      Map<String,Object> map = new HashMap<>();
      map.put(JMS_MAP_NAME, "AuState");
      map.put(JMS_MAP_AUID, auid);
      map.put(JMS_MAP_JSON, json);
      map.put(JMS_MAP_COMPLETE, Boolean.toString(complete));
      try {
	jmsProducer.sendMap(map);
      } catch (JMSException e) {
	log.error("Couldn't send StateChanged notification", e);
      }
    }
  }

  /** Incoming AuEvent message */
  void receiveStateChangedNotification(Map map) {
    log.debug2("Received notification: " + map);
    try {
      String name = (String)map.get(JMS_MAP_NAME);
      String auid = (String)map.get(JMS_MAP_AUID);
      String json = (String)map.get(JMS_MAP_JSON);
      boolean complete = Boolean.valueOf((String)map.get(JMS_MAP_JSON));
      switch (name) {
      case "AuState":
	receiveAuState(auid, json, complete);
	break;
      default:
	log.warn("Receive state update for unknown object: {}", name);
      }
    } catch (ClassCastException e) {
      log.error("Wrong type field in message: {}", map, e);
    }
  }

  void receiveAuState(String auid, String json, boolean complete) {
  }

  private class MyMessageListener
    extends Consumer.SubscriptionListener {

    MyMessageListener(String listenerName) {
      super(listenerName);
    }

    @Override
    public void onMessage(Message message) {
      log.debug2("onMessage: {}", message);
      try {
        Object msgObject =  Consumer.convertMessage(message);
	if (msgObject instanceof Map) {
	  receiveStateChangedNotification((Map)msgObject);
	} else {
	  log.warn("Unknown notification type: " + msgObject);
	}
      } catch (JMSException e) {
	log.warn("foo", e);
      }
    }
  }


}
