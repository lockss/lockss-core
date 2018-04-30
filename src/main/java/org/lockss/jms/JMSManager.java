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

package org.lockss.jms;

import java.io.*;
import java.util.*;
import org.apache.activemq.broker.*;
import org.apache.activemq.store.*;

import org.lockss.app.*;
import org.lockss.daemon.*;
import org.lockss.util.*;
import org.lockss.config.*;

/** Manages (starts & stops) an embedded ActiveMQ broker */
public class JMSManager extends BaseLockssManager
  implements ConfigurableManager  {

  protected static Logger log = Logger.getLogger("JMSManager");

  public static final String PREFIX = Configuration.PREFIX + "jms.";
  public static final String BROKER_PREFIX = PREFIX + "broker.";
  public static final String CONNECT_PREFIX = PREFIX + "connect.";

  /** If true, an ActiveMQ broker will be started at the address specified
   * by {@value #PARAM_BROKER_URI} or {@value #PARAM_CONNECT_URI}.  Only
   * checked at startup. */
  public static final String PARAM_START_BROKER = BROKER_PREFIX + "start";
  public static final boolean DEFAULT_START_BROKER = false;

  /** URL specifying protocol & address at which the broker will be started
   * (if {@value #PARAM_START_BROKER} is true), and (unless {@value
   * #PARAM_CONNECT_URI} is set), the URI to which producers and consumers
   * will connect.  Only checked at startup.  Usually one of:<ul>
   * <li><code>tcp://<i>hostname</i>:<i>port</i></code></li>
   * <li><code>vm://localhost?create=false</code></li></ul>
   *
   * <a href="http://activemq.apache.org/configuring-transports.html">See
   * here</a> for a full list of transport protocols. */
  public static final String PARAM_BROKER_URI = BROKER_PREFIX + "uri";
  public static String DEFAULT_BROKER_URI =
    "vm://localhost?create=false";
//   public static String DEFAULT_BROKER_URI = "tcp://localhost:61616";

  /** Broker URI to which producers and consumers will connect.  <i>Eg</i>,
   * <code>failover:tcp://<i>hostname</i>:<i>port</i></code>&nbsp;.  If not
   * set {@value #PARAM_START_BROKER} is used. */
  public static final String PARAM_CONNECT_URI = CONNECT_PREFIX + "uri";

  /** If true, <code>failover:</code> will be prepended to the value of
   * {@value #PARAM_BROKER_URI} to obtain the broker URI to which producers
   * and consumers will connect. */
  public static final String PARAM_CONNECT_FAILOVER =
    CONNECT_PREFIX + "failover";
  public static boolean DEFAULT_CONNECT_FAILOVER = false;

  /** If true, the broker will use a persistent store */
  public static final String PARAM_IS_PERSISTENT =
    BROKER_PREFIX + "isPersistent";
  public static final boolean DEFAULT_IS_PERSISTENT = false;

  /** Persistent storage directory path.  If not set, defaults to
   * <i>diskSpacePaths</i><code>/activemq</code if that is set */
  public static final String PARAM_PERSISTENT_DIR =
    BROKER_PREFIX + "persistentDir";
  public static final String DEFAULT_PERSISTENT_DIR = "activemq";

  /** If true the broker will be accessible via JMX */
  public static final String PARAM_USE_JMX = BROKER_PREFIX + "useJmx";
  public static final boolean DEFAULT_USE_JMS = false;

  private BrokerService broker;
  private String brokerUri = DEFAULT_BROKER_URI;
  private String connectUri = DEFAULT_BROKER_URI;

  public void startService() {
    super.startService();
    Configuration config = ConfigManager.getCurrentConfig();
    brokerUri = config.get(PARAM_BROKER_URI, DEFAULT_BROKER_URI);

    String curi;
    if (config.getBoolean(PARAM_CONNECT_FAILOVER, DEFAULT_CONNECT_FAILOVER)) {
      curi = "failover:(" + brokerUri + ")";
    } else {
      curi = config.get(PARAM_CONNECT_URI, brokerUri);
    }
    connectUri = curi;
    if (config.getBoolean(PARAM_START_BROKER, DEFAULT_START_BROKER)) {
      broker = createBroker(brokerUri); 
    }
  }

  public void stopService() {
    if (broker != null) {
      try {
	broker.stop();
      } catch (Exception e) {
	log.error("Couldn't stop ActiveMQ broker", e);
      }
      broker = null;
    }
    super.stopService();
  }

  public void setConfig(Configuration config, Configuration oldConfig,
			Configuration.Differences changedKeys) {
    if (changedKeys.contains(PREFIX)) {
    }
  }

  /** Start and return a broker with config given by the params under
   * {@value #BROKER_PREFIX} */
  public static BrokerService createBroker(String uri) {
    Configuration config = ConfigManager.getCurrentConfig();
    boolean isPersistent = config.getBoolean(PARAM_IS_PERSISTENT,
					     DEFAULT_IS_PERSISTENT);
    File persistentDir = null;
    if (isPersistent) {
      ConfigManager cfgMgr = ConfigManager.getConfigManager();
      persistentDir = cfgMgr.findConfiguredDataDir(PARAM_PERSISTENT_DIR,
						      DEFAULT_PERSISTENT_DIR);
    }
    boolean useJmx = config.getBoolean(PARAM_USE_JMX, DEFAULT_USE_JMS);

    try {
    BrokerService res = new BrokerService(); 
//     res.setBrokerName("foo");
      StringBuilder sb = new StringBuilder();
      sb.append("Started broker ");
      sb.append(uri);

      res.setPersistent(isPersistent);
      sb.append(", persistent: ");
      sb.append(isPersistent);

      if (isPersistent && persistentDir != null) {
	PersistenceAdapter pa = res.getPersistenceAdapter();
	pa.setDirectory(new File(persistentDir.toString()));
	sb.append(" (");
	sb.append(persistentDir.toString());
	sb.append(")");
      }

      res.setUseJmx(useJmx);
      if (useJmx) sb.append(", useJmx");

      res.addConnector(uri); 
      res.start();
      log.info(sb.toString());
      return res;
    } catch (Exception e) {
      log.error("Couldn't start ActiveMQ broker: " + uri, e);
      return null;
    }
  }

//   public Broker getBroker() {
//     return BrokerService.getBroker();
//   }

  public String getConnectUri() {
    return connectUri;
  }
}
