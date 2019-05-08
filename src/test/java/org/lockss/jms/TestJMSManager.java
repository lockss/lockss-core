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


import java.util.*;
import javax.jms.*;
import org.junit.*;

import org.lockss.test.*;
import org.lockss.util.*;

public class TestJMSManager extends LockssTestCase4 {
  protected static Logger log = Logger.getLogger();


  MockLockssDaemon daemon;
  JMSManager mgr;

  @Before
  public void setUpObjs() throws Exception {
    // ensure manager doesn't start another broker
    ConfigurationUtil.addFromArgs(JMSManager.PARAM_START_BROKER, "false");
    daemon = getMockLockssDaemon();
    daemon.startManagers(JMSManager.class);

  }

  @Test
  public void testConfig1() throws JMSException {
    ConfigurationUtil.addFromArgs(JMSManager.PARAM_CONNECT_FAILOVER,
				  "false",
				  JMSManager.PARAM_BROKER_URI,
				  "tcp://1.2.2.1:8888");
    mgr = daemon.getManagerByType(JMSManager.class);
    assertEquals("tcp://1.2.2.1:8888", mgr.getBrokerUri());
    assertEquals("tcp://1.2.2.1:8888", mgr.getConnectUri());
  }

  @Test
  public void testConfig2() throws JMSException {
    ConfigurationUtil.addFromArgs(JMSManager.PARAM_CONNECT_FAILOVER,
				  "false",
				  JMSManager.PARAM_BROKER_URI,
				  "tcp://0.0.0.0:6421",
				  JMSManager.PARAM_CONNECT_URI,
				  "tcp://1.2.3.1:8888");
    mgr = daemon.getManagerByType(JMSManager.class);
    assertEquals("tcp://0.0.0.0:6421", mgr.getBrokerUri());
    assertEquals("tcp://1.2.3.1:8888", mgr.getConnectUri());
  }

  @Test
  public void testConfig1F() throws JMSException {
    ConfigurationUtil.addFromArgs(JMSManager.PARAM_CONNECT_FAILOVER,
				  "true",
				  JMSManager.PARAM_BROKER_URI,
				  "tcp://1.2.2.1:8888");
    mgr = daemon.getManagerByType(JMSManager.class);
    assertEquals("tcp://1.2.2.1:8888", mgr.getBrokerUri());
    assertEquals("failover:(tcp://1.2.2.1:8888)", mgr.getConnectUri());
  }

  @Test
  public void testConfig2F() throws JMSException {
    ConfigurationUtil.addFromArgs(JMSManager.PARAM_CONNECT_FAILOVER,
				  "true",
				  JMSManager.PARAM_BROKER_URI,
				  "tcp://0.0.0.0:6421",
				  JMSManager.PARAM_CONNECT_URI,
				  "tcp://1.2.3.1:8888");
    mgr = daemon.getManagerByType(JMSManager.class);
    assertEquals("tcp://0.0.0.0:6421", mgr.getBrokerUri());
    assertEquals("failover:(tcp://1.2.3.1:8888)", mgr.getConnectUri());
  }
}
