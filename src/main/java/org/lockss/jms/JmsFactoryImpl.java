/*
 * Copyright (c) 2018-2019 Board of Trustees of Leland Stanford Jr. University,
 * all rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 * STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 * IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * Except as contained in this notice, the name of Stanford University shall not
 * be used in advertising or otherwise to promote the sale, use or other dealings
 * in this Software without prior written authorization from Stanford University.
 */

package org.lockss.jms;

import java.io.Serializable;
import java.util.*;
import javax.jms.*;

import org.lockss.util.*;
import org.lockss.util.jms.*;

/** A JmsFactory that can be given out to modules that depend only on
 * lockss-util, but which are running in a context that includes the JMS
 * support in lockss-core.  (I.e., repo-core) */
public class JmsFactoryImpl implements JmsFactory {

  private JMSManager jmsMgr;

  JmsFactoryImpl(JMSManager jmsMgr) {
    this.jmsMgr = jmsMgr;
  }

  public JmsConsumer createTopicConsumer(String clientId,
					 String topicName)
      throws JMSException {
    return createTopicConsumer(clientId, topicName, null);
  }

  public JmsConsumer createTopicConsumer(String clientId,
					 String topicName,
					 MessageListener listener)
      throws JMSException {
    return createTopicConsumer(clientId, topicName, false, listener);
  }

  public JmsConsumer createTopicConsumer(String clientId,
					 String topicName,
					 boolean noLocal,
					 MessageListener listener)
      throws JMSException {
    return createTopicConsumer(clientId, topicName, noLocal, listener, null);
  }

  public JmsConsumer createTopicConsumer(String clientId,
					 String topicName,
					 boolean noLocal,
					 MessageListener listener,
					 Connection connection)
      throws JMSException {
    JmsConsumerImpl res = new JmsConsumerImpl(jmsMgr);
    res.createTopic(clientId, topicName, noLocal, listener, connection);
    return res;
  }

  public JmsProducer createTopicProducer(String clientId,
					 String topicName)
      throws JMSException {
    return createTopicProducer(clientId, topicName, null);
  }

  public JmsProducer createTopicProducer(String clientId,
					 String topicName,
					 Connection connection)
      throws JMSException {
    JmsProducerImpl res = new JmsProducerImpl(jmsMgr);
    res.createTopic(clientId, topicName, connection);
    return res;
  }

}
