/*
 * Copyright (c) 2018 Board of Trustees of Leland Stanford Jr. University,
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.jms.*;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import org.lockss.app.*;
import org.lockss.util.*;

public class Producer {

  private static final Logger log = Logger.getLogger();

  private String clientId;
  private Connection connection;
  private Session session;
  private MessageProducer messageProducer;

  public static Producer createTopicProducer(String clientId,
					       String topicName)
      throws JMSException {
    Producer res = new Producer();
    res.createTopic(clientId, topicName);
    return res;
  }

  private Producer createTopic(String clientId, String topicName)
      throws JMSException {
    this.clientId = clientId;

    JMSManager mgr = LockssApp.getManagerByTypeStatic(JMSManager.class);
    // create a Connection Factory
    log.debug("Creating producer for topic: " + topicName +
	      ", client: " + clientId + " at " +
	      mgr.getConnectUri());
    ConnectionFactory connectionFactory =
      new ActiveMQConnectionFactory(mgr.getConnectUri());

    // create a Connection
    connection = connectionFactory.createConnection();
    connection.setClientID(clientId);
    log.debug3("Created session for topic: " + topicName +
	       ", client: " + clientId + " at " +
	       mgr.getConnectUri());

    // create a Session
    session =
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    // create the Topic to which messages will be sent
    Topic topic = session.createTopic(topicName);

    // create a MessageProducer for sending messages
    messageProducer = session.createProducer(topic);
    log.debug("Created producer for topic: " + topicName +
	      ", client: " + clientId + " at " +
	      mgr.getConnectUri());
    return this;
  }

  public void closeConnection() throws JMSException {
    if(connection != null) {
      connection.close();
    }
  }

  /**
   * Send a text message.
   * @param text the text string to send
   * @throws JMSException if the TextMessage is cannot be created.
   */
  public void sendText(String text) throws JMSException {
    // create a JMS TextMessage
    TextMessage textMessage = session.createTextMessage(text);

    // send the message to the topic destination
    messageProducer.send(textMessage);

    if (log.isDebug()) {
      log.debug(clientId + ": sent to topic " +
		messageProducer.getDestination() +
		" message with text='" + text + "'");
    }
  }

  /**
   * Send a map of keys: values in which keys are java Strings and values are
   * a java object for which toString() allows understandable reconstruction.
   * @param map The map to send.
   * @throws JMSException if the map is MapMessage can not be created or set.
   */
  public void sendMap(Map<?,?> map) throws JMSException, IllegalArgumentException {
    MapMessage msg = session.createMapMessage();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (!(entry.getKey() instanceof String)) {
        String classname = entry.getKey().getClass().getName();
        throw new IllegalArgumentException("Cannot convert non-String key of type [" + classname
            +"] to JMS MapMessage entry key.");
      }
      msg.setObject((String) entry.getKey(), entry.getValue());
    }
    messageProducer.send(msg);
    if (log.isDebug()) {
      log.debug(clientId + ": sent message with map='" + map + "'");
    }
  }

  /**
   * Send a byte array
   * @param bytes the array of bytes to send.
   * @throws JMSException if the BytesMessage can not be created or set.
   */
  public void sendBytes(byte[] bytes) throws JMSException {

    BytesMessage msg = session.createBytesMessage();
    msg.writeBytes(bytes);
    messageProducer.send(msg);
    if (log.isDebug()) {
      log.debug(clientId + ": sent message of bytes ='" + bytes + "'");
    }
  }

  /**
   * Send a serializable java object
   * @param obj the java object to send.
   * @throws JMSException if the ObjectMessage can not be created or set.
   */
  public void sendObject(Serializable obj) throws JMSException {
    ObjectMessage msg = session.createObjectMessage();
    msg.setObject(obj);
    messageProducer.send(msg);
    if (log.isDebug()) {
      log.debug(clientId + ": sent serialiable object ='" + msg + "'");
    }
  }

}
