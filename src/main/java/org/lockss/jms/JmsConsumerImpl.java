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

import org.lockss.app.*;
import org.lockss.util.*;
import org.lockss.util.jms.*;

public class JmsConsumerImpl implements JmsConsumer {

  private static final Logger log = Logger.getLogger();

  protected JMSManager jmsMgr;
  protected String clientId;
  protected MessageConsumer messageConsumer;
  protected Session session;

  JmsConsumerImpl(JMSManager mgr) {
    this.jmsMgr = mgr;
  }

  JmsConsumer createTopic(String clientId,
			  String topicName,
			  boolean noLocal,
			  MessageListener listener,
			  Connection connection)
      throws JMSException {

    this.clientId = clientId;

    log.debug("Creating " + (noLocal ? "NoLocal " : "") +
	      "consumer for topic: " + topicName +
	      ", client: " + clientId + " at " +
	      jmsMgr.getConnectUri());

    // Get shared connection from JMSManager if none supplied
    if (connection == null) {
      connection = jmsMgr.getConnection();
    }
    // create a Session
    log.debug3("Creating session for topic: " + topicName +
	       ", client: " + clientId + " at " +
	       jmsMgr.getConnectUri());
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    // create the Topic from which messages will be received
    Topic topic = session.createTopic(topicName);

    // create a MessageConsumer for receiving messages
    messageConsumer = session.createConsumer(topic, null, noLocal);
    if (listener != null) {
      messageConsumer.setMessageListener(listener);
    }

    return this;
  }

  public synchronized void close() throws JMSException {
    if (messageConsumer != null) {
	messageConsumer.close();
        messageConsumer = null;
    }
  }

  public synchronized void setListener(MessageListener listener) throws JMSException {
    if (messageConsumer != null) {
      messageConsumer.setMessageListener(listener);
    }
  }

  public synchronized Object receive(int timeout) throws JMSException {
    Message message = null;
    message = messageConsumer.receive(timeout);
    if (message != null) {
      return JmsUtil.convertMessage(message);
    }
    return null;
  }

  /**
   * Receive a text message from the message queue.
   *
   * @param timeout the time to wait for the message to be received.
   * @return the resulting String message.
   * @throws JMSException if thrown by JMS methods
   */
  public String receiveText(int timeout) throws JMSException {
    Object received = receive(timeout);

    // check if a message was received
    if (received != null && received instanceof String) {
      // cast the message to the correct type
      String text = (String) received;
      if (log.isDebug()) {
	log.debug(clientId + ": received text ='" + text + "'");
      }
      return text;
    }
    else {
      log.debug(clientId + ": String message not received");
    }
    return null;
  }


  /**
   * Return a Map with string keys and object values from the message queue.
   *
   * @param timeout the time to wait for the message to be received.
   * @return the resulting Map
   * @throws JMSException if thrown by JMS methods
   */
  @SuppressWarnings("unchecked")
  public Map<String, Object> receiveMap(int timeout) throws JMSException {
    Object received = receive(timeout);
    if (received == null) {
      log.debug(clientId + ": receive timed out in " + timeout);
      return null;
    }
    // check if a message was received
    if (received instanceof Map) {
      if (log.isDebug2()) {
	log.debug2(clientId + ": received map: " + received);
      } else {
	log.debug(clientId + ": received map.");
      }
      return (Map<String, Object>) received;
    } else {
      log.warning(clientId + ": Expected map but received " +
		  received.getClass());
    }
    return null;
  }

  /**
   * Return a byte array from the message queue
   *
   * @param timeout the time to wait for the message to be received.
   * @return the byte array.
   * @throws JMSException if thrown by JMS methods
   */
  public byte[] receiveBytes(int timeout) throws JMSException {
    Object received = receive(timeout);
    if (received != null && received instanceof byte[]) {
      byte[] bytes = (byte[]) received;
      if (log.isDebug()) {
	log.debug(clientId + ": received bytes ='" + bytes + "'");
      }
      return bytes;
    }
    else {
      log.debug(clientId + ": no bytes received");
    }
    return null;
  }

  /**
   * Return a serializable object from the message queue.
   *
   * @param timeout for the message consumer receive
   * @return the resulting Serializable object
   * @throws JMSException if thrown by JMS methods
   */
  public Serializable receiveObject(int timeout)
      throws JMSException {
    Object received = receive(timeout);
    if (received != null && received instanceof Serializable) {
      Serializable obj = (Serializable) received;
      if (log.isDebug()) {
	log.debug(clientId + ": received serializable object ='" +
		  obj.toString() + "'");
      }
      return obj;
    }
    else {
      log.debug(clientId + ": no message received");
    }
    return null;
  }

  /**
   * A Basic MessageListener.  Override the onMessage to appropriate functionality.
   */
  public abstract static class SubscriptionListener implements MessageListener {

    protected String listenerName;

    public SubscriptionListener(String listenerName) {
      this.listenerName = listenerName;
    }

    String getListenerName() {
      return listenerName;
    }

  }


}
