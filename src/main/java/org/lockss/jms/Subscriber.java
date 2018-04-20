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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Subscriber {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(Subscriber.class);

  protected String clientId;
  protected Connection connection;
  protected MessageConsumer messageConsumer;
  protected Session session;

  public void create(String clientId, String topicName) throws JMSException {
    this.create(clientId, topicName, null);
  }

  public void create(String clientId, String topicName, MessageListener listener)
      throws JMSException {
    this.clientId = clientId;

    // create a Connection Factory
    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
        ActiveMQConnection.DEFAULT_BROKER_URL);

    // create a Connection
    connection = connectionFactory.createConnection();
    connection.setClientID(clientId);

    // create a Session
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    // create the Topic from which messages will be received
    Topic topic = session.createTopic(topicName);

    // create a MessageConsumer for receiving messages
    messageConsumer = session.createConsumer(topic);
    if (listener != null) {
      messageConsumer.setMessageListener(listener);
    }

    // start the connection in order to receive messages
    connection.start();
  }

  public void closeConnection() throws JMSException {
    if(connection != null)
      connection.close();
  }

  public void setListener(MessageListener listener) throws JMSException {
    if(messageConsumer != null) {
      messageConsumer.setMessageListener(listener);
    }
  }

  public Object receive(int timeout) throws JMSException {
    Message message = messageConsumer.receive(timeout);
    if (message != null) {
      return convertMessage(message);
    }
    return null;
  }

  /**
   * This implementation converts a Message to the underlying type.
   * TextMessage back to a String, a ByteMessage back to a byte array,
   * a MapMessage back to a Map, and an ObjectMessage back to a Serializable object. Returns
   * the plain Message object in case of an unknown message type.
   */
  public static Object convertMessage(Message message) throws JMSException {
    if (message instanceof TextMessage) {
      return ((TextMessage) message).getText();
    }
    else if (message instanceof BytesMessage) {
      BytesMessage bytesMessage = (BytesMessage) message;
      byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
      bytesMessage.readBytes(bytes);
      return bytes;
    }
    else if (message instanceof MapMessage) {
      MapMessage mapMessage = (MapMessage) message;
      Map<String, Object> map = new HashMap<String, Object>();
      Enumeration<String> en = mapMessage.getMapNames();
      while (en.hasMoreElements()) {
        String key = en.nextElement();
        map.put(key, mapMessage.getObject(key));
      }
      return map;
    }
    else if (message instanceof ObjectMessage) {
      return ((ObjectMessage) message).getObject();
    }
    else {
      return message;
    }
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
      LOGGER.debug(clientId + ": received text ='{}'", text);
      return text;
    }
    else {
      LOGGER.debug(clientId + ": String message not received");
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
    // check if a message was received
    if (received != null && received instanceof Map) {
      LOGGER.debug(clientId + ": received map.");
      return (Map<String, Object>) received;
    }
    else {
      LOGGER.debug(clientId + ": Map not received");
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
      LOGGER.debug(clientId + ": received bytes ='{}'", bytes);
      return bytes;
    }
    else {
      LOGGER.debug(clientId + ": no bytes received");
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
      LOGGER.debug(clientId + ": received serializable object ='{}'", obj.toString());
      return obj;
    }
    else {
      LOGGER.debug(clientId + ": no message received");
    }
    return null;
  }

  /**
   * A Basic MessageListener.  Override the onMessage to appropriate functionality.
   */
  public static class SubscriptionListener implements MessageListener {

    protected String listenerName;
    protected Object msgObject;

    SubscriptionListener(String listenerName) {
      this.listenerName = listenerName;
    }

    @Override
    public void onMessage(Message message) {
      try {
        msgObject =  Subscriber.convertMessage(message);
      }
      catch (JMSException e) {
        LOGGER.error("onMessage received invalid obj");
      }
    }
    String getListenerName() {
      return listenerName;
    }

    Object getMsgObject() {
      return msgObject;
    }

  }


}
